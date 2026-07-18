// Command golang-collector runs an HTCondor condor_collector as a Go daemon: it
// receives daemon ClassAd updates, answers queries, invalidates and expires
// ads, all backed by the classad collections engine. It loads its policy from
// the HTCondor configuration and runs under condor_master (shared-port endpoint,
// DC_SET_READY / DC_CHILDALIVE) like any other HTCondor daemon.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/PelicanPlatform/classad/dbrpc"
	ccbserver "github.com/bbockelm/golang-ccb"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/authz"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"
	"github.com/bbockelm/golang-htcondor/sessioncache/sqlite"

	collector "github.com/bbockelm/golang-collector"
	"github.com/bbockelm/golang-collector/metrics"
	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/accountant"
	"github.com/bbockelm/golang-collector/negotiator/cycle"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
	"github.com/bbockelm/golang-collector/negotiator/source"
	"github.com/bbockelm/golang-collector/server"
	"github.com/bbockelm/golang-collector/store"
)

// version is stamped at build time via `-ldflags "-X main.version=..."` (see the
// Makefile); it is "dev" for a plain `go build`.
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "golang-collector:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":9618", "fallback TCP listen address when not inheriting a shared-port endpoint")
	showVersion := flag.Bool("version", false, "print version and exit")
	debug := flag.Bool("debug", false, "enable verbose debug logging on every destination (including the cedar security handshake); overrides COLLECTOR_DEBUG")
	// condor_master appends these standard DaemonCore flags when it launches a
	// daemon not in its built-in list; accept them so flag.Parse does not reject
	// our launch. -local-name additionally scopes config lookups.
	localName := flag.String("local-name", "", "HTCondor subsystem local-name; passed by condor_master")
	_ = flag.String("sock", "", "HTCondor shared-port endpoint name; accepted for compatibility (fd inherited via CONDOR_INHERIT)")
	metricsAddr := flag.String("metrics", "", "if set (e.g. \":9720\"), serve Prometheus metrics at /metrics on this address; overrides COLLECTOR_METRICS_ADDRESS")
	flag.Parse()

	if *showVersion {
		fmt.Println("htc-collector", version)
		return nil
	}

	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "COLLECTOR", LocalName: *localName})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if *debug {
		// Turn every log destination up to debug -- including the cedar destination,
		// which otherwise defaults to Warn and hides the security handshake. Set both
		// the bare and (if scoped) local-name-prefixed knob so it wins regardless of
		// how the daemon resolves COLLECTOR_DEBUG.
		const allDebug = "general:debug collector:debug security:debug cedar:debug metrics:debug http:debug schedd:debug mcp:debug"
		cfg.Set("COLLECTOR_DEBUG", allDebug)
		if *localName != "" {
			cfg.Set(strings.ToUpper(*localName)+".COLLECTOR_DEBUG", allDebug)
		}
	}

	// Bootstrap logging and condor_master integration (drops privileges to the
	// condor user when started as root).
	d, err := daemon.New(daemon.Options{Subsys: "COLLECTOR", Config: cfg})
	if err != nil {
		return err
	}
	log := d.Logger()
	// Route cedar's security/server slog output into CollectorLog.
	slog.SetDefault(d.Slog())

	// Server-side security policy from the HTCondor configuration (SEC_* knobs),
	// per authorization level -- exactly like the C++ collector, which serves
	// QUERY_*_ADS at READ (monitoring is public, so condor_status works without
	// daemon credentials) but UPDATE_*/INVALIDATE_* at ADVERTISE (only
	// authenticated daemons may publish). The collector maps each command to its
	// level (server.CommandLevel) and negotiates using the matching policy.
	secForLevel := map[string]*security.SecurityConfig{}
	for _, lvl := range []struct {
		name string
		cmd  int
	}{
		{server.LevelRead, commands.QUERY_ANY_ADS},
		{server.LevelAdvertise, commands.UPDATE_STARTD_AD},
		{server.LevelNegotiator, commands.QUERY_STARTD_PVT_ADS},
	} {
		s, err := htcondor.GetServerSecurityConfig(d.Config(), lvl.cmd, lvl.name)
		if err != nil {
			return fmt.Errorf("building %s security config: %w", lvl.name, err)
		}
		secForLevel[lvl.name] = s
	}
	// READ is the permissive baseline; it also backstops any command not mapped to
	// a level (e.g. the DC_* defaults), so tools like condor_ping/condor_who work.
	sec := secForLevel[server.LevelRead]

	// The collector core -- store, protocol handlers, CONDOR_VIEW_HOST forwarding,
	// and background dictionary retraining + ad expiry -- exactly as the embeddable
	// collector library provides it. This daemon wraps it with the condor_master
	// glue: config, logging, the command socket, the address file, DC_* commands,
	// metrics, and the embedded CCB.
	backend, err := buildBackend(cfg, log)
	if err != nil {
		return err
	}
	c, err := collector.New(collector.Config{
		Security:            sec,
		SecurityForLevel:    secForLevel,
		Backend:             backend, // nil selects the default in-memory store
		ViewHosts:           viewHosts(cfg),
		DictRetrainInterval: configSeconds(cfg, "COLLECTOR_DICT_RETRAIN_INTERVAL", 15*time.Minute),
		DictSampleSize:      configInt(cfg, "COLLECTOR_DICT_SAMPLE_SIZE", 4000),
		ExpireInterval:      configSeconds(cfg, "COLLECTOR_UPDATE_INTERVAL", 900*time.Second),
		Logger:              d.Slog(),
	})
	if err != nil {
		return err
	}
	// Final expiry sweep + backend flush/close at shutdown (runs after the
	// background loops stop, since it is deferred first).
	defer func() { _ = c.Close() }()
	st, srv := c.Store(), c.Server()
	// DC_NOP / DC_CONFIG_VAL / etc. so condor_who, condor_ping and condor_config_val work.
	d.RegisterDefaultCommands(srv)

	// Command-socket listener: the shared-port endpoint inherited from
	// condor_master if present, otherwise a plain TCP bind on the collector's
	// well-known port (from COLLECTOR_HOST), falling back to -listen.
	listenAddr := collectorListenAddr(d.Config(), *listen)
	ln, err := d.Listener(func() (net.Listener, error) {
		return net.Listen("tcp", listenAddr)
	})
	if err != nil {
		log.Error(logging.DestinationGeneral, "listener setup failed", "listen_addr", listenAddr, "err", err.Error())
		return err
	}
	defer func() { _ = ln.Close() }()

	// Publish our command address so the rest of the pool (and tools like
	// condor_status) can find us, exactly like the C++ collector's
	// COLLECTOR_ADDRESS_FILE.
	if path := writeAddressFile(d, cfg, ln); path != "" {
		defer func() { _ = os.Remove(path) }()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Optional embedded CCB server (ENABLE_CCB_SERVER), sharing this collector's
	// command socket exactly like the C++ collector: NAT'd daemons register here
	// and public clients reach them by connection reversal brokered through us.
	if err := maybeStartEmbeddedCCB(ctx, d, cfg, sec, srv, collectorAddr(d, ln)); err != nil {
		return err
	}

	// Optional embedded negotiator (NEGOTIATOR_EMBEDDED), reading this
	// collector's ad store directly and serving the userprio/RESCHEDULE
	// protocol on the same command socket.
	if err := maybeStartEmbeddedNegotiator(ctx, d, cfg, srv, st, collectorAddr(d, ln)); err != nil {
		return err
	}

	// Optional Prometheus metrics endpoint reporting the compressed storage
	// footprint per ad type (plus Go/process metrics), so a pool can be sized from
	// live numbers rather than a profiler.
	if addr := metricsListenAddr(cfg, *metricsAddr); addr != "" {
		startMetrics(ctx, addr, st, log)
	}

	// Background maintenance -- dictionary retraining (COLLECTOR_DICT_RETRAIN_INTERVAL,
	// COLLECTOR_DICT_SAMPLE_SIZE) and ad expiry (COLLECTOR_UPDATE_INTERVAL) -- on the
	// intervals passed to collector.New above.
	defer c.StartBackground(ctx)()

	// Optional encrypted-at-rest persistence of the CEDAR security session cache
	// (COLLECTOR_PERSIST_SESSIONS), so clients resume sessions across a restart
	// instead of re-authenticating in a thundering herd. The database is encrypted
	// with the pool signing key, read as root (see maybeEnableSessionPersistence).
	closeSessions, err := maybeEnableSessionPersistence(d, cfg)
	if err != nil {
		return err
	}
	if closeSessions != nil {
		defer closeSessions()
	}

	log.Info(logging.DestinationGeneral, "golang-collector starting",
		"listen", ln.Addr().String(), "under_master", d.UnderMaster())

	return d.Serve(ctx, ln, srv.Serve)
}

// maybeEnableSessionPersistence turns on encrypted persistence of the CEDAR
// session cache when COLLECTOR_PERSIST_SESSIONS is set. It is a no-op (returns a
// nil closer) otherwise.
//
// The session database is encrypted at rest with the pool signing key(s) from
// SEC_PASSWORD_DIRECTORY: htcondor.LoadSigningKeys reads those root-owned 0600
// files as root (re-elevating from the dropped-to condor account), and the
// signing key wraps the database's data-encryption key. This is why persistence
// requires a signing key -- without one the store cannot be encrypted, which is a
// fatal misconfiguration rather than a silent fallback to plaintext.
//
// The store is opened AFTER daemon.New (which drops privileges), so the database
// file is created owned by the condor account, not root. daemon.Serve drives the
// periodic snapshot + final snapshot; the returned closer must be deferred so the
// database is closed after Serve returns.
func maybeEnableSessionPersistence(d *daemon.Daemon, cfg *config.Config) (func(), error) {
	if !configBool(cfg, "COLLECTOR_PERSIST_SESSIONS", false) {
		return nil, nil
	}

	dbPath := configString(cfg, "COLLECTOR_SESSION_CACHE_FILE")
	if dbPath == "" {
		spool, ok := cfg.Get("SPOOL")
		if !ok || spool == "" {
			return nil, fmt.Errorf("COLLECTOR_PERSIST_SESSIONS is set but neither COLLECTOR_SESSION_CACHE_FILE nor SPOOL is configured")
		}
		dbPath = filepath.Join(spool, "collector_sessions.db")
	}

	// Load the pool signing keys as root (SEC_PASSWORD_DIRECTORY is root-owned
	// 0600); these key-encrypt the session database.
	rawKeys, err := htcondor.LoadSigningKeys(cfg)
	if err != nil {
		return nil, fmt.Errorf("session persistence: loading pool signing keys: %w", err)
	}
	if len(rawKeys) == 0 {
		return nil, fmt.Errorf("session persistence: COLLECTOR_PERSIST_SESSIONS is set but no signing keys are available (set SEC_PASSWORD_DIRECTORY); the session cache cannot be encrypted without one")
	}
	keys := make([]sqlite.SigningKey, 0, len(rawKeys))
	for id, material := range rawKeys {
		keys = append(keys, sqlite.SigningKey{ID: id, Material: material})
	}

	store, err := sqlite.Open(dbPath, keys, d.Slog())
	if err != nil {
		return nil, fmt.Errorf("session persistence: opening %s: %w", dbPath, err)
	}
	if err := d.EnableSessionPersistence(store, configSeconds(cfg, "COLLECTOR_SESSION_SNAPSHOT_INTERVAL", 0)); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("session persistence: %w", err)
	}
	d.Logger().Info(logging.DestinationGeneral, "session persistence enabled", "path", dbPath, "signing_keys", len(keys))
	return func() { _ = store.Close() }, nil
}

// viewHosts returns the CONDOR_VIEW_HOST collector addresses to forward to,
// comma/space separated, with any entry equal to our own COLLECTOR_HOST dropped
// (a self-reference would forward every ad back to ourselves in a loop).
func viewHosts(cfg *config.Config) []string {
	raw, ok := cfg.Get("CONDOR_VIEW_HOST")
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	self, _ := cfg.Get("COLLECTOR_HOST")
	self = strings.TrimSpace(self)
	var hosts []string
	for _, h := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		if h != "" && h != self {
			hosts = append(hosts, h)
		}
	}
	return hosts
}

// writeAddressFile publishes the collector's command address to
// COLLECTOR_ADDRESS_FILE (default $(LOG)/.collector_address) as a sinful string:
// the shared-port sinful when running under condor_master, otherwise the plain
// listen address. Returns the path written (for cleanup), or "" if none.
func writeAddressFile(d *daemon.Daemon, cfg *config.Config, ln net.Listener) string {
	path, ok := cfg.Get("COLLECTOR_ADDRESS_FILE")
	if !ok || path == "" {
		logDir, ok := cfg.Get("LOG")
		if !ok || logDir == "" {
			return ""
		}
		path = filepath.Join(logDir, ".collector_address")
	}
	if err := os.WriteFile(path, []byte("<"+collectorAddr(d, ln)+">\n"), 0o644); err != nil {
		slog.Warn("could not write collector address file", "path", path, "err", err)
		return ""
	}
	return path
}

// collectorAddr is this collector's externally reachable command address: the
// shared-port sinful when running under condor_master, otherwise the plain listen
// address.
func collectorAddr(d *daemon.Daemon, ln net.Listener) string {
	if sinful, ok := d.AdvertisedSinful(); ok {
		return sinful
	}
	return ln.Addr().String()
}

// maybeStartEmbeddedCCB starts an embedded CCB server on the collector's own
// command socket when ENABLE_CCB_SERVER is set, mirroring the C++ collector. It
// registers the CCB handlers onto srv (so CCB commands arrive on the shared port)
// and starts CCB background maintenance under ctx. pubAddr is the collector's
// reachable address, used to build the "<addr>#<id>" CCB contact strings.
func maybeStartEmbeddedCCB(ctx context.Context, d *daemon.Daemon, cfg *config.Config, sec *security.SecurityConfig, srv *cedarserver.Server, pubAddr string) error {
	if !configBool(cfg, "ENABLE_CCB_SERVER", false) {
		return nil
	}
	ccbSrv, err := ccbserver.New(ccbserver.Config{
		PublicAddress: pubAddr,
		Security:      sec,
		Logger:        d.Slog(),
	})
	if err != nil {
		return fmt.Errorf("embedded CCB server: %w", err)
	}
	ccbSrv.RegisterOn(srv)
	ccbSrv.StartBackground(ctx)
	slog.Info("embedded CCB server enabled", "public_address", pubAddr)
	return nil
}

// maybeStartEmbeddedNegotiator starts an embedded negotiator when
// NEGOTIATOR_EMBEDDED is set (default false), following the embedded-CCB
// pattern: it reads the pool directly from this collector's store (no
// self-queries), registers the userprio + RESCHEDULE handlers onto srv (so
// they arrive on the shared command socket), and runs the cycle timer +
// NegotiatorAd publisher in the background. pubAddr is the collector's
// reachable address, advertised as the negotiator's own.
//
// The accountant state lives in ACCOUNTANT_DATABASE_FILE, defaulting to
// $(SPOOL)/GoAccountant.log — the Go-native transaction-log format, NOT the
// C++ Accountantnew.log ClassAdLog (whose importer is deferred; design doc
// 3.4). Point the knob at a fresh path when migrating from a C++ negotiator.
func maybeStartEmbeddedNegotiator(ctx context.Context, d *daemon.Daemon, cfg *config.Config, srv *cedarserver.Server, st store.Backend, pubAddr string) error {
	if !configBool(cfg, "NEGOTIATOR_EMBEDDED", false) {
		return nil
	}

	src, err := source.NewEmbedded(st, source.Config{
		SlotConstraint:      configString(cfg, "NEGOTIATOR_SLOT_CONSTRAINT"),
		SubmitterConstraint: configString(cfg, "NEGOTIATOR_SUBMITTER_CONSTRAINT"),
		SlotWeightExpr:      configString(cfg, "SLOT_WEIGHT"),
		Logger:              d.Slog(),
	})
	if err != nil {
		return fmt.Errorf("embedded negotiator: %w", err)
	}

	acctCfg := accountant.ConfigFromKnobs(cfg.Get)
	acctCfg.LogFile = accountantLogFile(cfg)
	acct, err := accountant.New(acctCfg)
	if err != nil {
		return fmt.Errorf("embedded negotiator: %w", err)
	}

	// Client security for the NEGOTIATE sessions toward schedds. Encryption is
	// REQUIRED: the claim ids in PERMISSION_AND_AD are secrets the C++ schedd
	// reads with get_secret, which is a plain string on an encrypted channel.
	sessionSec, err := htcondor.GetSecurityConfig(cfg, commands.NEGOTIATE, "CLIENT")
	if err != nil {
		return fmt.Errorf("embedded negotiator: schedd security config: %w", err)
	}
	sessionSec.Encryption = security.SecurityRequired

	cycleCfg := cycle.ConfigFromKnobs(cfg.Get)
	sf := protocol.NewFactory(sessionSec, protocol.WithNegotiatorName(cycleCfg.NegotiatorName))
	cyc, err := cycle.New(src, acct, sf, cycleCfg)
	if err != nil {
		return fmt.Errorf("embedded negotiator: %w", err)
	}

	// Per-command ALLOW_/DENY_ authorization for the userprio setters
	// (enforced inside the negotiator's handlers; the collector's own
	// handlers are unaffected).
	policy, err := authz.NewPolicy(cfg, "NEGOTIATOR")
	if err != nil {
		return fmt.Errorf("embedded negotiator: authorization policy: %w", err)
	}

	neg, err := negotiator.New(negotiator.Config{
		Source:         src,
		Accountant:     acct,
		Cycle:          cyc,
		NegotiatorName: cycleCfg.NegotiatorName,
		AdvertisedAddr: pubAddr,
		Interval:       configSeconds(cfg, "NEGOTIATOR_INTERVAL", 60*time.Second),
		CycleDelay:     configSeconds(cfg, "NEGOTIATOR_CYCLE_DELAY", 20*time.Second),
		MinInterval:    configSeconds(cfg, "NEGOTIATOR_MIN_INTERVAL", 5*time.Second),
		UpdateInterval: configSeconds(cfg, "NEGOTIATOR_UPDATE_INTERVAL", 300*time.Second),
		Authorizer:     policy.Authorize,
		Logger:         d.Slog(),
	})
	if err != nil {
		return fmt.Errorf("embedded negotiator: %w", err)
	}
	neg.RegisterOn(srv)
	neg.StartBackground(ctx)
	slog.Info("embedded negotiator enabled", "advertised_address", pubAddr,
		"accountant_db", acctCfg.LogFile)
	return nil
}

// accountantLogFile resolves the accountant state file: ACCOUNTANT_DATABASE_FILE
// if set, else $(SPOOL)/GoAccountant.log, else "" (memory-only, with a warning).
// GoAccountant.log is the Go-native format — deliberately NOT Accountantnew.log,
// so a C++ negotiator's ClassAdLog is never clobbered or misparsed.
func accountantLogFile(cfg *config.Config) string {
	if v, ok := cfg.Get("ACCOUNTANT_DATABASE_FILE"); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if spool, ok := cfg.Get("SPOOL"); ok && strings.TrimSpace(spool) != "" {
		return filepath.Join(strings.TrimSpace(spool), "GoAccountant.log")
	}
	slog.Warn("no ACCOUNTANT_DATABASE_FILE or SPOOL configured; accountant state is memory-only")
	return ""
}

// configString reads a string knob, trimmed ("" when unset).
func configString(cfg *config.Config, key string) string {
	v, _ := cfg.Get(key)
	return strings.TrimSpace(v)
}

// configBool reads an HTCondor boolean knob (true/t/yes/1, case-insensitive).
func configBool(cfg *config.Config, key string, def bool) bool {
	v, ok := cfg.Get(key)
	if !ok {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "t", "yes", "y", "1":
		return true
	case "false", "f", "no", "n", "0":
		return false
	}
	return def
}

// metricsListenAddr resolves the Prometheus metrics listen address: the -metrics
// flag if set, else the COLLECTOR_METRICS_ADDRESS config knob, else "" (disabled).
func metricsListenAddr(cfg *config.Config, flagAddr string) string {
	if flagAddr != "" {
		return flagAddr
	}
	if v, ok := cfg.Get("COLLECTOR_METRICS_ADDRESS"); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// startMetrics serves the collector's Prometheus metrics at /metrics on addr
// until ctx is cancelled. Bind failures are logged, not fatal -- metrics are
// observability, not core function.
func startMetrics(ctx context.Context, addr string, st store.Backend, log *logging.Logger) {
	// Per-table metrics come from store.Statser; a backend that doesn't expose
	// them (e.g. a remote database) simply has no metrics endpoint.
	statser, ok := st.(store.Statser)
	if !ok {
		log.Info(logging.DestinationGeneral, "metrics endpoint disabled: backend has no stats")
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(statser))
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Info(logging.DestinationGeneral, "metrics endpoint listening", "addr", addr, "path", "/metrics")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error(logging.DestinationGeneral, "metrics endpoint stopped", "err", err.Error())
		}
	}()
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
}

// buildBackend selects the collector's ad-store backend from COLLECTOR_STORE:
// "memory" (default) keeps ads in memory (fastest, lost on restart); "db"
// persists them in an embedded database under COLLECTOR_DB_PATH (default
// $(LOCAL_DIR)/collector-db), so the collector resumes with its pool after a
// restart. Returns nil for the in-memory default, which collector.New maps to
// store.New(). (A remote-database backend is selected the same way once wired.)
func buildBackend(cfg *config.Config, log *logging.Logger) (store.Backend, error) {
	kind, _ := cfg.Get("COLLECTOR_STORE")
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "memory", "mem":
		return nil, nil
	case "db", "database", "embedded":
		path := collectorDBPath(cfg)
		log.Info(logging.DestinationGeneral, "collector ad store: embedded database", "path", path)
		b, err := store.NewDBBackend(path)
		if err != nil {
			return nil, fmt.Errorf("collector: embedded db store: %w", err)
		}
		return b, nil
	case "dbrpc", "remote":
		addr, ok := cfg.Get("COLLECTOR_DB_HOST")
		if !ok || strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("collector: COLLECTOR_STORE=%s requires COLLECTOR_DB_HOST (the htcondordb daemon's address)", kind)
		}
		log.Info(logging.DestinationGeneral, "collector ad store: remote database over CEDAR", "host", addr)
		return store.NewRPCBackend(context.Background(), dbrpcDial(cfg, strings.TrimSpace(addr))), nil
	default:
		return nil, fmt.Errorf("collector: unknown COLLECTOR_STORE %q (want \"memory\", \"db\", or \"dbrpc\")", kind)
	}
}

// dbSessionCommand is htcondordb's DBSession CEDAR command (command.DBSession =
// TRANSFERD_BASE 74000): the multiplexed dbrpc session. Defined locally to avoid
// a dependency on the htcondordb daemon module for one protocol constant.
const dbSessionCommand = 74000

// dbrpcDial returns a dial that opens a fresh authenticated CEDAR DBSession to the
// remote database at addr and wraps its stream as a dbrpc MsgConn. The collector
// authenticates (PREFERRED) so it maps to a privileged identity that can write
// and read private ads -- it applies per-client redaction itself. The returned
// MsgConn's Close also closes the CEDAR connection.
func dbrpcDial(cfg *config.Config, addr string) func(context.Context) (dbrpc.MsgConn, error) {
	return func(ctx context.Context) (dbrpc.MsgConn, error) {
		sec, err := htcondor.GetSecurityConfig(cfg, dbSessionCommand, "CLIENT")
		if err != nil {
			return nil, fmt.Errorf("building db-session security config: %w", err)
		}
		sec.Command = dbSessionCommand
		if sec.Authentication == security.SecurityOptional {
			sec.Authentication = security.SecurityPreferred
		}
		connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		cl, err := cedarclient.ConnectAndAuthenticate(connCtx, addr, sec)
		if err != nil {
			return nil, fmt.Errorf("connecting to remote database %s: %w", addr, err)
		}
		return &closingMsgConn{MsgConn: dbrpc.NewCedarConn(ctx, cl.GetStream()), also: cl.Close}, nil
	}
}

// closingMsgConn augments a dbrpc MsgConn's Close to also tear down the CEDAR
// connection the stream rides on.
type closingMsgConn struct {
	dbrpc.MsgConn
	also func() error
}

func (c *closingMsgConn) Close() error {
	err := c.MsgConn.Close()
	if c.also != nil {
		_ = c.also()
	}
	return err
}

// collectorDBPath is the on-disk directory for the embedded database store:
// COLLECTOR_DB_PATH if set, else $(LOCAL_DIR)/collector-db.
func collectorDBPath(cfg *config.Config) string {
	if p, ok := cfg.Get("COLLECTOR_DB_PATH"); ok && strings.TrimSpace(p) != "" {
		return p
	}
	if p, ok := cfg.Get("LOCAL_DIR"); ok && strings.TrimSpace(p) != "" {
		return filepath.Join(p, "collector-db")
	}
	return "collector-db"
}

// collectorListenAddr picks the fallback TCP bind address when not inheriting a
// shared-port endpoint: the port from COLLECTOR_HOST (the collector's well-known
// address, how the C++ collector learns its port), else CONDOR_HOST, else the
// -listen flag. Returns ":<port>" so it binds every interface.
func collectorListenAddr(cfg *config.Config, fallback string) string {
	for _, key := range []string{"COLLECTOR_HOST", "CONDOR_HOST"} {
		v, ok := cfg.Get(key)
		if !ok {
			continue
		}
		// COLLECTOR_HOST may be "host:port", a "<host:port>" sinful, or bare host.
		v = strings.TrimPrefix(strings.TrimSuffix(strings.TrimSpace(v), ">"), "<")
		host, port, err := net.SplitHostPort(v)
		if err != nil || port == "" || port == "0" {
			continue
		}
		if host == "" {
			host = "127.0.0.1"
		}
		return net.JoinHostPort(host, port)
	}
	return fallback
}

func configSeconds(cfg *config.Config, key string, def time.Duration) time.Duration {
	if v, ok := cfg.Get(key); ok {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return def
}

func configInt(cfg *config.Config, key string, def int) int {
	if v, ok := cfg.Get(key); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
