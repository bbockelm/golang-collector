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
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/PelicanPlatform/classad/dbrpc"
	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	ccbserver "github.com/bbockelm/golang-ccb"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/authz"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

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
	d, err := daemon.New(daemon.Options{Subsys: "COLLECTOR", LocalName: *localName, Config: cfg})
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

	// Debugging aids for chasing stalls (see docs). The slow-op threshold controls the
	// "slow update/flush" WARNs; SIGUSR1 dumps every goroutine's stack to the log
	// on demand (no HTTP surface); pprof is served only when explicitly enabled.
	if ms := configInt(cfg, "COLLECTOR_SLOW_OP_MS", 2000); ms >= 0 {
		store.SlowOpThreshold = time.Duration(ms) * time.Millisecond
	}
	installGoroutineDumpSignal(log)

	// Optional Prometheus metrics endpoint reporting the compressed storage
	// footprint per ad type (plus Go/process metrics), so a pool can be sized from
	// live numbers rather than a profiler.
	if addr := metricsListenAddr(cfg, *metricsAddr); addr != "" {
		startMetrics(ctx, addr, st, log, configBool(cfg, "COLLECTOR_DEBUG_PPROF", false))
	}

	// Background maintenance -- dictionary retraining (COLLECTOR_DICT_RETRAIN_INTERVAL,
	// COLLECTOR_DICT_SAMPLE_SIZE) and ad expiry (COLLECTOR_UPDATE_INTERVAL) -- on the
	// intervals passed to collector.New above.
	defer c.StartBackground(ctx)()

	log.Info(logging.DestinationGeneral, "golang-collector starting",
		"listen", ln.Addr().String(), "under_master", d.UnderMaster())

	return d.Serve(ctx, ln, srv.Serve)
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
func startMetrics(ctx context.Context, addr string, st store.Backend, log *logging.Logger, pprofOn bool) {
	// The operational metrics (update/batch/backoff timings, counts) are always
	// served -- they matter most for the remote-database backend, which is
	// precisely the one with no store.Statser. The per-ad-type storage gauges are
	// added only when the backend exposes them.
	statser, _ := st.(store.Statser) // nil for a backend without stats; Handler tolerates it
	dbd := dbDiagnoser(st)           // non-nil only for the remote-database backend
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(statser, dbd))
	if pprofOn {
		// Opt-in only (COLLECTOR_DEBUG_PPROF): the profiler/goroutine surface is a
		// debugging tool, not something to leave exposed by default. Mounted on this
		// same private mux so it shares the endpoint's binding.
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		log.Info(logging.DestinationGeneral, "pprof debug endpoints enabled", "addr", addr, "path", "/debug/pprof/")
	}
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

// installGoroutineDumpSignal makes SIGUSR1 dump every goroutine's stack to the log.
// It is the on-demand alternative to always-listening pprof: send the daemon SIGUSR1
// during a stall and the blocked handler's stack (which lock/syscall/channel it is
// parked on) lands in the collector log -- no HTTP surface, no restart, always
// available. The dump is one log record so it stays greppable.
func installGoroutineDumpSignal(log *logging.Logger) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	go func() {
		for range ch {
			buf := make([]byte, 1<<20)
			for {
				n := runtime.Stack(buf, true) // true = all goroutines
				if n < len(buf) {
					buf = buf[:n]
					break
				}
				buf = make([]byte, 2*len(buf)) // grew past the buffer; retry larger
			}
			log.Info(logging.DestinationGeneral, "SIGUSR1: goroutine stack dump", "goroutines", runtime.NumGoroutine(), "stacks", string(buf))
		}
	}()
}

// dbDiagnoser finds a metrics.DBDiagnoser (the remote database's per-table diagnostics
// source) in st, unwrapping the BufferedBackend that fronts the RPCBackend. Returns nil
// for a local backend, so the collector's /metrics adds the remote-database metrics
// only when there is a remote database behind it.
func dbDiagnoser(st store.Backend) metrics.DBDiagnoser {
	if d, ok := st.(metrics.DBDiagnoser); ok {
		return d
	}
	if u, ok := st.(interface{ Base() store.Backend }); ok {
		if d, ok := u.Base().(metrics.DBDiagnoser); ok {
			return d
		}
	}
	return nil
}

// buildBackend selects the collector's ad-store backend from COLLECTOR_STORE:
// "memory" (default) keeps ads in memory (fastest, lost on restart); "embedded"
// persists them in a local database under COLLECTOR_DB_PATH (encrypted at rest by
// default), so the collector resumes with its pool after a restart; "db" stores
// them in an external database daemon over CEDAR (COLLECTOR_DB_HOST). Returns nil
// for the in-memory default, which collector.New maps to store.New(). The
// persistent backends are wrapped with COLLECTOR_BATCH_WINDOW_MS update batching.
func buildBackend(cfg *config.Config, log *logging.Logger) (store.Backend, error) {
	base, err := buildBaseBackend(cfg, log)
	if err != nil || base == nil {
		return base, err // nil = in-memory default (no batching)
	}
	// Ad-update batching: COLLECTOR_BATCH_WINDOW_MS buffers non-ack updates for
	// that many milliseconds (deduplicated by ad) and commits them in one
	// transaction -- fewer round trips to a remote database, rapid re-advertises
	// collapsed, and startup storms coalesced. On by default (100ms) for the
	// persistent backends; set to 0 to disable. Reads flush the buffer first, so
	// the window bounds only write-visibility latency (well under the C++
	// collector's update cadence), and ACK updates bypass it (store.DurableUpdate).
	wms := configInt(cfg, "COLLECTOR_BATCH_WINDOW_MS", 100)
	if wms <= 0 {
		return base, nil
	}
	maxBuf := configInt(cfg, "COLLECTOR_BATCH_MAX_ADS", 2048)
	buffered, err := store.NewBufferedBackend(base, time.Duration(wms)*time.Millisecond, maxBuf,
		func(e error) { log.Warn(logging.DestinationGeneral, "ad-update batch flush failed", "err", e.Error()) })
	if err != nil {
		log.Info(logging.DestinationGeneral, "ad-update batching unavailable for this backend; continuing unbuffered", "err", err.Error())
		return base, nil
	}
	log.Info(logging.DestinationGeneral, "ad-update batching enabled", "window_ms", wms, "max_ads", maxBuf)
	return buffered, nil
}

// embeddedDBKeys returns the pool signing keys that encrypt the embedded ad
// database at rest. Encryption is on by default (COLLECTOR_DB_ENCRYPTION); the keys
// come from SEC_PASSWORD_DIRECTORY, read as root -- the same source and mechanism
// the session cache uses. When encryption is required but no keys are available it
// is a fatal misconfiguration (rather than a silent fall back to plaintext); set
// COLLECTOR_DB_ENCRYPTION=false to store ads unencrypted (e.g. for testing). It
// returns nil keys when encryption is disabled, opening the database in plaintext.
func embeddedDBKeys(cfg *config.Config) ([]store.KEK, error) {
	if !configBool(cfg, "COLLECTOR_DB_ENCRYPTION", true) {
		return nil, nil
	}
	raw, err := htcondor.LoadSigningKeys(cfg)
	if err != nil {
		return nil, fmt.Errorf("collector: embedded ad store encryption: loading pool signing keys: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("collector: embedded ad store encryption is enabled but no pool signing keys are available (set SEC_PASSWORD_DIRECTORY, or COLLECTOR_DB_ENCRYPTION=false to store ads unencrypted)")
	}
	keys := make([]store.KEK, 0, len(raw))
	for id, material := range raw {
		keys = append(keys, store.KEK{ID: id, Material: material})
	}
	return keys, nil
}

// buildBaseBackend selects the underlying ad-store backend from COLLECTOR_STORE:
// "memory" (in-memory, the default), "embedded" (a local database file), or "db"
// (an external database daemon reached over CEDAR).
func buildBaseBackend(cfg *config.Config, log *logging.Logger) (store.Backend, error) {
	kind, _ := cfg.Get("COLLECTOR_STORE")
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "memory", "mem":
		return nil, nil
	case "embedded", "local":
		path := collectorDBPath(cfg)
		keys, err := embeddedDBKeys(cfg)
		if err != nil {
			return nil, err
		}
		log.Info(logging.DestinationGeneral, "collector ad store: embedded database", "path", path, "encrypted", len(keys) > 0)
		b, err := store.NewDBBackendEncrypted(path, keys)
		if err != nil {
			return nil, fmt.Errorf("collector: embedded ad store: %w", err)
		}
		return b, nil
	case "db", "database", "remote":
		addr, ok := cfg.Get("COLLECTOR_DB_HOST")
		if !ok || strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("collector: COLLECTOR_STORE=%s requires COLLECTOR_DB_HOST (the external database daemon's address)", kind)
		}
		policy := dbRetryPolicy(cfg)
		readConns := configInt(cfg, "COLLECTOR_DB_READ_CONNS", store.DefaultReadConns)
		writeConns := configInt(cfg, "COLLECTOR_DB_WRITE_CONNS", store.DefaultWriteConns)
		log.Info(logging.DestinationGeneral, "collector ad store: external database over CEDAR",
			"host", addr, "retry_max_elapsed", policy.MaxElapsed.String(), "read_conns", readConns, "write_conns", writeConns)
		return store.NewRPCBackendPool(context.Background(), dbrpcDial(cfg, strings.TrimSpace(addr)), policy, readConns, writeConns), nil
	default:
		return nil, fmt.Errorf("collector: unknown COLLECTOR_STORE %q (want \"memory\", \"embedded\", or \"db\")", kind)
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

func configFloat(cfg *config.Config, key string, def float64) float64 {
	if v, ok := cfg.Get(key); ok {
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil && f > 0 {
			return f
		}
	}
	return def
}

// dbRetryPolicy builds the external-database retry/backoff policy from config. The
// budget (COLLECTOR_DB_RETRY_MAX_ELAPSED) is a duration in seconds -- how long one
// operation rides out an outage before giving up (drop + log for a buffered write);
// the backoff shape is sub-second and expressed in milliseconds.
func dbRetryPolicy(cfg *config.Config) store.RetryPolicy {
	p := store.DefaultRetryPolicy
	p.MaxElapsed = configSeconds(cfg, "COLLECTOR_DB_RETRY_MAX_ELAPSED", p.MaxElapsed)
	p.Initial = time.Duration(configInt(cfg, "COLLECTOR_DB_RETRY_INITIAL_MS", int(p.Initial/time.Millisecond))) * time.Millisecond
	p.Max = time.Duration(configInt(cfg, "COLLECTOR_DB_RETRY_MAX_MS", int(p.Max/time.Millisecond))) * time.Millisecond
	p.Multiplier = configFloat(cfg, "COLLECTOR_DB_RETRY_MULTIPLIER", p.Multiplier)
	p.Jitter = configFloat(cfg, "COLLECTOR_DB_RETRY_JITTER", p.Jitter)
	return p
}
