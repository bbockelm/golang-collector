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

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	ccbserver "github.com/bbockelm/golang-ccb"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	collector "github.com/bbockelm/golang-collector"
	"github.com/bbockelm/golang-collector/metrics"
	"github.com/bbockelm/golang-collector/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "golang-collector:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":9618", "fallback TCP listen address when not inheriting a shared-port endpoint")
	// condor_master appends these standard DaemonCore flags when it launches a
	// daemon not in its built-in list; accept them so flag.Parse does not reject
	// our launch. -local-name additionally scopes config lookups.
	localName := flag.String("local-name", "", "HTCondor subsystem local-name; passed by condor_master")
	_ = flag.String("sock", "", "HTCondor shared-port endpoint name; accepted for compatibility (fd inherited via CONDOR_INHERIT)")
	metricsAddr := flag.String("metrics", "", "if set (e.g. \":9720\"), serve Prometheus metrics at /metrics on this address; overrides COLLECTOR_METRICS_ADDRESS")
	flag.Parse()

	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "COLLECTOR", LocalName: *localName})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
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
	// so this collector authenticates and encrypts exactly like the C++ one.
	sec, err := htcondor.GetServerSecurityConfig(d.Config(), commands.QUERY_STARTD_ADS, "DAEMON")
	if err != nil {
		return fmt.Errorf("building security config: %w", err)
	}

	// The collector core -- store, protocol handlers, CONDOR_VIEW_HOST forwarding,
	// and background dictionary retraining + ad expiry -- exactly as the embeddable
	// collector library provides it. This daemon wraps it with the condor_master
	// glue: config, logging, the command socket, the address file, DC_* commands,
	// metrics, and the embedded CCB.
	c, err := collector.New(collector.Config{
		Security:            sec,
		ViewHosts:           viewHosts(cfg),
		DictRetrainInterval: configSeconds(cfg, "COLLECTOR_DICT_RETRAIN_INTERVAL", 15*time.Minute),
		DictSampleSize:      configInt(cfg, "COLLECTOR_DICT_SAMPLE_SIZE", 4000),
		ExpireInterval:      configSeconds(cfg, "COLLECTOR_UPDATE_INTERVAL", 900*time.Second),
		Logger:              d.Slog(),
	})
	if err != nil {
		return err
	}
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
func startMetrics(ctx context.Context, addr string, st *store.Store, log *logging.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(st))
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
