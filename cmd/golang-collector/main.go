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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bbockelm/cedar/commands"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/golang-collector/server"
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

	st := store.New()

	// Server-side security policy from the HTCondor configuration (SEC_* knobs),
	// so this collector authenticates and encrypts exactly like the C++ one.
	sec, err := htcondor.GetServerSecurityConfig(d.Config(), commands.QUERY_STARTD_ADS, "DAEMON")
	if err != nil {
		return fmt.Errorf("building security config: %w", err)
	}

	srv := server.New(st, sec)
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
	defer ln.Close()

	// Publish our command address so the rest of the pool (and tools like
	// condor_status) can find us, exactly like the C++ collector's
	// COLLECTOR_ADDRESS_FILE.
	if path := writeAddressFile(d, cfg, ln); path != "" {
		defer os.Remove(path)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go housekeep(ctx, d.Config(), st, log)

	log.Info(logging.DestinationGeneral, "golang-collector starting",
		"listen", ln.Addr().String(), "under_master", d.UnderMaster())

	return d.Serve(ctx, ln, srv.Serve)
}

// housekeep periodically reaps ads whose ATTR_LAST_HEARD_FROM + lifetime has
// passed, on the COLLECTOR_UPDATE_INTERVAL timer (matching the C++ collector).
func housekeep(ctx context.Context, cfg *config.Config, st *store.Store, log *logging.Logger) {
	interval := configSeconds(cfg, "COLLECTOR_UPDATE_INTERVAL", 900*time.Second)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := st.Expire(); n > 0 {
				log.Info(logging.DestinationGeneral, "reaped expired ads", "count", n)
			}
		}
	}
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
	addr := ln.Addr().String()
	if sinful, ok := d.AdvertisedSinful(); ok {
		addr = sinful
	}
	if err := os.WriteFile(path, []byte("<"+addr+">\n"), 0o644); err != nil {
		slog.Warn("could not write collector address file", "path", path, "err", err)
		return ""
	}
	return path
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
