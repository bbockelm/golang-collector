// Command golang-negotiator runs an HTCondor condor_negotiator as a Go daemon:
// on every negotiation cycle it queries a collector for the pool's machine,
// submitter, and startd-private ads, runs the fair-share pie spin, and hands
// matches (with their claim ids) to the schedds over the NEGOTIATE protocol.
// It serves the condor_userprio command surface (GET_PRIORITY/SET_*) and
// RESCHEDULE, loads its policy from the HTCondor configuration, and runs under
// condor_master (shared-port endpoint, DC_SET_READY / DC_CHILDALIVE) like any
// other HTCondor daemon — mirroring cmd/golang-collector's structure.
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
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/authz"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/accountant"
	"github.com/bbockelm/golang-collector/negotiator/cycle"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
	"github.com/bbockelm/golang-collector/negotiator/source"
)

// version is stamped at build time via `-ldflags "-X main.version=..."` (see the
// Makefile); it is "dev" for a plain `go build`.
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "golang-negotiator:", err)
		os.Exit(1)
	}
}

func run() error {
	listen := flag.String("listen", ":0", "fallback TCP listen address when not inheriting a shared-port endpoint (the negotiator has no well-known port; peers find it via the collector)")
	showVersion := flag.Bool("version", false, "print version and exit")
	// condor_master appends these standard DaemonCore flags when it launches a
	// daemon not in its built-in list; accept them so flag.Parse does not reject
	// our launch. -local-name additionally scopes config lookups.
	localName := flag.String("local-name", "", "HTCondor subsystem local-name; passed by condor_master")
	_ = flag.String("sock", "", "HTCondor shared-port endpoint name; accepted for compatibility (fd inherited via CONDOR_INHERIT)")
	importLog := flag.String("import", "", "path to a C++ negotiator Accountantnew.log to import ONCE into the native accountant store (overrides ACCOUNTANT_IMPORT_LOG); imported only when the native store has no existing Customer records")
	flag.Parse()

	if *showVersion {
		fmt.Println("htc-negotiator", version)
		return nil
	}

	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "NEGOTIATOR", LocalName: *localName})
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Bootstrap logging and condor_master integration (drops privileges to the
	// condor user when started as root).
	d, err := daemon.New(daemon.Options{Subsys: "NEGOTIATOR", Config: cfg})
	if err != nil {
		return err
	}
	log := d.Logger()
	// Route cedar's security/server slog output into NegotiatorLog.
	slog.SetDefault(d.Slog())

	// Server-side security policy from the HTCondor configuration (SEC_* knobs)
	// for our own command socket (userprio + RESCHEDULE).
	sec, err := htcondor.GetServerSecurityConfig(d.Config(), commands.GET_PRIORITY, "DAEMON")
	if err != nil {
		return fmt.Errorf("building security config: %w", err)
	}
	srv := cedarserver.New(sec)

	// Per-command ALLOW_/DENY_ authorization (getters READ, setters
	// ADMINISTRATOR, SET_PRIORITYFACTOR WRITE, RESCHEDULE DAEMON), exactly
	// like the C++ negotiator's Register_Command levels.
	policy, err := authz.NewPolicy(d.Config(), "NEGOTIATOR")
	if err != nil {
		return fmt.Errorf("building authorization policy: %w", err)
	}
	srv.Authorizer = policy.Authorize

	// DC_NOP / DC_CONFIG_VAL / etc. so condor_who, condor_ping and condor_config_val work.
	d.RegisterDefaultCommands(srv)

	// Command-socket listener: the shared-port endpoint inherited from
	// condor_master if present, otherwise a plain TCP bind.
	ln, err := d.Listener(func() (net.Listener, error) {
		return net.Listen("tcp", *listen)
	})
	if err != nil {
		log.Error(logging.DestinationGeneral, "listener setup failed", "err", err.Error())
		return err
	}
	defer func() { _ = ln.Close() }()

	// Publish our command address (NEGOTIATOR_ADDRESS_FILE, default
	// $(LOG)/.negotiator_address) so condor_userprio and friends can find us
	// without a collector round trip.
	if path := writeAddressFile(d, cfg, ln); path != "" {
		defer func() { _ = os.Remove(path) }()
	}

	// The pool view: CEDAR queries against the collector, like the C++
	// negotiator. The startd-private-ad query (claim ids) needs NEGOTIATOR
	// authorization at the collector.
	collectorHost, err := resolveCollectorAddr(cfg)
	if err != nil {
		return err
	}
	querySec, err := htcondor.GetSecurityConfig(d.Config(), commands.QUERY_STARTD_ADS, "CLIENT")
	if err != nil {
		return fmt.Errorf("building collector client security config: %w", err)
	}
	src, err := source.NewRemote(source.Config{
		SlotConstraint:      configString(cfg, "NEGOTIATOR_SLOT_CONSTRAINT"),
		SubmitterConstraint: configString(cfg, "NEGOTIATOR_SUBMITTER_CONSTRAINT"),
		SlotWeightExpr:      configString(cfg, "SLOT_WEIGHT"),
		CollectorAddr:       collectorHost,
		Security:            querySec,
		Logger:              d.Slog(),
	})
	if err != nil {
		return err
	}

	// Accountant state: ACCOUNTANT_DATABASE_FILE, defaulting to
	// $(SPOOL)/GoAccountant.log — the Go-native transaction-log format, NOT
	// the C++ Accountantnew.log ClassAdLog (importer deferred; design doc
	// 3.4). Point the knob at a fresh path when migrating from C++.
	acctCfg := accountant.ConfigFromKnobs(cfg.Get)
	acctCfg.LogFile = accountantLogFile(cfg)
	// One-shot C++ Accountantnew.log import: the -import flag, else the
	// ACCOUNTANT_IMPORT_LOG knob. The accountant applies it only when its native
	// store has no existing Customer records (idempotent across restarts), so a
	// pool can migrate its accumulated priority/usage in place from a running
	// C++ negotiator without resetting fair-share history.
	if v := strings.TrimSpace(*importLog); v != "" {
		acctCfg.ImportFrom = v
	} else {
		acctCfg.ImportFrom = configString(cfg, "ACCOUNTANT_IMPORT_LOG")
	}
	if acctCfg.ImportFrom != "" {
		log.Info(logging.DestinationGeneral, "accountant one-shot C++ log import configured (applied once, only when the native store has no Customer records)",
			"import_from", acctCfg.ImportFrom, "native_store", acctCfg.LogFile)
	}
	acct, err := accountant.New(acctCfg)
	if err != nil {
		return err
	}
	defer func() { _ = acct.Close() }()

	// Client security for the NEGOTIATE sessions toward schedds. Encryption is
	// REQUIRED: the claim ids in PERMISSION_AND_AD are secrets the C++ schedd
	// reads with get_secret, which is a plain string on an encrypted channel.
	sessionSec, err := htcondor.GetSecurityConfig(d.Config(), commands.NEGOTIATE, "CLIENT")
	if err != nil {
		return fmt.Errorf("building schedd client security config: %w", err)
	}
	sessionSec.Encryption = security.SecurityRequired

	cycleCfg := cycle.ConfigFromKnobs(cfg.Get)
	sf := protocol.NewFactory(sessionSec, protocol.WithNegotiatorName(cycleCfg.NegotiatorName))
	defer sf.CloseAll()
	cyc, err := cycle.New(src, acct, sf, cycleCfg)
	if err != nil {
		return err
	}

	neg, err := negotiator.New(negotiator.Config{
		Source:           src,
		Accountant:       acct,
		Cycle:            cyc,
		NegotiatorName:   cycleCfg.NegotiatorName,
		AdvertisedAddr:   negotiatorAddr(d, ln),
		Interval:         configSeconds(cfg, "NEGOTIATOR_INTERVAL", 60*time.Second),
		CycleDelay:       configSeconds(cfg, "NEGOTIATOR_CYCLE_DELAY", 20*time.Second),
		MinInterval:      configSeconds(cfg, "NEGOTIATOR_MIN_INTERVAL", 5*time.Second),
		UpdateInterval:   configSeconds(cfg, "NEGOTIATOR_UPDATE_INTERVAL", 300*time.Second),
		CycleStatsLength: configInt(cfg, "NEGOTIATOR_CYCLE_STATS_LENGTH", 3),
		Authorizer:       policy.Authorize,
		Logger:           d.Slog(),
	})
	if err != nil {
		return err
	}
	neg.RegisterOn(srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The negotiation-cycle timer (first cycle immediately) and the
	// NegotiatorAd publisher.
	defer neg.StartBackground(ctx)()

	log.Info(logging.DestinationGeneral, "golang-negotiator starting",
		"listen", ln.Addr().String(), "collector", collectorHost,
		"accountant_db", acctCfg.LogFile, "under_master", d.UnderMaster())

	return d.Serve(ctx, ln, srv.Serve)
}

// negotiatorAddr is this negotiator's externally reachable command address: the
// shared-port sinful when running under condor_master, otherwise the plain
// listen address.
func negotiatorAddr(d *daemon.Daemon, ln net.Listener) string {
	if sinful, ok := d.AdvertisedSinful(); ok {
		return sinful
	}
	return ln.Addr().String()
}

// writeAddressFile publishes the negotiator's command address to
// NEGOTIATOR_ADDRESS_FILE (default $(LOG)/.negotiator_address) as a sinful
// string. Returns the path written (for cleanup), or "" if none.
func writeAddressFile(d *daemon.Daemon, cfg *config.Config, ln net.Listener) string {
	path, ok := cfg.Get("NEGOTIATOR_ADDRESS_FILE")
	if !ok || path == "" {
		logDir, ok := cfg.Get("LOG")
		if !ok || logDir == "" {
			return ""
		}
		path = filepath.Join(logDir, ".negotiator_address")
	}
	if err := os.WriteFile(path, []byte("<"+negotiatorAddr(d, ln)+">\n"), 0o644); err != nil {
		slog.Warn("could not write negotiator address file", "path", path, "err", err)
		return ""
	}
	return path
}

// accountantLogFile resolves the accountant state file: ACCOUNTANT_DATABASE_FILE
// if set, else $(SPOOL)/GoAccountant.log, else "" (memory-only, with a warning).
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

// firstConfigured returns the first non-empty knob among keys, trimmed.
// resolveCollectorAddr locates the collector to query. It prefers the
// collector's address file (COLLECTOR_ADDRESS_FILE, default
// $(LOG)/.collector_address) -- the canonical mechanism for a co-located daemon
// and the only source of the exact sinful when the collector runs on a
// shared-port / ephemeral port (COLLECTOR_HOST then has no usable port). It
// falls back to COLLECTOR_HOST / CONDOR_HOST for a remote central manager on
// the well-known port.
func resolveCollectorAddr(cfg *config.Config) (string, error) {
	addrFile := configString(cfg, "COLLECTOR_ADDRESS_FILE")
	if addrFile == "" {
		if logDir, ok := cfg.Get("LOG"); ok && logDir != "" {
			addrFile = filepath.Join(logDir, ".collector_address")
		}
	}
	if addrFile != "" {
		if data, err := os.ReadFile(addrFile); err == nil {
			// The first non-empty line is the sinful (version banner follows).
			for _, line := range strings.Split(string(data), "\n") {
				if line = strings.TrimSpace(line); line != "" {
					return line, nil
				}
			}
		}
	}
	if host := firstConfigured(cfg, "COLLECTOR_HOST", "CONDOR_HOST"); host != "" {
		return host, nil
	}
	return "", fmt.Errorf("cannot locate the collector: set COLLECTOR_HOST or COLLECTOR_ADDRESS_FILE")
}

func firstConfigured(cfg *config.Config, keys ...string) string {
	for _, key := range keys {
		if v, ok := cfg.Get(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// configString reads a string knob, trimmed ("" when unset).
func configString(cfg *config.Config, key string) string {
	v, _ := cfg.Get(key)
	return strings.TrimSpace(v)
}

func configSeconds(cfg *config.Config, key string, def time.Duration) time.Duration {
	if v, ok := cfg.Get(key); ok {
		if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return def
}

// configInt reads a positive integer knob, falling back to def.
func configInt(cfg *config.Config, key string, def int) int {
	if v, ok := cfg.Get(key); ok {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			return n
		}
	}
	return def
}
