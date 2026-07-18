// Package collector is an embeddable HTCondor condor_collector: an in-memory,
// compressed, indexed ClassAd store served over the CEDAR collector protocol
// (UPDATE_*/QUERY_*/INVALIDATE_*_ADS, multi-ad queries, ack'd updates, private
// startd ads).
//
// It can run standalone (Serve on a listener) or be embedded in another daemon by
// registering its command handlers onto a cedar command-dispatch server the host
// already owns -- so a host can serve the collector protocol on its own shared
// command socket, the way the C++ collector is embedded in condor_master-managed
// daemons. This mirrors the embeddable golang-ccb server.
//
//	c, err := collector.New(collector.Config{Security: sec})
//	if err != nil { ... }
//	stop := c.StartBackground(ctx)   // dictionary retrain + ad expiry
//	defer stop()
//
//	// standalone:
//	c.Serve(ctx, listener)
//
//	// embedded, sharing a host's cedar command server:
//	c.RegisterOn(hostServer)
package collector

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/golang-collector/server"
	"github.com/bbockelm/golang-collector/store"
)

// Config configures an embeddable Collector.
type Config struct {
	// Security is the CEDAR server-side security policy applied to the Collector's
	// own command server (used by Serve and by New's internal server). It may be a
	// plaintext/no-auth config. Required. When embedding via RegisterOn, the host
	// server's own security policy governs the shared socket instead. When
	// SecurityForLevel is set, Security is only the fallback for commands whose
	// level is not in the map.
	Security *security.SecurityConfig

	// SecurityForLevel, if set, gives a distinct CEDAR security policy per HTCondor
	// authorization level ("READ", "ADVERTISE", "NEGOTIATOR"; see server.CommandLevel),
	// so the collector negotiates each command at its own level -- monitoring
	// (READ) can be permissive enough for an unauthenticated condor_status while
	// publishing (ADVERTISE) still requires authentication. A level absent from the
	// map falls back to Security. Optional.
	SecurityForLevel map[string]*security.SecurityConfig

	// Backend is the ad store. Optional; nil selects the default in-memory store
	// (store.New()). Set it to a persistent/remote backend (store.NewDBBackend for
	// an embedded database, or an external-database store over CEDAR) to give
	// the collector restart-survivable or externally-shared storage. StartBackground
	// runs an expiry sweep at startup (pruning ads a persistent backend reloaded
	// that went stale while down) and Close runs one at shutdown.
	Backend store.Backend

	// ViewHosts are CONDOR_VIEW_HOST collector addresses to relay every update and
	// invalidation to (never private startd ads). Optional; nil forwards nothing.
	ViewHosts []string

	// DictRetrainInterval, if > 0, is how often StartBackground retrains the ClassAd
	// compression dictionary over up to DictSampleSize stored ads. A fresh store
	// keeps ads uncompressed (identity codec) until the first retrain, after which a
	// dictionary trained on the live pool compresses similar ads several-fold.
	DictRetrainInterval time.Duration

	// DictSampleSize bounds how many ads are decoded to train the dictionary
	// (default 4000). This is the dominant transient cost of a retrain, so keep it
	// modest.
	DictSampleSize int

	// ExpireInterval, if > 0, is how often StartBackground reaps ads whose
	// ATTR_LAST_HEARD_FROM + lifetime has passed (the collector's ClassAd timeout).
	ExpireInterval time.Duration

	// Authorizer, if set, is installed on the Collector's own command server to
	// enforce per-command HTCondor ALLOW_/DENY_ authorization. Optional; nil allows
	// any authenticated peer (the Security authentication policy still applies).
	// When embedding via RegisterOn, set the authorizer on the host server instead.
	Authorizer func(perm, peerAddr, user string) bool

	// Logger for operational logging (default slog.Default()).
	Logger *slog.Logger
}

// Collector is an embeddable collector: a ClassAd store plus the CEDAR collector
// protocol handlers, with optional CONDOR_VIEW_HOST forwarding and background
// dictionary retraining and ad expiry.
type Collector struct {
	cfg   Config
	log   *slog.Logger
	store store.Backend
	fwd   *server.Forwarder
	srv   *cedarserver.Server
}

// New creates a Collector with an empty store and its protocol handlers registered
// on an internal command server (used by Serve). To embed the collector in a host
// daemon's own command server, additionally call RegisterOn(hostServer).
func New(cfg Config) (*Collector, error) {
	if cfg.Security == nil {
		return nil, fmt.Errorf("collector: Security is required")
	}
	if cfg.DictSampleSize == 0 {
		cfg.DictSampleSize = 4000
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	st := cfg.Backend
	if st == nil {
		st = store.New()
	}
	c := &Collector{
		cfg:   cfg,
		log:   cfg.Logger,
		store: st,
		fwd:   server.NewForwarder(cfg.ViewHosts, cfg.Security),
	}
	c.srv = cedarserver.New(cfg.Security)
	if cfg.Authorizer != nil {
		c.srv.Authorizer = cfg.Authorizer
	}
	if cfg.SecurityForLevel != nil {
		// Negotiate each command at its HTCondor authorization level: map the
		// command to its level (server.CommandLevel) and hand cedar that level's
		// policy. A level absent from the map returns nil, so cedar falls back to
		// the default Security config.
		c.srv.SecurityConfigForCommand = func(command int) *security.SecurityConfig {
			return cfg.SecurityForLevel[server.CommandLevel(command)]
		}
	}
	c.RegisterOn(c.srv)
	return c, nil
}

// RegisterOn registers the collector protocol handlers (UPDATE_*/QUERY_*/
// INVALIDATE_*_ADS, multi-ad queries, ack'd updates) onto an existing cedar
// command-dispatch server, so a host daemon can serve the collector on a command
// socket it already owns. It registers only the collector protocol; the host is
// responsible for DC_* default commands (NOP, CONFIG_VAL, ...) and for running the
// serve loop. New calls this on its internal server for the standalone case.
func (c *Collector) RegisterOn(cs *cedarserver.Server) {
	server.Register(cs, c.store, c.fwd)
}

// StartBackground starts the configured background maintenance -- periodic
// dictionary retraining and ad expiry -- under ctx, and returns a function that
// stops it (and waits for the loops to exit). It is a no-op with both intervals
// unset. Safe to call once.
func (c *Collector) StartBackground(ctx context.Context) func() {
	// Startup expiry sweep: a persistent backend reloads the ads a prior run left
	// behind, some of which went stale while the collector was down. Prune them
	// before serving so a long outage does not resurrect dead ads (a short one
	// keeps the pool warm). A no-op for the empty in-memory backend.
	if n, err := c.store.Expire(); err != nil {
		c.log.Warn("collector: startup expiry sweep failed", "error", err)
	} else if n > 0 {
		c.log.Info("collector: pruned stale ads at startup", "count", n)
	}

	var stops []func()
	// Dictionary retraining is an in-memory-backend optimization (store.Retrainer);
	// a backend that manages its own storage (a database) does not implement it.
	if r, ok := c.store.(store.Retrainer); ok && c.cfg.DictRetrainInterval > 0 {
		c.log.Info("collector: dictionary auto-retraining enabled",
			"interval", c.cfg.DictRetrainInterval.String(), "sample_size", c.cfg.DictSampleSize)
		stops = append(stops, r.StartAutoRetrain(c.cfg.DictRetrainInterval, c.cfg.DictSampleSize))
	}
	if c.cfg.ExpireInterval > 0 {
		loopCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); c.expireLoop(loopCtx) }()
		stops = append(stops, func() { cancel(); wg.Wait() })
	}
	return func() {
		for i := len(stops) - 1; i >= 0; i-- {
			stops[i]()
		}
	}
}

func (c *Collector) expireLoop(ctx context.Context) {
	t := time.NewTicker(c.cfg.ExpireInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n, err := c.store.Expire()
			if err != nil {
				c.log.Warn("collector: ad expiry sweep failed", "error", err)
			} else if n > 0 {
				c.log.Info("collector: reaped expired ads", "count", n)
			}
		}
	}
}

// Serve accepts connections on l and dispatches the collector protocol until ctx is
// cancelled, using the Collector's own command server (see New). A host embedding
// via RegisterOn runs its own serve loop instead and does not call Serve.
func (c *Collector) Serve(ctx context.Context, l net.Listener) error {
	return c.srv.Serve(ctx, l)
}

// ServeConn dispatches the collector protocol on a single already-accepted
// connection, for a host with its own accept loop (e.g. a shared-port endpoint).
func (c *Collector) ServeConn(ctx context.Context, conn net.Conn) error {
	return c.srv.ServeConn(ctx, conn)
}

// Store returns the underlying ad store backend, for introspection, metrics
// (assert store.Statser for per-table counts/byte footprints), and the embedded
// negotiator's in-process reads.
func (c *Collector) Store() store.Backend { return c.store }

// Server returns the Collector's internal cedar command server, for callers that
// want to register additional commands (e.g. DC_* defaults) or set an Authorizer
// before Serve.
func (c *Collector) Server() *cedarserver.Server { return c.srv }

// Close runs a final expiry sweep (keeping a persistent backend's at-rest state
// pruned) and closes the store backend, flushing and releasing its database. It
// should be called once, after the serve loop stops. The in-memory backend's
// Close is a no-op. Callers embedding via RegisterOn that own the backend should
// close it themselves instead.
func (c *Collector) Close() error {
	if n, err := c.store.Expire(); err != nil {
		c.log.Warn("collector: shutdown expiry sweep failed", "error", err)
	} else if n > 0 {
		c.log.Info("collector: pruned stale ads at shutdown", "count", n)
	}
	return c.store.Close()
}
