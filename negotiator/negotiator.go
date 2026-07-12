// The Negotiator daemon object: the embeddable shell around the negotiation
// engine. It mirrors the collector's embedding seam (collector.New /
// RegisterOn / StartBackground) so a host daemon can serve the negotiator's
// command protocol on a cedar command server it already owns:
//
//	neg, err := negotiator.New(negotiator.Config{Source: src, Accountant: acct, Cycle: cyc})
//	neg.RegisterOn(hostServer)           // userprio + RESCHEDULE handlers
//	stop := neg.StartBackground(ctx)     // cycle timer + NegotiatorAd publisher
//	defer stop()
//
// Because this root package defines the interfaces the negotiator/* sub-
// packages implement (they import it), Config takes the CONSTRUCTED
// collaborators (AdSource, Accountant, Cycle) rather than their configs — the
// mains wire accountant.New + protocol.NewFactory + cycle.New and hand the
// results in. C++ behavioral reference: src/condor_negotiator.V6/
// matchmaker.cpp (command registration :552-615, RESCHEDULE :980-993, timer
// semantics :1868-1880 and :2160-2166, updateCollector :6197-6218).
package negotiator

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/cedar/version"
)

// Config configures an embeddable Negotiator daemon object.
type Config struct {
	// Source publishes the NegotiatorAd (and, inside Cycle, gathers the pool
	// snapshot). Required.
	Source AdSource
	// Accountant backs the userprio command handlers. Required.
	Accountant Accountant
	// Cycle runs one negotiation cycle per timer fire. Required.
	Cycle Cycle

	// NegotiatorName is NEGOTIATOR_NAME; defaults to the hostname (the C++
	// build_valid_daemon_name fallback).
	NegotiatorName string
	// AdvertisedAddr is this daemon's reachable command address (host:port or
	// sinful), stamped as NegotiatorIpAddr/MyAddress on the NegotiatorAd.
	AdvertisedAddr string
	// Machine is the Machine attribute on the NegotiatorAd; defaults to the
	// hostname.
	Machine string

	// Interval is NEGOTIATOR_INTERVAL (default 60s): the steady-state period
	// of the negotiation-cycle timer.
	Interval time.Duration
	// CycleDelay is NEGOTIATOR_CYCLE_DELAY (default 20s): the minimum quiet
	// time after one cycle COMPLETES before the next may start.
	CycleDelay time.Duration
	// MinInterval is NEGOTIATOR_MIN_INTERVAL (default 5s): the minimum time
	// between cycle STARTS.
	MinInterval time.Duration
	// UpdateInterval is NEGOTIATOR_UPDATE_INTERVAL (default 300s): how often
	// the NegotiatorAd is published.
	UpdateInterval time.Duration

	// Authorizer, if set, enforces per-command HTCondor ALLOW_/DENY_
	// authorization (perm is a DCpermission name, e.g. "READ"); the same
	// levels are registered on the command server so ValidCommands advertises
	// them. Nil allows any authenticated peer (the host server's security
	// policy still applies). Mirrors collector.Config.Authorizer.
	Authorizer func(perm, peerAddr, user string) bool

	// Logger for operational logging (default slog.Default()).
	Logger *slog.Logger
}

// Negotiator is the embeddable negotiator daemon: the userprio + RESCHEDULE
// command handlers plus the background cycle timer and NegotiatorAd publisher.
type Negotiator struct {
	cfg Config
	log *slog.Logger

	// wake is the RESCHEDULE signal: buffered depth 1 so a burst of
	// RESCHEDULEs collapses into one pending early cycle, exactly like the
	// C++ GotRescheduleCmd flag (matchmaker.cpp:989).
	wake chan struct{}

	mu        sync.Mutex
	lastStats *CycleStats
}

// New validates cfg, fills the HTCondor defaults, and returns the Negotiator.
func New(cfg Config) (*Negotiator, error) {
	if cfg.Source == nil || cfg.Accountant == nil || cfg.Cycle == nil {
		return nil, fmt.Errorf("negotiator: Source, Accountant, and Cycle are required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.CycleDelay <= 0 {
		cfg.CycleDelay = 20 * time.Second
	}
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = 5 * time.Second
	}
	if cfg.UpdateInterval <= 0 {
		cfg.UpdateInterval = 300 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	host, _ := os.Hostname()
	if cfg.NegotiatorName == "" {
		cfg.NegotiatorName = host
	}
	if cfg.Machine == "" {
		cfg.Machine = host
	}
	return &Negotiator{
		cfg:  cfg,
		log:  cfg.Logger,
		wake: make(chan struct{}, 1),
	}, nil
}

// LastCycleStats returns the stats of the most recently completed cycle, or
// nil before the first one finishes.
func (n *Negotiator) LastCycleStats() *CycleStats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastStats
}

// ---------------------------------------------------------------------------
// Command handlers (RegisterOn surface)
// ---------------------------------------------------------------------------

// RegisterOn registers the negotiator's command handlers — the condor_userprio
// protocol (design doc 3.5) and RESCHEDULE — onto an existing cedar command-
// dispatch server, at the C++ registration levels (matchmaker.cpp:552-615):
// getters at READ, setters at ADMINISTRATOR, SET_PRIORITYFACTOR at WRITE
// (with the handler itself requiring ADMINISTRATOR, as the C++ does absent a
// PRIORITY_FACTOR_AUTHORIZATION user map), RESCHEDULE at DAEMON with the
// WRITE/ADVERTISE_SCHEDD/ADMINISTRATOR alternates.
//
// GET_RESLIST (463) is NOT registered: its command int is not defined in
// cedar/commands yet (design doc 3.5 lists it as the one missing constant);
// condor_userprio -getreslist is therefore unsupported until cedar adds it.
// The lease-based MANAGE_* commands (ceiling/floor/priofactor leases) are
// Phase 7.
func (n *Negotiator) RegisterOn(cs *cedarserver.Server) {
	n.handle(cs, commands.GET_PRIORITY, n.handleGetState(false), "READ")
	n.handle(cs, commands.GET_PRIORITY_ROLLUP, n.handleGetState(true), "READ")

	n.handle(cs, commands.SET_PRIORITY,
		n.handleSetDouble("SET_PRIORITY", n.cfg.Accountant.SetPriority), "ADMINISTRATOR")
	n.handle(cs, commands.SET_ACCUMUSAGE,
		n.handleSetDouble("SET_ACCUMUSAGE", n.cfg.Accountant.SetAccumUsage), "ADMINISTRATOR")
	n.handle(cs, commands.SET_BEGINTIME,
		n.handleSetInt("SET_BEGINTIME", func(sub string, v int) error {
			return n.cfg.Accountant.SetBeginTime(sub, time.Unix(int64(v), 0))
		}), "ADMINISTRATOR")
	n.handle(cs, commands.SET_LASTTIME,
		n.handleSetInt("SET_LASTTIME", func(sub string, v int) error {
			return n.cfg.Accountant.SetLastTime(sub, time.Unix(int64(v), 0))
		}), "ADMINISTRATOR")
	n.handle(cs, commands.SET_CEILING,
		n.handleSetInt("SET_CEILING", func(sub string, v int) error {
			return n.cfg.Accountant.SetCeiling(sub, int64(v))
		}), "ADMINISTRATOR")
	n.handle(cs, commands.SET_FLOOR,
		n.handleSetInt("SET_FLOOR", func(sub string, v int) error {
			return n.cfg.Accountant.SetFloor(sub, int64(v))
		}), "ADMINISTRATOR")
	n.handle(cs, commands.RESET_USAGE,
		n.handleSetName("RESET_USAGE", n.cfg.Accountant.ResetUsage), "ADMINISTRATOR")
	n.handle(cs, commands.DELETE_USER,
		n.handleSetName("DELETE_USER", n.cfg.Accountant.DeleteRecord), "ADMINISTRATOR")
	n.handle(cs, commands.RESET_ALL_USAGE, n.handleResetAllUsage, "ADMINISTRATOR")

	// SET_PRIORITYFACTOR enforces ADMINISTRATOR itself (with the versioned
	// {ErrorCode} reply ad), so it is registered directly at WRITE.
	cs.Handle(commands.SET_PRIORITYFACTOR, n.handleSetPriorityFactor, "WRITE")

	n.handle(cs, commands.RESCHEDULE, n.handleReschedule,
		"DAEMON", "ADVERTISE_SCHEDD", "ADMINISTRATOR", "WRITE")
}

// handle registers fn wrapped with the ALLOW_/DENY_ authorization gate for
// perms (a peer passing ANY one level is authorized), mirroring the
// golang-ccb Server.authorize pattern.
func (n *Negotiator) handle(cs *cedarserver.Server, cmd int, fn cedarserver.HandlerFunc, perms ...string) {
	wrapped := func(ctx context.Context, c *cedarserver.Conn) error {
		if err := n.authorize(c, perms); err != nil {
			n.log.Warn("negotiator: command denied", "command", commands.GetCommandName(cmd), "err", err.Error())
			return err
		}
		return fn(ctx, c)
	}
	cs.Handle(cmd, wrapped, perms...)
}

// authorize applies the configured policy for the connection's command,
// verifying the peer's authenticated identity against the given levels.
func (n *Negotiator) authorize(c *cedarserver.Conn, perms []string) error {
	if n.cfg.Authorizer == nil {
		return nil
	}
	user := ""
	if c.Negotiation != nil {
		user = c.Negotiation.User
	}
	for _, perm := range perms {
		if n.cfg.Authorizer(perm, c.RemoteAddr, user) {
			return nil
		}
	}
	return fmt.Errorf("negotiator: authorization denied for command %d from %s (user %q)",
		c.Command, c.RemoteAddr, user)
}

// payload returns the message the command's payload rides on: the follow-on
// message for a kept-alive session, else a fresh message on the stream.
func payload(c *cedarserver.Conn) *message.Message {
	if c.Message != nil {
		return c.Message
	}
	return message.NewMessageFromStream(c.Stream)
}

// handleGetState serves GET_PRIORITY / GET_PRIORITY_ROLLUP: the request is
// bare (command + EOM), the reply is the ReportState ad sent WITHOUT the
// MyType/TargetType trailer — the C++ replies with PUT_CLASSAD_NO_TYPES and
// condor_userprio reads with getClassAdNoTypes (user_prio.cpp:1259).
func (n *Negotiator) handleGetState(rollup bool) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		ad := n.cfg.Accountant.ReportState(rollup)
		out := message.NewMessageForStream(c.Stream)
		if err := out.PutClassAdWithOptions(ctx, ad, &message.PutClassAdConfig{
			Options: message.PutClassAdNoTypes,
		}); err != nil {
			return fmt.Errorf("negotiator: sending priority state: %w", err)
		}
		return out.FinishMessage(ctx)
	}
}

// handleSetName serves the "string submitter + EOM" mutators (RESET_USAGE,
// DELETE_USER).
func (n *Negotiator) handleSetName(name string, set func(string) error) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		submitter, err := payload(c).GetString(ctx)
		if err != nil {
			return fmt.Errorf("negotiator: %s: reading submitter: %w", name, err)
		}
		n.log.Info("negotiator: userprio command", "command", name, "submitter", submitter)
		return set(submitter)
	}
}

// handleSetDouble serves the "string submitter + double value + EOM" mutators
// (SET_PRIORITY, SET_ACCUMUSAGE).
func (n *Negotiator) handleSetDouble(name string, set func(string, float64) error) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		in := payload(c)
		submitter, err := in.GetString(ctx)
		if err != nil {
			return fmt.Errorf("negotiator: %s: reading submitter: %w", name, err)
		}
		value, err := in.GetDouble(ctx)
		if err != nil {
			return fmt.Errorf("negotiator: %s: reading value: %w", name, err)
		}
		n.log.Info("negotiator: userprio command", "command", name, "submitter", submitter, "value", value)
		return set(submitter, value)
	}
}

// handleSetInt serves the "string submitter + int value + EOM" mutators
// (SET_BEGINTIME, SET_LASTTIME, SET_CEILING, SET_FLOOR).
func (n *Negotiator) handleSetInt(name string, set func(string, int) error) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		in := payload(c)
		submitter, err := in.GetString(ctx)
		if err != nil {
			return fmt.Errorf("negotiator: %s: reading submitter: %w", name, err)
		}
		value, err := in.GetInt(ctx)
		if err != nil {
			return fmt.Errorf("negotiator: %s: reading value: %w", name, err)
		}
		n.log.Info("negotiator: userprio command", "command", name, "submitter", submitter, "value", value)
		return set(submitter, value)
	}
}

// handleResetAllUsage serves RESET_ALL_USAGE (bare command + EOM).
func (n *Negotiator) handleResetAllUsage(ctx context.Context, c *cedarserver.Conn) error {
	n.log.Info("negotiator: resetting the usage of all users")
	return n.cfg.Accountant.ResetAllUsage()
}

// handleSetPriorityFactor serves SET_PRIORITYFACTOR: wire "string submitter +
// double factor + EOM", registered at WRITE but requiring ADMINISTRATOR to
// mutate — the C++ registers at WRITE and the handler then demands
// ADMINISTRATOR or a PRIORITY_FACTOR_AUTHORIZATION user-map hit
// (matchmaker.cpp:1082-1148); the user-map path is deferred to Phase 7, so
// non-admin WRITE peers are refused with the versioned error reply. Peers
// built since 8.9.9 receive a {ErrorCode[, ErrorString]} reply ad (with
// types, plain putClassAd), older peers get no reply.
func (n *Negotiator) handleSetPriorityFactor(ctx context.Context, c *cedarserver.Conn) error {
	in := payload(c)
	submitter, err := in.GetString(ctx)
	if err != nil {
		return fmt.Errorf("negotiator: SET_PRIORITYFACTOR: reading submitter: %w", err)
	}
	factor, err := in.GetDouble(ctx)
	if err != nil {
		return fmt.Errorf("negotiator: SET_PRIORITYFACTOR: reading factor: %w", err)
	}

	errCode := 0
	errString := ""
	if err := n.authorize(c, []string{"ADMINISTRATOR"}); err != nil {
		errCode = 4 // the C++ errstack code for "not authorized"
		errString = fmt.Sprintf("client is not authorized to set the priority factor of %s", submitter)
	} else if err := n.cfg.Accountant.SetPriorityFactor(submitter, factor); err != nil {
		errCode = 1
		errString = err.Error()
	} else {
		n.log.Info("negotiator: setting priority factor", "submitter", submitter, "factor", factor)
	}

	if v, ok := version.Parse(c.PeerVersion()); ok && v.AtLeast(version.CondorVersion{Major: 8, Minor: 9, Sub: 9}) {
		reply := classad.New()
		_ = reply.Set("ErrorCode", int64(errCode))
		if errString != "" {
			_ = reply.Set("ErrorString", errString)
		}
		out := message.NewMessageForStream(c.Stream)
		if err := out.PutClassAd(ctx, reply); err != nil {
			return fmt.Errorf("negotiator: SET_PRIORITYFACTOR: sending reply: %w", err)
		}
		if err := out.FinishMessage(ctx); err != nil {
			return fmt.Errorf("negotiator: SET_PRIORITYFACTOR: finishing reply: %w", err)
		}
	}
	if errCode != 0 {
		return fmt.Errorf("negotiator: SET_PRIORITYFACTOR failed: %s", errString)
	}
	return nil
}

// handleReschedule serves RESCHEDULE (421): a bare command that fires the
// negotiation-cycle timer early. Like the C++ (matchmaker.cpp:980-993, the
// GotRescheduleCmd dedup + Reset_Timer(0, NegotiatorInterval)), repeated
// RESCHEDULEs before the cycle starts collapse into one, and the cycle-delay/
// min-interval guards still apply.
func (n *Negotiator) handleReschedule(ctx context.Context, c *cedarserver.Conn) error {
	select {
	case n.wake <- struct{}{}:
		n.log.Info("negotiator: RESCHEDULE received; scheduling an early cycle")
	default: // one already pending
	}
	return nil
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

// StartBackground starts the negotiation-cycle timer loop and the NegotiatorAd
// publisher under ctx and returns a function that stops them (and waits for a
// cycle in flight to observe cancellation and exit). Safe to call once.
func (n *Negotiator) StartBackground(ctx context.Context) func() {
	loopCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); n.cycleLoop(loopCtx) }()
	go func() { defer wg.Done(); n.publishLoop(loopCtx) }()
	return func() {
		cancel()
		wg.Wait()
	}
}

// cycleLoop is the negotiation timer (C++ negotiation_timerID). The first
// cycle runs immediately (Register_Timer(0, NegotiatorInterval)); afterwards
// the timer re-arms to max(CycleDelay, Interval - lastCycleDuration)
// (matchmaker.cpp:2160-2166). A RESCHEDULE wake fires it early. Both paths
// then pass the design-doc 4.1 guards: a cycle never starts sooner than
// CycleDelay after the previous one COMPLETED or MinInterval after it STARTED
// (matchmaker.cpp:1868-1880 defers the timer rather than dropping the fire).
func (n *Negotiator) cycleLoop(ctx context.Context) {
	var started, completed time.Time
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-n.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		// Guards: too soon after the last cycle -> re-arm for the earliest
		// allowed instant and go around (absorbing the wake; the pending
		// cycle already covers any queued RESCHEDULE).
		earliest := completed.Add(n.cfg.CycleDelay)
		if e := started.Add(n.cfg.MinInterval); e.After(earliest) {
			earliest = e
		}
		if wait := time.Until(earliest); !started.IsZero() && wait > 0 {
			timer.Reset(wait)
			continue
		}

		// Consume any RESCHEDULE that arrived before this cycle started; a
		// wake DURING the cycle stays queued and schedules the next one.
		select {
		case <-n.wake:
		default:
		}

		started = time.Now()
		stats, err := n.cfg.Cycle.Run(ctx)
		completed = time.Now()
		if ctx.Err() != nil {
			return // graceful stop mid-cycle
		}
		if err != nil {
			n.log.Error("negotiator: negotiation cycle failed", "err", err.Error())
		}
		if stats != nil {
			n.mu.Lock()
			n.lastStats = stats
			n.mu.Unlock()
		}
		if stats != nil && err == nil {
			n.log.Info("negotiator: negotiation cycle complete",
				"duration", completed.Sub(started).String(),
				"slots", stats.TotalSlots, "submitters", stats.Submitters,
				"matches", stats.Matches, "rejections", stats.Rejections)
		}

		next := n.cfg.Interval - completed.Sub(started)
		if next < n.cfg.CycleDelay {
			next = n.cfg.CycleDelay
		}
		timer.Reset(next)
	}
}

// publishLoop pushes the NegotiatorAd on its own NEGOTIATOR_UPDATE_INTERVAL
// timer (C++ update_collector_tid, first fire immediate).
func (n *Negotiator) publishLoop(ctx context.Context) {
	t := time.NewTicker(n.cfg.UpdateInterval)
	defer t.Stop()
	n.publishAd(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.publishAd(ctx)
		}
	}
}

func (n *Negotiator) publishAd(ctx context.Context) {
	if err := n.cfg.Source.PublishNegotiatorAd(ctx, n.buildNegotiatorAd()); err != nil && ctx.Err() == nil {
		n.log.Warn("negotiator: publishing negotiator ad failed", "err", err.Error())
	}
}

// negotiatorCondorVersion / negotiatorCondorPlatform are the identity banners
// stamped on the NegotiatorAd. The real condor_negotiator gets these for free
// from daemonCore->publish() (matchmaker.cpp:6193); the Go ad must set them
// explicitly. CondorVersion in particular is *load-bearing for locate*:
// condor_daemon_client Daemon::locate(DT_NEGOTIATOR) runs getInfoFromAd(), which
// returns false — failing the whole locate — if the ad has no ATTR_VERSION
// (daemon.cpp:2098-2101). Without it, condor_userprio reports "Can't locate
// negotiator in local pool" even though the ad (with a valid address) is in the
// collector. See buildNegotiatorAd's test for the required-attribute contract.
const (
	negotiatorCondorVersion  = "$CondorVersion: 25.4.0 2025-11-07 BuildID: golang-negotiator $"
	negotiatorCondorPlatform = "$CondorPlatform: X86_64-golang $"
)

// buildNegotiatorAd renders the negotiator's daemon ad: identity (the C++
// init_public_ad, matchmaker.cpp:6176-6193) plus the last completed cycle's
// stats (a documented subset of publishNegotiationCycleStats,
// matchmaker.cpp:6455-6544 — see publishCycleStats).
func (n *Negotiator) buildNegotiatorAd() *classad.ClassAd {
	ad := classad.New()
	_ = ad.Set("MyType", "Negotiator")
	_ = ad.Set("Name", n.cfg.NegotiatorName)
	_ = ad.Set("Machine", n.cfg.Machine)
	// Identity banners the C++ negotiator publishes via daemonCore->publish();
	// ATTR_VERSION is required for Daemon::locate(DT_NEGOTIATOR) to succeed.
	_ = ad.Set("CondorVersion", negotiatorCondorVersion)
	_ = ad.Set("CondorPlatform", negotiatorCondorPlatform)
	if n.cfg.AdvertisedAddr != "" {
		sinful := bracketAddr(n.cfg.AdvertisedAddr)
		_ = ad.Set("NegotiatorIpAddr", sinful)
		_ = ad.Set("MyAddress", sinful)
	}
	n.mu.Lock()
	stats := n.lastStats
	n.mu.Unlock()
	if stats != nil {
		publishCycleStats(ad, stats)
	}
	return ad
}

// publishCycleStats stamps the "0"-suffixed last-cycle attributes. The C++
// keeps a ring of the last N cycles (suffixes 0..N-1) and adds CPU-time,
// match-rate, and failure counters; this Go port publishes only the most
// recent cycle (suffix 0) and the subset of counters CycleStats tracks:
// times/durations (whole seconds, like the C++ time_t math), slot counts,
// submitter/job counts, matches, rejections, and pie spins. The remaining C++
// attrs (Period, SlotShareIter, NumSchedulers, Pies, CpuTime*, MatchRate*,
// ScheddsOutOfTime, SubmittersFailed/OutOfTime/ShareLimit) are Phase 7.
func publishCycleStats(ad *classad.ClassAd, s *CycleStats) {
	secs := func(d time.Duration) int64 { return int64(d / time.Second) }
	_ = ad.Set("LastNegotiationCycleTime0", s.Start.Unix())
	_ = ad.Set("LastNegotiationCycleEnd0", s.End.Unix())
	_ = ad.Set("LastNegotiationCycleDuration0", secs(s.End.Sub(s.Start)))
	_ = ad.Set("LastNegotiationCyclePhase1Duration0", secs(s.Phase1Duration))
	_ = ad.Set("LastNegotiationCyclePhase2Duration0", secs(s.Phase2Duration))
	_ = ad.Set("LastNegotiationCyclePhase3Duration0", secs(s.Phase3Duration))
	_ = ad.Set("LastNegotiationCyclePhase4Duration0", secs(s.Phase4Duration))
	_ = ad.Set("LastNegotiationCyclePrefetchDuration0", secs(s.PrefetchDuration))
	_ = ad.Set("LastNegotiationCycleTotalSlots0", int64(s.TotalSlots))
	_ = ad.Set("LastNegotiationCycleTrimmedSlots0", int64(s.TrimmedSlots))
	_ = ad.Set("LastNegotiationCycleCandidateSlots0", int64(s.CandidateSlots))
	_ = ad.Set("LastNegotiationCycleActiveSubmitterCount0", int64(s.Submitters))
	_ = ad.Set("LastNegotiationCycleNumIdleJobs0", int64(s.IdleJobs))
	_ = ad.Set("LastNegotiationCycleNumJobsConsidered0", int64(s.JobsConsidered))
	_ = ad.Set("LastNegotiationCycleMatches0", int64(s.Matches))
	_ = ad.Set("LastNegotiationCycleRejections0", int64(s.Rejections))
	_ = ad.Set("LastNegotiationCyclePieSpins0", int64(s.PieSpins))
}

// bracketAddr renders an address as a sinful string ("<host:port>") unless it
// already is one.
func bracketAddr(addr string) string {
	if strings.HasPrefix(addr, "<") {
		return addr
	}
	return "<" + addr + ">"
}
