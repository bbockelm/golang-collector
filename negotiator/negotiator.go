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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
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
	// CycleStatsLength is NEGOTIATOR_CYCLE_STATS_LENGTH (default 3, cap 100):
	// how many recent cycles' stats to keep and publish on the NegotiatorAd
	// (suffixes 0..N-1, 0 = newest). Mirrors the C++ ring
	// (matchmaker.h MAX_NEGOTIATION_CYCLE_STATS = 100).
	CycleStatsLength int

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
	// cycleRing holds the most recent cycles' stats, newest first, capped at
	// cfg.CycleStatsLength — the C++ negotiation_cycle_stats ring.
	cycleRing []*CycleStats
}

const (
	defaultCycleStatsLength = 3   // C++ NEGOTIATOR_CYCLE_STATS_LENGTH default
	maxCycleStatsLength     = 100 // C++ MAX_NEGOTIATION_CYCLE_STATS
)

// recordCycle pushes a completed cycle onto the stats ring (newest first),
// trimming to CycleStatsLength. Caller holds n.mu.
func (n *Negotiator) recordCycleLocked(s *CycleStats) {
	n.lastStats = s
	n.cycleRing = append([]*CycleStats{s}, n.cycleRing...)
	if len(n.cycleRing) > n.cfg.CycleStatsLength {
		n.cycleRing = n.cycleRing[:n.cfg.CycleStatsLength]
	}
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
	if cfg.CycleStatsLength <= 0 {
		cfg.CycleStatsLength = defaultCycleStatsLength
	}
	if cfg.CycleStatsLength > maxCycleStatsLength {
		cfg.CycleStatsLength = maxCycleStatsLength
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
	// GET_RESLIST (condor_userprio -getreslist): the per-submitter resource list
	// (matchmaker.cpp:603-605, READ). Referenced by SCHED_VERS offset because
	// cedar/commands does not yet name it.
	n.handle(cs, commandGetResList, n.handleGetResList(), "READ")

	// The collector-style direct queries a modern condor_userprio -modular /
	// condor_status -direct sends to the negotiator instead of GET_PRIORITY
	// (matchmaker.cpp:606-611, both READ). Without QUERY_ACCOUNTING_ADS a stock
	// condor_userprio -modular reports "Can't query negotiator for ads".
	n.handle(cs, commands.QUERY_ACCOUNTING_ADS,
		n.handleQueryAds(func() []*classad.ClassAd {
			return n.cfg.Accountant.ReportStateAds(n.cfg.NegotiatorName, time.Now())
		}), "READ")
	n.handle(cs, commands.QUERY_NEGOTIATOR_ADS,
		n.handleQueryAds(func() []*classad.ClassAd {
			return []*classad.ClassAd{n.buildNegotiatorAd()}
		}), "READ")

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

// commandGetResList is GET_RESLIST (condor_userprio -getreslist): SCHED_VERS+63
// (463). cedar/commands does not name it yet, so it is referenced by offset —
// the same base the named userprio commands use (commands.GET_PRIORITY = +51).
const commandGetResList = commands.SCHED_VERS + 63

// handleGetResList serves GET_RESLIST: wire "string submitter + EOM", reply the
// per-submitter resource-list ad (Name<i>/StartTime<i>) WITHOUT the
// MyType/TargetType trailer, which condor_userprio reads with getClassAdNoTypes
// (user_prio.cpp:1122; matchmaker.cpp GET_RESLIST_commandHandler).
func (n *Negotiator) handleGetResList() cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		submitter, err := payload(c).GetString(ctx)
		if err != nil {
			return fmt.Errorf("negotiator: GET_RESLIST: reading submitter: %w", err)
		}
		ad := n.cfg.Accountant.ResList(submitter)
		out := message.NewMessageForStream(c.Stream)
		if err := out.PutClassAdWithOptions(ctx, ad, &message.PutClassAdConfig{
			Options: message.PutClassAdNoTypes,
		}); err != nil {
			return fmt.Errorf("negotiator: GET_RESLIST: sending reply: %w", err)
		}
		return out.FinishMessage(ctx)
	}
}

// handleQueryAds serves the collector-style QUERY_*_ADS commands a modern
// condor_userprio -modular / condor_status -direct sends STRAIGHT to the
// negotiator (QUERY_ACCOUNTING_ADS, QUERY_NEGOTIATOR_ADS) rather than the legacy
// GET_PRIORITY. It mirrors Matchmaker::QUERY_ADS_commandHandler
// (matchmaker.cpp:1535): read the query ad, gather the candidate ads, drop those
// that fail its Requirements, honor a projection, and stream the matches as
// PutInt32(1)+ad per ad terminated by PutInt32(0) — the same framing the
// collector query protocol uses (server queryHandler). gather() produces the
// candidate ads for this command.
func (n *Negotiator) handleQueryAds(gather func() []*classad.ClassAd) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		queryAd, err := message.NewMessageFromStream(c.Stream).GetClassAd(ctx)
		if err != nil {
			return fmt.Errorf("negotiator: query: reading query ad: %w", err)
		}
		matcher, projection, limit, err := parseAdQuery(queryAd)
		if err != nil {
			return fmt.Errorf("negotiator: query: bad constraint: %w", err)
		}
		resp := message.NewMessageForStream(c.Stream)
		count := 0
		for _, ad := range gather() {
			if limit > 0 && count >= limit {
				break
			}
			if matcher != nil && !matcher.Matches(ad) {
				continue
			}
			if err := resp.PutInt32(ctx, 1); err != nil {
				return err
			}
			if err := resp.PutClassAd(ctx, projectAd(ad, projection)); err != nil {
				return err
			}
			count++
		}
		if err := resp.PutInt32(ctx, 0); err != nil {
			return err
		}
		return resp.FlushFrame(ctx, true)
	}
}

// parseAdQuery pulls the constraint (Requirements), projection and result limit
// out of a query ad, mirroring the collector's parseQuery. A nil *vm.Query means
// "match everything" (absent or literally-true Requirements). It accepts either
// the whitespace-separated Projection or the comma-separated ProjectionAttributes.
func parseAdQuery(queryAd *classad.ClassAd) (*vm.Query, []string, int, error) {
	var q *vm.Query
	if expr, ok := queryAd.Lookup("Requirements"); ok {
		s := strings.TrimSpace(expr.String())
		if s != "" && !strings.EqualFold(s, "true") {
			var err error
			if q, err = vm.Parse(s); err != nil {
				return nil, nil, 0, err
			}
		}
	}
	var projection []string
	if s, ok := queryAd.EvaluateAttrString("Projection"); ok && strings.TrimSpace(s) != "" {
		projection = splitQueryAttrs(s)
	} else if s, ok := queryAd.EvaluateAttrString("ProjectionAttributes"); ok && strings.TrimSpace(s) != "" {
		projection = splitQueryAttrs(s)
	}
	limit := 0
	if l, ok := queryAd.EvaluateAttrInt("LimitResults"); ok && l > 0 {
		limit = int(l)
	}
	return q, projection, limit, nil
}

func splitQueryAttrs(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
}

// projectAd returns a copy of ad holding only the whitelisted attributes that
// are present; an empty whitelist returns ad unchanged.
func projectAd(ad *classad.ClassAd, attrs []string) *classad.ClassAd {
	if len(attrs) == 0 {
		return ad
	}
	out := classad.New()
	for _, a := range attrs {
		if e, ok := ad.Lookup(a); ok {
			out.InsertExpr(a, e)
		}
	}
	return out
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
			n.recordCycleLocked(stats)
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
	ring := append([]*CycleStats(nil), n.cycleRing...)
	n.mu.Unlock()
	publishCycleRing(ad, ring)
	return ad
}

// publishCycleRing stamps the last-N-cycles attributes on the NegotiatorAd,
// suffix 0..N-1 (0 = newest), mirroring the C++ ring
// (publishNegotiationCycleStats, matchmaker.cpp:6455-6544). Period[i] is the
// end-to-end gap to the next-older cycle; MatchRate/MatchRateSustained are the
// matches over duration / period, as in C++.
func publishCycleRing(ad *classad.ClassAd, ring []*CycleStats) {
	for i, s := range ring {
		var period time.Duration
		if i+1 < len(ring) {
			period = s.End.Sub(ring[i+1].End)
		}
		publishCycleStats(ad, s, i, period)
	}
}

// publishCycleStats stamps one cycle's attributes with the given "<i>" suffix.
// Durations are whole seconds (the C++ time_t math). CPU time is whole-process
// (getrusage) rather than the C++ single-threaded per-phase rusage, so only the
// aggregate CpuTime is published; per-phase CPU attrs are left to the wall-clock
// phase durations' companions. SubmittersShareLimit is not yet classified and is
// published as 0 (see NEGOTIATOR_CPP_DIFFERENCES.md).
func publishCycleStats(ad *classad.ClassAd, s *CycleStats, i int, period time.Duration) {
	secs := func(d time.Duration) int64 { return int64(d / time.Second) }
	suf := strconv.Itoa(i)
	set := func(name string, v any) { _ = ad.Set("LastNegotiationCycle"+name+suf, v) }

	dur := s.End.Sub(s.Start)
	set("Time", s.Start.Unix())
	set("End", s.End.Unix())
	set("Period", secs(period))
	set("Duration", secs(dur))
	set("Phase1Duration", secs(s.Phase1Duration))
	set("Phase2Duration", secs(s.Phase2Duration))
	set("Phase3Duration", secs(s.Phase3Duration))
	set("Phase4Duration", secs(s.Phase4Duration))
	set("PrefetchDuration", secs(s.PrefetchDuration))
	set("CpuTime", s.CpuTime.Seconds())
	set("TotalSlots", int64(s.TotalSlots))
	set("TrimmedSlots", int64(s.TrimmedSlots))
	set("CandidateSlots", int64(s.CandidateSlots))
	set("SlotShareIter", int64(s.SlotShareIter))
	set("NumSchedulers", int64(s.NumSchedulers))
	set("ActiveSubmitterCount", int64(s.ActiveSubmitters))
	set("NumIdleJobs", int64(s.IdleJobs))
	set("NumJobsConsidered", int64(s.JobsConsidered))
	set("Matches", int64(s.Matches))
	set("Rejections", int64(s.Rejections))
	set("Pies", int64(s.Pies))
	set("PieSpins", int64(s.PieSpins))
	set("ScheddsOutOfTime", int64(s.ScheddsOutOfTime))
	set("SubmittersFailed", int64(s.SubmittersFailed))
	set("SubmittersOutOfTime", int64(s.SubmittersOutOfTime))
	set("SubmittersShareLimit", int64(s.SubmittersShareLimit))
	matchRate := 0.0
	if dur > 0 {
		matchRate = float64(s.Matches) / dur.Seconds()
	}
	set("MatchRate", matchRate)
	sustained := 0.0
	if period > 0 {
		sustained = float64(s.Matches) / period.Seconds()
	}
	set("MatchRateSustained", sustained)
}

// bracketAddr renders an address as a sinful string ("<host:port>") unless it
// already is one.
func bracketAddr(addr string) string {
	if strings.HasPrefix(addr, "<") {
		return addr
	}
	return "<" + addr + ">"
}
