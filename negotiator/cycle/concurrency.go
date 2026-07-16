// Fast-mode machinery: concurrent RRL prefetch and asynchronous, ordered
// match delivery (design doc section 2, items 2 and 4).
//
// Determinism contract: every matchmaking DECISION is made on the single
// serial spine in submitter-sorted order in both modes. Fast mode overlaps
// only I/O:
//
//   - prefetch: each spin's NEGOTIATE Begin + first request batch runs in a
//     bounded worker pool BEFORE the spine consumes them. Work is sharded by
//     schedd address, so the per-schedd order of NEGOTIATE rounds equals the
//     submitter-sorted order exactly as in compat mode (which performs the
//     same prefetch serially -- mirroring the C++, which prefetches RRLs for
//     every listed submitter each spin, matchmaker.cpp:2561).
//   - delivery: PERMISSION_AND_AD / REJECTED_WITH_REASON writes are enqueued
//     to a per-submitter worker goroutine that executes all session I/O in
//     decision order; the spine moves on to the next decision immediately. A
//     delivery error marks the session broken and the submitter is MM_ERROR'd
//     at the next decision point -- identical decisions to compat mode, with
//     error DETECTION differing in timing only (compat fails the very call,
//     fast fails the submitter at its next decision).
package cycle

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// endTimeout bounds the asynchronous END_NEGOTIATE write (the C++
// NEGOTIATOR_TIMEOUT default).
const endTimeout = 30 * time.Second

// subState is one submitter's cycle-lived state: identity, budgets, the
// current NEGOTIATE round, and (fast mode) its ordered I/O worker.
type subState struct {
	ad         *classad.ClassAd
	name       string
	scheddName string
	scheddAddr string
	tag        string
	idleJobs   int
	lastHeard  int64
	origIdx    int // snapshot order; the final sort tiebreak

	timeUsed   time.Duration // cumulative negotiation time this cycle
	starvation float64       // stamped by calculatePieLeft each spin

	// Current round (owned by the spine; worker closures capture the session
	// by value so late async ops never race these fields).
	sess      negotiator.ScheddSession
	sessOpen  bool
	queue     []*negotiator.Request
	exhausted bool

	w *subWorker // nil in compat mode

	mu    sync.Mutex
	ioErr error
}

// setErr records the first session I/O error (worker or spine side).
func (s *subState) setErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.ioErr == nil {
		s.ioErr = err
	}
	s.mu.Unlock()
}

// err returns the recorded session error, if any.
func (s *subState) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ioErr
}

func (s *subState) clearErr() {
	s.mu.Lock()
	s.ioErr = nil
	s.mu.Unlock()
}

// run executes f in session order: inline in compat mode, else enqueued on
// the worker and waited for. All blocking session reads go through here so
// they order after any pending async writes on the same session.
func (s *subState) run(f func()) {
	if s.w == nil {
		f()
		return
	}
	done := make(chan struct{})
	s.w.enqueue(func() {
		defer close(done)
		f()
	})
	<-done
}

// runAsync executes f in session order without waiting (fast mode); inline in
// compat mode.
func (s *subState) runAsync(f func()) {
	if s.w == nil {
		f()
		return
	}
	s.w.enqueue(f)
}

// ensureWorker lazily starts the per-submitter I/O worker (fast mode only).
func (s *subState) ensureWorker() {
	if s.w != nil {
		return
	}
	w := &subWorker{
		ops:  make(chan func(), 1024),
		done: make(chan struct{}),
	}
	go func() {
		defer close(w.done)
		for f := range w.ops {
			f()
		}
	}()
	s.w = w
}

// stopWorker drains and stops the worker; idempotent.
func (s *subState) stopWorker() {
	if s.w == nil {
		return
	}
	close(s.w.ops)
	<-s.w.done
	s.w = nil
}

// subWorker executes session operations strictly in enqueue order.
type subWorker struct {
	ops  chan func()
	done chan struct{}
}

func (w *subWorker) enqueue(f func()) { w.ops <- f }

// prefetchSpin opens (or re-opens) every listed submitter's NEGOTIATE round
// and fetches its first request batch. Compat mode does this serially in
// list order; fast mode shards by schedd address (preserving per-schedd
// order) across a pool bounded by PrefetchWorkers.
func (c *Cycle) prefetchSpin(ctx context.Context, st *runState, subs []*subState) {
	if c.cfg.CompatMode {
		for _, sub := range subs {
			c.beginAndFetch(ctx, st, sub)
		}
		return
	}

	var order []string
	byAddr := make(map[string][]*subState)
	for _, sub := range subs {
		sub.ensureWorker()
		if _, ok := byAddr[sub.scheddAddr]; !ok {
			order = append(order, sub.scheddAddr)
		}
		byAddr[sub.scheddAddr] = append(byAddr[sub.scheddAddr], sub)
	}

	workers := c.cfg.PrefetchWorkers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, addr := range order {
		list := byAddr[addr]
		wg.Add(1)
		go func(list []*subState) {
			defer wg.Done()
			for _, sub := range list {
				sem <- struct{}{}
				c.beginAndFetch(ctx, st, sub)
				<-sem
			}
		}(list)
	}
	wg.Wait()
}

// beginAndFetch opens one submitter's NEGOTIATE round (Begin + header) and
// fetches the first request batch, all in session order (so it queues after
// the previous round's END_NEGOTIATE on the same worker).
func (c *Cycle) beginAndFetch(ctx context.Context, st *runState, sub *subState) {
	if sub.sessOpen {
		return // already open (defensive; rounds are closed before re-prefetch)
	}
	hdr := c.headerFor(st, sub)

	var (
		sess      negotiator.ScheddSession
		reqs      []*negotiator.Request
		exhausted bool
	)
	sub.run(func() {
		// Every round is a fresh chance: a failed END on the previous round
		// must not poison this one (the protocol layer re-dials on Begin).
		sub.clearErr()
		s := c.sf.Session(sub.name, sub.scheddName, sub.scheddAddr, sub.ad)
		if err := s.Begin(ctx, hdr); err != nil {
			sub.setErr(err)
			_ = s.Close()
			return
		}
		sess = s
		r, err := s.FetchRequests(ctx, c.cfg.RequestListSize)
		if err != nil {
			sub.setErr(err)
			return
		}
		reqs = r
		exhausted = len(r) == 0
	})

	if sess != nil {
		sub.sess = sess
		sub.sessOpen = true
	}
	sub.queue = reqs
	sub.exhausted = exhausted
}

// headerFor builds the NEGOTIATE header for one submitter: Owner is the
// submitter ad's OriginalName when present (matchmaker.cpp:4066-4072).
func (c *Cycle) headerFor(st *runState, sub *subState) *negotiator.NegotiateHeader {
	owner := sub.name
	if orig, ok := sub.ad.EvaluateAttrString("OriginalName"); ok && orig != "" {
		owner = orig
	}
	return &negotiator.NegotiateHeader{
		Owner:            owner,
		AutoClusterAttrs: st.sigAttrs,
		SubmitterTag:     sub.tag,
		NegotiatorName:   c.cfg.NegotiatorName,
		JobConstraint:    c.cfg.JobConstraint,
	}
}

// sendMatch delivers PERMISSION_AND_AD: synchronously in compat mode
// (returning false on failure, the C++ matchmakingProtocol MM_ERROR path),
// asynchronously in fast mode (errors surface at the submitter's next
// decision point).
func (c *Cycle) sendMatch(ctx context.Context, sub *subState, mr *negotiator.MatchResult) bool {
	sess := sub.sess
	if sub.w == nil {
		if err := sess.SendMatch(ctx, mr); err != nil {
			sub.setErr(err)
			return false
		}
		return true
	}
	sub.w.enqueue(func() {
		if sub.err() != nil {
			return
		}
		if err := sess.SendMatch(ctx, mr); err != nil {
			sub.setErr(err)
		}
	})
	return true
}

// startdInformer is the SessionFactory capability the NEGOTIATOR_INFORM_STARTD
// path needs: send a MATCH_INFO to a startd. The concrete protocol.Factory
// implements it; type-asserting here keeps the notify optional without widening
// the negotiator.SessionFactory interface.
type startdInformer interface {
	InformStartd(ctx context.Context, startdAddr, claimID string) error
}

// informStartd best-effort notifies the matched slot's startd of the match
// (NEGOTIATOR_INFORM_STARTD, matchmaker.cpp:5412-5426). It is a side channel to
// the startd, not part of the schedd match list, so a failure never fails the
// match; the error is dropped (the C++ send is likewise best-effort). Runs on
// the submitter's ordered worker in fast mode (lifecycle-managed by stopWorker),
// synchronously in compat mode. No-op if the factory cannot inform startds, the
// claim is absent, or the offer carries no startd address.
func (c *Cycle) informStartd(ctx context.Context, sub *subState, offer *classad.ClassAd, claimID string) {
	if claimID == "" || claimID == "null" {
		return
	}
	informer, ok := c.sf.(startdInformer)
	if !ok {
		return
	}
	addr := startdAddrOf(offer)
	if addr == "" {
		return
	}
	notify := func() { _ = informer.InformStartd(ctx, addr, claimID) }
	if sub.w == nil {
		notify()
		return
	}
	sub.w.enqueue(notify)
}

// startdAddrOf returns the startd command address from a match ad, preferring
// StartdIpAddr (what matchmakingProtocol reads, matchmaker.cpp:5470) and falling
// back to MyAddress.
func startdAddrOf(offer *classad.ClassAd) string {
	if offer == nil {
		return ""
	}
	if v, ok := offer.EvaluateAttrString("StartdIpAddr"); ok && v != "" {
		return v
	}
	if v, ok := offer.EvaluateAttrString("MyAddress"); ok && v != "" {
		return v
	}
	return ""
}

// sendReject delivers REJECTED_WITH_REASON with the same sync/async split as
// sendMatch, preserving on-wire ordering with prior matches.
func (c *Cycle) sendReject(ctx context.Context, sub *subState, req *negotiator.Request, reason string) bool {
	sess := sub.sess
	if sub.w == nil {
		if err := sess.Reject(ctx, req, reason); err != nil {
			sub.setErr(err)
			return false
		}
		return true
	}
	sub.w.enqueue(func() {
		if sub.err() != nil {
			return
		}
		if err := sess.Reject(ctx, req, reason); err != nil {
			sub.setErr(err)
		}
	})
	return true
}

// endRound closes out the submitter's current NEGOTIATE round: END_NEGOTIATE
// (returning the socket to the warm cache) on the happy path, Close on the
// broken path. No-op when no round is open. The END travels in session order
// after any queued deliveries, under its own timeout so cycle teardown never
// hangs on a dead peer.
func (c *Cycle) endRound(sub *subState, broken bool) {
	if !sub.sessOpen {
		return
	}
	sess := sub.sess
	sub.sess = nil
	sub.sessOpen = false
	sub.queue = nil
	sub.exhausted = false
	sub.runAsync(func() {
		if broken || sub.err() != nil {
			_ = sess.Close()
			return
		}
		ectx, cancel := context.WithTimeout(context.Background(), endTimeout)
		defer cancel()
		if err := sess.End(ectx); err != nil {
			sub.setErr(err)
		}
	})
}
