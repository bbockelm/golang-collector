package matchmaker

import (
	"sync"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// rejCounters tallies the per-scan rejection reasons the reject-info selection
// needs, mirroring the C++ rej* counters (matchmaker.cpp). Concurrency/network
// counters are deferred (roadmap #3), so only submitter-limit and the two
// preemption counters are live.
type rejCounters struct {
	submitterLimit int // rejForSubmitterLimit
	preemptPolicy  int // rejPreemptForPolicy (PREEMPTION_REQUIREMENTS == False)
	preemptRank    int // rejPreemptForRank   (rankCondPrioPreempt false)
}

func (r *rejCounters) add(o rejCounters) {
	r.submitterLimit += o.submitterLimit
	r.preemptPolicy += o.preemptPolicy
	r.preemptRank += o.preemptRank
}

// parallelScan evaluates every candidate against reqAd, sharding the candidate
// slice across m.workers goroutines. Each worker keeps its own best candidate
// and local reject counters; the final reduce uses negotiator.Candidate.Better,
// which tie-breaks on ScanIndex -- so the sharded result is identical to a
// serial scan regardless of how the index space is split.
//
// Each worker gets its OWN clone of the request ad and its OWN reused
// MatchClassAd (via ReplaceRightAd per candidate, never a per-candidate
// allocation). Cloning the request per worker is required for correctness:
// MatchClassAd.ReplaceRightAd writes the request ad's TARGET pointer, so a
// shared request ad would be a data race under -race. Candidate (right) ads are
// disjoint across workers (each owns a contiguous index range), so mutating
// their TARGET pointers is safe.
func (m *Matchmaker) parallelScan(reqAd *classad.ClassAd, cands []candRef, limits *negotiator.MatchLimits) (*negotiator.Candidate, rejCounters) {
	n := len(cands)
	if n == 0 {
		return nil, rejCounters{}
	}

	nw := m.workers
	if nw > n {
		nw = n
	}
	if nw <= 1 {
		best, rej := m.scanRange(reqAd, cands, limits)
		return best, rej
	}

	type result struct {
		best *negotiator.Candidate
		rej  rejCounters
	}
	results := make([]result, nw)
	var wg sync.WaitGroup

	// Contiguous, deterministic partition of [0,n).
	base := n / nw
	extra := n % nw
	start := 0
	for w := 0; w < nw; w++ {
		size := base
		if w < extra {
			size++
		}
		lo, hi := start, start+size
		start = hi
		wg.Add(1)
		go func(w, lo, hi int) {
			defer wg.Done()
			results[w].best, results[w].rej = m.scanRange(reqAd, cands[lo:hi], limits)
		}(w, lo, hi)
	}
	wg.Wait()

	var best *negotiator.Candidate
	var rej rejCounters
	for w := 0; w < nw; w++ {
		rej.add(results[w].rej)
		if results[w].best == nil {
			continue
		}
		if best == nil || results[w].best.Better(best) {
			best = results[w].best
		}
	}
	return best, rej
}

// scanRange evaluates a contiguous slice of candidates with a single reused
// MatchClassAd, returning the local best and reject counters.
func (m *Matchmaker) scanRange(reqAd *classad.ClassAd, cands []candRef, limits *negotiator.MatchLimits) (*negotiator.Candidate, rejCounters) {
	if len(cands) == 0 {
		return nil, rejCounters{}
	}

	// Per-worker request clone + reused MatchClassAd (job = left, machine =
	// right). The clone shares the parsed expression trees (read-only during
	// evaluation) but owns its own TARGET pointer.
	job := cloneAd(reqAd)
	mc := classad.NewMatchClassAd(job, nil)

	var best *negotiator.Candidate
	var rej rejCounters

	for _, c := range cands {
		cand := m.evalCandidate(mc, c, limits, &rej)
		if cand == nil {
			continue
		}
		if best == nil || cand.Better(best) {
			best = cand
		}
	}
	return best, rej
}

// evalCandidate runs the full per-candidate pipeline for one offer, porting the
// C++ matchmakingAlgorithm inner body (matchmaker.cpp:4950-5078):
//  1. bilateral Requirements (job.Requirements with TARGET=machine AND
//     machine.Requirements with TARGET=job) via MatchClassAd.Symmetry;
//  2. preemption classification (when ConsiderPreemption): determine remoteUser
//     and set the preemption tier (NO/RANK/PRIO), gating PRIO on
//     PREEMPTION_REQUIREMENTS and rankCondPrioPreempt;
//  3. the tier-specific submitter-limit gate (SubmitterLimitPermits);
//  4. the rank tuple (calculateRanks), including PREEMPTION_RANK when preempting.
//
// Returns nil (and bumps the appropriate reject counter) when the candidate is
// filtered out. Requirements failures bump NO counter, matching the C++ `continue`
// (matchmaker.cpp:4951-4954).
func (m *Matchmaker) evalCandidate(mc *classad.MatchClassAd, c candRef, limits *negotiator.MatchLimits, rej *rejCounters) *negotiator.Candidate {
	mc.ReplaceRightAd(c.slot)

	// (1) bilateral Requirements. (The C++ pslotMultiMatch retry for
	// WantPslotPreemption jobs is deferred, roadmap #6; a Requirements failure
	// is simply "not a match".)
	if !mc.Symmetry("Requirements", "Requirements") {
		return nil
	}

	// (2) preemption classification + (3) submitter-limit gate.
	tier := noPreemption
	if m.considerPreemption {
		var ok bool
		if tier, ok = m.classifyPreemption(mc, c.slot, limits, rej); !ok {
			return nil
		}
	}

	cost := m.slotWeight(c.slot)
	if !m.submitterLimitGate(tier, limits, cost) {
		rej.submitterLimit++
		return nil
	}

	// (4) rank tuple (calculateRanks). PreemptRank is the NO_PREEMPTION sentinel
	// unless this is a preempting match (matchmaker.cpp:5231-5236).
	preemptRank := preemptRankDef
	if tier != noPreemption {
		preemptRank = rankRight(mc, m.preemptionRank)
	}
	return &negotiator.Candidate{
		Slot:        c.slot,
		PreJobRank:  rankRight(mc, m.preJobRank),
		Rank:        jobRank(mc),
		PostJobRank: rankRight(mc, m.postJobRank),
		PreemptTier: tier,
		PreemptRank: preemptRank,
		ScanIndex:   c.idx,
	}
}

// classifyPreemption ports the remoteUser lookup + preemption-tier decision
// (matchmaker.cpp:4961-5064). It returns the tier and ok=false when the
// candidate must be skipped (bumping the relevant reject counter). Only called
// when ConsiderPreemption is on. mc's right ad is already the candidate.
func (m *Matchmaker) classifyPreemption(mc *classad.MatchClassAd, slot *classad.ClassAd, limits *negotiator.MatchLimits, rej *rejCounters) (tier int, ok bool) {
	// remoteUser: preempting user if any, else the running user. Lookup order
	// matches matchmaker.cpp:4963-4968 exactly.
	remoteUser := lookupRemoteUser(slot)

	// only_for_startdrank branch (matchmaker.cpp:4977-5006): only claimed slots
	// this offer strictly prefers become RANK_PREEMPTION candidates; everything
	// else is skipped (no reject counter, matching the C++ `continue`).
	if limits.OnlyForStartdRank {
		if remoteUser == "" {
			return 0, false // unclaimed: cannot eval startd rank
		}
		if !evalBoolRight(mc, m.rankCondStd) {
			return 0, false // offer does not strictly prefer this request
		}
		return rankPreemption, true
	}

	// Normal branch (matchmaker.cpp:5010-5064): classify a claimed slot.
	if remoteUser != "" {
		switch {
		case evalBoolRight(mc, m.rankCondStd):
			// Offer strictly prefers this request: preempt for rank.
			return rankPreemption, true
		case remoteUser != limits.SubmitterName:
			// Different user (or same user, different group): prio preemption,
			// gated by PREEMPTION_REQUIREMENTS then rankCondPrioPreempt.
			if m.preemptionReq != nil && !evalBoolRight(mc, m.preemptionReq) {
				rej.preemptPolicy++
				return 0, false
			}
			if !evalBoolRight(mc, m.rankCondPrioPreempt) {
				rej.preemptRank++
				return 0, false
			}
			return prioPreemption, true
		default:
			// Same user and not startd-rank-preferred: skip.
			return 0, false
		}
	}

	// Unclaimed slot: a plain (no-preemption) match.
	return noPreemption, true
}

// submitterLimitGate applies the tier-specific submitter-limit check
// (matchmaker.cpp:5066-5070). With preemption off, every candidate is
// NO_PREEMPTION and gates on SubmitterLimit/LimitUsed (byte-identical to the
// MVP). With preemption on, NO_PREEMPTION gates on the Unclaimed variant, PRIO
// on the full limit, and RANK bypasses the limit (startd-rank preemptions are
// allowed regardless of user priority).
func (m *Matchmaker) submitterLimitGate(tier int, limits *negotiator.MatchLimits, cost float64) bool {
	if !m.considerPreemption {
		return submitterLimitPermits(limits.LimitUsed, limits.SubmitterLimit, cost)
	}
	switch tier {
	case rankPreemption:
		return true
	case prioPreemption:
		return submitterLimitPermits(limits.LimitUsed, limits.SubmitterLimit, cost)
	default: // noPreemption
		return submitterLimitPermits(limits.LimitUsedUnclaimed, limits.SubmitterLimitUnclaimed, cost)
	}
}

// lookupRemoteUser returns the accounting principal to preempt on a slot,
// using the C++ precedence: PreemptingAccountingGroup, PreemptingUser,
// AccountingGroup, RemoteUser (matchmaker.cpp:4963-4968). "" means the slot is
// unclaimed.
func lookupRemoteUser(slot *classad.ClassAd) string {
	for _, attr := range []string{
		"PreemptingAccountingGroup",
		"PreemptingUser",
		"AccountingGroup",
		"RemoteUser",
	} {
		if v, ok := slot.EvaluateAttrString(attr); ok && v != "" {
			return v
		}
	}
	return ""
}

// cloneAd makes a shallow copy of src: a fresh ClassAd carrying the same
// attribute expression trees. The trees are read-only during evaluation, so
// sharing them across the clone is safe; what matters is that each clone owns
// its own TARGET pointer for concurrent matchmaking.
func cloneAd(src *classad.ClassAd) *classad.ClassAd {
	dst := classad.New()
	for _, name := range src.GetAttributes() {
		if e, ok := src.Lookup(name); ok {
			_ = dst.Set(name, e)
		}
	}
	return dst
}
