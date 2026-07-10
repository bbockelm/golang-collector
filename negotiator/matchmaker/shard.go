package matchmaker

import (
	"sync"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// rejCounters tallies the per-scan rejection reasons the reject-info selection
// needs. Preemption/concurrency/network counters are deferred (design doc 4.3),
// so only the submitter-limit counter is live.
type rejCounters struct {
	submitterLimit int
}

func (r *rejCounters) add(o rejCounters) {
	r.submitterLimit += o.submitterLimit
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

// evalCandidate runs the full per-candidate pipeline for one offer:
//  1. bilateral Requirements (job.Requirements with TARGET=machine AND
//     machine.Requirements with TARGET=job) via MatchClassAd.Symmetry;
//  2. the submitter-limit gate (SubmitterLimitPermits);
//  3. the rank tuple (calculateRanks).
//
// Returns nil (and bumps the appropriate reject counter) when the candidate is
// filtered out. Requirements failures bump NO counter, matching the C++ `continue`
// (matchmaker.cpp:4951-4954).
func (m *Matchmaker) evalCandidate(mc *classad.MatchClassAd, c candRef, limits *negotiator.MatchLimits, rej *rejCounters) *negotiator.Candidate {
	mc.ReplaceRightAd(c.slot)

	// (1) bilateral Requirements.
	if !mc.Symmetry("Requirements", "Requirements") {
		return nil
	}

	// (2) submitter-limit gate (unclaimed variant; preemption deferred).
	cost := m.slotWeight(c.slot)
	if !submitterLimitPermits(limits.LimitUsed, limits.SubmitterLimit, cost) {
		rej.submitterLimit++
		return nil
	}

	// (3) rank tuple (calculateRanks).
	return &negotiator.Candidate{
		Slot:        c.slot,
		PreJobRank:  rankRight(mc, m.preJobRank),
		Rank:        jobRank(mc),
		PostJobRank: rankRight(mc, m.postJobRank),
		PreemptTier: noPreemption,
		PreemptRank: preemptRankDef,
		ScanIndex:   c.idx,
	}
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
