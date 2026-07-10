// Package matchmaker implements the negotiator's per-request matchmaking: for
// one resource request it scans the cycle's live slot view, applies the C++
// negotiator's bilateral-Requirements test and submitter-limit gate, computes
// the rank tuple, and returns the single best candidate (or a rejection).
//
// It is Phase 2 of the design in docs/NEGOTIATOR_DESIGN.md (sections 4.3, 4.4).
// The scan is sharded across workers but the winner is byte-identical to a
// serial scan: negotiator.Candidate.Better tie-breaks on ScanIndex, so the
// deterministic reduce is order-independent.
//
// C++ reference: src/condor_negotiator.V6/matchmaker.cpp
//   - matchmakingAlgorithm      :4692-5182 (the candidate scan + lexicographic best)
//   - calculateRanks            :5192-5246 (PreJobRank / Rank / PostJobRank)
//   - EvalNegotiatorMatchRank    :4532-4551 (rank-expr eval context)
//   - SubmitterLimitPermits      :4554-4583 (the submitter-limit gate)
//   - reject reason strings      :4324-4361
package matchmaker

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"strings"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/parser"

	"github.com/bbockelm/golang-collector/negotiator"
)

// Preemption tier constant. Preemption is deferred (design doc 4.3), so every
// match is NO_PREEMPTION; the value 2 matches the C++ PreemptState enum where
// NO_PREEMPTION=2 beats RANK_PREEMPTION=1 beats PRIO_PREEMPTION=0.
const noPreemption = 2

// Rank defaults, mirroring the C++ negotiator:
//   - EvalNegotiatorMatchRank returns -(DBL_MAX) when the expr is absent or does
//     not evaluate to a number (matchmaker.cpp:4536).
//   - job Rank defaults to 0.0 when EvalFloat fails (matchmaker.cpp:5222-5224).
//   - PreemptRank is -(FLT_MAX) for NO_PREEMPTION (matchmaker.cpp:5231).
const (
	rankUnset      = -math.MaxFloat64
	preemptRankDef = -math.MaxFloat32
)

// Reject reason strings, verbatim from matchmaker.cpp:4324 / :4360.
const (
	reasonNoMatch        = "no match found"
	reasonSubmitterLimit = "submitter limit exceeded"
)

// Config configures a Matchmaker. PreJobRank/PostJobRank are the
// NEGOTIATOR_PRE_JOB_RANK / NEGOTIATOR_POST_JOB_RANK expressions, parsed once
// here. Serial forces a single worker (compat mode); Workers overrides the
// default worker count (GOMAXPROCS).
//
// Slot weights are ON by default (the NEGOTIATOR_USE_SLOT_WEIGHTS default is
// true); set DisableSlotWeights to force every match cost to 1.0. The inverted
// field keeps the zero-value Config matching the C++ default.
type Config struct {
	PreJobRank         string
	PostJobRank        string
	Serial             bool
	Workers            int
	DisableSlotWeights bool
}

// Matchmaker implements negotiator.Matchmaker. It is safe for sequential Match
// calls; a single Match call is internally concurrent but self-contained.
type Matchmaker struct {
	preJobRank  ast.Expr // parsed NEGOTIATOR_PRE_JOB_RANK, nil if unset
	postJobRank ast.Expr // parsed NEGOTIATOR_POST_JOB_RANK, nil if unset

	serial         bool
	workers        int
	useSlotWeights bool
}

var _ negotiator.Matchmaker = (*Matchmaker)(nil)

// New builds a Matchmaker, parsing the rank expressions once. Slot weights are
// enabled unless cfg.DisableSlotWeights is set (C++ NEGOTIATOR_USE_SLOT_WEIGHTS
// default is true).
func New(cfg Config) (*Matchmaker, error) {
	m := &Matchmaker{
		serial:         cfg.Serial,
		workers:        cfg.Workers,
		useSlotWeights: !cfg.DisableSlotWeights,
	}

	var err error
	if m.preJobRank, err = parseRankExpr("NEGOTIATOR_PRE_JOB_RANK", cfg.PreJobRank); err != nil {
		return nil, err
	}
	if m.postJobRank, err = parseRankExpr("NEGOTIATOR_POST_JOB_RANK", cfg.PostJobRank); err != nil {
		return nil, err
	}

	if m.serial {
		m.workers = 1
	} else if m.workers <= 0 {
		m.workers = runtime.GOMAXPROCS(0)
	}
	if m.workers < 1 {
		m.workers = 1
	}
	return m, nil
}

// parseRankExpr parses one rank expression string; empty -> nil (unset).
func parseRankExpr(name, s string) (ast.Expr, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	expr, err := parser.ParseExpr(s)
	if err != nil {
		return nil, fmt.Errorf("parsing %s %q: %w", name, s, err)
	}
	return expr, nil
}

// Match scans the live candidates in view for the single best match to req,
// honoring the C++ ranking order (design doc 4.3). Returns (candidate, nil, nil)
// on a match, or (nil, rejectInfo, nil) when nothing matched. Deterministic:
// identical inputs yield the identical winner regardless of worker count.
//
// Gate decisions (documented for the caller / cycle orchestrator):
//   - Submitter-limit gate: SubmitterLimitPermits (matchmaker.cpp:4554-4583).
//     Permit iff (LimitUsed + cost) <= SubmitterLimit, OR the round-up rule
//     (LimitUsed <= 0 && SubmitterLimit > 0). The C++ signature takes pieLeft
//     but the parameter is commented out (`double /*pieLeft*/`) and never used;
//     pieLeft interacts only by capping SubmitterLimit in the OUTER pie-spin
//     loop (matchmaker.cpp:2665-2666) before Match is called. So MatchLimits.
//     SubmitterLimit is expected to already be the pieLeft-capped value and we
//     do NOT re-check PieLeft here.
//   - Ceiling: NOT gated per-candidate. In C++ the ceiling is enforced in the
//     outer negotiate loop (`limitUsed >= submitterCeiling` -> stop, matchmaker.
//     cpp:4216, headroom computed at :2669-2680), never inside
//     matchmakingAlgorithm. MatchLimits.Ceiling is therefore ignored by Match;
//     the cycle orchestrator stops offering to a submitter once its ceiling is
//     hit.
//   - Preemption: deferred; every candidate is NO_PREEMPTION so we only ever
//     apply the "unclaimed" submitter-limit variant (design doc 4.3).
func (m *Matchmaker) Match(ctx context.Context, req *negotiator.Request, view negotiator.SlotView, limits *negotiator.MatchLimits) (*negotiator.Candidate, *negotiator.RejectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if req == nil || req.Ad == nil {
		return nil, nil, fmt.Errorf("matchmaker: nil request ad")
	}
	if limits == nil {
		return nil, nil, fmt.Errorf("matchmaker: nil match limits")
	}

	cands := gather(view)

	best, rej := m.parallelScan(req.Ad, cands, limits)
	if best != nil {
		return best, nil, nil
	}

	// No candidate. Reproduce the C++ dominant-reason selection
	// (matchmaker.cpp:4324-4361): "no match found" unless a submitter-limit
	// rejection occurred, in which case "submitter limit exceeded". Requirements
	// failures increment no counter (they just `continue`), so an all-fail scan
	// reports "no match found".
	ri := &negotiator.RejectInfo{}
	if rej.submitterLimit > 0 {
		ri.Reason = reasonSubmitterLimit
		ri.ForSubmitterLimit = rej.submitterLimit
	} else {
		ri.Reason = reasonNoMatch
	}
	return nil, ri, nil
}

// candRef is one live candidate in canonical scan order.
type candRef struct {
	idx  int
	slot *classad.ClassAd
}

// gather collects the live candidates in the view's canonical order. This is a
// cheap serial pass; the expensive per-candidate evaluation is what the scan
// parallelizes.
func gather(view negotiator.SlotView) []candRef {
	cands := make([]candRef, 0, view.Len())
	view.Scan(func(i int, slot *classad.ClassAd) bool {
		cands = append(cands, candRef{idx: i, slot: slot})
		return true
	})
	return cands
}

// slotWeight computes the accounting match cost of a candidate, mirroring
// Accountant::GetSlotWeight (Accountant.cpp:2082): with slot weights disabled
// the cost is 1.0; otherwise evaluate the SlotWeight expression, and if it is
// missing / non-numeric / negative fall back (the AdSource fixup defaults the
// SlotWeight expression to Cpus, design doc 4.1, so we try Cpus before the
// final 1.0 fallback for ads that were not fixed up).
func (m *Matchmaker) slotWeight(slot *classad.ClassAd) float64 {
	if !m.useSlotWeights {
		return 1.0
	}
	if f, ok := numberOf(slot.EvaluateAttr("SlotWeight")); ok && f >= 0 {
		return f
	}
	if f, ok := numberOf(slot.EvaluateAttr("Cpus")); ok && f >= 0 {
		return f
	}
	return 1.0
}

// submitterLimitPermits is Matchmaker::SubmitterLimitPermits
// (matchmaker.cpp:4554-4583). pieLeft is intentionally absent (see the C++
// signature: the parameter is commented out and unused).
func submitterLimitPermits(used, allowed, cost float64) bool {
	if used+cost <= allowed {
		return true
	}
	// Round-up rule: a submitter with any share is allowed one (weighted) slot
	// so crumbs are not left behind between too many users.
	if used <= 0 && allowed > 0 {
		return true
	}
	return false
}

// numberOf coerces a Value to float64, treating integers and reals uniformly
// (the C++ EvalFloat / IsNumber path).
func numberOf(v classad.Value) (float64, bool) {
	if v.IsReal() {
		r, err := v.RealValue()
		return r, err == nil
	}
	if v.IsInteger() {
		i, err := v.IntValue()
		return float64(i), err == nil
	}
	return 0, false
}
