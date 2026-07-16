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

// Preemption tier constants, matching the C++ PreemptState enum where
// NO_PREEMPTION=2 beats RANK_PREEMPTION=1 beats PRIO_PREEMPTION=0 (more is
// better in Candidate.Better). With preemption off every candidate is
// noPreemption.
const (
	prioPreemption = 0
	rankPreemption = 1
	noPreemption   = 2
)

// Rank defaults, mirroring the C++ negotiator:
//   - EvalNegotiatorMatchRank returns -(DBL_MAX) when the expr is absent or does
//     not evaluate to a number (matchmaker.cpp:4536).
//   - job Rank defaults to 0.0 when EvalFloat fails (matchmaker.cpp:5222-5224).
//   - PreemptRank is -(FLT_MAX) for NO_PREEMPTION (matchmaker.cpp:5231).
const (
	rankUnset      = -math.MaxFloat64
	preemptRankDef = -math.MaxFloat32
)

// Reject reason strings, verbatim from the C++ reject-reason ladder
// (matchmaker.cpp:4336-4361).
const (
	reasonNoMatch          = "no match found"
	reasonSubmitterLimit   = "submitter limit exceeded"
	reasonPreemptionPolicy = "PREEMPTION_REQUIREMENTS == False"
	// reasonConcurrencyLimitMatch is the per-match concurrency reject reason. It
	// is generic (no limit name) because per-candidate expressions may hit
	// different limits; a name from one candidate would not be deterministic.
	reasonConcurrencyLimitMatch = "concurrency limit reached for all candidate matches"
)

// reasonConcurrencyLimit builds the verbatim C++ concurrency-limit reject
// message: "concurrency limit " + <joined rejected limit names> + " reached"
// (matchmaker.cpp:4351-4352, diagnostic_message). The short D_MATCH diag_reason
// is "concurrency limit reached", but the reason string sent on the wire is the
// message form, which names the offending limit(s).
func reasonConcurrencyLimit(names string) string {
	return "concurrency limit " + names + " reached"
}

// rankCondStd / rankCondPrioPreempt are the fixed rank-condition expressions the
// C++ negotiator parses once in its ctor (matchmaker.cpp:419-423). Both are
// evaluated in the machine (MY=candidate) context with TARGET=request, so
// MY.Rank is the machine's Rank expression scored against this request and
// MY.CurrentRank is the machine's rank of the job it is currently running.
//
//	rankCondStd         = MY.Rank >  MY.CurrentRank   (strictly prefers)
//	rankCondPrioPreempt = MY.Rank >= MY.CurrentRank   (at least as good)
const (
	rankCondStdExpr         = "MY.Rank > MY.CurrentRank"
	rankCondPrioPreemptExpr = "MY.Rank >= MY.CurrentRank"
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

	// ConsiderPreemption mirrors NEGOTIATOR_CONSIDER_PREEMPTION. When false the
	// matchmaker pins every candidate to NO_PREEMPTION and gates on the single
	// SubmitterLimit (the preemption-off path, byte-identical to the MVP). When
	// true, claimed candidates are classified into RANK/PRIO preemption tiers.
	ConsiderPreemption bool
	// PreemptionRequirements / PreemptionRank are the PREEMPTION_REQUIREMENTS /
	// PREEMPTION_RANK expressions, parsed once here (like Pre/PostJobRank). Both
	// evaluate with MY=candidate (machine), TARGET=request (job) --
	// EvalExprToBool(PreemptionReq, candidate, &request) / EvalNegotiatorMatchRank
	// (matchmaker.cpp:5030-5031, :5234-5236, :1633-1634). Cross-ad references
	// must be qualified TARGET.<attr> (the Go classad evaluator does not fall
	// unqualified references through to TARGET, matching how job Requirements/
	// Rank already qualify their references in this port).
	PreemptionRequirements string
	PreemptionRank         string
}

// Matchmaker implements negotiator.Matchmaker. It is safe for sequential Match
// calls; a single Match call is internally concurrent but self-contained.
type Matchmaker struct {
	preJobRank  ast.Expr // parsed NEGOTIATOR_PRE_JOB_RANK, nil if unset
	postJobRank ast.Expr // parsed NEGOTIATOR_POST_JOB_RANK, nil if unset

	considerPreemption  bool
	preemptionReq       ast.Expr // parsed PREEMPTION_REQUIREMENTS, nil if unset (=> always allow)
	preemptionRank      ast.Expr // parsed PREEMPTION_RANK, nil if unset
	rankCondStd         ast.Expr // MY.Rank > MY.CurrentRank
	rankCondPrioPreempt ast.Expr // MY.Rank >= MY.CurrentRank

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
		serial:             cfg.Serial,
		workers:            cfg.Workers,
		useSlotWeights:     !cfg.DisableSlotWeights,
		considerPreemption: cfg.ConsiderPreemption,
	}

	var err error
	if m.preJobRank, err = parseRankExpr("NEGOTIATOR_PRE_JOB_RANK", cfg.PreJobRank); err != nil {
		return nil, err
	}
	if m.postJobRank, err = parseRankExpr("NEGOTIATOR_POST_JOB_RANK", cfg.PostJobRank); err != nil {
		return nil, err
	}
	if m.preemptionReq, err = parseRankExpr("PREEMPTION_REQUIREMENTS", cfg.PreemptionRequirements); err != nil {
		return nil, err
	}
	if m.preemptionRank, err = parseRankExpr("PREEMPTION_RANK", cfg.PreemptionRank); err != nil {
		return nil, err
	}
	// The rank-condition expressions are fixed; parse errors are impossible but
	// checked for defensiveness.
	if m.rankCondStd, err = parser.ParseExpr(rankCondStdExpr); err != nil {
		return nil, fmt.Errorf("parsing rankCondStd: %w", err)
	}
	if m.rankCondPrioPreempt, err = parser.ParseExpr(rankCondPrioPreemptExpr); err != nil {
		return nil, fmt.Errorf("parsing rankCondPrioPreempt: %w", err)
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
//   - Preemption: when Config.ConsiderPreemption is false every candidate is
//     NO_PREEMPTION and only the SubmitterLimit/LimitUsed gate applies (the
//     unclaimed variant). When true, claimed candidates are classified into
//     RANK/PRIO preemption tiers (evalCandidate, shard.go) and the tier selects
//     which submitter-limit accumulator gates them (matchmaker.cpp:5066-5070):
//     PRIO uses SubmitterLimit/LimitUsed, NO_PREEMPTION uses the Unclaimed
//     variant, and RANK bypasses the submitter limit entirely.
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

	// Concurrency-limit gate. Two paths, mirroring the C++ evaluate_limits_with_match
	// selection (matchmaker.cpp:4730-4737):
	//   - Literal / job-only ConcurrencyLimits (evaluates to a string against the
	//     job alone): reject the whole request UP FRONT if it would push a named
	//     limit over its max -- one cheap check, keeping the sharded scan a pure
	//     function of its inputs.
	//   - A match-referencing expression (does not evaluate to a string without a
	//     TARGET, e.g. a per-CPU license "license:<TARGET.Cpus>"): defer to a
	//     PER-CANDIDATE check that evaluates ConcurrencyLimits against each match
	//     (evalCandidate), so the increment can depend on the matched slot.
	matchConc := false
	if limits.Concurrency != nil {
		if cl, ok := req.Ad.EvaluateAttrString("ConcurrencyLimits"); ok && cl != "" {
			if rejected, names := rejectForConcurrencyLimits(cl, limits.Concurrency); rejected {
				return nil, &negotiator.RejectInfo{
					ForConcurrencyLim: 1,
					Reason:            reasonConcurrencyLimit(names),
				}, nil
			}
		} else if !ok {
			if _, present := req.Ad.Lookup("ConcurrencyLimits"); present {
				matchConc = true
			}
		}
	}

	cands := gather(view)

	best, rej := m.parallelScan(req.Ad, cands, limits, matchConc)
	if best != nil {
		return best, nil, nil
	}

	// No candidate. Reproduce the C++ dominant-reason ladder
	// (matchmaker.cpp:4336-4361), in priority order. Only the counters this
	// matchmaker tracks are represented: network/bandwidth (never set here) is
	// omitted, so the reachable ladder is
	//   concurrency limit reached (per-match)  (rejForConcurrencyLimit)
	//   PREEMPTION_REQUIREMENTS == False        (rejPreemptForPolicy)
	//   submitter limit exceeded                (rejForSubmitterLimit)
	//   no match found                          (fallthrough)
	// Note rejPreemptForRank is tracked for diagnostics but, as in C++, is NOT a
	// distinct reason: on its own the scan falls through to "no match found". The
	// per-match concurrency reason is generic (no single limit name): different
	// candidates may hit different limits, and a name chosen from one would not be
	// deterministic across the sharded scan.
	ri := &negotiator.RejectInfo{
		ForSubmitterLimit:   rej.submitterLimit,
		ForPreemptionPolicy: rej.preemptPolicy,
		ForPreemptionRank:   rej.preemptRank,
		ForConcurrencyLim:   rej.concurrency,
	}
	switch {
	case rej.concurrency > 0:
		ri.Reason = reasonConcurrencyLimitMatch
	case rej.preemptPolicy > 0:
		ri.Reason = reasonPreemptionPolicy
	case rej.submitterLimit > 0:
		ri.Reason = reasonSubmitterLimit
	default:
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
