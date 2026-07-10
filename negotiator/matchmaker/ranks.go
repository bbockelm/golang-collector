package matchmaker

import (
	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
)

// Rank-expression evaluation, mirroring Matchmaker::calculateRanks
// (matchmaker.cpp:5192-5246) and EvalNegotiatorMatchRank (:4532-4551).
//
// Evaluation context (verified against the C++):
//
//   - NEGOTIATOR_PRE_JOB_RANK / NEGOTIATOR_POST_JOB_RANK:
//     EvalNegotiatorMatchRank calls EvalExprToNumber(expr, resource, &request)
//     (matchmaker.cpp:4538), i.e. MY = the machine/candidate (resource) and
//     TARGET = the job (request). In MatchClassAd terms the machine is the RIGHT
//     ad, so we evaluate with EvaluateExprRight: MY = right (machine),
//     TARGET = right.target = left (job). On failure the value is -DBL_MAX so a
//     non-evaluating candidate sorts last on that key.
//
//   - job Rank: EvalFloat(ATTR_RANK, &request, candidate) (matchmaker.cpp:5222),
//     i.e. MY = the job (request), TARGET = the machine/candidate. The job is
//     the LEFT ad, so EvaluateRankLeft evaluates the left ad's "Rank" attribute
//     with TARGET = left.target = right (machine). On failure it is 0.0.

// rankRight evaluates a parsed NEGOTIATOR_*_JOB_RANK expression in the machine
// (right) context with the job as TARGET. A nil expression (unset config) or a
// non-numeric result yields rankUnset (-DBL_MAX), exactly as
// EvalNegotiatorMatchRank returns -(DBL_MAX) for a missing/failed expression.
func rankRight(mc *classad.MatchClassAd, expr ast.Expr) float64 {
	if expr == nil {
		return rankUnset
	}
	if f, ok := numberOf(mc.EvaluateExprRight(expr)); ok {
		return f
	}
	return rankUnset
}

// jobRank evaluates the job ad's Rank attribute against the machine (MY = job,
// TARGET = machine). Absent or non-numeric -> 0.0 (matchmaker.cpp:5222-5224).
func jobRank(mc *classad.MatchClassAd) float64 {
	if v, ok := mc.EvaluateRankLeft(); ok {
		return v
	}
	return 0.0
}
