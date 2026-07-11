package matchmaker

import (
	"strconv"
	"strings"

	"github.com/bbockelm/golang-collector/negotiator"
)

// Concurrency-limit gate (roadmap #3). Ports the common C++ path where a job's
// ConcurrencyLimits is a plain string literal: matchmakingAlgorithm evaluates it
// ONCE, before the candidate scan (evaluate_limits_with_match==false,
// matchmaker.cpp:4730-4737), and rejects the whole request if consuming any
// named limit would exceed its configured max (rejectForConcurrencyLimits,
// matchmaker.cpp:4585-4644). Doing the check up front on the single Match spine
// (not per candidate) keeps the sharded scan a pure function of its inputs, so
// compat==fast determinism is untouched.
//
// The rarer path -- ConcurrencyLimits as an expression evaluated per candidate
// (evaluate_limits_with_match==true) -- is not ported: the negotiator's own
// enrichment always stamps a literal string, and every candidate would evaluate
// the same literal identically anyway.
//
// The token parser is a compact re-implementation of the accountant's
// ParseConcurrencyLimit (kept local so this package need not import accountant).

// rejectForConcurrencyLimits reports whether the request's ConcurrencyLimits
// list (a comma/space separated set of "name" or "name:weight" tokens) would
// push any named limit over its max given the live usage view. On rejection it
// returns the joined names that triggered it, for the reject reason. Mirrors the
// C++ loop at matchmaker.cpp:4595-4643 (including the count<0 error rejection).
func rejectForConcurrencyLimits(limits string, v negotiator.ConcurrencyLimits) (bool, string) {
	limits = strings.ToLower(limits)
	for _, tok := range splitLimits(limits) {
		name, inc, ok := parseLimitToken(tok)
		if !ok {
			continue
		}
		count := v.Usage(name)
		max := v.Max(name)
		if count < 0 {
			return true, name
		}
		if count+inc > max {
			return true, name
		}
	}
	return false, ""
}

// parseLimitToken splits "<name>[:<weight>]" and validates the name, matching
// accountant.ParseConcurrencyLimit (see there for the dotted-name rules). The
// returned name is lowercased.
func parseLimitToken(token string) (name string, increment float64, ok bool) {
	increment = 1
	s := strings.TrimSpace(token)
	if i := strings.IndexByte(s, ':'); i >= 0 {
		if f, err := strconv.ParseFloat(strings.TrimSpace(s[i+1:]), 64); err == nil {
			increment = f
		}
		s = s[:i]
	}
	if increment <= 0 {
		increment = 1
	}
	name = strings.ToLower(strings.TrimSpace(s))
	if !validLimitName(name) {
		return "", 0, false
	}
	return name, increment, true
}

func validLimitName(name string) bool {
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		return isValidAttrName(name[:dot]) && isValidAttrName(name[dot+1:])
	}
	return isValidAttrName(name)
}

func isValidAttrName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		alpha := c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if i == 0 {
			if !alpha {
				return false
			}
			continue
		}
		if !alpha && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func splitLimits(limits string) []string {
	return strings.FieldsFunc(limits, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
}
