package cycle

import (
	"strconv"
	"strings"
	"time"

	"github.com/bbockelm/golang-collector/negotiator/accountant"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
)

// negotiatorMatchExprPrefix is the C++ ATTR_NEGOTIATOR_MATCH_EXPR
// (condor_attributes.h:1127): the attribute-name prefix the schedd expects.
const negotiatorMatchExprPrefix = "NegotiatorMatchExpr"

// ConfigFromKnobs builds the cycle configuration from HTCondor knobs (design
// doc section 9, cycle set): NEGOTIATOR_RESOURCE_REQUEST_LIST_SIZE, the
// NEGOTIATOR_MAX_TIME_PER_* budgets, NEGOTIATOR_GO_COMPAT_MODE,
// NEGOTIATOR_PRE_JOB_RANK / NEGOTIATOR_POST_JOB_RANK (passed through raw; no
// baked-in C++ param-table default expression), NEGOTIATOR_USE_SLOT_WEIGHTS,
// NEGOTIATOR_NAME, NEGOTIATOR_JOB_CONSTRAINT, NEGOTIATOR_ADVERTISE_ACCOUNTING,
// plus the full group set via accountant.GroupConfigFromKnobs. Unset knobs
// keep the HTCondor defaults. The interval/guard timers are the daemon
// object's knobs, not the cycle's (see negotiator.Config).
func ConfigFromKnobs(get accountant.KnobGetter) Config {
	cfg := DefaultConfig()
	if v, ok := knobInt(get, "NEGOTIATOR_RESOURCE_REQUEST_LIST_SIZE"); ok && v > 0 {
		cfg.RequestListSize = v
	}
	if d, ok := knobSeconds(get, "NEGOTIATOR_MAX_TIME_PER_CYCLE"); ok {
		cfg.MaxTimePerCycle = d
	}
	if d, ok := knobSeconds(get, "NEGOTIATOR_MAX_TIME_PER_SUBMITTER"); ok {
		cfg.MaxTimePerSubmitter = d
	}
	if d, ok := knobSeconds(get, "NEGOTIATOR_MAX_TIME_PER_PIESPIN"); ok {
		cfg.MaxTimePerSpin = d
	}
	cfg.CompatMode = knobBool(get, "NEGOTIATOR_GO_COMPAT_MODE", false)
	if v, ok := get("NEGOTIATOR_PRE_JOB_RANK"); ok {
		cfg.PreJobRank = strings.TrimSpace(v)
	}
	if v, ok := get("NEGOTIATOR_POST_JOB_RANK"); ok {
		cfg.PostJobRank = strings.TrimSpace(v)
	}
	cfg.DisableSlotWeights = !knobBool(get, "NEGOTIATOR_USE_SLOT_WEIGHTS", true)
	cfg.ConsiderPreemption = knobBool(get, "NEGOTIATOR_CONSIDER_PREEMPTION", true)
	if v, ok := get("PREEMPTION_REQUIREMENTS"); ok {
		cfg.PreemptionRequirements = strings.TrimSpace(v)
	}
	if v, ok := get("PREEMPTION_RANK"); ok {
		cfg.PreemptionRank = strings.TrimSpace(v)
	}
	if v, ok := get("NEGOTIATOR_NAME"); ok {
		cfg.NegotiatorName = strings.TrimSpace(v)
	}
	if v, ok := get("NEGOTIATOR_JOB_CONSTRAINT"); ok {
		cfg.JobConstraint = strings.TrimSpace(v)
	}
	cfg.MatchExprs = matchExprsFromKnobs(get)
	cfg.InformStartd = knobBool(get, "NEGOTIATOR_INFORM_STARTD", false)
	cfg.DisableAccountingAds = !knobBool(get, "NEGOTIATOR_ADVERTISE_ACCOUNTING", true)
	cfg.Group = accountant.GroupConfigFromKnobs(get)
	// Concurrency-limit max resolver (roadmap #3): the <NAME>_LIMIT /
	// CONCURRENCY_LIMIT_DEFAULT[_<PREFIX>] config, resolved live per limit name
	// over the same KnobGetter (the accountant holds no concurrency config).
	cfg.ConcurrencyLimitMax = func(name string) float64 {
		return accountant.GetLimitMax(get, name)
	}
	return cfg
}

// matchExprsFromKnobs parses NEGOTIATOR_MATCH_EXPRS (matchmaker.cpp:728-746):
// a whitespace/comma-separated list of names. Each name is resolved to its
// expression the way the C++ does -- param(name) reads the macro's value -- and
// renamed with the NegotiatorMatchExpr prefix the schedd expects. As a
// convenience the Go form also accepts an inline "name=expr" entry (no separate
// macro needed). A name that resolves to no value is skipped (the C++ logs a
// warning and continues). Order is preserved so the injection is deterministic.
func matchExprsFromKnobs(get accountant.KnobGetter) []protocol.MatchExpr {
	raw, ok := get("NEGOTIATOR_MATCH_EXPRS")
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []protocol.MatchExpr
	for _, tok := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	}) {
		name, expr := tok, ""
		if i := strings.IndexByte(tok, '='); i >= 0 {
			name = strings.TrimSpace(tok[:i])
			expr = strings.TrimSpace(tok[i+1:])
		} else if v, ok := get(tok); ok {
			expr = strings.TrimSpace(v)
		}
		if name == "" || expr == "" {
			continue // undefined macro: warn-and-skip, as the C++ does
		}
		if !strings.HasPrefix(name, negotiatorMatchExprPrefix) {
			name = negotiatorMatchExprPrefix + name
		}
		out = append(out, protocol.MatchExpr{Name: name, Expr: expr})
	}
	return out
}

// knobSeconds reads an integer-seconds knob as a duration.
func knobSeconds(get accountant.KnobGetter, key string) (time.Duration, bool) {
	v, ok := knobInt(get, key)
	if !ok || v <= 0 {
		return 0, false
	}
	return time.Duration(v) * time.Second, true
}

// knobInt / knobBool mirror the accountant package's parsers (kept local so
// the helpers stay unexported in both packages).
func knobInt(get accountant.KnobGetter, key string) (int, bool) {
	v, ok := get(key)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, false
	}
	return n, true
}

func knobBool(get accountant.KnobGetter, key string, def bool) bool {
	v, ok := get(key)
	if !ok {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "t", "yes", "y", "1":
		return true
	case "false", "f", "no", "n", "0":
		return false
	}
	return def
}
