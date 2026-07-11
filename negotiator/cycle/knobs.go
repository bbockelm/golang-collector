package cycle

import (
	"strconv"
	"strings"
	"time"

	"github.com/bbockelm/golang-collector/negotiator/accountant"
)

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
	cfg.DisableAccountingAds = !knobBool(get, "NEGOTIATOR_ADVERTISE_ACCOUNTING", true)
	cfg.Group = accountant.GroupConfigFromKnobs(get)
	return cfg
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
