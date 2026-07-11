package cycle

import (
	"time"

	"github.com/bbockelm/golang-collector/negotiator/accountant"
)

// Config carries the cycle orchestrator's tunables (design doc section 9,
// cycle set). Interval/guard timers (NEGOTIATOR_INTERVAL, NEGOTIATOR_MIN_INTERVAL,
// NEGOTIATOR_CYCLE_DELAY) are the daemon's concern: Run executes exactly one
// cycle when called.
//
// Zero values mean "HTCondor default" for the numeric knobs; DefaultConfig
// returns a fully-populated Config.
type Config struct {
	// RequestListSize is NEGOTIATOR_RESOURCE_REQUEST_LIST_SIZE (default 200):
	// how many resource requests to ask a schedd for per fetch.
	RequestListSize int

	// MaxTimePerCycle is NEGOTIATOR_MAX_TIME_PER_CYCLE (default 1200s).
	MaxTimePerCycle time.Duration
	// MaxTimePerSubmitter is NEGOTIATOR_MAX_TIME_PER_SUBMITTER (default 60s).
	MaxTimePerSubmitter time.Duration
	// MaxTimePerSpin is NEGOTIATOR_MAX_TIME_PER_PIESPIN (default 120s).
	// (NEGOTIATOR_MAX_TIME_PER_SCHEDD is not implemented in the MVP; the
	// submitter and spin budgets subsume it for the common one-submitter-per-
	// schedd deployment.)
	MaxTimePerSpin time.Duration

	// CompatMode (NEGOTIATOR_GO_COMPAT_MODE) forces fully-serial execution:
	// no concurrent RRL prefetch, no async match delivery, and a serial
	// matchmaker scan. Decisions are identical either way (the determinism
	// test enforces it); compat mode exists as the reference behavior.
	CompatMode bool
	// PrefetchWorkers bounds the concurrent RRL prefetch I/O in fast mode
	// (default GOMAXPROCS). Ignored in compat mode.
	PrefetchWorkers int

	// PreJobRank / PostJobRank are NEGOTIATOR_PRE_JOB_RANK /
	// NEGOTIATOR_POST_JOB_RANK, passed through to the matchmaker.
	PreJobRank  string
	PostJobRank string

	// DisableSlotWeights mirrors NEGOTIATOR_USE_SLOT_WEIGHTS=false: every
	// match costs 1.0 and the pie is counted in slots. The inverted spelling
	// keeps the zero-value Config on the HTCondor default (weights on).
	DisableSlotWeights bool

	// ConsiderPreemption mirrors NEGOTIATOR_CONSIDER_PREEMPTION (C++ default
	// true; DefaultConfig sets true). When false the C++ preemption-off code
	// paths are selected everywhere (claimed non-pslot ads trimmed up-front,
	// unclaimed submitter limits, submitter limits respected on every spin) --
	// this is the preemption-off regression path and what the differential
	// harness sets for a fair comparison against a preemption-off C++ pool.
	// When true, claimed slots are retained as preemption candidates, spin-1
	// bypasses the submitter limit for startd-rank-preferred jobs, and the
	// matchmaker classifies candidates into RANK/PRIO preemption tiers.
	ConsiderPreemption bool

	// PreemptionRequirements / PreemptionRank are the PREEMPTION_REQUIREMENTS /
	// PREEMPTION_RANK expressions, passed through to the matchmaker. Empty means
	// unset: an unset PREEMPTION_REQUIREMENTS allows prio preemption (subject to
	// rankCondPrioPreempt), matching the C++ `if (PreemptionReq && ...)` guard.
	PreemptionRequirements string
	PreemptionRank         string

	// NegotiatorName is NEGOTIATOR_NAME, stamped in NEGOTIATE headers and on
	// the published accounting ads.
	NegotiatorName string
	// JobConstraint is NEGOTIATOR_JOB_CONSTRAINT (optional), forwarded in the
	// NEGOTIATE header and folded into the significant-attribute computation.
	JobConstraint string

	// DisableAccountingAds mirrors NEGOTIATOR_ADVERTISE_ACCOUNTING=false: skip
	// publishing per-submitter/group Accounting ads at the end of the cycle.
	DisableAccountingAds bool

	// Group carries the accounting-group / hierarchical-quota configuration
	// (GROUP_NAMES etc.). An empty GroupNames selects the traditional flat
	// (single-root) path. Callers should start from DefaultConfig(), which
	// seeds this with accountant.DefaultGroupConfig().
	Group accountant.GroupConfig

	// ConcurrencyLimitMax resolves a concurrency limit's configured maximum
	// (roadmap #3): <NAME>_LIMIT, else CONCURRENCY_LIMIT_DEFAULT_<PREFIX>, else
	// CONCURRENCY_LIMIT_DEFAULT, else a large "unlimited" default. ConfigFromKnobs
	// wires it to accountant.GetLimitMax over the KnobGetter. A nil resolver means
	// every limit is unlimited, so concurrency limits never reject -- the
	// safe-by-default behavior when no limits are configured.
	ConcurrencyLimitMax func(name string) float64
}

// DefaultConfig returns a Config populated with the HTCondor defaults.
// ConsiderPreemption defaults true, matching the C++
// NEGOTIATOR_CONSIDER_PREEMPTION default (matchmaker.cpp:820); set it false to
// select the preemption-off path (e.g. the differential harness).
func DefaultConfig() Config {
	return Config{
		RequestListSize:     200,
		MaxTimePerCycle:     1200 * time.Second,
		MaxTimePerSubmitter: 60 * time.Second,
		MaxTimePerSpin:      120 * time.Second,
		ConsiderPreemption:  true,
		Group:               accountant.DefaultGroupConfig(),
	}
}

// withDefaults fills zero-valued knobs with their defaults.
func (c Config) withDefaults() Config {
	if c.RequestListSize <= 0 {
		c.RequestListSize = 200
	}
	if c.MaxTimePerCycle <= 0 {
		c.MaxTimePerCycle = 1200 * time.Second
	}
	if c.MaxTimePerSubmitter <= 0 {
		c.MaxTimePerSubmitter = 60 * time.Second
	}
	if c.MaxTimePerSpin <= 0 {
		c.MaxTimePerSpin = 120 * time.Second
	}
	if c.Group.GroupSortExpr == "" {
		c.Group.GroupSortExpr = accountant.DefaultGroupSortExpr
	}
	if c.Group.MaxAllocationRounds <= 0 {
		c.Group.MaxAllocationRounds = 3
	}
	// UsingWeightedSlots feeds the group allocator's unweighted-pool special
	// cases (remainder recovery + round robin); it tracks the slot-weight knob.
	c.Group.UsingWeightedSlots = !c.DisableSlotWeights
	// The group allocator's surplus/quota-halt logic depends on preemption too
	// (groupalloc.go): keep the accountant's view in sync with the cycle knob.
	c.Group.ConsiderPreemption = c.ConsiderPreemption
	return c
}
