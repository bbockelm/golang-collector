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

	// ConsiderPreemption mirrors NEGOTIATOR_CONSIDER_PREEMPTION. Preemption is
	// deferred (design doc scope): the only supported value is false, which
	// selects the C++ preemption-off code paths everywhere (claimed non-pslot
	// ads trimmed up-front, unclaimed submitter limits, submitter limits
	// respected on every spin). New rejects a true value.
	ConsiderPreemption bool

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
}

// DefaultConfig returns a Config populated with the HTCondor defaults.
func DefaultConfig() Config {
	return Config{
		RequestListSize:     200,
		MaxTimePerCycle:     1200 * time.Second,
		MaxTimePerSubmitter: 60 * time.Second,
		MaxTimePerSpin:      120 * time.Second,
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
	return c
}
