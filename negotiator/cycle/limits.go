package cycle

import (
	"strings"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/accountant"
)

// concurrencyTracker is the cycle's live view of concurrency-limit usage. It
// implements negotiator.ConcurrencyLimits: the matchmaker reads Usage/Max from
// it (on the single Match spine, before the sharded scan), and the cycle
// increments it as matches commit.
//
// Why a per-cycle tracker rather than reading the accountant's counts live: the
// accountant's cross-cycle counts (LimitStore.GetLimit) are rebuilt from the
// claimed pool at cycle start (CheckMatches) and give the starting usage, but
// the matchmaker gate must also see this cycle's not-yet-persisted matches. We
// seed each limit lazily from the accountant's count on first touch and add the
// weight of every committed match to a plain in-memory map, so the gate stays a
// pure map read (no accountant lock on the hot path) and the map = cross-cycle
// base + in-cycle increments. The accountant's own AddMatch keeps its store
// counts in step for the NEXT cycle's seeding; the two never double count
// because the tracker caches its base once and only adds local increments.
type concurrencyTracker struct {
	usage map[string]float64        // lowercased name -> current in-cycle usage
	seed  func(name string) float64 // cross-cycle base (accountant.GetLimit), may be nil
	max   func(name string) float64 // configured max resolver, may be nil (=> unlimited)
}

var _ negotiator.ConcurrencyLimits = (*concurrencyTracker)(nil)

// newConcurrencyTracker builds a tracker. seed supplies the cross-cycle base for
// a limit (nil => start at 0); max resolves the configured maximum (nil => the
// large default, i.e. effectively unlimited so nothing is rejected).
func newConcurrencyTracker(seed, max func(name string) float64) *concurrencyTracker {
	return &concurrencyTracker{usage: map[string]float64{}, seed: seed, max: max}
}

// Usage returns the current in-cycle usage of a limit, lazily seeding the base
// from the accountant on first touch.
func (t *concurrencyTracker) Usage(name string) float64 {
	name = strings.ToLower(name)
	if v, ok := t.usage[name]; ok {
		return v
	}
	base := 0.0
	if t.seed != nil {
		base = t.seed(name)
	}
	t.usage[name] = base
	return base
}

// Max returns a limit's configured maximum.
func (t *concurrencyTracker) Max(name string) float64 {
	if t.max == nil {
		return accountant.DefaultConcurrencyLimitMax
	}
	return t.max(strings.ToLower(name))
}

// add charges the weights in a request's ConcurrencyLimits list against the
// tracker, called when a match commits (mirrors the accountant's IncrementLimits
// but on the per-cycle map the gate reads).
func (t *concurrencyTracker) add(limits string) {
	if limits == "" {
		return
	}
	for _, tok := range splitConcurrencyLimits(strings.ToLower(limits)) {
		name, inc, ok := accountant.ParseConcurrencyLimit(tok)
		if !ok {
			continue
		}
		t.usage[name] = t.Usage(name) + inc
	}
}

// newConcurrencyTracker builds the per-cycle tracker for this Cycle, seeding
// from the accountant's cross-cycle counts (discovered via the optional
// LimitStore capability, like RRTimeStore) and resolving maxes from the config.
// CheckMatches has already rebuilt the accountant's counts from the claimed pool
// by the time this seed is read, so the base reflects currently-running usage.
func (c *Cycle) newConcurrencyTracker() *concurrencyTracker {
	var seed func(name string) float64
	if ls, ok := c.acct.(accountant.LimitStore); ok {
		seed = ls.GetLimit
	}
	return newConcurrencyTracker(seed, c.cfg.ConcurrencyLimitMax)
}

// splitConcurrencyLimits tokenizes a limit list on commas/whitespace (the C++
// StringTokenIterator separators).
func splitConcurrencyLimits(limits string) []string {
	return strings.FieldsFunc(limits, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
}
