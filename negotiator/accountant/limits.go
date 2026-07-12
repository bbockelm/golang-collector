package accountant

import (
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// Concurrency-limit accounting (roadmap #3). A faithful port of the C++
// Accountant concurrency-limit map (Accountant.cpp:1934-2075) and the
// negotiator's rejectForConcurrencyLimits gate (matchmaker.cpp:4585-4644).
//
// Storage: the C++ Accountant keeps an in-memory concurrencyLimits map that is
// rebuilt from the claimed slot list every cycle (LoadLimits) and incremented /
// decremented live by AddMatch / RemoveMatch. This port cannot add a field to
// the (off-limits) Accountant struct, so the per-limit counts live in the state
// store under the tableAcct namespace, one record per limit keyed
// "ConcurrencyLimit.<lowercased-name>" with a single "Count" float attribute.
// tableAcct otherwise holds only the singleton "" record (LastUpdateTime), so
// these keys neither collide with it nor with the Customer / Resource tables,
// and they never surface in ReportState (which iterates tableCustomer only).
const (
	// concurrencyLimitKeyPrefix namespaces the per-limit count records within
	// tableAcct.
	concurrencyLimitKeyPrefix = "ConcurrencyLimit."
	// attrLimitCount is the single float attribute on a limit count record.
	attrLimitCount = "Count"

	// attrMatchedConcurrencyLimits is stamped on the enriched match ad
	// (protocol.EnrichMatchAd) and mirrored onto the Resource record so
	// RemoveMatch can decrement the right limits (C++ ATTR_MATCHED_CONCURRENCY_LIMITS,
	// Accountant.cpp:925-928).
	attrMatchedConcurrencyLimits = "MatchedConcurrencyLimits"
	// slotConcurrencyLimits is the ConcurrencyLimits attribute a claimed slot
	// carries when rebuilding counts from the live pool (C++ LoadLimits reads
	// ATTR_CONCURRENCY_LIMITS off each resource ad, Accountant.cpp:1945).
	slotConcurrencyLimits = "ConcurrencyLimits"

	// DefaultConcurrencyLimitMax is param CONCURRENCY_LIMIT_DEFAULT's default
	// (Accountant::GetLimitMax, Accountant.cpp:1991): effectively unlimited, so
	// concurrency limits never reject unless a limit is configured.
	DefaultConcurrencyLimitMax = 2308032.0
)

// LimitStore is the optional capability the cycle discovers on its Accountant
// (via type assertion, mirroring RRTimeStore) to seed the per-cycle
// concurrency-usage tracker from the cross-cycle counts. Test stubs that do not
// implement it simply seed from zero.
type LimitStore interface {
	// GetLimit returns the current cross-cycle weighted usage of a concurrency
	// limit (0 for an unknown limit), matching Accountant::GetLimit.
	GetLimit(name string) float64
}

var _ LimitStore = (*Accountant)(nil)

// GetLimit returns the current cross-cycle weighted usage of a concurrency
// limit, or 0 when the limit is unknown (Accountant.cpp:1976).
func (a *Accountant) GetLimit(name string) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.getLimitLocked(name)
}

func (a *Accountant) getLimitLocked(name string) float64 {
	v, _ := a.store.getFloat(tableAcct, concurrencyLimitKeyPrefix+strings.ToLower(name), attrLimitCount)
	return v
}

func (a *Accountant) setLimitLocked(name string, v float64) {
	a.store.setFloat(tableAcct, concurrencyLimitKeyPrefix+strings.ToLower(name), attrLimitCount, v)
}

// incrementLimitsLocked adds each parsed limit's weight to its count
// (Accountant::IncrementLimits, Accountant.cpp:2069). Invalid tokens are
// ignored, matching ParseConcurrencyLimit's reject path.
func (a *Accountant) incrementLimitsLocked(limits string) {
	for _, tok := range splitConcurrencyLimits(limits) {
		name, inc, ok := ParseConcurrencyLimit(tok)
		if !ok {
			continue
		}
		a.setLimitLocked(name, a.getLimitLocked(name)+inc)
	}
}

// decrementLimitsLocked subtracts each parsed limit's weight from its count
// (Accountant::DecrementLimits, Accountant.cpp:2075).
func (a *Accountant) decrementLimitsLocked(limits string) {
	for _, tok := range splitConcurrencyLimits(limits) {
		name, inc, ok := ParseConcurrencyLimit(tok)
		if !ok {
			continue
		}
		a.setLimitLocked(name, a.getLimitLocked(name)-inc)
	}
}

// clearLimitsLocked zeroes every stored limit count, the first half of the
// per-cycle rebuild (Accountant::ClearLimits, Accountant.cpp:2032). It zeroes
// rather than deletes so the record set stays stable across cycles.
func (a *Accountant) clearLimitsLocked() {
	var keys []string
	a.store.forEach(tableAcct, func(k string, _ *record) bool {
		if strings.HasPrefix(k, concurrencyLimitKeyPrefix) {
			keys = append(keys, k)
		}
		return true
	})
	for _, k := range keys {
		a.store.setFloat(tableAcct, k, attrLimitCount, 0)
	}
}

// concurrencyLimitsOf returns the concurrency-limit list a slot/match ad
// carries, preferring the negotiator-stamped MatchedConcurrencyLimits and
// falling back to the slot's own ConcurrencyLimits, lowercased. "" means none.
func concurrencyLimitsOf(ad *classad.ClassAd) string {
	if s, ok := classad.GetAs[string](ad, attrMatchedConcurrencyLimits); ok && s != "" {
		return strings.ToLower(s)
	}
	if s, ok := classad.GetAs[string](ad, slotConcurrencyLimits); ok && s != "" {
		return strings.ToLower(s)
	}
	return ""
}

// GetLimitMax resolves a concurrency limit's configured maximum from the config
// knobs, a pure port of Accountant::GetLimitMax (Accountant.cpp:1989-1999):
//
//	max = <NAME>_LIMIT, else
//	      CONCURRENCY_LIMIT_DEFAULT_<PREFIX> (for a dotted "prefix.name"), else
//	      CONCURRENCY_LIMIT_DEFAULT, else
//	      DefaultConcurrencyLimitMax (a large "unlimited" default).
//
// The limit name is lowercased (limit strings are lowercased before this point,
// as in the C++ path); the supplied KnobGetter must be case-insensitive on the
// knob key (HTCondor's param is), so e.g. "gpu_LIMIT" resolves a GPU_LIMIT knob.
func GetLimitMax(get KnobGetter, limit string) float64 {
	limit = strings.ToLower(limit)
	deflim := DefaultConcurrencyLimitMax
	if v, ok := knobFloat(get, "CONCURRENCY_LIMIT_DEFAULT"); ok {
		deflim = v
	}
	if pos := strings.LastIndexByte(limit, '.'); pos >= 0 {
		if v, ok := knobFloat(get, "CONCURRENCY_LIMIT_DEFAULT_"+limit[:pos]); ok {
			deflim = v
		}
	}
	if v, ok := knobFloat(get, limit+"_LIMIT"); ok {
		return v
	}
	return deflim
}

// ParseConcurrencyLimit ports NegotiationUtils.cpp ParseConcurrencyLimit. A
// token is "<name>" or "<name>:<weight>" (weight defaults to 1; a non-positive
// or unparseable weight becomes 1). The name may be a single-dotted
// "prefix.suffix"; both parts must be valid ClassAd attribute names (a name with
// more than one dot is invalid, matching the C++ first-dot split). The returned
// name is lowercased. ok=false means the token is invalid and should be ignored.
func ParseConcurrencyLimit(token string) (name string, increment float64, ok bool) {
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

// validLimitName is the ParseConcurrencyLimit validity test: the name is a
// valid attribute name, or a single-dotted "prefix.suffix" whose two halves are
// each valid attribute names (C++ splits on the FIRST dot and validates both
// sides, so multi-dot names fail because the suffix still contains a dot).
func validLimitName(name string) bool {
	if dot := strings.IndexByte(name, '.'); dot >= 0 {
		return isValidAttrName(name[:dot]) && isValidAttrName(name[dot+1:])
	}
	return isValidAttrName(name)
}

// isValidAttrName mirrors classad IsValidAttrName: a non-empty identifier of
// [A-Za-z_][A-Za-z0-9_]*.
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
		if !alpha && (c < '0' || c > '9') {
			return false
		}
	}
	return true
}

// splitConcurrencyLimits tokenizes a limit list on commas and whitespace,
// matching the C++ StringTokenIterator default separators.
func splitConcurrencyLimits(limits string) []string {
	return strings.FieldsFunc(limits, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
}
