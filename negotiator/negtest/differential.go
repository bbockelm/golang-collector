package negtest

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// This file holds the shared, daemon-agnostic pieces of the C++ vs. Go
// negotiator differential harness (roadmap item #1): a parser that turns
// `condor_userprio -l -modular` output into a comparable per-submitter struct,
// a float-with-tolerance comparator, and a state-diff helper. The integration
// tests in integration/negotiator_differential_test.go drive a real
// condor_negotiator and the Go negotiator through the identical userprio
// mutation sequence and compare the parsed states with these helpers.

// SubmitterPrio is the accountant state for one submitter (or accounting
// group) entry, as reconstructed by condor_userprio from the negotiator's
// ReportState reply. EffectivePriority is Priority in the ad, i.e. the C++
// "effective" priority = real priority x priority factor.
type SubmitterPrio struct {
	Name                     string
	EffectivePriority        float64
	PriorityFactor           float64
	WeightedAccumulatedUsage float64
	AccumulatedUsage         float64
	ResourcesUsed            int64
	IsAccountingGroup        bool
	// Raw holds every attribute parsed for this entry (values with quotes
	// stripped), for ad-hoc assertions beyond the typed fields above.
	Raw map[string]string
}

var attrLineRE = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s*$`)

// ParseUserprioModular parses the output of `condor_userprio -l -modular`
// (one ClassAd per accountant record, blank-line separated) into a map keyed
// by submitter Name. Lines that are not "Attr = value" (e.g. a leading
// "Can't locate negotiator..." locate warning) and blocks without a Name
// attribute are ignored, so the known userprio locate warning does not derail
// the parse.
func ParseUserprioModular(out string) (map[string]SubmitterPrio, error) {
	result := map[string]SubmitterPrio{}
	for _, block := range splitBlocks(out) {
		attrs := map[string]string{}
		for _, line := range strings.Split(block, "\n") {
			m := attrLineRE.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			attrs[m[1]] = unquote(strings.TrimSpace(m[2]))
		}
		name, ok := attrs["Name"]
		if !ok || name == "" {
			continue
		}
		sp := SubmitterPrio{Name: name, Raw: attrs}
		sp.EffectivePriority = asFloat(attrs["Priority"])
		sp.PriorityFactor = asFloat(attrs["PriorityFactor"])
		sp.WeightedAccumulatedUsage = asFloat(attrs["WeightedAccumulatedUsage"])
		sp.AccumulatedUsage = asFloat(attrs["AccumulatedUsage"])
		sp.ResourcesUsed = int64(asFloat(attrs["ResourcesUsed"]))
		sp.IsAccountingGroup = strings.EqualFold(attrs["IsAccountingGroup"], "true")
		result[name] = sp
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no userprio records parsed from output:\n%s", out)
	}
	return result, nil
}

// Submitters returns only the non-group entries (those condor_userprio marks
// IsAccountingGroup = false), which are the ones seeded over the userprio
// protocol.
func Submitters(state map[string]SubmitterPrio) map[string]SubmitterPrio {
	out := map[string]SubmitterPrio{}
	for name, sp := range state {
		if !sp.IsAccountingGroup {
			out[name] = sp
		}
	}
	return out
}

// FloatClose reports whether a and b agree within an absolute OR relative
// tolerance (either satisfied passes). relTol is applied to max(|a|,|b|).
func FloatClose(a, b, absTol, relTol float64) bool {
	d := math.Abs(a - b)
	if d <= absTol {
		return true
	}
	scale := math.Max(math.Abs(a), math.Abs(b))
	return d <= relTol*scale
}

// DiffField is one mismatched attribute between the two negotiators.
type DiffField struct {
	Submitter string
	Field     string
	Cpp       float64
	Go        float64
}

func (d DiffField) String() string {
	return fmt.Sprintf("%s.%s: cpp=%g go=%g (delta=%g)", d.Submitter, d.Field, d.Cpp, d.Go, math.Abs(d.Cpp-d.Go))
}

// DiffPrioStates compares the priority/factor/weighted-usage of every submitter
// present in either state and returns the fields that differ beyond tolerance.
// A submitter missing from one side is reported as a diff with NaN on that
// side. Only non-group entries are compared.
func DiffPrioStates(cpp, goneg map[string]SubmitterPrio, absTol, relTol float64) []DiffField {
	cppSubs := Submitters(cpp)
	goSubs := Submitters(goneg)

	names := map[string]struct{}{}
	for n := range cppSubs {
		names[n] = struct{}{}
	}
	for n := range goSubs {
		names[n] = struct{}{}
	}
	ordered := make([]string, 0, len(names))
	for n := range names {
		ordered = append(ordered, n)
	}
	sort.Strings(ordered)

	var diffs []DiffField
	for _, n := range ordered {
		c, hasC := cppSubs[n]
		g, hasG := goSubs[n]
		if !hasC {
			diffs = append(diffs, DiffField{Submitter: n, Field: "presence", Cpp: math.NaN(), Go: 0})
			continue
		}
		if !hasG {
			diffs = append(diffs, DiffField{Submitter: n, Field: "presence", Cpp: 0, Go: math.NaN()})
			continue
		}
		for _, f := range []struct {
			name string
			c, g float64
		}{
			{"EffectivePriority", c.EffectivePriority, g.EffectivePriority},
			{"PriorityFactor", c.PriorityFactor, g.PriorityFactor},
			{"WeightedAccumulatedUsage", c.WeightedAccumulatedUsage, g.WeightedAccumulatedUsage},
		} {
			if !FloatClose(f.c, f.g, absTol, relTol) {
				diffs = append(diffs, DiffField{Submitter: n, Field: f.name, Cpp: f.c, Go: f.g})
			}
		}
	}
	return diffs
}

func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

func asFloat(s string) float64 {
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}
