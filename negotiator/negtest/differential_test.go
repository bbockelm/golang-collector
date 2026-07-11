package negtest

import (
	"math"
	"testing"
)

// sampleModular is representative `condor_userprio -l -modular` output: the
// root accounting group followed by two submitters, blank-line separated, with
// a stray leading locate-warning line that the parser must skip.
const sampleModular = `Can't locate negotiator in local pool

AccountingGroup = "<none>"
IsAccountingGroup = true
Name = "<none>"
Priority = 500.0
PriorityFactor = 0.0
WeightedAccumulatedUsage = 0.0

AccountingGroup = "<none>"
IsAccountingGroup = false
Name = "alice@differ.test"
Priority = 40.0
PriorityFactor = 10.0
ResourcesUsed = 0
WeightedAccumulatedUsage = 123.5

AccountingGroup = "<none>"
IsAccountingGroup = false
Name = "bob@differ.test"
Priority = 1000.0
PriorityFactor = 2000.0
ResourcesUsed = 2
WeightedAccumulatedUsage = 0.0
`

func TestParseUserprioModular(t *testing.T) {
	state, err := ParseUserprioModular(sampleModular)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(state) != 3 {
		t.Fatalf("got %d entries, want 3", len(state))
	}
	subs := Submitters(state)
	if len(subs) != 2 {
		t.Fatalf("got %d submitters, want 2 (group must be excluded)", len(subs))
	}
	alice := subs["alice@differ.test"]
	if alice.EffectivePriority != 40 || alice.PriorityFactor != 10 || alice.WeightedAccumulatedUsage != 123.5 {
		t.Errorf("alice parsed wrong: %+v", alice)
	}
	bob := subs["bob@differ.test"]
	if bob.PriorityFactor != 2000 || bob.ResourcesUsed != 2 {
		t.Errorf("bob parsed wrong: %+v", bob)
	}
	if grp := state["<none>"]; !grp.IsAccountingGroup {
		t.Errorf("root group should be flagged IsAccountingGroup")
	}
}

func TestParseUserprioModularEmpty(t *testing.T) {
	if _, err := ParseUserprioModular("no ads here\n"); err == nil {
		t.Errorf("expected error on output with no records")
	}
}

func TestFloatCloseAndDiff(t *testing.T) {
	if !FloatClose(40.0, 40.0000001, 1e-3, 1e-6) {
		t.Errorf("values within absolute tolerance should be close")
	}
	if FloatClose(40.0, 41.0, 1e-3, 1e-6) {
		t.Errorf("values 1.0 apart should not be close at these tolerances")
	}
	// Relative tolerance carries large-magnitude values (e.g. 1e7 remote factor).
	if !FloatClose(1e7, 1e7+1, 1e-3, 1e-6) {
		t.Errorf("large values within relative tolerance should be close")
	}

	cpp := map[string]SubmitterPrio{
		"a@x": {Name: "a@x", EffectivePriority: 40, PriorityFactor: 10, WeightedAccumulatedUsage: 100},
		"b@x": {Name: "b@x", EffectivePriority: 500, PriorityFactor: 1000},
	}
	goSt := map[string]SubmitterPrio{
		"a@x": {Name: "a@x", EffectivePriority: 40, PriorityFactor: 10, WeightedAccumulatedUsage: 100},
		"b@x": {Name: "b@x", EffectivePriority: 501, PriorityFactor: 1000}, // diverges
	}
	diffs := DiffPrioStates(cpp, goSt, 1e-3, 1e-6)
	if len(diffs) != 1 || diffs[0].Submitter != "b@x" || diffs[0].Field != "EffectivePriority" {
		t.Fatalf("expected one EffectivePriority diff on b@x, got %v", diffs)
	}

	// A submitter present on only one side is a presence diff (NaN sentinel).
	goSt2 := map[string]SubmitterPrio{"a@x": cpp["a@x"]}
	d2 := DiffPrioStates(cpp, goSt2, 1e-3, 1e-6)
	if len(d2) != 1 || d2[0].Field != "presence" || !math.IsNaN(d2[0].Go) {
		t.Fatalf("expected one presence diff with NaN Go, got %v", d2)
	}
}
