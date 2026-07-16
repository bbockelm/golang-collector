package accountant

import (
	"fmt"
	"testing"
	"time"
)

// TestResList covers the GET_RESLIST resource-list rendering: every Resource
// record charged to the queried submitter appears as Name<i>/StartTime<i>, and
// resources charged to other submitters are excluded.
func TestResList(t *testing.T) {
	a := newMem(t, func(*Config) {})
	base := time.Unix(1_700_000_000, 0)
	const alice = "alice@pool.test"
	const bob = "bob@pool.test"

	a.AddMatch(alice, slotAd("s1@e1", "<1:9>", "Claimed", alice, 2), base)
	a.AddMatch(alice, slotAd("s2@e2", "<2:9>", "Claimed", alice, 3), base.Add(time.Minute))
	a.AddMatch(bob, slotAd("s3@e3", "<3:9>", "Claimed", bob, 1), base)

	names := resListNames(t, a.ResList(alice))
	if len(names) != 2 {
		t.Fatalf("ResList(alice) returned %d resources, want 2: %v", len(names), names)
	}
	for _, n := range names {
		if n == "" {
			t.Errorf("ResList(alice) emitted an empty Name")
		}
	}

	if got := resListNames(t, a.ResList(bob)); len(got) != 1 {
		t.Errorf("ResList(bob) returned %d resources, want 1: %v", len(got), got)
	}
	if got := resListNames(t, a.ResList("carol@pool.test")); len(got) != 0 {
		t.Errorf("ResList for an unknown submitter returned %d resources, want 0: %v", len(got), got)
	}
}

// resListNames extracts the Name<i> values from a GET_RESLIST ad and asserts a
// StartTime<i> accompanies each.
func resListNames(t *testing.T, ad interface {
	EvaluateAttrString(string) (string, bool)
	EvaluateAttrInt(string) (int64, bool)
}) []string {
	t.Helper()
	var out []string
	for i := 1; ; i++ {
		name, ok := ad.EvaluateAttrString(fmt.Sprintf("Name%d", i))
		if !ok {
			break
		}
		out = append(out, name)
		if _, ok := ad.EvaluateAttrInt(fmt.Sprintf("StartTime%d", i)); !ok {
			t.Errorf("resource %q (index %d) has no StartTime", name, i)
		}
	}
	return out
}
