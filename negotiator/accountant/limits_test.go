package accountant

import (
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// limitSlotAd is slotAd plus a MatchedConcurrencyLimits attribute (what the
// negotiator stamps on the enriched offer it charges via AddMatch).
func limitSlotAd(name, ip, state, user string, weight float64, matched string) *classad.ClassAd {
	ad := slotAd(name, ip, state, user, weight)
	if matched != "" {
		ad.InsertAttrString(attrMatchedConcurrencyLimits, matched)
	}
	return ad
}

func TestGetLimitMaxResolution(t *testing.T) {
	knobs := map[string]float64{
		"CONCURRENCY_LIMIT_DEFAULT":       10,
		"CONCURRENCY_LIMIT_DEFAULT_GROUP": 5,
		"GPU_LIMIT":                       3,
	}
	// A case-insensitive getter (HTCondor param is): the limit names arrive
	// lowercased, so uppercase knob keys still resolve.
	get := func(key string) (string, bool) {
		for k, v := range knobs {
			if equalFold(k, key) {
				return floatStr(v), true
			}
		}
		return "", false
	}

	cases := []struct {
		limit string
		want  float64
	}{
		{"gpu", 3},       // GPU_LIMIT wins
		{"other", 10},    // falls to CONCURRENCY_LIMIT_DEFAULT
		{"group.foo", 5}, // CONCURRENCY_LIMIT_DEFAULT_GROUP for the prefix
		{"group.gpu", 5}, // prefix default (no group.gpu_LIMIT knob)
	}
	for _, c := range cases {
		if got := GetLimitMax(get, c.limit); got != c.want {
			t.Errorf("GetLimitMax(%q) = %g, want %g", c.limit, got, c.want)
		}
	}

	// No knobs at all -> the large unlimited default.
	none := func(string) (string, bool) { return "", false }
	if got := GetLimitMax(none, "gpu"); got != DefaultConcurrencyLimitMax {
		t.Errorf("GetLimitMax with no config = %g, want %g", got, DefaultConcurrencyLimitMax)
	}
}

func TestAddRemoveMatchLimits(t *testing.T) {
	a := newMem(t, nil)
	base := time.Unix(1_500_000, 0)
	a.UpdatePriorities(base)
	user := "alice@pool.test"

	// Two matches, one weighted "gpu:2". Resource keys are Name+"@"+StartdIpAddr,
	// i.e. "s1@e1" and "s2@e2".
	a.AddMatch(user, limitSlotAd("s1", "e1", "Claimed", user, 1, "gpu, license"), base)
	a.AddMatch(user, limitSlotAd("s2", "e2", "Claimed", user, 1, "gpu:2"), base)

	if got := a.GetLimit("gpu"); got != 3 {
		t.Fatalf("gpu after adds: got %g want 3", got)
	}
	if got := a.GetLimit("license"); got != 1 {
		t.Fatalf("license after adds: got %g want 1", got)
	}
	// Case-insensitive lookup.
	if got := a.GetLimit("GPU"); got != 3 {
		t.Fatalf("GPU (upper) lookup: got %g want 3", got)
	}

	// Removing the weighted match returns its 2 units of gpu.
	a.RemoveMatch("s2@e2", base)
	if got := a.GetLimit("gpu"); got != 1 {
		t.Fatalf("gpu after removing s2: got %g want 1", got)
	}
	if got := a.GetLimit("license"); got != 1 {
		t.Fatalf("license after removing s2: got %g want 1", got)
	}

	a.RemoveMatch("s1@e1", base)
	if got := a.GetLimit("gpu"); got != 0 {
		t.Fatalf("gpu after removing s1: got %g want 0", got)
	}
	if got := a.GetLimit("license"); got != 0 {
		t.Fatalf("license after removing s1: got %g want 0", got)
	}
}

func TestCheckMatchesRebuildsLimits(t *testing.T) {
	a := newMem(t, nil)
	base := time.Unix(1_600_000, 0)
	a.UpdatePriorities(base)
	user := "bob@pool.test"

	// Prime a stale count that should be wiped by the rebuild.
	a.AddMatch(user, limitSlotAd("gone@e", "<9:9>", "Claimed", user, 1, "gpu:5"), base)
	if got := a.GetLimit("gpu"); got != 5 {
		t.Fatalf("precondition gpu: got %g want 5", got)
	}

	// The live pool: two claimed slots carrying ConcurrencyLimits (as a startd
	// advertises them), plus an unclaimed slot that must not count. "gone@e" is
	// absent, so its stale count must be dropped.
	live := []*classad.ClassAd{
		concurrencySlot("live1@e", "<1:9>", "Claimed", user, "gpu"),
		concurrencySlot("live2@e", "<2:9>", "Claimed", user, "gpu:2, license"),
		concurrencySlot("idle@e", "<3:9>", "Unclaimed", "", "gpu"),
	}
	a.CheckMatches(live, base)

	if got := a.GetLimit("gpu"); got != 3 {
		t.Fatalf("gpu after rebuild: got %g want 3 (1+2, unclaimed excluded)", got)
	}
	if got := a.GetLimit("license"); got != 1 {
		t.Fatalf("license after rebuild: got %g want 1", got)
	}
}

// concurrencySlot builds a claimed slot ad carrying a ConcurrencyLimits attr
// (the fall-back source CheckMatches reads when there is no stamped
// MatchedConcurrencyLimits).
func concurrencySlot(name, ip, state, user, limits string) *classad.ClassAd {
	ad := slotAd(name, ip, state, user, 1)
	if limits != "" {
		ad.InsertAttrString(slotConcurrencyLimits, limits)
	}
	return ad
}

func TestParseConcurrencyLimit(t *testing.T) {
	cases := []struct {
		in   string
		name string
		inc  float64
		ok   bool
	}{
		{"GPU", "gpu", 1, true},
		{"gpu:2", "gpu", 2, true},
		{"gpu:1.5", "gpu", 1.5, true},
		{"gpu:0", "gpu", 1, true},
		{"grp.gpu", "grp.gpu", 1, true},
		{"a.b.c", "", 0, false},
		{"9bad", "", 0, false},
		{"", "", 0, false},
	}
	for _, c := range cases {
		name, inc, ok := ParseConcurrencyLimit(c.in)
		if ok != c.ok || name != c.name || (ok && inc != c.inc) {
			t.Errorf("ParseConcurrencyLimit(%q) = (%q,%v,%v), want (%q,%v,%v)",
				c.in, name, inc, ok, c.name, c.inc, c.ok)
		}
	}
}

// --- tiny local helpers (avoid importing strconv/strings just for tests) ----

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

func floatStr(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
