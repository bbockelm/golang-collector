package matchmaker

import (
	"context"
	"math"
	"testing"

	"github.com/bbockelm/golang-collector/negotiator"
)

// stubLimits is a map-backed negotiator.ConcurrencyLimits for the gate tests.
// Names are lowercased on access (the gate lowercases the request's list, so
// callers seed lowercased keys). An absent max defaults to unlimited.
type stubLimits struct {
	usage map[string]float64
	max   map[string]float64
}

func (s stubLimits) Usage(name string) float64 { return s.usage[name] }

func (s stubLimits) Max(name string) float64 {
	if m, ok := s.max[name]; ok {
		return m
	}
	return math.MaxFloat64
}

// limitsWith builds an openLimits() with a concurrency view attached.
func limitsWith(v negotiator.ConcurrencyLimits) *negotiator.MatchLimits {
	l := openLimits()
	l.Concurrency = v
	return l
}

// reqWithLimits builds a matchable request carrying a ConcurrencyLimits string.
func reqWithLimits(t *testing.T, cl string) *negotiator.Request {
	return reqOf(mustAd(t, "[ Requirements = true; ConcurrencyLimits = \""+cl+"\" ]"))
}

func TestConcurrencyGateAdmitsBelowMax(t *testing.T) {
	m := mustNew(t, Config{})
	view := viewOf(mustAd(t, "[ Requirements = true ]"))
	lim := stubLimits{usage: map[string]float64{"gpu": 1}, max: map[string]float64{"gpu": 4}}

	cand, rej, err := m.Match(context.Background(), reqWithLimits(t, "GPU"), view, limitsWith(lim))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand == nil {
		t.Fatalf("expected a match (usage 1 + 1 <= 4), got reject %+v", rej)
	}
}

func TestConcurrencyGateRejectsAtMax(t *testing.T) {
	m := mustNew(t, Config{})
	view := viewOf(mustAd(t, "[ Requirements = true ]"))
	// usage 4 + weight 1 > max 4 -> reject.
	lim := stubLimits{usage: map[string]float64{"gpu": 4}, max: map[string]float64{"gpu": 4}}

	cand, rej, err := m.Match(context.Background(), reqWithLimits(t, "GPU"), view, limitsWith(lim))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected rejection, got match %v", cand.Slot)
	}
	if rej == nil || rej.ForConcurrencyLim != 1 {
		t.Fatalf("expected ForConcurrencyLim=1, got %+v", rej)
	}
	if want := "concurrency limit gpu reached"; rej.Reason != want {
		t.Fatalf("reason: got %q want %q", rej.Reason, want)
	}
}

func TestConcurrencyGateWeighted(t *testing.T) {
	m := mustNew(t, Config{})
	view := viewOf(mustAd(t, "[ Requirements = true ]"))
	// "gpu:2" consumes weight 2; usage 3 + 2 = 5 > max 4 -> reject.
	lim := stubLimits{usage: map[string]float64{"gpu": 3}, max: map[string]float64{"gpu": 4}}
	cand, rej, err := m.Match(context.Background(), reqWithLimits(t, "GPU:2"), view, limitsWith(lim))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected weighted rejection, got match")
	}
	if rej == nil || rej.ForConcurrencyLim != 1 || rej.Reason != "concurrency limit gpu reached" {
		t.Fatalf("unexpected reject: %+v", rej)
	}

	// Same usage but weight fits: usage 3 + 1 = 4 <= 4 -> match.
	cand2, rej2, err := m.Match(context.Background(), reqWithLimits(t, "GPU:1"), view, limitsWith(lim))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand2 == nil {
		t.Fatalf("expected match for weight 1 (3+1<=4), got reject %+v", rej2)
	}
}

func TestConcurrencyGateMultipleLimitsAnyOverRejects(t *testing.T) {
	m := mustNew(t, Config{})
	view := viewOf(mustAd(t, "[ Requirements = true ]"))
	// license is fine (0+1<=10) but gpu is saturated (4+1>4) -> reject on gpu.
	lim := stubLimits{
		usage: map[string]float64{"gpu": 4, "license": 0},
		max:   map[string]float64{"gpu": 4, "license": 10},
	}
	cand, rej, err := m.Match(context.Background(), reqWithLimits(t, "license, GPU"), view, limitsWith(lim))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected rejection when any limit is over")
	}
	if rej == nil || rej.Reason != "concurrency limit gpu reached" {
		t.Fatalf("expected gpu to be the offending limit, got %+v", rej)
	}
}

func TestConcurrencyGateDefaultMaxUnlimited(t *testing.T) {
	m := mustNew(t, Config{})
	view := viewOf(mustAd(t, "[ Requirements = true ]"))
	// No max configured for "gpu" -> stub returns MaxFloat64 -> never rejects,
	// even at high usage.
	lim := stubLimits{usage: map[string]float64{"gpu": 1e6}}
	cand, rej, err := m.Match(context.Background(), reqWithLimits(t, "GPU"), view, limitsWith(lim))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand == nil {
		t.Fatalf("expected a match with unlimited default max, got reject %+v", rej)
	}
}

func TestConcurrencyGateNoLimitsAttrIgnored(t *testing.T) {
	m := mustNew(t, Config{})
	view := viewOf(mustAd(t, "[ Requirements = true ]"))
	// A request without ConcurrencyLimits is never gated, even with a view present.
	lim := stubLimits{usage: map[string]float64{}, max: map[string]float64{}}
	cand, _, err := m.Match(context.Background(), reqOf(mustAd(t, "[ Requirements = true ]")), view, limitsWith(lim))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand == nil {
		t.Fatalf("expected a match for a request with no ConcurrencyLimits")
	}
}

// TestConcurrencyGateExpression verifies that ConcurrencyLimits may be an
// EXPRESSION over the job's own attributes (not just a literal string): the
// negotiator evaluates it to a string once per request (EvaluateAttrString) and
// gates on the resulting limit name. This is the per-request expression path
// (evaluate_limits_with_match == false); a TARGET-referencing per-candidate
// expression stays out of scope (documented in concurrency.go).
func TestConcurrencyGateExpression(t *testing.T) {
	m := mustNew(t, Config{})
	view := viewOf(mustAd(t, "[ Requirements = true ]"))
	// ConcurrencyLimits is strcat("cms_", AcctGroup) -> "cms_prod".
	req := func() *negotiator.Request {
		return reqOf(mustAd(t, `[ Requirements = true; AcctGroup = "prod"; ConcurrencyLimits = strcat("cms_", AcctGroup) ]`))
	}

	// usage 3 + 1 <= 4 -> admit against the evaluated limit "cms_prod".
	admit := stubLimits{usage: map[string]float64{"cms_prod": 3}, max: map[string]float64{"cms_prod": 4}}
	if cand, rej, err := m.Match(context.Background(), req(), view, limitsWith(admit)); err != nil {
		t.Fatalf("Match: %v", err)
	} else if cand == nil {
		t.Fatalf("expected match (cms_prod 3+1<=4), got reject %+v", rej)
	}

	// usage 4 + 1 > 4 -> reject; proves the expression resolved to the gated name.
	full := stubLimits{usage: map[string]float64{"cms_prod": 4}, max: map[string]float64{"cms_prod": 4}}
	cand, rej, err := m.Match(context.Background(), req(), view, limitsWith(full))
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand != nil {
		t.Fatalf("expected rejection (cms_prod at max), got match %v", cand.Slot)
	}
	if rej == nil || rej.ForConcurrencyLim != 1 {
		t.Fatalf("expected ForConcurrencyLim=1, got %+v", rej)
	}
	if want := "concurrency limit cms_prod reached"; rej.Reason != want {
		t.Fatalf("reason: got %q want %q", rej.Reason, want)
	}
}

func TestParseLimitToken(t *testing.T) {
	cases := []struct {
		in   string
		name string
		inc  float64
		ok   bool
	}{
		{"gpu", "gpu", 1, true},
		{"GPU", "gpu", 1, true},
		{"gpu:2", "gpu", 2, true},
		{"gpu:2.5", "gpu", 2.5, true},
		{"gpu:0", "gpu", 1, true},  // non-positive weight -> 1
		{"gpu:-3", "gpu", 1, true}, // negative -> 1
		{"grp.gpu", "grp.gpu", 1, true},
		{"grp.gpu:3", "grp.gpu", 3, true},
		{"a.b.c", "", 0, false}, // multi-dot invalid
		{"1bad", "", 0, false},  // must start with letter/underscore
		{"", "", 0, false},
	}
	for _, c := range cases {
		name, inc, ok := parseLimitToken(c.in)
		if ok != c.ok || name != c.name || (ok && inc != c.inc) {
			t.Errorf("parseLimitToken(%q) = (%q,%v,%v), want (%q,%v,%v)",
				c.in, name, inc, ok, c.name, c.inc, c.ok)
		}
	}
}
