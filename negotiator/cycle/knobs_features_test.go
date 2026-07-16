package cycle

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
)

// deptJobAd builds a representative job ad carrying a Department attribute so a
// NEGOTIATOR_JOB_CONSTRAINT can select or reject it.
func deptJobAd(t *testing.T, dept string) *classad.ClassAd {
	t.Helper()
	ad := jobAd(t, 1, "")
	ad.InsertAttrString("Department", dept)
	return ad
}

// TestJobConstraintFiltersRequests verifies NEGOTIATOR_JOB_CONSTRAINT is
// enforced locally: only requests satisfying the constraint are offered a match;
// non-matching requests are silently skipped (no reject sent). Runs in both
// modes to keep the filter deterministic (compat==fast).
func TestJobConstraintFiltersRequests(t *testing.T) {
	for _, mode := range []struct {
		name   string
		compat bool
	}{{"compat", true}, {"fast", false}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := testCtx(t)

			// 4 one-cpu slots.
			var ads []*classad.ClassAd
			for i := 1; i <= 4; i++ {
				name := fmt.Sprintf("slot%02d@ep", i)
				ads = append(ads, machineAd(t, name, 1), pvtAd(name, claimForSlot(name)))
			}

			// alice offers a physics group (2 jobs) and a chem group (2 jobs).
			physics := negtest.Group{
				RepCluster: 1, RepProc: 0, AutoClusterID: 100,
				Members: []negtest.Job{negtest.J(1, 0), negtest.J(1, 1)},
				RepAd:   deptJobAd(t, "physics"),
			}
			chem := negtest.Group{
				RepCluster: 2, RepProc: 0, AutoClusterID: 200,
				Members: []negtest.Job{negtest.J(2, 0), negtest.J(2, 1)},
				RepAd:   deptJobAd(t, "chem"),
			}
			sched := startSchedd(t, ctx, [][]negtest.Group{{physics, chem}})
			ads = append(ads, submitterAd("alice@pool", "schedd_alice", sched.Addr(), 4))

			acct := newAccountant(t)
			if err := acct.SetPriority("alice@pool", 1); err != nil {
				t.Fatalf("SetPriority: %v", err)
			}

			st := seedStore(t, ads...)
			cf := newCountingFactory(newFactory())
			cfg := DefaultConfig()
			cfg.CompatMode = mode.compat
			cfg.NegotiatorName = "negotiator@test"
			cfg.JobConstraint = `Department == "physics"`
			cyc, err := New(embeddedSource(t, st), acct, cf, cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if _, err := cyc.Run(ctx); err != nil {
				t.Fatalf("Run: %v", err)
			}
			waitSched(t, ctx, cf, sched)

			// Only the two physics jobs matched; the chem group was skipped.
			matches := matchesByOwner(sched)["alice@pool"]
			if len(matches) != 2 {
				t.Fatalf("matches = %d, want 2 (physics only)", len(matches))
			}
			for _, m := range sched.Logs()[0].Matches {
				if m.AssignedCluster != 1 {
					t.Errorf("matched cluster %d, want 1 (physics)", m.AssignedCluster)
				}
			}
			// A silent skip, not a reject.
			if got := totalRejects(sched); got != 0 {
				t.Errorf("rejects = %d, want 0 (constraint skips silently)", got)
			}
		})
	}
}

// TestJobConstraintInvalidRejected confirms an unparseable NEGOTIATOR_JOB_CONSTRAINT
// fails construction rather than silently disabling filtering.
func TestJobConstraintInvalidRejected(t *testing.T) {
	st := seedStore(t, machineAd(t, "slot01@ep", 1))
	cfg := DefaultConfig()
	cfg.JobConstraint = "Department ==" // truncated expression
	_, err := New(embeddedSource(t, st), newAccountant(t), newFactory(), cfg)
	if err == nil {
		t.Fatal("New with invalid NEGOTIATOR_JOB_CONSTRAINT: want error, got nil")
	}
}

// TestMatchExprsFromKnobs covers the NEGOTIATOR_MATCH_EXPRS parse: a bare macro
// name is resolved to its config value and prefixed; an inline name=expr is
// taken verbatim; an already-prefixed name is not double-prefixed; an undefined
// macro is skipped; order is preserved.
func TestMatchExprsFromKnobs(t *testing.T) {
	cfg := map[string]string{
		"NEGOTIATOR_MATCH_EXPRS": "NanoTime, Inline=WantFoo, NegotiatorMatchExprPre, Missing",
		"NanoTime":               "time()",
		"NegotiatorMatchExprPre": "42",
		// Inline provides its own expr; Missing has no macro value.
	}
	get := func(k string) (string, bool) { v, ok := cfg[k]; return v, ok }

	got := matchExprsFromKnobs(get)
	want := []struct{ name, expr string }{
		{"NegotiatorMatchExprNanoTime", "time()"},
		{"NegotiatorMatchExprInline", "WantFoo"},
		{"NegotiatorMatchExprPre", "42"},
	}
	if len(got) != len(want) {
		t.Fatalf("matchExprsFromKnobs = %+v, want %d entries", got, len(want))
	}
	for i, w := range want {
		if got[i].Name != w.name || got[i].Expr != w.expr {
			t.Errorf("entry %d = {%q,%q}, want {%q,%q}", i, got[i].Name, got[i].Expr, w.name, w.expr)
		}
	}
}

// TestMatchExprsInjectedIntoCycle verifies a configured NEGOTIATOR_MATCH_EXPRS
// entry rides all the way through a real cycle onto the match ad the schedd
// receives.
func TestMatchExprsInjectedIntoCycle(t *testing.T) {
	ctx := testCtx(t)

	var ads []*classad.ClassAd
	for i := 1; i <= 2; i++ {
		name := fmt.Sprintf("slot%02d@ep", i)
		ads = append(ads, machineAd(t, name, 1), pvtAd(name, claimForSlot(name)))
	}
	sched := startSchedd(t, ctx, [][]negtest.Group{{
		group(t, 1, 100, 1, 1, ""),
	}})
	ads = append(ads, submitterAd("alice@pool", "schedd_alice", sched.Addr(), 1))

	acct := newAccountant(t)
	if err := acct.SetPriority("alice@pool", 1); err != nil {
		t.Fatalf("SetPriority: %v", err)
	}

	st := seedStore(t, ads...)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	cfg.NegotiatorName = "negotiator@test"
	cfg.MatchExprs = []protocol.MatchExpr{{Name: "NegotiatorMatchExprTier", Expr: `"gold"`}}
	cyc, err := New(embeddedSource(t, st), acct, cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := cyc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitSched(t, ctx, cf, sched)

	logs := sched.Logs()
	if len(logs) == 0 || len(logs[0].Matches) == 0 {
		t.Fatal("no matches delivered")
	}
	for _, m := range logs[0].Matches {
		if v, ok := m.MatchAd.EvaluateAttrString("NegotiatorMatchExprTier"); !ok || v != "gold" {
			t.Errorf("delivered match ad NegotiatorMatchExprTier = %q ok=%v, want gold", v, ok)
		}
	}
}

// TestInformStartdKnobDefault confirms NEGOTIATOR_INFORM_STARTD defaults off and
// flips on only when explicitly set true.
func TestInformStartdKnobDefault(t *testing.T) {
	off := ConfigFromKnobs(func(string) (string, bool) { return "", false })
	if off.InformStartd {
		t.Error("InformStartd defaulted true, want false")
	}
	on := ConfigFromKnobs(func(k string) (string, bool) {
		if k == "NEGOTIATOR_INFORM_STARTD" {
			return "true", true
		}
		return "", false
	})
	if !on.InformStartd {
		t.Error("InformStartd not set from NEGOTIATOR_INFORM_STARTD=true")
	}
}
