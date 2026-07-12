package protocol

import (
	"testing"

	"github.com/bbockelm/golang-collector/negotiator"
)

// TestEnrichMatchExprs verifies NEGOTIATOR_MATCH_EXPRS injection: each entry is
// stamped verbatim on the match ad as an unevaluated expression (so a reference
// resolves against the offer), an unparseable expr falls back to a string
// literal, and the input ad is never mutated.
func TestEnrichMatchExprs(t *testing.T) {
	req := &negotiator.Request{Cluster: 1, Proc: 0, AutoClusterID: 7, Count: 1}
	slot := slotAd("slotE@host") // carries SlotWeight=1.0

	out := EnrichMatchAd(slot, req, MatchContext{
		MatchExprs: []MatchExpr{
			{Name: "NegotiatorMatchExprAnswer", Expr: "40 + 2"},
			{Name: "NegotiatorMatchExprDouble", Expr: "SlotWeight * 2"},
			{Name: "NegotiatorMatchExprLabel", Expr: `"tier-a"`},
			{Name: "NegotiatorMatchExprBroken", Expr: "this is not )( an expr"},
		},
	})

	// Numeric literal expression evaluates.
	if v, ok := out.EvaluateAttrInt("NegotiatorMatchExprAnswer"); !ok || v != 42 {
		t.Errorf("NegotiatorMatchExprAnswer = %d ok=%v, want 42", v, ok)
	}
	// Reference expression resolves against the offer ad (SlotWeight=1.0).
	if v, ok := out.EvaluateAttrReal("NegotiatorMatchExprDouble"); !ok || v != 2.0 {
		t.Errorf("NegotiatorMatchExprDouble = %v ok=%v, want 2.0", v, ok)
	}
	// String literal expression evaluates to the string.
	if v, ok := out.EvaluateAttrString("NegotiatorMatchExprLabel"); !ok || v != "tier-a" {
		t.Errorf("NegotiatorMatchExprLabel = %q ok=%v, want tier-a", v, ok)
	}
	// Unparseable expr: kept as a string literal rather than dropping the ad.
	if v, ok := out.EvaluateAttrString("NegotiatorMatchExprBroken"); !ok || v != "this is not )( an expr" {
		t.Errorf("NegotiatorMatchExprBroken = %q ok=%v, want the raw string", v, ok)
	}

	// The input slot ad is not mutated.
	if _, ok := slot.EvaluateAttrString("NegotiatorMatchExprLabel"); ok {
		t.Error("EnrichMatchAd mutated the input slot ad with a match expr")
	}
}

// TestEnrichMatchExprsEmpty confirms the default (no exprs) leaves the ad
// otherwise untouched -- the injection is a strict addition.
func TestEnrichMatchExprsEmpty(t *testing.T) {
	req := &negotiator.Request{Cluster: 1, Proc: 0, Count: 1}
	out := EnrichMatchAd(slotAd("slotE@host"), req, MatchContext{})
	for _, a := range out.GetAttributes() {
		if len(a) > len("NegotiatorMatchExpr") && a[:len("NegotiatorMatchExpr")] == "NegotiatorMatchExpr" && a != "NegotiatorMatchExprFoo" {
			t.Errorf("unexpected match-expr attr %q with no MatchExprs configured", a)
		}
	}
}
