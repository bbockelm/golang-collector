package matchmaker

import (
	"context"
	"math"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// preemptLimits builds MatchLimits for a preemption scan: both the claimed and
// unclaimed submitter limits are wide open, the submitter negotiating is
// "alice@dom", and only_for_startdrank is off unless overridden.
func preemptLimits() *negotiator.MatchLimits {
	return &negotiator.MatchLimits{
		SubmitterLimit:          1e9,
		LimitUsed:               0,
		PieLeft:                 1e9,
		Ceiling:                 math.MaxFloat64,
		SubmitterLimitUnclaimed: 1e9,
		LimitUsedUnclaimed:      0,
		SubmitterName:           "alice@dom",
	}
}

// preemptCfg is a matchmaker config with preemption on and the given
// PREEMPTION_REQUIREMENTS / PREEMPTION_RANK expressions.
func preemptCfg(req, rank string) Config {
	return Config{
		ConsiderPreemption:     true,
		PreemptionRequirements: req,
		PreemptionRank:         rank,
	}
}

// claimedSlot builds a machine ad claimed by remoteUser with the given machine
// Rank expression and CurrentRank. A trivially-true Requirements keeps the
// bilateral match passing so only the preemption classification is under test.
// remoteUser == "" makes an unclaimed slot.
func claimedSlot(t *testing.T, remoteUser, rankExpr string, currentRank float64) *classad.ClassAd {
	t.Helper()
	ad := classad.New()
	ad.InsertAttrBool("Requirements", true)
	ad.InsertAttrString("State", "Claimed")
	if remoteUser != "" {
		ad.InsertAttrString("RemoteUser", remoteUser)
	} else {
		ad.InsertAttrString("State", "Unclaimed")
	}
	if rankExpr != "" {
		e, err := classad.ParseExpr(rankExpr)
		if err != nil {
			t.Fatalf("parse rank %q: %v", rankExpr, err)
		}
		ad.InsertExpr("Rank", e)
	} else {
		ad.InsertAttrFloat("Rank", 0)
	}
	ad.InsertAttrFloat("CurrentRank", currentRank)
	return ad
}

// TestClassifyRankPreemption: a claimed slot whose machine Rank of this request
// exceeds its CurrentRank (rankCondStd) is a RANK_PREEMPTION candidate and
// bypasses the submitter limit even when the limit is exhausted.
func TestClassifyRankPreemption(t *testing.T) {
	m := mustNew(t, preemptCfg("", ""))
	// Machine ranks THIS request at 10; its current job at 5 -> rankCondStd true.
	slot := claimedSlot(t, "bob@dom", "10", 5)
	view := viewOf(slot)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))

	// Exhaust the submitter limit: NO_PREEMPTION/PRIO candidates would be gated
	// out, but RANK preemptions ignore the limit.
	lim := preemptLimits()
	lim.SubmitterLimit = 0
	lim.LimitUsed = 100
	lim.SubmitterLimitUnclaimed = 0
	lim.LimitUsedUnclaimed = 100

	c, rej, err := m.Match(context.Background(), job, view, lim)
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatalf("expected a RANK_PREEMPTION match, got reject %+v", rej)
	}
	if c.PreemptTier != rankPreemption {
		t.Fatalf("PreemptTier: got %d want %d (RANK)", c.PreemptTier, rankPreemption)
	}
}

// TestClassifyPrioPreemption: a claimed slot the machine does NOT prefer
// (rankCondStd false) but held by a DIFFERENT user is a PRIO_PREEMPTION
// candidate, gated by PREEMPTION_REQUIREMENTS and rankCondPrioPreempt.
func TestClassifyPrioPreemption(t *testing.T) {
	// Machine ranks this request equal to its current job (5 == 5): rankCondStd
	// (>) false, rankCondPrioPreempt (>=) true.
	slot := claimedSlot(t, "bob@dom", "5", 5)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))

	t.Run("allowed by PREEMPTION_REQUIREMENTS", func(t *testing.T) {
		m := mustNew(t, preemptCfg("true", ""))
		c, rej, err := m.Match(context.Background(), job, viewOf(slot), preemptLimits())
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			t.Fatalf("expected PRIO_PREEMPTION match, got reject %+v", rej)
		}
		if c.PreemptTier != prioPreemption {
			t.Fatalf("PreemptTier: got %d want %d (PRIO)", c.PreemptTier, prioPreemption)
		}
	})

	t.Run("denied by PREEMPTION_REQUIREMENTS", func(t *testing.T) {
		m := mustNew(t, preemptCfg("false", ""))
		c, rej, err := m.Match(context.Background(), job, viewOf(slot), preemptLimits())
		if err != nil {
			t.Fatal(err)
		}
		if c != nil {
			t.Fatalf("expected reject, got candidate tier %d", c.PreemptTier)
		}
		if rej == nil || rej.Reason != reasonPreemptionPolicy {
			t.Fatalf("reason: got %+v want %q", rej, reasonPreemptionPolicy)
		}
		if rej.ForPreemptionPolicy != 1 {
			t.Fatalf("ForPreemptionPolicy: got %d want 1", rej.ForPreemptionPolicy)
		}
	})
}

// TestClassifyPrioPreemptionRankGate: PREEMPTION_REQUIREMENTS passes but the
// machine ranks this request lower than its current job (rankCondPrioPreempt
// false) -> rejected for rank; the counter is tracked but the reason falls back
// to "no match found" (C++ has no rejPreemptForRank reason branch).
func TestClassifyPrioPreemptionRankGate(t *testing.T) {
	m := mustNew(t, preemptCfg("true", ""))
	// Machine ranks this request 3 < current job 5: rankCondStd false AND
	// rankCondPrioPreempt (>=) false.
	slot := claimedSlot(t, "bob@dom", "3", 5)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))
	c, rej, err := m.Match(context.Background(), job, viewOf(slot), preemptLimits())
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatalf("expected reject, got candidate tier %d", c.PreemptTier)
	}
	if rej.ForPreemptionRank != 1 {
		t.Fatalf("ForPreemptionRank: got %d want 1", rej.ForPreemptionRank)
	}
	if rej.Reason != reasonNoMatch {
		t.Fatalf("reason: got %q want %q", rej.Reason, reasonNoMatch)
	}
}

// TestClassifySameUserSkipped: a claimed slot held by the SAME user as the
// submitter, not startd-rank-preferred, is skipped (no candidate, no counter).
func TestClassifySameUserSkipped(t *testing.T) {
	m := mustNew(t, preemptCfg("true", ""))
	// remoteUser == submitterName ("alice@dom"), rankCondStd false (5==5).
	slot := claimedSlot(t, "alice@dom", "5", 5)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))
	c, rej, err := m.Match(context.Background(), job, viewOf(slot), preemptLimits())
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatalf("expected skip, got candidate tier %d", c.PreemptTier)
	}
	// Same-user skip bumps no counter -> "no match found".
	if rej.Reason != reasonNoMatch || rej.ForPreemptionPolicy != 0 || rej.ForPreemptionRank != 0 {
		t.Fatalf("expected clean no-match, got %+v", rej)
	}
}

// TestClassifySameUserRankPreferred: even the SAME user can preempt via startd
// rank when the machine strictly prefers the new request (rankCondStd true).
func TestClassifySameUserRankPreferred(t *testing.T) {
	m := mustNew(t, preemptCfg("false", ""))    // policy false must NOT matter for RANK
	slot := claimedSlot(t, "alice@dom", "9", 5) // 9 > 5 -> rankCondStd true
	job := reqOf(mustAd(t, "[ Requirements = true ]"))
	c, rej, err := m.Match(context.Background(), job, viewOf(slot), preemptLimits())
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatalf("expected RANK match, got reject %+v", rej)
	}
	if c.PreemptTier != rankPreemption {
		t.Fatalf("PreemptTier: got %d want %d (RANK)", c.PreemptTier, rankPreemption)
	}
}

// TestTierOrdering: with equal job ranks, an unclaimed NO_PREEMPTION candidate
// beats a RANK candidate beats a PRIO candidate (Candidate.Better on the
// preemption tier). Verified by consuming the winner in order.
func TestTierOrdering(t *testing.T) {
	m := mustNew(t, preemptCfg("true", ""))
	// idx0: PRIO (claimed by bob, not preferred, 5==5)
	// idx1: RANK (claimed by bob, preferred, 9>5)
	// idx2: NO_PREEMPTION (unclaimed)
	slots := []*classad.ClassAd{
		claimedSlot(t, "bob@dom", "5", 5),
		claimedSlot(t, "bob@dom", "9", 5),
		claimedSlot(t, "", "0", 0),
	}
	view := viewOf(slots...)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))

	wantTiers := []int{noPreemption, rankPreemption, prioPreemption}
	for i, wantTier := range wantTiers {
		c, rej, err := m.Match(context.Background(), job, view, preemptLimits())
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			t.Fatalf("pass %d: expected candidate, got reject %+v", i, rej)
		}
		if c.PreemptTier != wantTier {
			t.Fatalf("pass %d: tier got %d want %d", i, c.PreemptTier, wantTier)
		}
		view.Consume(c.ScanIndex)
	}
}

// TestPreemptionRankTiebreak: among two PRIO candidates with otherwise equal
// rank tuples, PREEMPTION_RANK breaks the tie (higher wins).
func TestPreemptionRankTiebreak(t *testing.T) {
	// PREEMPTION_RANK = TARGET.JobPref picks the machine whose "Pref" attr is
	// higher. Both slots are PRIO candidates (different user, 5==5).
	m := mustNew(t, preemptCfg("true", "MY.Pref"))
	loser := claimedSlot(t, "bob@dom", "5", 5)
	loser.InsertAttrFloat("Pref", 1)
	winner := claimedSlot(t, "bob@dom", "5", 5)
	winner.InsertAttrFloat("Pref", 100)

	view := viewOf(loser, winner) // winner is idx1
	job := reqOf(mustAd(t, "[ Requirements = true ]"))
	c, rej, err := m.Match(context.Background(), job, view, preemptLimits())
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatalf("expected match, got reject %+v", rej)
	}
	if c.ScanIndex != 1 {
		t.Fatalf("PREEMPTION_RANK tiebreak: got idx %d want 1 (higher Pref)", c.ScanIndex)
	}
	if c.PreemptRank != 100 {
		t.Fatalf("PreemptRank: got %v want 100", c.PreemptRank)
	}
}

// TestNoPreemptionGatedByUnclaimedLimit: an unclaimed NO_PREEMPTION candidate is
// gated by the UNCLAIMED submitter limit (not the full one). Exhaust the
// unclaimed limit and it is rejected for submitter limit even though the full
// limit is open.
func TestNoPreemptionGatedByUnclaimedLimit(t *testing.T) {
	m := mustNew(t, preemptCfg("true", ""))
	slot := claimedSlot(t, "", "0", 0) // unclaimed -> NO_PREEMPTION
	slot.InsertAttrFloat("Cpus", 1)    // cost 1
	view := viewOf(slot)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))

	lim := preemptLimits()
	lim.SubmitterLimitUnclaimed = 0 // no unclaimed headroom
	lim.LimitUsedUnclaimed = 0
	// Full limit wide open (would permit a claimed prio candidate).
	c, rej, err := m.Match(context.Background(), job, view, lim)
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatalf("expected submitter-limit reject, got candidate tier %d", c.PreemptTier)
	}
	if rej.Reason != reasonSubmitterLimit || rej.ForSubmitterLimit != 1 {
		t.Fatalf("expected submitter-limit reject, got %+v", rej)
	}
}

// TestOnlyForStartdRank: with only_for_startdrank set, unclaimed slots are
// skipped and only startd-rank-preferred claimed slots match (as RANK).
func TestOnlyForStartdRank(t *testing.T) {
	m := mustNew(t, preemptCfg("true", ""))
	job := reqOf(mustAd(t, "[ Requirements = true ]"))

	t.Run("unclaimed skipped", func(t *testing.T) {
		lim := preemptLimits()
		lim.OnlyForStartdRank = true
		c, rej, err := m.Match(context.Background(), job, viewOf(claimedSlot(t, "", "0", 0)), lim)
		if err != nil {
			t.Fatal(err)
		}
		if c != nil {
			t.Fatalf("expected unclaimed skip, got tier %d", c.PreemptTier)
		}
		if rej.Reason != reasonNoMatch {
			t.Fatalf("reason: got %q want %q", rej.Reason, reasonNoMatch)
		}
	})

	t.Run("preferred claimed matches as RANK", func(t *testing.T) {
		lim := preemptLimits()
		lim.OnlyForStartdRank = true
		c, rej, err := m.Match(context.Background(), job, viewOf(claimedSlot(t, "bob@dom", "9", 5)), lim)
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			t.Fatalf("expected RANK match, got reject %+v", rej)
		}
		if c.PreemptTier != rankPreemption {
			t.Fatalf("tier: got %d want %d (RANK)", c.PreemptTier, rankPreemption)
		}
	})

	t.Run("not-preferred claimed skipped", func(t *testing.T) {
		lim := preemptLimits()
		lim.OnlyForStartdRank = true
		c, _, err := m.Match(context.Background(), job, viewOf(claimedSlot(t, "bob@dom", "5", 5)), lim)
		if err != nil {
			t.Fatal(err)
		}
		if c != nil {
			t.Fatalf("expected skip (not preferred), got tier %d", c.PreemptTier)
		}
	})
}

// TestPreemptionOffRegression: with ConsiderPreemption off, a claimed slot is
// classified NO_PREEMPTION and gated on the single SubmitterLimit exactly as
// before (the preemption fields are ignored). A claimed slot still matches
// (trimming happens in the cycle, not the matchmaker) as NO_PREEMPTION.
func TestPreemptionOffRegression(t *testing.T) {
	m := mustNew(t, Config{}) // ConsiderPreemption false
	// A claimed slot with a remote user: OFF path ignores all of that.
	slot := claimedSlot(t, "bob@dom", "9", 5)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))

	c, rej, err := m.Match(context.Background(), job, viewOf(slot), openLimits())
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatalf("expected NO_PREEMPTION match, got reject %+v", rej)
	}
	if c.PreemptTier != noPreemption {
		t.Fatalf("tier: got %d want %d (NO_PREEMPTION)", c.PreemptTier, noPreemption)
	}
	if c.PreemptRank != preemptRankDef {
		t.Fatalf("PreemptRank: got %v want sentinel %v", c.PreemptRank, preemptRankDef)
	}

	// And the single SubmitterLimit still gates it.
	lim := openLimits()
	lim.SubmitterLimit = 0
	lim.LimitUsed = 100
	slot.InsertAttrFloat("Cpus", 1)
	c2, rej2, err := m.Match(context.Background(), job, viewOf(slot), lim)
	if err != nil {
		t.Fatal(err)
	}
	if c2 != nil {
		t.Fatalf("expected submitter-limit reject, got tier %d", c2.PreemptTier)
	}
	if rej2.Reason != reasonSubmitterLimit {
		t.Fatalf("reason: got %q want %q", rej2.Reason, reasonSubmitterLimit)
	}
}
