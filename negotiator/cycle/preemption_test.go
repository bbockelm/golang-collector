package cycle

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/matchmaker"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
)

// (kept minimal: these tests drive the cycle helpers + matchmaker + accountant
// directly rather than through the negtest loopback schedd.)

// claimedMachineAd builds an Unclaimed or Claimed machine ad for the cycle-level
// preemption tests. A trivially-true Requirements keeps the bilateral match
// passing; the Rank expression drives the startd-rank conditions.
func claimedMachineAd(name, remoteUser string, currentRank float64) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("MyType", "Machine")
	ad.InsertAttrString("Name", name)
	ad.InsertAttrBool("Requirements", true)
	ad.InsertAttrFloat("Rank", 0) // machine indifferent -> rankCondStd false
	ad.InsertAttrFloat("CurrentRank", currentRank)
	ad.InsertAttr("Cpus", 1)
	if remoteUser != "" {
		ad.InsertAttrString("State", "Claimed")
		ad.InsertAttrString("RemoteUser", remoteUser)
	} else {
		ad.InsertAttrString("State", "Unclaimed")
	}
	return ad
}

// TestCyclePreemptionDecision exercises the real cycle helpers (trimSlots +
// addRemoteUserPrios) together with a real matchmaker and accountant: a
// higher-priority submitter (alice) can preempt a lower-priority submitter's
// (bob's) claimed slot when preemption is ON, and cannot when OFF (the claimed
// slot is trimmed out of the candidate set).
func TestCyclePreemptionDecision(t *testing.T) {
	acct := newAccountant(t)
	// Lower effective priority value = better. alice better than bob.
	if err := acct.SetPriority("alice@dom", 10); err != nil {
		t.Fatalf("SetPriority alice: %v", err)
	}
	if err := acct.SetPriority("bob@dom", 100); err != nil {
		t.Fatalf("SetPriority bob: %v", err)
	}

	// The classic prio-preemption policy: preempt when the submitter's priority
	// is (strictly) better than the running user's. MY=machine, TARGET=request.
	const preemptReq = "TARGET.SubmitterUserPrio < MY.RemoteUserPrio"

	// A request from alice, enriched with her SubmitterUserPrio as the cycle
	// would (EnrichRequestAd stamps SubmitterUserPrio).
	buildReq := func() *negotiator.Request {
		job := classad.New()
		job.InsertAttrBool("Requirements", true)
		protocol.EnrichRequestAd(job, protocol.SubmitterContext{
			UserPrio: acct.GetPriority("alice@dom"),
		})
		return &negotiator.Request{Ad: job, Count: 1}
	}

	run := func(t *testing.T, considerPreemption bool) *negotiator.Candidate {
		c := &Cycle{acct: acct, cfg: Config{ConsiderPreemption: considerPreemption}}
		// Machine is indifferent (Rank 0 == CurrentRank 0): rankCondStd (>) false
		// so this is not a startd-rank preemption, but rankCondPrioPreempt (>=)
		// holds so prio preemption is allowed once PREEMPTION_REQUIREMENTS passes.
		slot := claimedMachineAd("slot1@node", "bob@dom", 0)

		// Cycle step: trim, then addRemoteUserPrios (only when preemption on).
		trimmed := trimSlots([]*classad.ClassAd{slot}, considerPreemption)
		if considerPreemption {
			for _, s := range trimmed {
				c.addRemoteUserPrios(s)
			}
		}

		mm, err := matchmaker.New(matchmaker.Config{
			ConsiderPreemption:     considerPreemption,
			PreemptionRequirements: preemptReq,
		})
		if err != nil {
			t.Fatalf("matchmaker.New: %v", err)
		}
		view := matchmaker.NewSlotView(&negotiator.PoolSnapshot{Slots: trimmed})
		limits := &negotiator.MatchLimits{
			SubmitterLimit: 1e9, PieLeft: 1e9, Ceiling: 1e18,
			SubmitterLimitUnclaimed: 1e9, SubmitterName: "alice@dom",
		}
		cand, _, err := mm.Match(context.Background(), buildReq(), view, limits)
		if err != nil {
			t.Fatalf("Match: %v", err)
		}
		return cand
	}

	t.Run("preemption on: alice preempts bob", func(t *testing.T) {
		cand := run(t, true)
		if cand == nil {
			t.Fatal("expected alice to preempt bob's claimed slot")
		}
		if got := cand.PreemptTier; got != 0 { // 0 = PRIO_PREEMPTION
			t.Fatalf("PreemptTier: got %d want 0 (PRIO)", got)
		}
	})

	t.Run("preemption off: bob's claimed slot is trimmed", func(t *testing.T) {
		cand := run(t, false)
		if cand != nil {
			t.Fatalf("expected no candidate (claimed slot trimmed), got %+v", cand)
		}
	})
}

// TestCalculateSubmitterLimitVariants verifies the two-output form of
// calculateSubmitterLimit: with preemption OFF the full limit collapses to the
// group-capped unclaimed variant; with preemption ON the full limit stays
// uncapped by the group headroom.
func TestCalculateSubmitterLimitVariants(t *testing.T) {
	acct := newAccountant(t)
	if err := acct.SetPriority("alice@dom", 10); err != nil {
		t.Fatal(err)
	}
	// One submitter, so normalFactor = 1 and maxPrio = prio -> share = 1.
	env := &spinEnv{maxPrio: 10, normalFactor: 1, slotWeightTotal: 100, groupusage: 90}
	env.prios = map[string]float64{"alice@dom": 10}

	// Group round with quota 100 and 90 already used -> unclaimed headroom 10.
	ri := roundInfo{name: "group_a", quota: 100}

	t.Run("off collapses to group-capped", func(t *testing.T) {
		c := &Cycle{acct: acct, cfg: Config{ConsiderPreemption: false}}
		limit, unclaimed, _, _, _ := c.calculateSubmitterLimit("alice@dom", ri, env, false)
		if unclaimed != 10 {
			t.Fatalf("unclaimed: got %v want 10 (quota-usage)", unclaimed)
		}
		if limit != unclaimed {
			t.Fatalf("off: limit %v should equal unclaimed %v", limit, unclaimed)
		}
	})

	t.Run("on keeps full uncapped", func(t *testing.T) {
		c := &Cycle{acct: acct, cfg: Config{ConsiderPreemption: true}}
		limit, unclaimed, _, _, _ := c.calculateSubmitterLimit("alice@dom", ri, env, false)
		if unclaimed != 10 {
			t.Fatalf("unclaimed: got %v want 10", unclaimed)
		}
		// full = share*slotWeightTotal - usage = 1*100 - 0 = 100 (uncapped).
		if limit != 100 {
			t.Fatalf("on: full limit got %v want 100 (uncapped by group)", limit)
		}
	})
}
