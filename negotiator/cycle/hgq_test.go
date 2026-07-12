package cycle

import (
	"fmt"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// TestHGQGroupQuotas: two accounting groups with static quotas share a
// six-slot pool through the HGQ dispatch path. Group quotas cap each group's
// matches, and groups negotiate in starvation order (group "a" before "b" on
// the zero-usage tie, observed through the shared schedd's round order).
func TestHGQGroupQuotas(t *testing.T) {
	ctx := testCtx(t)

	var ads []*classad.ClassAd
	for i := 1; i <= 6; i++ {
		name := fmt.Sprintf("g%d@ep", i)
		ads = append(ads, machineAd(t, name, 1), pvtAd(name, claimForSlot(name)))
	}

	// One shared loopback: round order proves group negotiation order.
	sched := startSchedd(t, ctx, [][]negtest.Group{
		{group(t, 10, 100, 6, 1, "")}, // round 0: a.alice's requests
		{group(t, 20, 200, 6, 1, "")}, // round 1: b.bob's requests
	})
	ads = append(ads,
		submitterAd("a.alice@pool", "schedd1", sched.Addr(), 6),
		submitterAd("b.bob@pool", "schedd1", sched.Addr(), 6),
	)

	st := seedStore(t, ads...)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	cfg.CompatMode = true
	cfg.Group.GroupNames = []string{"a", "b"}
	cfg.Group.GroupQuota = map[string]float64{"a": 2, "b": 4}
	cyc, err := New(embeddedSource(t, st), newAccountant(t), cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stats, err := cyc.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitSched(t, ctx, cf, sched)

	byOwner := matchesByOwner(sched)
	if got := len(byOwner["a.alice@pool"]); got != 2 {
		t.Errorf("group a matches = %d, want 2 (quota)", got)
	}
	if got := len(byOwner["b.bob@pool"]); got != 4 {
		t.Errorf("group b matches = %d, want 4 (quota)", got)
	}

	// Negotiation order: group a's submitter negotiated first.
	logs := sched.Logs()
	if len(logs) < 2 {
		t.Fatalf("rounds = %d, want >= 2", len(logs))
	}
	if logs[0].Owner != "a.alice@pool" || logs[1].Owner != "b.bob@pool" {
		t.Errorf("negotiation order = [%s, %s], want [a.alice@pool, b.bob@pool]",
			logs[0].Owner, logs[1].Owner)
	}

	// The accountant rolled the matches up into the group records.
	acctUsage := func(name string) float64 { return cyc.acct.GetWeightedResourcesUsed(name) }
	if got := acctUsage("a"); got != 2 {
		t.Errorf("group a usage = %g, want 2", got)
	}
	if got := acctUsage("b"); got != 4 {
		t.Errorf("group b usage = %g, want 4", got)
	}
	if stats.Matches != 6 {
		t.Errorf("stats.Matches = %d, want 6", stats.Matches)
	}
}

// TestFloorRound: a submitter below its Floor is negotiated first (up to the
// floor) even though its priority is worse.
func TestFloorRound(t *testing.T) {
	ctx := testCtx(t)

	var ads []*classad.ClassAd
	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("fl%d@ep", i)
		ads = append(ads, machineAd(t, name, 1), pvtAd(name, claimForSlot(name)))
	}

	aliceSched := startSchedd(t, ctx, [][]negtest.Group{
		{group(t, 30, 300, 3, 1, "")},
	})
	bobSched := startSchedd(t, ctx, [][]negtest.Group{
		{group(t, 40, 400, 3, 1, "")},
	})
	ads = append(ads,
		submitterAd("alice@pool", "schedd_a", aliceSched.Addr(), 3),
		submitterAd("bob@pool", "schedd_b", bobSched.Addr(), 3),
	)

	raw := newAccountant(t)
	if err := raw.SetPriority("alice@pool", 1); err != nil {
		t.Fatal(err)
	}
	if err := raw.SetPriority("bob@pool", 3); err != nil {
		t.Fatal(err)
	}
	// bob (the worse-priority submitter) has a floor of 2.
	acct := &capAcct{Accountant: raw, floor: map[string]float64{"bob@pool": 2}}

	st := seedStore(t, ads...)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	cfg.CompatMode = true
	cyc, err := New(embeddedSource(t, st), acct, cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := cyc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitSched(t, ctx, cf, aliceSched)
	waitSched(t, ctx, cf, bobSched)

	// The floor round gave bob his floor (2) before alice negotiated; alice
	// then got the single remaining slot despite her better priority.
	bobMatches := matchesByOwner(bobSched)["bob@pool"]
	aliceMatches := matchesByOwner(aliceSched)["alice@pool"]
	if len(bobMatches) != 2 {
		t.Errorf("bob matches = %d, want 2 (his floor)", len(bobMatches))
	}
	if len(aliceMatches) != 1 {
		t.Errorf("alice matches = %d, want 1", len(aliceMatches))
	}
	// Bob's matches were delivered in his FIRST round -- the floor round.
	if logs := bobSched.Logs(); len(logs) == 0 || len(logs[0].Matches) != 2 {
		t.Errorf("bob's floor round should carry both matches: %+v", logs)
	}
}

// TestCeiling: a submitter with a Ceiling stops receiving matches at the
// ceiling even with pie and requests to spare.
func TestCeiling(t *testing.T) {
	ctx := testCtx(t)

	var ads []*classad.ClassAd
	for i := 1; i <= 5; i++ {
		name := fmt.Sprintf("ce%d@ep", i)
		ads = append(ads, machineAd(t, name, 1), pvtAd(name, claimForSlot(name)))
	}
	sched := startSchedd(t, ctx, [][]negtest.Group{
		{group(t, 50, 500, 5, 1, "")},
	})
	ads = append(ads, submitterAd("erin@pool", "schedd_e", sched.Addr(), 5))

	acct := &capAcct{Accountant: newAccountant(t), ceil: map[string]float64{"erin@pool": 2}}

	st := seedStore(t, ads...)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	cfg.CompatMode = true
	cyc, err := New(embeddedSource(t, st), acct, cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stats, err := cyc.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitSched(t, ctx, cf, sched)

	if got := totalMatches(sched); got != 2 {
		t.Errorf("matches = %d, want 2 (ceiling)", got)
	}
	if got := acct.GetWeightedResourcesUsed("erin@pool"); got != 2 {
		t.Errorf("weighted usage = %g, want 2", got)
	}
	if stats.Matches != 2 {
		t.Errorf("stats.Matches = %d, want 2", stats.Matches)
	}
}
