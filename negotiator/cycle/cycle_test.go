package cycle

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/negtest"
	"github.com/bbockelm/golang-collector/store"
)

// countingFactory wraps a SessionFactory, counting the sessions minted per
// schedd address (== NEGOTIATE rounds begun). Tests use the counts with
// LoopbackSchedd.WaitRounds to know when every round's END_NEGOTIATE has been
// processed server-side, so log assertions never race async delivery.
type countingFactory struct {
	inner negotiator.SessionFactory

	mu     sync.Mutex
	rounds map[string]int
}

func newCountingFactory(inner negotiator.SessionFactory) *countingFactory {
	return &countingFactory{inner: inner, rounds: map[string]int{}}
}

func (f *countingFactory) Session(submitter, scheddName, scheddAddr string, ad *classad.ClassAd) negotiator.ScheddSession {
	f.mu.Lock()
	f.rounds[scheddAddr]++
	f.mu.Unlock()
	return f.inner.Session(submitter, scheddName, scheddAddr, ad)
}

func (f *countingFactory) roundsFor(addr string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rounds[addr]
}

// waitSched blocks until every round begun toward sched has fully completed.
func waitSched(t *testing.T, ctx context.Context, cf *countingFactory, sched *negtest.LoopbackSchedd) {
	t.Helper()
	if err := sched.WaitRounds(ctx, cf.roundsFor(sched.Addr())); err != nil {
		t.Fatalf("WaitRounds: %v", err)
	}
}

// TestFlatCycleEndToEnd drives a full flat-pool cycle against two loopback
// schedds: fair-share split by priority, request-group member assignment,
// claim-id delivery, accountant charging, reject reasons, and warm-socket
// reuse on a second cycle. Runs in both compat and fast mode (the headline
// equality test is separate; this asserts absolute behavior).
func TestFlatCycleEndToEnd(t *testing.T) {
	for _, mode := range []struct {
		name   string
		compat bool
	}{{"compat", true}, {"fast", false}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := testCtx(t)

			// 10 one-cpu slots with claim ids.
			var ads []*classad.ClassAd
			for i := 1; i <= 10; i++ {
				name := fmt.Sprintf("slot%02d@ep", i)
				ads = append(ads, machineAd(t, name, 1), pvtAd(name, claimForSlot(name)))
			}

			// alice prio 1, bob prio 3 => shares 0.75 / 0.25 of 10 slots =>
			// limits 7.5 / 2.5 => exactly 7 and 2 matches.
			aliceSched := startSchedd(t, ctx, [][]negtest.Group{{
				group(t, 1, 100, 4, 1, ""),
				group(t, 2, 200, 4, 1, ""),
			}})
			bobSched := startSchedd(t, ctx, [][]negtest.Group{{
				group(t, 3, 300, 4, 1, ""),
			}})
			ads = append(ads,
				submitterAd("alice@pool", "schedd_alice", aliceSched.Addr(), 8),
				submitterAd("bob@pool", "schedd_bob", bobSched.Addr(), 4),
			)

			acct := newAccountant(t)
			if err := acct.SetPriority("alice@pool", 1); err != nil {
				t.Fatalf("SetPriority: %v", err)
			}
			if err := acct.SetPriority("bob@pool", 3); err != nil {
				t.Fatalf("SetPriority: %v", err)
			}

			st := seedStore(t, ads...)
			cf := newCountingFactory(newFactory())
			cfg := DefaultConfig()
			cfg.CompatMode = mode.compat
			cfg.NegotiatorName = "negotiator@test"
			cyc, err := New(embeddedSource(t, st), acct, cf, cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			stats, err := cyc.Run(ctx)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			waitSched(t, ctx, cf, aliceSched)
			waitSched(t, ctx, cf, bobSched)

			// Fair share: alice 7, bob 2.
			aliceMatches := matchesByOwner(aliceSched)["alice@pool"]
			bobMatches := matchesByOwner(bobSched)["bob@pool"]
			if len(aliceMatches) != 7 || len(bobMatches) != 2 {
				t.Fatalf("fair-share split: alice=%d bob=%d, want 7/2", len(aliceMatches), len(bobMatches))
			}

			// Group-member assignment + claim ids + both id spellings.
			log0 := aliceSched.Logs()[0]
			wantAssign := [][2]int{{1, 0}, {1, 1}, {1, 2}, {1, 3}, {2, 0}, {2, 1}, {2, 2}}
			if len(log0.Matches) != 7 {
				t.Fatalf("alice round 0: %d matches, want 7", len(log0.Matches))
			}
			for i, m := range log0.Matches {
				if [2]int{m.AssignedCluster, m.AssignedProc} != wantAssign[i] {
					t.Errorf("match %d assigned to %d.%d, want %d.%d",
						i, m.AssignedCluster, m.AssignedProc, wantAssign[i][0], wantAssign[i][1])
				}
				if !m.HasCondorSpelling || !m.HasPlainSpelling {
					t.Errorf("match %d missing resource-request id spelling (condor=%v plain=%v)",
						i, m.HasCondorSpelling, m.HasPlainSpelling)
				}
				if m.ClaimID != claimForSlot(m.SlotName) {
					t.Errorf("match %d claim %q, want %q", i, m.ClaimID, claimForSlot(m.SlotName))
				}
			}

			// The NEGOTIATE header advertises the significant attributes.
			hdrAttrs, _ := log0.Header.EvaluateAttrString("AutoClusterAttrs")
			for _, want := range []string{"RequestCpus", "RequestMemory", "Requirements", "Rank", "ConcurrencyLimits"} {
				if !strings.Contains(hdrAttrs, want) {
					t.Errorf("AutoClusterAttrs %q missing %s", hdrAttrs, want)
				}
			}

			// Both submitters were stopped by their submitter limit: one
			// reasoned reject each.
			if got := totalRejects(aliceSched); got != 1 {
				t.Errorf("alice rejects = %d, want 1", got)
			}
			if rl := aliceSched.Logs()[0].Rejects; len(rl) == 1 {
				if rl[0].Reason != "submitter limit exceeded" {
					t.Errorf("alice reject reason %q, want %q", rl[0].Reason, "submitter limit exceeded")
				}
				if !rl[0].HasRep || rl[0].RepCluster != 2 {
					t.Errorf("alice reject rep = %+v, want cluster 2", rl[0])
				}
			}

			// Accountant charged the matches (weighted usage).
			if got := acct.GetWeightedResourcesUsed("alice@pool"); got != 7 {
				t.Errorf("alice weighted usage = %g, want 7", got)
			}
			if got := acct.GetWeightedResourcesUsed("bob@pool"); got != 2 {
				t.Errorf("bob weighted usage = %g, want 2", got)
			}

			if stats.Matches != 9 || stats.Rejections != 2 {
				t.Errorf("stats matches/rejections = %d/%d, want 9/2", stats.Matches, stats.Rejections)
			}
			if stats.TotalSlots != 10 || stats.CandidateSlots != 10 || stats.Submitters != 2 {
				t.Errorf("stats slots/submitters = %d/%d/%d", stats.TotalSlots, stats.CandidateSlots, stats.Submitters)
			}
			if stats.PieSpins < 2 {
				t.Errorf("stats.PieSpins = %d, want >= 2", stats.PieSpins)
			}

			// Accounting ads were published back through the AdSource.
			nAcct := 0
			acctSeq, err := st.Query(context.Background(), store.AccountingAd, "", 0)
			if err != nil {
				t.Fatal(err)
			}
			for range acctSeq {
				nAcct++
			}
			if nAcct == 0 {
				t.Errorf("no accounting ads published to the store")
			}

			// Second cycle: the warm sockets are reused (no re-dial).
			if _, err := cyc.Run(ctx); err != nil {
				t.Fatalf("Run(2): %v", err)
			}
			waitSched(t, ctx, cf, aliceSched)
			waitSched(t, ctx, cf, bobSched)
			if got := aliceSched.Conns(); got != 1 {
				t.Errorf("alice schedd connections = %d, want 1 (warm reuse)", got)
			}
			if got := bobSched.Conns(); got != 1 {
				t.Errorf("bob schedd connections = %d, want 1 (warm reuse)", got)
			}
		})
	}
}

// TestRejectUnsatisfiable: a request no slot can satisfy is rejected with
// "no match found" and the representative id suffix.
func TestRejectUnsatisfiable(t *testing.T) {
	ctx := testCtx(t)

	sched := startSchedd(t, ctx, [][]negtest.Group{{
		group(t, 7, 700, 1, 64, ""), // RequestCpus=64: nothing fits
	}})
	ads := []*classad.ClassAd{
		machineAd(t, "s1@ep", 1), pvtAd("s1@ep", claimForSlot("s1@ep")),
		machineAd(t, "s2@ep", 1), pvtAd("s2@ep", claimForSlot("s2@ep")),
		submitterAd("carol@pool", "schedd_c", sched.Addr(), 1),
	}
	st := seedStore(t, ads...)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	cfg.CompatMode = true
	cyc, err := New(embeddedSource(t, st), newAccountant(t), cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stats, err := cyc.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitSched(t, ctx, cf, sched)

	if got := totalMatches(sched); got != 0 {
		t.Fatalf("matches = %d, want 0", got)
	}
	rejects := sched.Logs()[0].Rejects
	if len(rejects) != 1 {
		t.Fatalf("rejects = %d, want 1", len(rejects))
	}
	r := rejects[0]
	if r.Reason != "no match found" {
		t.Errorf("reject reason %q, want %q", r.Reason, "no match found")
	}
	if !r.HasRep || r.RepCluster != 7 || r.RepProc != 0 {
		t.Errorf("reject rep = %+v, want 7.0", r)
	}
	if stats.Rejections != 1 {
		t.Errorf("stats.Rejections = %d, want 1", stats.Rejections)
	}
}

// TestPslotMatchedMultipleTimes: a partitionable slot stays in the candidate
// set after a match (design doc 4.4) and satisfies a whole request group.
func TestPslotMatchedMultipleTimes(t *testing.T) {
	ctx := testCtx(t)

	// One 8-cpu pslot with an explicit SlotWeight of 1 plus four fillers the
	// job refuses, so the pie (5) leaves room for repeated pslot matches.
	pslot := machineAd(t, "pslot1@ep", 8)
	pslot.InsertAttrBool("PartitionableSlot", true)
	pslot.InsertAttrFloat("SlotWeight", 1.0)
	ads := []*classad.ClassAd{pslot, pvtAd("pslot1@ep", claimForSlot("pslot1@ep"))}
	for _, n := range []string{"f1@ep", "f2@ep", "f3@ep", "f4@ep"} {
		ads = append(ads, machineAd(t, n, 1), pvtAd(n, claimForSlot(n)))
	}

	sched := startSchedd(t, ctx, [][]negtest.Group{{
		group(t, 9, 900, 3, 1, "TARGET.PartitionableSlot =?= true"),
	}})
	ads = append(ads, submitterAd("dave@pool", "schedd_d", sched.Addr(), 3))

	st := seedStore(t, ads...)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	cfg.CompatMode = true
	cyc, err := New(embeddedSource(t, st), newAccountant(t), cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := cyc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitSched(t, ctx, cf, sched)

	got := matchesByOwner(sched)["dave@pool"]
	if len(got) != 3 {
		t.Fatalf("pslot matches = %d (%v), want 3", len(got), got)
	}
	for i, name := range got {
		if name != "pslot1@ep" {
			t.Errorf("match %d went to %q, want pslot1@ep", i, name)
		}
	}
	// All three group members were assigned.
	log0 := sched.Logs()[0]
	for i, m := range log0.Matches {
		if m.AssignedCluster != 9 || m.AssignedProc != i {
			t.Errorf("match %d assigned %d.%d, want 9.%d", i, m.AssignedCluster, m.AssignedProc, i)
		}
	}
}

// TestSignificantAttrs exercises the computation directly: union of slot-ad
// external references + always-significant set, bans applied.
func TestSignificantAttrs(t *testing.T) {
	slot := classad.New()
	req, err := classad.ParseExpr("TARGET.MyCustomAttr > 5 && TARGET.CurrentTime > 0 && TARGET.Slot1_Foo == 1")
	if err != nil {
		t.Fatal(err)
	}
	slot.InsertExpr("Requirements", req)
	rank, err := classad.ParseExpr("TARGET.JobPrioAttr")
	if err != nil {
		t.Fatal(err)
	}
	slot.InsertExpr("Rank", rank)

	got := computeSignificantAttrs([]*classad.ClassAd{slot}, "TARGET.PreRankAttr + 1")
	for _, want := range []string{"MyCustomAttr", "JobPrioAttr", "PreRankAttr", "Requirements", "Rank", "ConcurrencyLimits", "RequestCpus", "RequestMemory", "RequestDisk"} {
		if !strings.Contains(got, want) {
			t.Errorf("significant attrs %q missing %s", got, want)
		}
	}
	for _, banned := range []string{"CurrentTime", "Slot1_Foo"} {
		if strings.Contains(got, banned) {
			t.Errorf("significant attrs %q contains banned %s", got, banned)
		}
	}
}
