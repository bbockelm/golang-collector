package cycle

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// runOutcome is the observable result of one cycle: per submitter, the
// ordered list of slot names delivered and the count of rejects.
type runOutcome struct {
	Matches map[string][]string
	Rejects map[string]int
}

// runRandomized builds a seeded random fixture (slots, priorities, request
// sets) and runs one cycle in the given mode, returning the delivered match
// lists. The same seed always builds the identical fixture; the AdSource is a
// fixedSource so the candidate scan order is identical across runs (the
// collector-reply/store-iteration order is legitimately arbitrary input, not
// part of the determinism contract).
func runRandomized(t *testing.T, seed int64, compat bool) runOutcome {
	t.Helper()
	ctx := testCtx(t)
	rng := rand.New(rand.NewSource(seed))

	// 12 slots with mixed cpu counts (SlotWeight defaults to Cpus).
	var slots, pvts []*classad.ClassAd
	for i := 0; i < 12; i++ {
		cpus := []int64{1, 1, 2, 4}[rng.Intn(4)]
		name := fmt.Sprintf("rslot%02d@ep", i)
		slots = append(slots, machineAd(t, name, cpus))
		pvts = append(pvts, pvtAd(name, claimForSlot(name)))
	}

	// 4 submitters, each with its own loopback schedd, a random priority, and
	// a random request set.
	acct := newAccountant(t)
	var subs []*classad.ClassAd
	scheds := map[string]*negtest.LoopbackSchedd{}
	for s := 0; s < 4; s++ {
		name := fmt.Sprintf("user%d@pool", s)
		if err := acct.SetPriority(name, float64(1+rng.Intn(8))); err != nil {
			t.Fatal(err)
		}
		nGroups := 2 + rng.Intn(3)
		var groups []negtest.Group
		totalJobs := 0
		for g := 0; g < nGroups; g++ {
			count := 1 + rng.Intn(3)
			cpus := int64(1 + rng.Intn(2))
			groups = append(groups, group(t, 100*(s+1)+g, 1000*(s+1)+g, count, cpus, ""))
			totalJobs += count
		}
		sched := startSchedd(t, ctx, [][]negtest.Group{groups})
		scheds[name] = sched
		subs = append(subs, submitterAd(name, fmt.Sprintf("schedd%d", s), sched.Addr(), int64(totalJobs)))
	}

	src := newFixedSource(t, slots, subs, pvts)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	cfg.CompatMode = compat
	cyc, err := New(src, acct, cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := cyc.Run(ctx); err != nil {
		t.Fatalf("Run(compat=%v, seed=%d): %v", compat, seed, err)
	}

	out := runOutcome{Matches: map[string][]string{}, Rejects: map[string]int{}}
	for name, sched := range scheds {
		waitSched(t, ctx, cf, sched)
		out.Matches[name] = append([]string{}, matchesByOwner(sched)[name]...)
		out.Rejects[name] = totalRejects(sched)
	}
	return out
}

// runRandomizedPreempt is like runRandomized but seeds a mix of unclaimed and
// claimed slots (claimed by a low-priority "victim@pool") and a
// PREEMPTION_REQUIREMENTS policy, so the candidate scan produces a mix of
// NO_PREEMPTION and PRIO_PREEMPTION tiers. It exercises the determinism contract
// (sharded == serial) WITH preemption active: tier classification is a pure
// function of (request, candidate, limits), so compat and fast must still agree.
func runRandomizedPreempt(t *testing.T, seed int64, compat bool) runOutcome {
	t.Helper()
	ctx := testCtx(t)
	rng := rand.New(rand.NewSource(seed))

	acct := newAccountant(t)
	// The running (victim) user has a poor priority so preemption is permitted.
	if err := acct.SetPriority("victim@pool", 50); err != nil {
		t.Fatal(err)
	}

	var slots, pvts []*classad.ClassAd
	for i := 0; i < 12; i++ {
		cpus := []int64{1, 1, 2, 4}[rng.Intn(4)]
		name := fmt.Sprintf("pslot%02d@ep", i)
		ad := machineAd(t, name, cpus)
		// About half the slots are claimed by the victim (indifferent machine
		// rank: Rank 0 == CurrentRank 0 -> rankCondStd false, prio-preempt path).
		if rng.Intn(2) == 0 {
			ad.InsertAttrString("State", "Claimed")
			ad.InsertAttrString("RemoteUser", "victim@pool")
			ad.InsertAttrFloat("Rank", 0)
			ad.InsertAttrFloat("CurrentRank", 0)
		}
		slots = append(slots, ad)
		pvts = append(pvts, pvtAd(name, claimForSlot(name)))
	}

	var subs []*classad.ClassAd
	scheds := map[string]*negtest.LoopbackSchedd{}
	for s := 0; s < 3; s++ {
		name := fmt.Sprintf("good%d@pool", s)
		// Good priorities (1..8), all better than the victim's 50.
		if err := acct.SetPriority(name, float64(1+rng.Intn(8))); err != nil {
			t.Fatal(err)
		}
		nGroups := 2 + rng.Intn(3)
		var groups []negtest.Group
		totalJobs := 0
		for g := 0; g < nGroups; g++ {
			count := 1 + rng.Intn(3)
			cpus := int64(1 + rng.Intn(2))
			groups = append(groups, group(t, 100*(s+1)+g, 1000*(s+1)+g, count, cpus, ""))
			totalJobs += count
		}
		sched := startSchedd(t, ctx, [][]negtest.Group{groups})
		scheds[name] = sched
		subs = append(subs, submitterAd(name, fmt.Sprintf("schedd%d", s), sched.Addr(), int64(totalJobs)))
	}

	src := newFixedSource(t, slots, subs, pvts)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig() // ConsiderPreemption defaults true
	cfg.CompatMode = compat
	cfg.PreemptionRequirements = "TARGET.SubmitterUserPrio < MY.RemoteUserPrio"
	cyc, err := New(src, acct, cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := cyc.Run(ctx); err != nil {
		t.Fatalf("Run(compat=%v, seed=%d): %v", compat, seed, err)
	}

	out := runOutcome{Matches: map[string][]string{}, Rejects: map[string]int{}}
	for name, sched := range scheds {
		waitSched(t, ctx, cf, sched)
		out.Matches[name] = append([]string{}, matchesByOwner(sched)[name]...)
		out.Rejects[name] = totalRejects(sched)
	}
	return out
}

// TestCompatFastEqualityWithPreemption is the determinism contract under
// preemption: compat and fast must deliver identical per-submitter ordered
// match lists even when the pool has claimed (preemptable) slots and a
// PREEMPTION_REQUIREMENTS policy in force.
func TestCompatFastEqualityWithPreemption(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	sawMatch := false
	for iter := 0; iter < 12; iter++ {
		seed := int64(9000 + iter)
		compat := runRandomizedPreempt(t, seed, true)
		fast := runRandomizedPreempt(t, seed, false)
		for _, ms := range compat.Matches {
			if len(ms) > 0 {
				sawMatch = true
			}
		}
		if !reflect.DeepEqual(compat.Matches, fast.Matches) {
			t.Fatalf("iteration %d (seed %d): match lists diverge under preemption\ncompat: %v\nfast:   %v",
				iter, seed, compat.Matches, fast.Matches)
		}
		if !reflect.DeepEqual(compat.Rejects, fast.Rejects) {
			t.Fatalf("iteration %d (seed %d): reject counts diverge under preemption\ncompat: %v\nfast:   %v",
				iter, seed, compat.Rejects, fast.Rejects)
		}
	}
	if !sawMatch {
		t.Fatal("degenerate: no matches produced across preemption fixtures")
	}
}

// TestCompatFastEquality is the headline determinism test: on 20 seeded
// random fixtures, compat (fully serial) and fast (concurrent prefetch +
// async delivery) mode deliver IDENTICAL per-submitter ordered match lists.
func TestCompatFastEquality(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	for iter := 0; iter < 20; iter++ {
		seed := int64(4000 + iter)
		compat := runRandomized(t, seed, true)
		fast := runRandomized(t, seed, false)
		total := 0
		for _, ms := range compat.Matches {
			total += len(ms)
		}
		if total == 0 {
			t.Fatalf("iteration %d (seed %d): degenerate fixture produced no matches", iter, seed)
		}
		if !reflect.DeepEqual(compat.Matches, fast.Matches) {
			t.Fatalf("iteration %d (seed %d): match lists diverge\ncompat: %v\nfast:   %v",
				iter, seed, compat.Matches, fast.Matches)
		}
		if !reflect.DeepEqual(compat.Rejects, fast.Rejects) {
			t.Fatalf("iteration %d (seed %d): reject counts diverge\ncompat: %v\nfast:   %v",
				iter, seed, compat.Rejects, fast.Rejects)
		}
	}
}
