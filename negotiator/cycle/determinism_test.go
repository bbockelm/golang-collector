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
