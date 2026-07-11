package cycle

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// concJobAd is jobAd plus a ConcurrencyLimits string.
func concJobAd(t *testing.T, requestCpus int64, limits string) *classad.ClassAd {
	t.Helper()
	ad := jobAd(t, requestCpus, "")
	ad.InsertAttrString("ConcurrencyLimits", limits)
	return ad
}

// concGroup builds a request group of count identical jobs carrying a
// ConcurrencyLimits list. All members share one autocluster.
func concGroup(t *testing.T, cluster, autocluster, count int, requestCpus int64, limits string) negtest.Group {
	t.Helper()
	members := make([]negtest.Job, count)
	for i := range members {
		members[i] = negtest.J(cluster, i)
	}
	return negtest.Group{
		RepCluster:    cluster,
		RepProc:       0,
		AutoClusterID: autocluster,
		Members:       members,
		RepAd:         concJobAd(t, requestCpus, limits),
	}
}

// gpuLimitConfig sets GPU_LIMIT=max and leaves everything else unlimited.
func gpuLimitConfig(cfg *Config, max float64) {
	cfg.ConcurrencyLimitMax = func(name string) float64 {
		if name == "gpu" {
			return max
		}
		return math.MaxFloat64
	}
}

// TestConcurrencyLimitWithinCycle: a submitter with several GPU jobs matches
// exactly GPU_LIMIT of them in one cycle; the request that would exceed the
// limit is rejected (concurrency), which marks the shared autocluster rejected
// so the rest are skipped. Proves in-cycle usage grows as matches commit and
// later same-limit requests are gated.
func TestConcurrencyLimitWithinCycle(t *testing.T) {
	ctx := testCtx(t)

	var slots, pvts []*classad.ClassAd
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("gslot%02d@ep", i)
		slots = append(slots, machineAd(t, name, 1))
		pvts = append(pvts, pvtAd(name, claimForSlot(name)))
	}

	acct := newAccountant(t)
	name := "gpuuser@pool"
	if err := acct.SetPriority(name, 1); err != nil {
		t.Fatal(err)
	}

	// One group of 5 GPU jobs (one autocluster), GPU_LIMIT=2.
	grp := concGroup(t, 100, 1000, 5, 1, "GPU")
	sched := startSchedd(t, ctx, [][]negtest.Group{{grp}})
	subs := []*classad.ClassAd{submitterAd(name, "schedd0", sched.Addr(), 5)}

	src := newFixedSource(t, slots, subs, pvts)
	cf := newCountingFactory(newFactory())
	cfg := DefaultConfig()
	gpuLimitConfig(&cfg, 2)
	cyc, err := New(src, acct, cf, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := cyc.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	waitSched(t, ctx, cf, sched)

	matches := len(matchesByOwner(sched)[name])
	if matches != 2 {
		t.Fatalf("GPU matches: got %d want 2 (GPU_LIMIT)", matches)
	}
	if totalRejects(sched) < 1 {
		t.Fatalf("expected at least one concurrency rejection, got %d", totalRejects(sched))
	}
	// Cross-cycle count reflects the two committed GPU matches.
	if got := acct.GetLimit("gpu"); got != 2 {
		t.Fatalf("cross-cycle gpu usage: got %g want 2", got)
	}
}

// runConcurrency runs one randomized concurrency-limited cycle in the given
// mode; used by the determinism test.
func runConcurrency(t *testing.T, seed int64, compat bool) runOutcome {
	t.Helper()
	ctx := testCtx(t)
	rng := rand.New(rand.NewSource(seed))

	var slots, pvts []*classad.ClassAd
	for i := 0; i < 12; i++ {
		cpus := []int64{1, 1, 2, 4}[rng.Intn(4)]
		name := fmt.Sprintf("cslot%02d@ep", i)
		slots = append(slots, machineAd(t, name, cpus))
		pvts = append(pvts, pvtAd(name, claimForSlot(name)))
	}

	acct := newAccountant(t)
	limitNames := []string{"gpu", "license", "matlab"}
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
			lim := limitNames[rng.Intn(len(limitNames))]
			groups = append(groups, concGroup(t, 100*(s+1)+g, 1000*(s+1)+g, count, cpus, lim))
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
	// Tight-ish maxes so some requests are actually rejected for concurrency.
	cfg.ConcurrencyLimitMax = func(name string) float64 {
		switch name {
		case "gpu":
			return 3
		case "license":
			return 2
		case "matlab":
			return 4
		}
		return math.MaxFloat64
	}
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

// TestConcurrencyDeterminismCompatFast: with concurrency limits active (and
// rejecting), compat (serial) and fast (concurrent) mode still deliver
// identical per-submitter match lists and reject counts. The gate is a pure
// pre-scan function of (request limits, live usage view), evaluated on the
// single Match spine, so sharding cannot perturb it.
func TestConcurrencyDeterminismCompatFast(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	sawReject := false
	for iter := 0; iter < 12; iter++ {
		seed := int64(7000 + iter)
		compat := runConcurrency(t, seed, true)
		fast := runConcurrency(t, seed, false)
		for _, r := range compat.Rejects {
			if r > 0 {
				sawReject = true
			}
		}
		if !reflect.DeepEqual(compat.Matches, fast.Matches) {
			t.Fatalf("iteration %d (seed %d): match lists diverge with concurrency limits\ncompat: %v\nfast:   %v",
				iter, seed, compat.Matches, fast.Matches)
		}
		if !reflect.DeepEqual(compat.Rejects, fast.Rejects) {
			t.Fatalf("iteration %d (seed %d): reject counts diverge\ncompat: %v\nfast:   %v",
				iter, seed, compat.Rejects, fast.Rejects)
		}
	}
	if !sawReject {
		t.Fatal("degenerate: no concurrency rejections produced across fixtures")
	}
}
