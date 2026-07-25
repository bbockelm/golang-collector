package cycle

import (
	"math"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// gjpSubmitterAd builds a submitter ad optionally carrying a JobPrioArray.
func gjpSubmitterAd(name, scheddAddr, prioArray string) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("ScheddName", "schedd_"+name)
	ad.InsertAttrString("ScheddIpAddr", scheddAddr)
	ad.InsertAttr("IdleJobs", 5)
	if prioArray != "" {
		ad.InsertAttrString(attrJobPrioArray, prioArray)
	}
	return ad
}

func TestFanOutJobPrios(t *testing.T) {
	ads := []*classad.ClassAd{
		gjpSubmitterAd("alice", "<a>", "5,3,1"), // 3 rounds, high-to-low
		gjpSubmitterAd("bob", "<b>", ""),        // no array -> single INT_MIN round
	}
	out := fanOutJobPrios(ads)

	// 3 (alice) + 1 (bob) = 4 rounds, order preserved.
	if len(out) != 4 {
		t.Fatalf("got %d rounds, want 4", len(out))
	}
	wantPrio := []int64{5, 3, 1, math.MinInt32}
	wantName := []string{"alice", "alice", "alice", "bob"}
	for i, ad := range out {
		jp, ok := ad.EvaluateAttrInt(attrJobPrio)
		if !ok {
			t.Fatalf("round %d: no JobPrio", i)
		}
		if jp != wantPrio[i] {
			t.Errorf("round %d: JobPrio = %d, want %d", i, jp, wantPrio[i])
		}
		if n, _ := ad.EvaluateAttrString("Name"); n != wantName[i] {
			t.Errorf("round %d: Name = %q, want %q", i, n, wantName[i])
		}
		_, hasArr := ad.Lookup(attrJobPrioArray)
		if wantName[i] == "alice" && !hasArr {
			t.Errorf("round %d: alice copy lost JobPrioArray", i)
		}
		if wantName[i] == "bob" && hasArr {
			t.Errorf("round %d: bob copy should have no JobPrioArray", i)
		}
	}

	// copyAd must not mutate the source: alice's original still carries no JobPrio.
	if _, ok := ads[0].EvaluateAttrInt(attrJobPrio); ok {
		t.Error("fanOutJobPrios mutated the source ad (JobPrio leaked back)")
	}
}

// TestFanOutJobPriosEdgeCases pins the C++ atoi + StringTokenIterator semantics:
// a present-but-empty array drops the submitter (zero rounds), a malformed token
// becomes a JobPrio-0 round, and empty tokens between commas are skipped.
func TestFanOutJobPriosEdgeCases(t *testing.T) {
	t.Run("present empty array drops submitter", func(t *testing.T) {
		ad := gjpSubmitterAd("alice", "<a>", "x") // placeholder to force the attr...
		ad.InsertAttrString(attrJobPrioArray, "") // ...then set it empty
		if out := fanOutJobPrios([]*classad.ClassAd{ad}); len(out) != 0 {
			t.Fatalf("present-empty array: got %d rounds, want 0 (submitter dropped)", len(out))
		}
	})

	t.Run("malformed token becomes zero", func(t *testing.T) {
		ad := gjpSubmitterAd("alice", "<a>", "5,abc,3")
		out := fanOutJobPrios([]*classad.ClassAd{ad})
		got := make([]int64, len(out))
		for i, a := range out {
			got[i], _ = a.EvaluateAttrInt(attrJobPrio)
		}
		want := []int64{5, 0, 3} // atoi("abc") == 0
		if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("empty tokens skipped", func(t *testing.T) {
		ad := gjpSubmitterAd("alice", "<a>", "5,,3,") // double + trailing comma
		if out := fanOutJobPrios([]*classad.ClassAd{ad}); len(out) != 2 {
			t.Fatalf("got %d rounds, want 2 (empty tokens skipped)", len(out))
		}
	})
}

func TestAtoiC(t *testing.T) {
	cases := map[string]int{"5": 5, "-3": -3, "+7": 7, "abc": 0, "5x": 5, "": 0, "-": 0, "10a2": 10}
	for in, want := range cases {
		if got := atoiC(in); got != want {
			t.Errorf("atoiC(%q) = %d, want %d", in, got, want)
		}
	}
}

// TestFanOutIdleJobsDedup verifies NumIdleJobs counts a fanned-out submitter's
// idle jobs once (by name), not once per job-priority round.
func TestFanOutIdleJobsDedup(t *testing.T) {
	newState := func(gjp bool) *runState {
		st := &runState{
			stats: &negotiator.CycleStats{},
			subs:  make(map[*classad.ClassAd]*subState),
		}
		if gjp {
			st.idleCounted = make(map[string]struct{})
		}
		return st
	}

	t.Run("on: deduped by name", func(t *testing.T) {
		c := &Cycle{cfg: Config{WantGlobalJobPrio: true}}
		ad := gjpSubmitterAd("alice", "<a>", "5,3,1") // IdleJobs=5, 3 rounds
		st := newState(true)
		c.wrapSubmitters(st, fanOutJobPrios([]*classad.ClassAd{ad}))
		if st.stats.IdleJobs != 5 {
			t.Errorf("IdleJobs = %d, want 5 (deduped, not 15)", st.stats.IdleJobs)
		}
	})

	t.Run("off: plain accumulation unchanged", func(t *testing.T) {
		// Off path with two same-name ads still sums, byte-identical to before.
		c := &Cycle{cfg: Config{WantGlobalJobPrio: false}}
		a1 := gjpSubmitterAd("alice", "<a1>", "")
		a2 := gjpSubmitterAd("alice", "<a2>", "")
		st := newState(false)
		c.wrapSubmitters(st, []*classad.ClassAd{a1, a2})
		if st.stats.IdleJobs != 10 {
			t.Errorf("IdleJobs = %d, want 10 (off path sums per ad)", st.stats.IdleJobs)
		}
	})
}

// sub is a compact subState builder for the consolidation/sort tests.
func gjpSub(name, addr string, prio int, hasArray bool, origIdx int) *subState {
	return &subState{
		name: name, scheddAddr: addr,
		jobPrio: prio, jobPrioMin: prio, jobPrioMax: prio,
		hasJobPrioArray: hasArray, origIdx: origIdx,
	}
}

func TestConsolidateJobPrios(t *testing.T) {
	// Interleaved: alice@10, bob@5, alice@1 -- no two adjacent share a submitter,
	// so all three rounds survive (the whole point of the feature).
	t.Run("interleaved survives", func(t *testing.T) {
		subs := []*subState{
			gjpSub("alice", "<a>", 10, true, 0),
			gjpSub("bob", "<b>", 5, true, 1),
			gjpSub("alice", "<a>", 1, true, 2),
		}
		out := consolidateJobPrios(subs)
		if len(out) != 3 {
			t.Fatalf("got %d rounds, want 3 (interleaved must not merge)", len(out))
		}
	})

	// Contiguous: alice@10, alice@9, alice@1 collapse to one round spanning [1,10].
	t.Run("contiguous merges", func(t *testing.T) {
		subs := []*subState{
			gjpSub("alice", "<a>", 10, true, 0),
			gjpSub("alice", "<a>", 9, true, 1),
			gjpSub("alice", "<a>", 1, true, 2),
		}
		out := consolidateJobPrios(subs)
		if len(out) != 1 {
			t.Fatalf("got %d rounds, want 1", len(out))
		}
		if out[0].jobPrioMin != 1 || out[0].jobPrioMax != 10 {
			t.Errorf("band = [%d,%d], want [1,10]", out[0].jobPrioMin, out[0].jobPrioMax)
		}
		// The survivor keeps the first (highest-prio) round's sort position.
		if out[0].jobPrio != 10 {
			t.Errorf("survivor jobPrio = %d, want 10 (first of the run)", out[0].jobPrio)
		}
	})

	// Same name, different schedd address: not the same origin, never merged.
	t.Run("different schedd not merged", func(t *testing.T) {
		subs := []*subState{
			gjpSub("alice", "<a1>", 10, true, 0),
			gjpSub("alice", "<a2>", 9, true, 1),
		}
		if out := consolidateJobPrios(subs); len(out) != 2 {
			t.Fatalf("got %d rounds, want 2 (distinct schedds)", len(out))
		}
	})

	// A no-array round is never ranged and breaks a run.
	t.Run("no-array passes through and breaks the run", func(t *testing.T) {
		subs := []*subState{
			gjpSub("alice", "<a>", 10, true, 0),
			gjpSub("alice", "<a>", math.MinInt32, false, 1), // no array
			gjpSub("alice", "<a>", 1, true, 2),
		}
		out := consolidateJobPrios(subs)
		if len(out) != 3 {
			t.Fatalf("got %d rounds, want 3 (no-array breaks the run)", len(out))
		}
	})
}

// TestSortSubmittersJobPrioKey verifies the secondary job-priority sort key is
// applied only when USE_GLOBAL_JOB_PRIOS is set. Same user priority for both
// submitters, so the job-prio key decides order when on; when off the field is
// ignored and the stable original order stands (the differential invariant).
func TestSortSubmittersJobPrioKey(t *testing.T) {
	build := func() []*subState {
		return []*subState{
			gjpSub("alice", "<a>", 1, true, 0),  // low prio, first in original order
			gjpSub("alice", "<a>", 10, true, 1), // high prio, second
		}
	}
	env := func() *spinEnv {
		return &spinEnv{prios: map[string]float64{"alice": 5}}
	}

	t.Run("on: higher job prio first", func(t *testing.T) {
		c := &Cycle{cfg: Config{WantGlobalJobPrio: true}}
		subs := build()
		c.sortSubmitters(subs, env())
		if subs[0].jobPrio != 10 || subs[1].jobPrio != 1 {
			t.Errorf("order = [%d,%d], want [10,1]", subs[0].jobPrio, subs[1].jobPrio)
		}
	})

	t.Run("off: original order preserved", func(t *testing.T) {
		c := &Cycle{cfg: Config{WantGlobalJobPrio: false}}
		subs := build()
		c.sortSubmitters(subs, env())
		if subs[0].jobPrio != 1 || subs[1].jobPrio != 10 {
			t.Errorf("order = [%d,%d], want [1,10] (job-prio key must be inert when off)",
				subs[0].jobPrio, subs[1].jobPrio)
		}
	})
}

// TestHeaderForJobPrioBand verifies the NEGOTIATE header carries JOBPRIO_MIN/MAX
// only when the feature is on and the submitter advertised a JobPrioArray.
func TestHeaderForJobPrioBand(t *testing.T) {
	st := &runState{sigAttrs: "RequestCpus"}
	sub := gjpSub("alice", "<a>", 7, true, 0)
	sub.ad = gjpSubmitterAd("alice", "<a>", "7,3")
	sub.jobPrioMin, sub.jobPrioMax = 3, 7

	t.Run("on + array: band set", func(t *testing.T) {
		c := &Cycle{cfg: Config{WantGlobalJobPrio: true}}
		h := c.headerFor(st, sub)
		if !h.HasJobPrio || h.JobPrioMin != 3 || h.JobPrioMax != 7 {
			t.Errorf("header band = {%v,%d,%d}, want {true,3,7}", h.HasJobPrio, h.JobPrioMin, h.JobPrioMax)
		}
	})

	t.Run("on + no array: no band", func(t *testing.T) {
		c := &Cycle{cfg: Config{WantGlobalJobPrio: true}}
		noArr := gjpSub("bob", "<b>", math.MinInt32, false, 0)
		noArr.ad = gjpSubmitterAd("bob", "<b>", "")
		if h := c.headerFor(st, noArr); h.HasJobPrio {
			t.Error("no-array submitter should get no JOBPRIO band")
		}
	})

	t.Run("off: no band", func(t *testing.T) {
		c := &Cycle{cfg: Config{WantGlobalJobPrio: false}}
		if h := c.headerFor(st, sub); h.HasJobPrio {
			t.Error("feature off should never set a JOBPRIO band")
		}
	})

	// Sanity: the header type default is inert.
	var zero negotiator.NegotiateHeader
	if zero.HasJobPrio {
		t.Error("zero NegotiateHeader must have HasJobPrio false")
	}
}
