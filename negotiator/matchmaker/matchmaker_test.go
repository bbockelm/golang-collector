package matchmaker

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// ---- helpers ---------------------------------------------------------------

func mustAd(t testing.TB, s string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return ad
}

func viewOf(slots ...*classad.ClassAd) *SlotView {
	return NewSlotView(&negotiator.PoolSnapshot{Slots: slots})
}

func reqOf(ad *classad.ClassAd) *negotiator.Request {
	return &negotiator.Request{Ad: ad, Count: 1}
}

// openLimits permits every candidate (huge submitter limit, no ceiling).
func openLimits() *negotiator.MatchLimits {
	return &negotiator.MatchLimits{
		SubmitterLimit: 1e9,
		LimitUsed:      0,
		PieLeft:        1e9,
		Ceiling:        math.MaxFloat64,
	}
}

func mustNew(t testing.TB, cfg Config) *Matchmaker {
	t.Helper()
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

// ---- ranking ---------------------------------------------------------------

// TestRankingTable crafts machines with explicit PreJobRank/Rank/PostJobRank
// values, asserting the winner and the full ranked order (obtained by repeated
// Match + Consume of the static slots). Ties are resolved by first-seen
// (ScanIndex ascending).
func TestRankingTable(t *testing.T) {
	// Rank config: PreJobRank = machine.Pre (MY=machine); PostJobRank =
	// machine.Post. Job Rank = TARGET.R (the machine's R attr).
	cfg := Config{PreJobRank: "Pre", PostJobRank: "Post"}

	// Each slot: (Pre, R, Post). Lexicographic "more is better": Pre, then R,
	// then Post, then ScanIndex ascending on a full tie.
	type slot struct{ pre, r, post float64 }
	slots := []slot{
		{1, 0, 0}, // idx0
		{2, 0, 0}, // idx1  <- highest Pre -> overall winner
		{1, 5, 0}, // idx2  beats idx0 on R
		{1, 5, 9}, // idx3  beats idx2 on Post
		{1, 5, 9}, // idx4  ties idx3 exactly -> loses on ScanIndex
		{2, 0, 0}, // idx5  ties idx1 exactly -> loses on ScanIndex
	}
	// Expected consume order (descending quality, ties by index):
	// idx1 (Pre2) , idx5 (Pre2 tie later) , idx3 (Pre1,R5,Post9) , idx4 (tie) ,
	// idx2 (Pre1,R5,Post0) , idx0 (Pre1,R0,Post0).
	want := []int{1, 5, 3, 4, 2, 0}

	var ads []*classad.ClassAd
	for _, s := range slots {
		ads = append(ads, mustAd(t, fmt.Sprintf(
			"[ Requirements = true; Pre = %g; R = %g; Post = %g ]", s.pre, s.r, s.post)))
	}
	view := viewOf(ads...)
	job := reqOf(mustAd(t, "[ Requirements = true; Rank = TARGET.R ]"))

	m := mustNew(t, cfg)
	var got []int
	for {
		c, rej, err := m.Match(context.Background(), job, view, openLimits())
		if err != nil {
			t.Fatalf("Match: %v", err)
		}
		if c == nil {
			if rej == nil || rej.Reason != reasonNoMatch {
				t.Fatalf("expected no-match reason, got %+v", rej)
			}
			break
		}
		got = append(got, c.ScanIndex)
		view.Consume(c.ScanIndex)
	}

	if len(got) != len(want) {
		t.Fatalf("order length: got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ranked order mismatch:\n got  %v\n want %v", got, want)
		}
	}
}

// TestRankUnsetDefaults verifies that a candidate whose configured rank
// expression fails to evaluate sorts below one that evaluates (rankUnset =
// -DBL_MAX), and that job Rank defaults to 0.0 when absent.
func TestRankUnsetDefaults(t *testing.T) {
	cfg := Config{PreJobRank: "Pre"}
	// idx0 has no Pre attr -> rankUnset; idx1 has Pre=1 -> wins.
	view := viewOf(
		mustAd(t, "[ Requirements = true ]"),
		mustAd(t, "[ Requirements = true; Pre = 1 ]"),
	)
	job := reqOf(mustAd(t, "[ Requirements = true ]")) // no Rank attr -> 0.0
	m := mustNew(t, cfg)
	c, _, err := m.Match(context.Background(), job, view, openLimits())
	if err != nil {
		t.Fatal(err)
	}
	if c == nil || c.ScanIndex != 1 {
		t.Fatalf("want winner idx1 (has Pre), got %+v", c)
	}
	if c.PreJobRank != 1 {
		t.Fatalf("PreJobRank: got %v want 1", c.PreJobRank)
	}
	if c.Rank != 0.0 {
		t.Fatalf("job Rank default: got %v want 0", c.Rank)
	}
	if c.PreemptTier != noPreemption {
		t.Fatalf("PreemptTier: got %d want %d", c.PreemptTier, noPreemption)
	}
}

// ---- bilateral requirements ------------------------------------------------

func TestBilateralRequirements(t *testing.T) {
	// Job requires the machine have Memory >= 2048.
	// Machine requires the job's RequestCpus <= its Cpus.
	job := reqOf(mustAd(t, "[ Requirements = TARGET.Memory >= 2048; RequestCpus = 4 ]"))

	cases := []struct {
		name  string
		slot  string
		match bool
	}{
		{"both satisfied", "[ Requirements = TARGET.RequestCpus <= Cpus; Memory = 4096; Cpus = 8 ]", true},
		{"job req fails (low memory)", "[ Requirements = TARGET.RequestCpus <= Cpus; Memory = 1024; Cpus = 8 ]", false},
		{"machine req fails (too few cpus)", "[ Requirements = TARGET.RequestCpus <= Cpus; Memory = 4096; Cpus = 2 ]", false},
	}
	m := mustNew(t, Config{})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			view := viewOf(mustAd(t, tc.slot))
			c, rej, err := m.Match(context.Background(), job, view, openLimits())
			if err != nil {
				t.Fatal(err)
			}
			if tc.match && c == nil {
				t.Fatalf("expected match, got reject %+v", rej)
			}
			if !tc.match {
				if c != nil {
					t.Fatalf("expected no match, got candidate idx %d", c.ScanIndex)
				}
				if rej == nil || rej.Reason != reasonNoMatch {
					t.Fatalf("expected %q, got %+v", reasonNoMatch, rej)
				}
			}
		})
	}
}

// ---- submitter-limit gate --------------------------------------------------

func TestSubmitterLimitPermitsUnit(t *testing.T) {
	cases := []struct {
		name             string
		used, allowed, c float64
		want             bool
	}{
		{"fits exactly", 0, 1, 1, true},
		{"fits under", 0.5, 2, 1, true},
		{"over but round-up (used<=0, allowed>0)", 0, 0.5, 1, true},
		{"over, used>0 -> blocked", 1, 1.5, 1, false},
		{"over, allowed 0 -> blocked", 0, 0, 1, false},
		{"negative used still round-up", -0.1, 0.5, 5, true},
	}
	for _, tc := range cases {
		if got := submitterLimitPermits(tc.used, tc.allowed, tc.c); got != tc.want {
			t.Errorf("%s: submitterLimitPermits(%g,%g,%g)=%v want %v",
				tc.name, tc.used, tc.allowed, tc.c, got, tc.want)
		}
	}
}

// TestSubmitterLimitGateThroughMatch drives the gate via Match, exercising the
// round-up rule and the pieLeft-cap interaction (SubmitterLimit is pre-capped at
// pieLeft by the cycle, so a small SubmitterLimit models an exhausted pie).
func TestSubmitterLimitGateThroughMatch(t *testing.T) {
	m := mustNew(t, Config{})
	// SlotWeight defaults to Cpus (=2) via our GetSlotWeight fallback.
	slot := "[ Requirements = true; Cpus = 2 ]"
	job := reqOf(mustAd(t, "[ Requirements = true ]"))

	t.Run("round-up permits first slot", func(t *testing.T) {
		view := viewOf(mustAd(t, slot))
		lim := &negotiator.MatchLimits{SubmitterLimit: 0.5, LimitUsed: 0, PieLeft: 0.5, Ceiling: math.MaxFloat64}
		c, _, err := m.Match(context.Background(), job, view, lim)
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			t.Fatal("round-up rule should permit the first weighted slot")
		}
	})

	t.Run("blocked once used>0 and over limit", func(t *testing.T) {
		view := viewOf(mustAd(t, slot))
		lim := &negotiator.MatchLimits{SubmitterLimit: 0.5, LimitUsed: 1.0, PieLeft: 0.5, Ceiling: math.MaxFloat64}
		c, rej, err := m.Match(context.Background(), job, view, lim)
		if err != nil {
			t.Fatal(err)
		}
		if c != nil {
			t.Fatalf("expected submitter-limit rejection, got candidate idx %d", c.ScanIndex)
		}
		if rej == nil || rej.Reason != reasonSubmitterLimit {
			t.Fatalf("expected %q, got %+v", reasonSubmitterLimit, rej)
		}
		if rej.ForSubmitterLimit != 1 {
			t.Fatalf("ForSubmitterLimit: got %d want 1", rej.ForSubmitterLimit)
		}
	})

	t.Run("permitted when it fits under the limit", func(t *testing.T) {
		view := viewOf(mustAd(t, slot))
		lim := &negotiator.MatchLimits{SubmitterLimit: 10, LimitUsed: 3, PieLeft: 10, Ceiling: math.MaxFloat64}
		c, _, err := m.Match(context.Background(), job, view, lim)
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			t.Fatal("3 + 2 <= 10 should permit")
		}
	})
}

// ---- slot weight -----------------------------------------------------------

func TestSlotWeight(t *testing.T) {
	m := mustNew(t, Config{})
	cases := []struct {
		ad   string
		want float64
	}{
		{"[ SlotWeight = 3.5; Cpus = 8 ]", 3.5}, // explicit weight
		{"[ Cpus = 4 ]", 4},                     // fallback to Cpus
		{"[ SlotWeight = -1; Cpus = 6 ]", 6},    // negative weight -> Cpus
		{"[ Foo = 1 ]", 1.0},                    // neither -> 1.0
		{"[ SlotWeight = 2 * 3; Cpus = 1 ]", 6},                       // constant expression
		{"[ SlotWeight = Cpus * 2; Cpus = 8 ]", 16},                   // references a machine attr
		{"[ SlotWeight = Cpus + Memory / 1024; Cpus = 4; Memory = 2048 ]", 6}, // multi-attr cost expression
		{"[ SlotWeight = ifThenElse(Gpus > 0, Gpus * 10, Cpus); Gpus = 2; Cpus = 4 ]", 20}, // conditional cost
	}
	for _, tc := range cases {
		if got := m.slotWeight(mustAd(t, tc.ad)); got != tc.want {
			t.Errorf("slotWeight(%s)=%g want %g", tc.ad, got, tc.want)
		}
	}
	// Disabled slot weights always cost 1.0.
	md := mustNew(t, Config{DisableSlotWeights: true})
	if got := md.slotWeight(mustAd(t, "[ SlotWeight = 5; Cpus = 8 ]")); got != 1.0 {
		t.Errorf("disabled slot weights: got %g want 1.0", got)
	}
}

// ---- p-slot persistence ----------------------------------------------------

func TestPslotPersistence(t *testing.T) {
	// idx0 is a partitionable slot, idx1 is static. Both match; equal ranks so
	// first-seen (idx0, the pslot) always wins.
	view := viewOf(
		mustAd(t, "[ Requirements = true; PartitionableSlot = true; Name = \"pslot\"; StartdIpAddr = \"a\" ]"),
		mustAd(t, "[ Requirements = true; Name = \"static\"; StartdIpAddr = \"b\" ]"),
	)
	job := reqOf(mustAd(t, "[ Requirements = true ]"))
	m := mustNew(t, Config{})

	// Match twice in a row; the pslot must persist and win both times.
	for i := 0; i < 2; i++ {
		c, _, err := m.Match(context.Background(), job, view, openLimits())
		if err != nil {
			t.Fatal(err)
		}
		if c == nil || c.ScanIndex != 0 {
			t.Fatalf("round %d: want pslot idx0, got %+v", i, c)
		}
		view.Consume(c.ScanIndex) // consuming a pslot is a no-op
	}
	if view.Len() != 2 {
		t.Fatalf("pslot consume should not shrink the view: Len=%d want 2", view.Len())
	}

	// Now consume the static slot; it must disappear.
	view.Consume(1)
	if view.Len() != 1 {
		t.Fatalf("static consume: Len=%d want 1", view.Len())
	}
	// The only live slot left is the pslot at idx0.
	seen := map[int]bool{}
	view.Scan(func(i int, _ *classad.ClassAd) bool { seen[i] = true; return true })
	if !seen[0] || seen[1] {
		t.Fatalf("after consuming static: live set %v want {0}", seen)
	}
}

// ---- claim id lookup -------------------------------------------------------

func TestClaimIDKey(t *testing.T) {
	slot := mustAd(t, "[ Name = \"slot1@host\"; StartdIpAddr = \"<1.2.3.4:9618>\" ]")
	snap := &negotiator.PoolSnapshot{
		Slots:    []*classad.ClassAd{slot},
		ClaimIDs: map[string]string{"slot1@host<1.2.3.4:9618>": "secret-claim"},
	}
	sv := NewSlotView(snap)
	id, ok := sv.ClaimID(slot)
	if !ok || id != "secret-claim" {
		t.Fatalf("ClaimID: got (%q,%v) want (secret-claim,true)", id, ok)
	}
	// Missing entry.
	other := mustAd(t, "[ Name = \"nope\"; StartdIpAddr = \"x\" ]")
	if _, ok := sv.ClaimID(other); ok {
		t.Fatal("expected miss for unknown slot")
	}
}

// ---- determinism: serial vs sharded ---------------------------------------

// TestDeterminismSerialVsSharded builds a 1000-slot view with random rank
// values and asserts that the serial matchmaker and sharded matchmakers (4, 8,
// 16 workers) pick the IDENTICAL winner across 50 request rounds. Run under
// -race. Because Candidate.Better tie-breaks on ScanIndex, the reduce is
// order-independent, so all configurations must agree.
func TestDeterminismSerialVsSharded(t *testing.T) {
	const nSlots = 1000
	const rounds = 50

	cfg := Config{PreJobRank: "Pre", PostJobRank: "Post"}
	serial := mustNew(t, Config{PreJobRank: cfg.PreJobRank, PostJobRank: cfg.PostJobRank, Serial: true})
	sharded := map[int]*Matchmaker{}
	for _, w := range []int{4, 8, 16} {
		sharded[w] = mustNew(t, Config{PreJobRank: cfg.PreJobRank, PostJobRank: cfg.PostJobRank, Workers: w})
	}

	rng := rand.New(rand.NewSource(0xC0FFEE))
	job := reqOf(mustAd(t, "[ Requirements = true; Rank = TARGET.R ]"))

	for round := 0; round < rounds; round++ {
		// Deterministically regenerate the slot ranks for this round. Use a
		// coarse value set so ties are common (stresses the tie-break).
		ads := make([]*classad.ClassAd, nSlots)
		for i := 0; i < nSlots; i++ {
			pre := rng.Intn(3)
			r := rng.Intn(4)
			post := rng.Intn(3)
			ads[i] = mustAd(t, fmt.Sprintf(
				"[ Requirements = true; Pre = %d; R = %d; Post = %d ]", pre, r, post))
		}
		newView := func() *SlotView { return viewOf(ads...) }

		wantC, _, err := serial.Match(context.Background(), job, newView(), openLimits())
		if err != nil {
			t.Fatal(err)
		}
		if wantC == nil {
			t.Fatalf("round %d: serial found no match", round)
		}
		for w, mm := range sharded {
			gotC, _, err := mm.Match(context.Background(), job, newView(), openLimits())
			if err != nil {
				t.Fatal(err)
			}
			if gotC == nil || gotC.ScanIndex != wantC.ScanIndex {
				t.Fatalf("round %d workers=%d: winner idx %v != serial idx %d",
					round, w, gotC, wantC.ScanIndex)
			}
			// Rank tuple must be identical too, not just the index.
			if gotC.PreJobRank != wantC.PreJobRank || gotC.Rank != wantC.Rank || gotC.PostJobRank != wantC.PostJobRank {
				t.Fatalf("round %d workers=%d: rank tuple mismatch %+v vs %+v", round, w, gotC, wantC)
			}
		}
	}
}

// ---- benchmark -------------------------------------------------------------

func BenchmarkMatch(b *testing.B) {
	const nSlots = 10000
	rng := rand.New(rand.NewSource(1))
	ads := make([]*classad.ClassAd, nSlots)
	for i := 0; i < nSlots; i++ {
		ads[i] = mustAd(b, fmt.Sprintf(
			"[ Requirements = TARGET.RequestCpus <= Cpus; Pre = %d; R = %d; Post = %d; Cpus = %d; SlotWeight = %d ]",
			rng.Intn(5), rng.Intn(5), rng.Intn(5), 1+rng.Intn(16), 1+rng.Intn(16)))
	}
	view := viewOf(ads...)
	job := reqOf(mustAd(b, "[ Requirements = true; Rank = TARGET.R; RequestCpus = 1 ]"))
	m := mustNew(b, Config{PreJobRank: "Pre", PostJobRank: "Post"})
	lim := openLimits()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, _, err := m.Match(ctx, job, view, lim)
		if err != nil || c == nil {
			b.Fatalf("match failed: c=%v err=%v", c, err)
		}
	}
}

// ---- synthetic-pool sanity + scale benchmarks (roadmap #5) -----------------

// TestGeneratedPoolSanity guards the negtest.Generate synthetic-pool generator
// the scale benchmarks lean on: it must be deterministic and produce a
// realistic mix -- every request finds a match, but only a fraction of
// candidates pass bilateral Requirements (so the scan does a genuine mix of
// short-circuit-at-Symmetry and full rank-tuple evaluation, not one or the
// other). If this drifts, the benchmarks stop measuring the intended hot path.
func TestGeneratedPoolSanity(t *testing.T) {
	params := negtest.GenParams{
		Slots: 2000, PartitionableFrac: 0.2,
		Submitters: 10, Schedds: 3, RRLDepth: 30, AutoClusters: 8, Seed: 1,
	}
	pool := negtest.Generate(params)

	// Determinism: a second Generate with the same params yields identical ads.
	pool2 := negtest.Generate(params)
	if got, want := len(pool2.Snapshot.Slots), len(pool.Snapshot.Slots); got != want {
		t.Fatalf("nondeterministic slot count: %d != %d", got, want)
	}
	s1, _ := pool.Snapshot.Slots[0].EvaluateAttrString("Name")
	s2, _ := pool2.Snapshot.Slots[0].EvaluateAttrString("Name")
	if s1 != s2 {
		t.Fatalf("nondeterministic slot naming: %q != %q", s1, s2)
	}

	// Per-candidate selectivity: count how many slots satisfy the first request's
	// bilateral Requirements. Expect a middle band, not 0% or 100%.
	req := pool.AllRequests()[0]
	mc := classad.NewMatchClassAd(cloneAd(req.Ad), nil)
	pass := 0
	for _, s := range pool.Snapshot.Slots {
		mc.ReplaceRightAd(s)
		if mc.Symmetry("Requirements", "Requirements") {
			pass++
		}
	}
	frac := float64(pass) / float64(len(pool.Snapshot.Slots))
	if frac <= 0.05 || frac >= 0.95 {
		t.Fatalf("candidate pass fraction %.2f outside realistic band (0.05,0.95)", frac)
	}

	// Request-level: the representative request should still find a best match.
	m := mustNew(t, Config{PreJobRank: "Cpus", PostJobRank: "Memory"})
	c, _, err := m.Match(context.Background(), req, NewSlotView(pool.Snapshot), openLimits())
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("representative request found no match")
	}
}

// BenchmarkMatchScale measures the matchmaker's per-request hot path
// (Matchmaker.Match) on synthetic pools at 10k and 100k candidate slots, in both
// serial (compat) and sharded modes. The rank config exercises PreJobRank
// (machine Cpus) and PostJobRank (machine Memory), and the generated job ads
// carry a Rank (TARGET.Cpus), so all three rank evals plus bilateral Symmetry
// run per candidate -- the alloc-dominated path roadmap #5 targets. The serial
// vs sharded split quantifies the concurrency thesis (NEGOTIATOR_CPP_DIFFERENCES
// §1) on a single request's scan. ReportAllocs surfaces allocs/op.
func BenchmarkMatchScale(b *testing.B) {
	ctx := context.Background()
	for _, nSlots := range []int{10000, 100000} {
		pool := negtest.Generate(negtest.GenParams{
			Slots: nSlots, PartitionableFrac: 0.2,
			Submitters: 50, Schedds: 10, RRLDepth: 40, AutoClusters: 8, Seed: 1,
		})
		view := NewSlotView(pool.Snapshot)
		reqs := pool.AllRequests()
		lim := openLimits()

		for _, mode := range []struct {
			name string
			cfg  Config
		}{
			{"serial", Config{PreJobRank: "Cpus", PostJobRank: "Memory", Serial: true}},
			{"sharded", Config{PreJobRank: "Cpus", PostJobRank: "Memory"}},
		} {
			m := mustNew(b, mode.cfg)
			b.Run(fmt.Sprintf("slots=%d/%s", nSlots, mode.name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Rotate through the whole request set so the scan sees the
					// full autocluster mix, not one hot request.
					if _, _, err := m.Match(ctx, reqs[i%len(reqs)], view, lim); err != nil {
						b.Fatalf("match error: %v", err)
					}
				}
			})
		}
	}
}

// BenchmarkPieSpin measures matchmaking throughput for a full pie spin: one
// submitter's entire resource-request list matched against a 10k-slot pool with
// consumption between requests (static winners drop out; p-slots persist). It is
// the closest microbenchmark to a cycle's matchmaking phase without the NEGOTIATE
// protocol. The per-iteration SlotView rebuild is excluded from the timer.
func BenchmarkPieSpin(b *testing.B) {
	ctx := context.Background()
	pool := negtest.Generate(negtest.GenParams{
		Slots: 10000, PartitionableFrac: 0.2,
		Submitters: 20, Schedds: 5, RRLDepth: 50, AutoClusters: 8, Seed: 7,
	})
	reqs := pool.Requests[pool.SubmitterNames[0]]
	m := mustNew(b, Config{PreJobRank: "Cpus", PostJobRank: "Memory"})
	lim := openLimits()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		view := NewSlotView(pool.Snapshot) // fresh view: Consume mutates it
		b.StartTimer()
		for _, req := range reqs {
			c, _, err := m.Match(ctx, req, view, lim)
			if err != nil {
				b.Fatalf("match error: %v", err)
			}
			if c != nil {
				view.Consume(c.ScanIndex)
			}
		}
	}
}
