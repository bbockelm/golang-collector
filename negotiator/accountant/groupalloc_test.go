package accountant

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// negoResult captures the per-group callback invocations and the final tree.
type negoResult struct {
	root  *negotiator.GroupNode
	order []string           // group names in callback order
	limit map[string]float64 // last limit passed per group
	count map[string]int     // callback invocations per group
}

// runNegotiate prepares the tree and runs NegotiateAllGroups with a capturing
// callback.
func runNegotiate(t *testing.T, cfg GroupConfig, ads []*classad.ClassAd, total float64, usage func(string) float64) *negoResult {
	t.Helper()
	root, _, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	PrepareForMatchmaking(root, ads, total, cfg, usage)
	res := &negoResult{root: root, limit: map[string]float64{}, count: map[string]int{}}
	err = NegotiateAllGroups(root, total, cfg, usage, nil, func(g *negotiator.GroupNode, gAlloc float64) error {
		res.order = append(res.order, g.Name)
		res.limit[g.Name] = gAlloc
		res.count[g.Name]++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func gAlloc(root *negotiator.GroupNode, name string) float64 {
	return nodeByName(root, name).Allocated
}

// singleRoundSurplus builds a config for pure fairshare+surplus assertions:
// one round, weighted slots (skip round robin), zero usage.
func surplusCfg(names []string, fn func(*GroupConfig)) GroupConfig {
	c := cfgWith(names, fn)
	c.MaxAllocationRounds = 1
	c.UsingWeightedSlots = true
	return c
}

// (a) Cornucopia: surplus >= demand, a surplus-accepting sibling absorbs the
// unused quota of a non-accepting sibling.
func TestSurplusCornucopia(t *testing.T) {
	cfg := surplusCfg([]string{"a", "b"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 10, "b": 10}
		c.GroupAcceptSurplus = map[string]bool{"b": true} // a does NOT accept
	})
	ads := []*classad.ClassAd{subAd("a.u@d", 5, 0), subAd("b.u@d", 15, 0)}
	res := runNegotiate(t, cfg, ads, 20, zeroUsage)
	gApprox(t, "a", gAlloc(res.root, "a"), 5)
	gApprox(t, "b", gAlloc(res.root, "b"), 15) // 10 quota + 5 surplus from a
}

// (b) Scarcity split by quota weights across surplus-accepting siblings.
func TestSurplusScarcityByQuota(t *testing.T) {
	cfg := surplusCfg([]string{"donor", "x", "y"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"donor": 8, "x": 2, "y": 2}
		c.GroupAcceptSurplus = map[string]bool{"donor": true, "x": true, "y": true}
	})
	ads := []*classad.ClassAd{subAd("x.u@d", 100, 0), subAd("y.u@d", 100, 0)}
	res := runNegotiate(t, cfg, ads, 12, zeroUsage)
	gApprox(t, "donor", gAlloc(res.root, "donor"), 0)
	gApprox(t, "x", gAlloc(res.root, "x"), 6) // 2 + 4 (half of 8 surplus, equal quotas)
	gApprox(t, "y", gAlloc(res.root, "y"), 6)
}

// (c) Zero-quota group picks up leftover surplus in the second (non-by-quota) loop.
func TestSurplusZeroQuotaSecondLoop(t *testing.T) {
	cfg := surplusCfg([]string{"donor", "z"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"donor": 10, "z": 0}
		c.GroupAcceptSurplus = map[string]bool{"donor": true, "z": true}
	})
	ads := []*classad.ClassAd{subAd("z.u@d", 100, 0)}
	res := runNegotiate(t, cfg, ads, 10, zeroUsage)
	gApprox(t, "donor", gAlloc(res.root, "donor"), 0)
	gApprox(t, "z", gAlloc(res.root, "z"), 10) // all surplus via zero-quota mop-up
}

// (d) Parent competes as a peer of its children (equal quota weights -> even split).
func TestSurplusParentCompetesAsPeer(t *testing.T) {
	cfg := surplusCfg([]string{"donor", "a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"donor": 6, "a": 6}
		c.GroupAcceptSurplus = map[string]bool{"donor": true, "a": true}
	})
	// Root has direct demand (submitter with no group) plus child a demand.
	ads := []*classad.ClassAd{subAd("rootuser@d", 100, 0), subAd("a.u@d", 100, 0)}
	res := runNegotiate(t, cfg, ads, 18, zeroUsage)
	// root quota = 18-12 = 6; donor frees 6 surplus; split between a and root
	// (subtree_quota 6 each) -> 3 each on top of their fairshare 6.
	gApprox(t, "a", gAlloc(res.root, "a"), 9)
	gApprox(t, "root", gAlloc(res.root, RootGroupName), 9)
	gApprox(t, "donor", gAlloc(res.root, "donor"), 0)
}

// (e) accept_surplus=false isolates a sibling from the surplus pool.
func TestSurplusAcceptSurplusFalseIsolation(t *testing.T) {
	cfg := surplusCfg([]string{"donor", "x", "y"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"donor": 8, "x": 2, "y": 2}
		c.GroupAcceptSurplus = map[string]bool{"donor": true, "x": true, "y": false}
	})
	ads := []*classad.ClassAd{subAd("x.u@d", 100, 0), subAd("y.u@d", 100, 0)}
	res := runNegotiate(t, cfg, ads, 12, zeroUsage)
	gApprox(t, "x", gAlloc(res.root, "x"), 10) // 2 + all 8 surplus
	gApprox(t, "y", gAlloc(res.root, "y"), 2)  // isolated at its quota
}

// (f) Strict enforcement caps a child's allocation by a non-surplus ancestor's
// (config quota - live subtree usage).
func TestStrictEnforceAncestorCap(t *testing.T) {
	cfg := cfgWith([]string{"P", "P.c"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"P": 10}
		c.GroupQuotaDynamic = map[string]float64{"P.c": 1.0}
		c.GroupAcceptSurplus = map[string]bool{"P": false, "P.c": true}
		c.MaxAllocationRounds = 1
		c.UsingWeightedSlots = true
	})
	ads := []*classad.ClassAd{subAd("P.c.u@d", 100, 0)}
	// P subtree already over its config quota of 10 (5 at P + 8 at c = 13).
	usage := mapUsage(map[string]float64{"P.c": 8, "P": 5, RootGroupName: 0})
	res := runNegotiate(t, cfg, ads, 20, usage)
	// c fairshare-allocated 10, but P has 0 headroom (config 10 - subtree 13),
	// so the negotiated limit is capped down to c's current usage 8.
	gApprox(t, "c limit", res.limit["P.c"], 8)
}

// (g) Autoregroup submitters also negotiate in the root, which is presented last.
func TestAutoregroupRootLastFullPool(t *testing.T) {
	cfg := cfgWith([]string{"a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5}
		c.GroupAutoregroup = map[string]bool{"a": true}
		c.MaxAllocationRounds = 1
		c.UsingWeightedSlots = true
	})
	ads := []*classad.ClassAd{subAd("a.u@d", 8, 0)}
	res := runNegotiate(t, cfg, ads, 10, zeroUsage)
	// a negotiated first (limit 5), then root last with the whole pool (10).
	if len(res.order) != 2 || res.order[0] != "a" || res.order[1] != RootGroupName {
		t.Fatalf("expected order [a, <none>], got %v", res.order)
	}
	gApprox(t, "a limit", res.limit["a"], 5)
	gApprox(t, "root limit", res.limit[RootGroupName], 10)
}

// (h) Multi-round convergence: a callback that fully satisfies usage stops after
// one round; one that only partially fills forces a second round.
func TestMultiRoundConvergence(t *testing.T) {
	build := func() (*negotiator.GroupNode, GroupConfig) {
		cfg := cfgWith([]string{"a"}, func(c *GroupConfig) {
			c.GroupQuota = map[string]float64{"a": 10}
			c.MaxAllocationRounds = 3
			c.ConsiderPreemption = true // exercise genuine multi-round behavior
		})
		root, _, _ := BuildGroupTree(cfg)
		return root, cfg
	}
	ads := []*classad.ClassAd{subAd("a.u@d", 10, 0)}

	// Full satisfaction: usage jumps to the allocation -> exactly one round.
	{
		root, cfg := build()
		usg := map[string]float64{}
		u := mapUsage(usg)
		PrepareForMatchmaking(root, ads, 10, cfg, u)
		calls := 0
		_ = NegotiateAllGroups(root, 10, cfg, u, nil, func(g *negotiator.GroupNode, a float64) error {
			calls++
			usg[g.Name] = a // fully satisfy
			return nil
		})
		if calls != 1 {
			t.Errorf("full-satisfy: expected 1 callback (single round), got %d", calls)
		}
	}

	// Partial (one slot per call): forces a second round.
	{
		root, cfg := build()
		usg := map[string]float64{}
		u := mapUsage(usg)
		PrepareForMatchmaking(root, ads, 10, cfg, u)
		calls := 0
		_ = NegotiateAllGroups(root, 10, cfg, u, nil, func(g *negotiator.GroupNode, a float64) error {
			calls++
			usg[g.Name] = usg[g.Name] + 1 // only one slot filled per round
			return nil
		})
		if calls != 2 {
			t.Errorf("partial-satisfy: expected 2 callbacks (two rounds), got %d", calls)
		}
	}
}

// (i) Unweighted pools: fractional remainders recovered and handed out as whole
// slots via round robin, ordered by (rr_time,) subtree_quota desc.
func TestRoundRobinWholeSlots(t *testing.T) {
	cfg := cfgWith([]string{"a", "b", "c"}, func(c *GroupConfig) {
		// Binary-exact dynamic fractions so remainders sum to a clean 1.0.
		c.GroupQuotaDynamic = map[string]float64{"a": 0.5, "b": 0.25, "c": 0.25}
		c.GroupAcceptSurplus = map[string]bool{"a": true, "b": true, "c": true}
		c.MaxAllocationRounds = 1
		c.UsingWeightedSlots = false // enable remainder recovery + round robin
	})
	ads := []*classad.ClassAd{
		subAd("a.u@d", 100, 0), subAd("b.u@d", 100, 0), subAd("c.u@d", 100, 0),
	}
	res := runNegotiate(t, cfg, ads, 10, zeroUsage)
	// Fairshare: a=5, b=2.5, c=2.5. Remainders 0+0.5+0.5=1 -> one whole slot,
	// given to a (largest subtree_quota). Final: a=6, b=2, c=2.
	gApprox(t, "a", gAlloc(res.root, "a"), 6)
	gApprox(t, "b", gAlloc(res.root, "b"), 2)
	gApprox(t, "c", gAlloc(res.root, "c"), 2)
}

// (j) rr_time persistence: with the accountant as the RRTimeStore, the group
// the round robin served in one NegotiateAllGroups call is stamped, so the
// NEXT call (a fresh tree, as every cycle rebuilds it) serves the other group
// first — the whole surplus slot flips between the two calls.
func TestRoundRobinTimePersistsAcrossCycles(t *testing.T) {
	acct, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = acct.Close() }()

	newCfg := func() GroupConfig {
		return cfgWith([]string{"a", "b"}, func(c *GroupConfig) {
			// Equal binary-exact fractions: fairshare 5.5 each of 11 slots,
			// remainders recover exactly one whole slot for the round robin.
			c.GroupQuotaDynamic = map[string]float64{"a": 0.5, "b": 0.5}
			c.GroupAcceptSurplus = map[string]bool{"a": true, "b": true}
			c.MaxAllocationRounds = 1
			c.UsingWeightedSlots = false
		})
	}
	ads := func() []*classad.ClassAd {
		return []*classad.ClassAd{subAd("a.u@d", 100, 0), subAd("b.u@d", 100, 0)}
	}

	run := func() map[string]float64 {
		cfg := newCfg()
		root, _, err := BuildGroupTree(cfg)
		if err != nil {
			t.Fatal(err)
		}
		// The per-run usage tracks each group's negotiated allocation, so the
		// end-of-round reassessment sees every served group fully satisfied
		// (usage == allocated). That keeps the UNSERVED group's demand > 0,
		// which is what exempts it from the end-of-run rr_time stamp — the
		// state the cross-cycle round robin keys on.
		usg := map[string]float64{}
		u := mapUsage(usg)
		PrepareForMatchmaking(root, ads(), 11, cfg, u)
		if err := NegotiateAllGroups(root, 11, cfg, u, acct,
			func(g *negotiator.GroupNode, alloc float64) error {
				usg[g.Name] = alloc
				return nil
			}); err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, n := range BreadthFirst(root) {
			out[n.Name] = n.Allocated
		}
		return out
	}

	// Cycle 1: on a full rr_time tie the RR order is deterministic (breadth-
	// first index) — "a" gets the extra whole slot and is stamped.
	first := run()
	gApprox(t, "cycle1 a", first["a"], 6)
	gApprox(t, "cycle1 b", first["b"], 5)
	if rt, ok := acct.GetGroupRRTime("a"); !ok || rt <= 0 {
		t.Fatalf("cycle 1 did not persist a's rr_time (got %v ok=%v)", rt, ok)
	}

	// Cycle 2 (fresh tree): "b" still carries the older (absent -> zero)
	// rr_time, so the round robin serves it first — the slot flips.
	second := run()
	gApprox(t, "cycle2 a", second["a"], 5)
	gApprox(t, "cycle2 b", second["b"], 6)
}

// Determinism: identical inputs must yield identical allocations and callback
// order across many runs.
func TestNegotiateDeterministic(t *testing.T) {
	newCfg := func() GroupConfig {
		return surplusCfg([]string{"donor", "x", "y", "z"}, func(c *GroupConfig) {
			c.GroupQuota = map[string]float64{"donor": 8, "x": 2, "y": 2, "z": 0}
			c.GroupAcceptSurplus = map[string]bool{"donor": true, "x": true, "y": true, "z": true}
		})
	}
	ads := func() []*classad.ClassAd {
		return []*classad.ClassAd{
			subAd("x.u@d", 100, 0), subAd("y.u@d", 100, 0), subAd("z.u@d", 100, 0),
		}
	}

	var wantOrder []string
	want := map[string]float64{}
	for i := 0; i < 100; i++ {
		res := runNegotiate(t, newCfg(), ads(), 12, zeroUsage)
		got := map[string]float64{}
		for _, n := range BreadthFirst(res.root) {
			got[n.Name] = n.Allocated
		}
		if i == 0 {
			want = got
			wantOrder = res.order
			continue
		}
		for k, v := range want {
			if got[k] < v-gEps || got[k] > v+gEps {
				t.Fatalf("run %d: %s gAlloc %g != %g", i, k, got[k], v)
			}
		}
		if len(res.order) != len(wantOrder) {
			t.Fatalf("run %d: order length %d != %d", i, len(res.order), len(wantOrder))
		}
		for j := range wantOrder {
			if res.order[j] != wantOrder[j] {
				t.Fatalf("run %d: order[%d] %q != %q", i, j, res.order[j], wantOrder[j])
			}
		}
	}
}
