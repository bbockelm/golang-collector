package accountant

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// ---- Quota normalization (hgq_assign_quotas) ----

func TestQuotaStaticFirstDibs(t *testing.T) {
	// static quotas 8 + 4 = 12 exceed pool 10 -> rescaled proportionally.
	cfg := cfgWith([]string{"a", "b"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 8, "b": 4}
	})
	root, _, _ := BuildGroupTree(cfg)
	AssignQuotas(root, 10, cfg)
	gApprox(t, "a.Quota", nodeByName(root, "a").Quota, 8*(10.0/12.0))
	gApprox(t, "b.Quota", nodeByName(root, "b").Quota, 4*(10.0/12.0))
	gApprox(t, "root.Quota", root.Quota, 0) // all consumed
}

func TestQuotaOversubscription(t *testing.T) {
	cfg := cfgWith([]string{"a", "b"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 8, "b": 4}
		c.AllowQuotaOversubscription = true
	})
	root, _, _ := BuildGroupTree(cfg)
	AssignQuotas(root, 10, cfg)
	gApprox(t, "a.Quota", nodeByName(root, "a").Quota, 8)
	gApprox(t, "b.Quota", nodeByName(root, "b").Quota, 4)
	gApprox(t, "root.Quota", root.Quota, 0) // 10 - 12 clamped to 0
}

func TestQuotaDynamicRenormalized(t *testing.T) {
	// dynamic fractions 0.5 + 0.75 = 1.25 > 1 -> renormalized by 1.25.
	cfg := cfgWith([]string{"a", "b"}, func(c *GroupConfig) {
		c.GroupQuotaDynamic = map[string]float64{"a": 0.5, "b": 0.75}
	})
	root, _, _ := BuildGroupTree(cfg)
	AssignQuotas(root, 10, cfg)
	gApprox(t, "a.Quota", nodeByName(root, "a").Quota, 0.5*(10.0/1.25))
	gApprox(t, "b.Quota", nodeByName(root, "b").Quota, 0.75*(10.0/1.25))
}

func TestQuotaDynamicNoRenormRootKeepsRemainder(t *testing.T) {
	// dynamic fractions 0.3 + 0.2 = 0.5 < 1 -> NOT renormalized; root keeps 5.
	cfg := cfgWith([]string{"a", "b"}, func(c *GroupConfig) {
		c.GroupQuotaDynamic = map[string]float64{"a": 0.3, "b": 0.2}
	})
	root, _, _ := BuildGroupTree(cfg)
	AssignQuotas(root, 10, cfg)
	gApprox(t, "a.Quota", nodeByName(root, "a").Quota, 3)
	gApprox(t, "b.Quota", nodeByName(root, "b").Quota, 2)
	gApprox(t, "root.Quota", root.Quota, 5) // remainder, never double-counted
}

func TestQuotaMixedStaticDynamic(t *testing.T) {
	// static gets first dibs (3), dynamic shares the remainder (7).
	cfg := cfgWith([]string{"s", "d"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"s": 3}
		c.GroupQuotaDynamic = map[string]float64{"d": 0.5} // <1, no renorm
	})
	root, _, _ := BuildGroupTree(cfg)
	AssignQuotas(root, 10, cfg)
	gApprox(t, "s.Quota", nodeByName(root, "s").Quota, 3)
	gApprox(t, "d.Quota", nodeByName(root, "d").Quota, 0.5*7) // dqa = 10-3 = 7
	gApprox(t, "root.Quota", root.Quota, 10-3-3.5)
}

func TestQuotaZeroClampAndEmptyStatic(t *testing.T) {
	// all-static-zero must not divide by zero; quotas stay 0, root keeps all.
	cfg := cfgWith([]string{"a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 0}
	})
	root, _, _ := BuildGroupTree(cfg)
	AssignQuotas(root, 10, cfg)
	gApprox(t, "a.Quota", nodeByName(root, "a").Quota, 0)
	gApprox(t, "root.Quota", root.Quota, 10)
}

func TestQuotaNested(t *testing.T) {
	// P static 10 -> c dynamic 1.0 inside -> c gets 10, P keeps 0, root 10.
	cfg := cfgWith([]string{"P", "P.c"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"P": 10}
		c.GroupQuotaDynamic = map[string]float64{"P.c": 1.0}
	})
	root, _, _ := BuildGroupTree(cfg)
	AssignQuotas(root, 20, cfg)
	gApprox(t, "c.Quota", nodeByName(root, "P.c").Quota, 10)
	gApprox(t, "c.SubtreeQuota", nodeByName(root, "P.c").SubtreeQuota, 10)
	gApprox(t, "P.Quota", nodeByName(root, "P").Quota, 0)
	gApprox(t, "P.SubtreeQuota", nodeByName(root, "P").SubtreeQuota, 10)
	gApprox(t, "root.Quota", root.Quota, 10)
}

// ---- Submitter assignment + demand ----

func TestAssignSubmittersDemandWeighted(t *testing.T) {
	cfg := cfgWith([]string{"a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5}
	})
	root, _, _ := BuildGroupTree(cfg)
	ads := []*classad.ClassAd{
		subAdWeighted("a.user@dom", 4, 1, 8.0, 2.0), // weighted demand 10
		subAd("otheruser@dom", 3, 2),                // -> root, demand 5 (no weighted attrs)
	}
	AssignSubmitters(root, ads, cfg, zeroUsage)
	gApprox(t, "a.Requested", nodeByName(root, "a").Requested, 10)
	gApprox(t, "root.Requested", root.Requested, 5)
	if len(nodeByName(root, "a").Submitters) != 1 {
		t.Errorf("a should have 1 submitter")
	}
}

func TestAssignSubmittersUnweightedDemand(t *testing.T) {
	cfg := cfgWith([]string{"a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5}
		c.UseWeightedDemand = false
	})
	root, _, _ := BuildGroupTree(cfg)
	ads := []*classad.ClassAd{subAdWeighted("a.user@dom", 4, 1, 8.0, 2.0)}
	AssignSubmitters(root, ads, cfg, zeroUsage)
	// Unweighted: idle+running = 5, ignoring the weighted attrs.
	gApprox(t, "a.Requested", nodeByName(root, "a").Requested, 5)
}

func TestAssignSubmittersUsageReset(t *testing.T) {
	cfg := cfgWith([]string{"a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5}
	})
	root, _, _ := BuildGroupTree(cfg)
	usage := mapUsage(map[string]float64{"a": 3, RootGroupName: 1})
	AssignSubmitters(root, nil, cfg, usage)
	gApprox(t, "a.Usage", nodeByName(root, "a").Usage, 3)
	gApprox(t, "root.Usage", root.Usage, 1)
}

func TestAssignSubmittersAutoregroupAppendsToRoot(t *testing.T) {
	cfg := cfgWith([]string{"a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5}
		c.GroupAutoregroup = map[string]bool{"a": true}
	})
	root, _, _ := BuildGroupTree(cfg)
	ads := []*classad.ClassAd{subAd("a.user@dom", 8, 0)}
	AssignSubmitters(root, ads, cfg, zeroUsage)
	if len(root.Submitters) != 1 {
		t.Fatalf("autoregroup submitter not appended to root; got %d", len(root.Submitters))
	}
	// Demand is only credited to the assigned group, not root.
	gApprox(t, "a.Requested", nodeByName(root, "a").Requested, 8)
	gApprox(t, "root.Requested", root.Requested, 0)
}
