package accountant

import (
	"math"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// AssignSubmitters resets each group's per-cycle fields, reloads node-local
// usage from the injected usage lookup, maps every submitter ad to its
// accounting group (filling GroupNode.Submitters), and accumulates weighted
// demand into GroupNode.Requested. It is the submitter-loading portion of the
// C++ hgq_prepare_for_matchmaking (GroupEntry.cpp:245-331).
//
// usage(name) returns the weighted resources currently in use for a submitter
// or bare group name; it is called with each group's full name (root is
// queried as "<none>").
//
// Demand per submitter is WeightedIdleJobs+WeightedRunningJobs when
// cfg.UseWeightedDemand (falling back to IdleJobs+RunningJobs for whichever
// weighted attribute is absent), else IdleJobs+RunningJobs.
//
// If the root's Autoregroup is set (any group requested autoregroup), the
// submitter ads of every autoregroup child are additionally appended to the
// root's submitter list, so those submitters also negotiate in the root.
//
// Submitter ads with no Name or with a badly-formed name (no '@') are skipped.
func AssignSubmitters(root *negotiator.GroupNode, submitterAds []*classad.ClassAd, cfg GroupConfig, usage func(name string) float64) {
	groups := BreadthFirst(root)
	nameMap := BuildNameMap(root)

	// Reset per-cycle fields and reload usage (GroupEntry.cpp:250-261).
	for _, g := range groups {
		g.Quota = 0
		g.Requested = 0
		g.Allocated = 0
		g.SubtreeQuota = 0
		g.Submitters = g.Submitters[:0]
		g.Usage = usage(g.Name)
	}

	for _, ad := range submitterAds {
		name, ok := ad.EvaluateAttrString("Name")
		if !ok || name == "" {
			continue // no name
		}
		if !hasAt(name) {
			continue // badly-formed submitter name
		}

		group := GetAssignedGroup(root, nameMap, name)
		group.Submitters = append(group.Submitters, ad)
		group.Requested += submitterDemand(ad, cfg.UseWeightedDemand)
	}

	// Autoregroup: children with autoregroup also negotiate in the root.
	if root.Autoregroup {
		for _, g := range groups {
			if g == root || !g.Autoregroup {
				continue
			}
			root.Submitters = append(root.Submitters, g.Submitters...)
		}
	}
}

func hasAt(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '@' {
			return true
		}
	}
	return false
}

// submitterDemand computes the weighted (or unweighted) demand for one
// submitter ad.
func submitterDemand(ad *classad.ClassAd, weighted bool) float64 {
	numIdle, _ := ad.EvaluateAttrInt("IdleJobs")
	numRunning, _ := ad.EvaluateAttrInt("RunningJobs")
	if !weighted {
		return float64(numIdle + numRunning)
	}
	weightedIdle := float64(numIdle)
	weightedRunning := float64(numRunning)
	if v, ok := ad.EvaluateAttrReal("WeightedIdleJobs"); ok {
		weightedIdle = v
	} else if v, ok := ad.EvaluateAttrNumber("WeightedIdleJobs"); ok {
		weightedIdle = v
	}
	if v, ok := ad.EvaluateAttrReal("WeightedRunningJobs"); ok {
		weightedRunning = v
	} else if v, ok := ad.EvaluateAttrNumber("WeightedRunningJobs"); ok {
		weightedRunning = v
	}
	return weightedIdle + weightedRunning
}

// AssignQuotas normalizes quotas over the tree, distributing totalQuota from
// the root downward, mirroring GroupEntry::hgq_assign_quotas
// (GroupEntry.cpp:539-615).
//
// Static-quota children get first dibs on the incoming quota (bounded by it
// unless cfg.AllowQuotaOversubscription); dynamic children share the remainder
// with their fractions renormalized only if they sum > 1. A node keeps whatever
// its children do not consume; the root's own Quota is set to
// totalQuota - sum(child allocations) so root surplus is never double-counted.
// All assigned quotas are clamped to >= 0.
//
// Node.Quota and Node.SubtreeQuota are reset for the whole tree before
// distribution, so AssignQuotas may be called independently of AssignSubmitters.
func AssignQuotas(root *negotiator.GroupNode, totalQuota float64, cfg GroupConfig) {
	for _, g := range BreadthFirst(root) {
		g.Quota = 0
		g.SubtreeQuota = 0
	}
	assignQuotas(root, totalQuota, cfg.AllowQuotaOversubscription)
}

func assignQuotas(n *negotiator.GroupNode, quota float64, allowOversub bool) {
	// A zero-quota subtree keeps default zero quotas.
	if quota <= 0 {
		return
	}

	// Incoming quota is the quota for the whole subtree.
	n.SubtreeQuota = quota

	// Sum of static and dynamic child config quotas.
	var sqsum, dqsum float64
	for _, child := range n.Children {
		if child.StaticQuota {
			sqsum += child.ConfigQuota
		} else {
			dqsum += child.ConfigQuota
		}
	}

	// Static quotas get first dibs, bounded by the incoming quota unless
	// oversubscription is allowed.
	sqa := sqsum
	if !allowOversub {
		sqa = math.Min(sqsum, quota)
	}
	// Dynamic children get whatever remains.
	dqa := math.Max(0, quota-sqa)

	// Prevent (0/0) when all static quotas are zero.
	Zs := sqsum
	if Zs <= 0 {
		Zs = 1
	}
	// Renormalize dynamic quotas only if their fractions sum > 1.
	Zd := math.Max(dqsum, 1)

	var chq float64
	for _, child := range n.Children {
		var q float64
		if child.StaticQuota {
			q = child.ConfigQuota * (sqa / Zs)
		} else {
			q = child.ConfigQuota * (dqa / Zd)
		}
		if q < 0 {
			q = 0
		}
		assignQuotas(child, q, allowOversub)
		chq += q
	}

	// Current group keeps whatever remains after allocating to children (a leaf
	// keeps everything).
	if allowOversub {
		n.Quota = quota
	} else {
		n.Quota = quota - chq
	}

	// The root "<none>" quota represents the whole pool minus all child
	// allocations, so surplus is never double-counted (regardless of
	// oversubscription).
	if n.Name == RootGroupName {
		n.Quota = quota - chq
	}

	if n.Quota < 0 {
		n.Quota = 0
	}
}

// PrepareForMatchmaking runs the full C++ hgq_prepare_for_matchmaking: it
// reloads usage, maps submitters to groups and accumulates demand
// (AssignSubmitters), then normalizes quotas from totalQuota (AssignQuotas).
// It leaves the tree ready for NegotiateAllGroups.
func PrepareForMatchmaking(root *negotiator.GroupNode, submitterAds []*classad.ClassAd, totalQuota float64, cfg GroupConfig, usage func(name string) float64) {
	AssignSubmitters(root, submitterAds, cfg, usage)
	AssignQuotas(root, totalQuota, cfg)
}
