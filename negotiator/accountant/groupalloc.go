package accountant

import (
	"math"
	"sort"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// fltMax mirrors the C++ FLT_MAX used as the sort-key default when
// GROUP_SORT_EXPR fails to evaluate to a number.
const fltMax = math.MaxFloat32

// nodeState holds the per-cycle bookkeeping the C++ GroupEntry carries but the
// frozen negotiator.GroupNode struct does not expose. It is keyed by node in
// the allocator; see the GroupNode field gaps noted in the package docs.
type nodeState struct {
	subtreeRequested float64 // C++ subtree_requested
	subtreeUsage     float64 // C++ subtree_usage (strict-enforce)
	rr               bool    // C++ rr: served by the most recent round robin
	rrTime           float64 // C++ rr_time
	subtreeRRTime    float64 // C++ subtree_rr_time
}

// allocator holds the mutable per-call state for one NegotiateAllGroups run.
// All GroupNode fields it uses that DO exist on the frozen struct (Quota,
// SubtreeQuota, Requested, Allocated, Usage, AcceptSurplus) are mutated in
// place; the rest live in st.
type allocator struct {
	root     *negotiator.GroupNode
	groups   []*negotiator.GroupNode // breadth-first
	cfg      GroupConfig
	usage    func(name string) float64
	sortExpr *classad.Expr
	st       map[*negotiator.GroupNode]*nodeState
}

func roundForPrecision(x float64) float64 { return math.Floor(0.5 + x) }

// fairshare is GroupEntry::hgq_fairshare (GroupEntry.cpp:617-657): allocate
// min(requested, quota) at each node, recurse, then distribute surplus.
func (a *allocator) fairshare(n *negotiator.GroupNode) float64 {
	n.Allocated = math.Min(n.Requested, n.Quota)
	n.Requested -= n.Allocated
	a.st[n].subtreeRequested = n.Requested

	surplus := n.Quota - n.Allocated

	if len(n.Children) == 0 {
		return surplus
	}
	for _, child := range n.Children {
		surplus += a.fairshare(child)
		if child.AcceptSurplus {
			a.st[n].subtreeRequested += a.st[child].subtreeRequested
		}
	}
	surplus = a.allocateSurplus(n, surplus)
	return surplus
}

// allocateSurplus is GroupEntry::hgq_allocate_surplus (GroupEntry.cpp:659-758):
// the parent competes as a peer of its children (appended last); cornucopia
// satisfies everyone, else the two proportional scarcity loops run.
func (a *allocator) allocateSurplus(n *negotiator.GroupNode, surplus float64) float64 {
	if surplus <= 0 {
		return 0
	}
	if a.st[n].subtreeRequested <= 0 {
		return surplus
	}

	groups := append(append([]*negotiator.GroupNode{}, n.Children...), n)
	allocated := make([]float64, len(groups))

	// Temporarily make the current node behave like a surplus-accepting child.
	saveAccept := n.AcceptSurplus
	n.AcceptSurplus = true
	saveSubtreeQuota := n.SubtreeQuota
	n.SubtreeQuota = n.Quota
	requested := a.st[n].subtreeRequested
	a.st[n].subtreeRequested = n.Requested

	if surplus >= requested {
		// Cornucopia: everyone gets what they asked for.
		for j, grp := range groups {
			if grp.AcceptSurplus && a.st[grp].subtreeRequested > 0 {
				allocated[j] = a.st[grp].subtreeRequested
			}
		}
		surplus -= requested
		requested = 0
	} else {
		// Scarcity: compete by quota, then let zero-quota groups mop up.
		subtreeReq := make([]float64, len(groups))
		for j, grp := range groups {
			if grp.AcceptSurplus && a.st[grp].subtreeRequested > 0 {
				subtreeReq[j] = a.st[grp].subtreeRequested
			}
		}
		a.allocateSurplusLoop(true, groups, allocated, subtreeReq, &surplus, &requested)
		a.allocateSurplusLoop(false, groups, allocated, subtreeReq, &surplus, &requested)
	}

	// Recurse into children (all but the last, which is n itself).
	for j := 0; j < len(groups)-1; j++ {
		if allocated[j] > 0 {
			a.allocateSurplus(groups[j], allocated[j])
		}
	}

	// Allocate the current group.
	n.Allocated += allocated[len(allocated)-1]
	n.Requested -= allocated[len(allocated)-1]

	// Restore.
	a.st[n].subtreeRequested = requested
	n.AcceptSurplus = saveAccept
	n.SubtreeQuota = saveSubtreeQuota

	return surplus
}

// allocateSurplusLoop is the free function hgq_allocate_surplus_loop
// (GroupEntry.cpp:941-1001): proportional allocation iterated to convergence,
// weighted by subtree_quota (by_quota) or 1 (zero-quota mop-up).
func (a *allocator) allocateSurplusLoop(byQuota bool, groups []*negotiator.GroupNode, allocated, subtreeRequested []float64, surplus, requested *float64) {
	for *surplus > 0 {
		var Z float64
		for j, grp := range groups {
			if subtreeRequested[j] > 0 {
				if byQuota {
					Z += grp.SubtreeQuota
				} else {
					Z += 1.0
				}
			}
		}
		if Z <= 0 {
			break
		}

		neverGt := true
		var sumalloc float64
		for j, grp := range groups {
			if subtreeRequested[j] > 0 {
				N := 1.0
				if byQuota {
					N = grp.SubtreeQuota
				}
				alloc := *surplus * (N / Z)
				if alloc > subtreeRequested[j] {
					alloc = subtreeRequested[j]
					neverGt = false
				}
				allocated[j] += alloc
				subtreeRequested[j] -= alloc
				sumalloc += alloc
			}
		}
		*surplus -= sumalloc
		*requested -= sumalloc

		if neverGt || *surplus < 0 {
			*surplus = 0
		}
	}
}

// recoverRemainders is GroupEntry::hgq_recover_remainders
// (GroupEntry.cpp:772-814): reclaim fractional slot remainders as surplus, then
// hand whole slots out via round robin. Only used for unweighted pools.
func (a *allocator) recoverRemainders(n *negotiator.GroupNode) float64 {
	surplus := n.Allocated - math.Floor(n.Allocated)
	n.Allocated -= surplus
	n.Requested += surplus

	n.Allocated = roundForPrecision(n.Allocated)
	n.Requested = roundForPrecision(n.Requested)

	s := a.st[n]
	s.subtreeRequested = n.Requested
	if n.Requested > 0 {
		s.subtreeRRTime = s.rrTime
	} else {
		s.subtreeRRTime = math.MaxFloat64
	}

	if len(n.Children) == 0 {
		return surplus
	}
	for _, child := range n.Children {
		surplus += a.recoverRemainders(child)
		if child.AcceptSurplus {
			cs := a.st[child]
			s.subtreeRequested += cs.subtreeRequested
			if cs.subtreeRequested > 0 {
				s.subtreeRRTime = math.Min(s.subtreeRRTime, cs.subtreeRRTime)
			}
		}
	}
	surplus = a.roundRobin(n, surplus)
	return surplus
}

// roundRobin is GroupEntry::hgq_round_robin (GroupEntry.cpp:816-938): hand out
// whole slots one round at a time, ordered by subtree_rr_time (then
// subtree_quota desc, then subtree_requested desc).
func (a *allocator) roundRobin(n *negotiator.GroupNode, surplus float64) float64 {
	s := a.st[n]
	if s.subtreeRequested != math.Floor(s.subtreeRequested) {
		s.subtreeRequested = math.Floor(s.subtreeRequested)
	}
	if s.subtreeRequested <= 0 {
		return surplus
	}
	if surplus < 1 {
		return surplus
	}

	groups := append(append([]*negotiator.GroupNode{}, n.Children...), n)
	allocated := make([]float64, len(groups))

	saveAccept := n.AcceptSurplus
	n.AcceptSurplus = true
	saveSubtreeQuota := n.SubtreeQuota
	n.SubtreeQuota = n.Quota
	saveSubtreeRRTime := s.subtreeRRTime
	s.subtreeRRTime = s.rrTime
	requested := s.subtreeRequested
	s.subtreeRequested = n.Requested

	outstanding := 0.0
	subtreeRequested := make([]float64, len(groups))
	for j, grp := range groups {
		if grp.AcceptSurplus && a.st[grp].subtreeRequested > 0 {
			subtreeRequested[j] = a.st[grp].subtreeRequested
			outstanding += 1
		}
	}

	idx := make([]int, len(groups))
	for j := range idx {
		idx[j] = j
	}
	sort.SliceStable(idx, func(x, y int) bool {
		return a.rrLess(groups[idx[x]], groups[idx[y]])
	})

	for surplus >= 1 && requested > 0 {
		amax := math.Max(1, math.Floor(surplus/outstanding))

		outstanding = 0
		var sumalloc float64
		for _, j := range idx {
			grp := groups[j]
			if grp.AcceptSurplus && subtreeRequested[j] > 0 {
				alloc := math.Min(subtreeRequested[j], amax)
				allocated[j] += alloc
				subtreeRequested[j] -= alloc
				sumalloc += alloc
				surplus -= alloc
				requested -= alloc
				a.st[grp].rr = true
				if subtreeRequested[j] > 0 {
					outstanding += 1
				}
				if surplus < amax {
					break
				}
			}
		}
		if sumalloc < 1 {
			break
		}
	}

	for j := 0; j < len(groups)-1; j++ {
		if allocated[j] > 0 {
			a.roundRobin(groups[j], allocated[j])
		}
	}

	n.Allocated += allocated[len(allocated)-1]
	n.Requested -= allocated[len(allocated)-1]

	s.subtreeRequested = requested
	n.AcceptSurplus = saveAccept
	n.SubtreeQuota = saveSubtreeQuota
	s.subtreeRRTime = saveSubtreeRRTime

	return surplus
}

// rrLess is the ord_by_rr_time comparator (GroupEntry.h:131-149).
func (a *allocator) rrLess(x, y *negotiator.GroupNode) bool {
	xs, ys := a.st[x], a.st[y]
	if xs.subtreeRRTime < ys.subtreeRRTime {
		return true
	}
	if xs.subtreeRRTime > ys.subtreeRRTime {
		return false
	}
	if x.SubtreeQuota > y.SubtreeQuota {
		return true
	}
	if x.SubtreeQuota < y.SubtreeQuota {
		return false
	}
	return xs.subtreeRequested > ys.subtreeRequested
}

// calculateSubtreeUsage refreshes subtree_usage from live usage
// (GroupEntry.cpp:11-22), used by strict enforcement.
func (a *allocator) calculateSubtreeUsage(n *negotiator.GroupNode) float64 {
	total := 0.0
	for _, c := range n.Children {
		total += a.calculateSubtreeUsage(c)
	}
	total += a.usage(n.Name)
	a.st[n].subtreeUsage = total
	return total
}

// strictEnforce is GroupEntry::strict_enforce_quota (GroupEntry.cpp:1003-1038):
// cap a proposed allocation by every non-surplus ancestor's remaining
// (config/subtree quota - live subtree usage).
func (a *allocator) strictEnforce(n *negotiator.GroupNode, slots float64) float64 {
	if !a.cfg.StrictEnforceQuota {
		return slots
	}
	a.calculateSubtreeUsage(a.root)

	myNewAllocation := slots - n.Usage
	for limiting := n; limiting != nil; limiting = limiting.Parent {
		if limiting.AcceptSurplus {
			continue
		}
		var subtreeAvailable float64
		if limiting.StaticQuota {
			subtreeAvailable = limiting.ConfigQuota - a.st[limiting].subtreeUsage
		} else {
			subtreeAvailable = limiting.SubtreeQuota - a.st[limiting].subtreeUsage
		}
		if subtreeAvailable < 0 {
			subtreeAvailable = 0
		}
		if myNewAllocation > subtreeAvailable {
			myNewAllocation = subtreeAvailable
		}
	}
	return myNewAllocation + n.Usage
}

// computeSortKey evaluates GROUP_SORT_EXPR against a per-group sort ad carrying
// AccountingGroup, GroupQuota, GroupResourcesAllocated, and GroupResourcesInUse,
// defaulting to FLT_MAX on failure (GroupEntry.cpp:427-443).
func (a *allocator) computeSortKey(g *negotiator.GroupNode) float64 {
	ad := classad.New()
	ad.InsertAttrString("AccountingGroup", g.Name)
	ad.InsertAttrFloat("GroupQuota", g.Quota)
	ad.InsertAttrFloat("GroupResourcesAllocated", g.Allocated)
	ad.InsertAttrFloat("GroupResourcesInUse", a.usage(g.Name))
	v := a.sortExpr.Eval(ad)
	f, err := v.NumberValue()
	if err != nil {
		return fltMax
	}
	return f
}

// sortForNegotiation returns the groups in "starvation order" (group_order,
// GroupEntry.h:109-127): by sort key ascending, with the root last when
// autoregroup is active. Ties break on breadth-first index for determinism
// (the C++ std::sort is unstable here; we make it deterministic).
func (a *allocator) sortForNegotiation(autoregroup bool) []*negotiator.GroupNode {
	bfsIndex := make(map[*negotiator.GroupNode]int, len(a.groups))
	for i, g := range a.groups {
		bfsIndex[g] = i
	}
	out := append([]*negotiator.GroupNode{}, a.groups...)
	sort.SliceStable(out, func(i, j int) bool {
		gi, gj := out[i], out[j]
		if autoregroup {
			if gi == a.root {
				return false
			}
			if gj == a.root {
				return true
			}
		}
		if gi.SortKey != gj.SortKey {
			return gi.SortKey < gj.SortKey
		}
		return bfsIndex[gi] < bfsIndex[gj]
	})
	return out
}

// NegotiateAllGroups is the multi-round allocation driver, a port of
// GroupEntry::hgq_negotiate_with_all_groups (GroupEntry.cpp:341-537). It runs up
// to cfg.MaxAllocationRounds rounds; each round computes fairshare + surplus
// allocation over the tree (plus remainder recovery + round robin for unweighted
// pools), then presents groups in starvation order and, for each group with a
// positive allocation, invokes negotiateGroup(group, limit) with the
// strict-enforced per-group slot limit.
//
// The signature differs from the C++ (which takes an Accountant and an int
// callback): usage is injected as a pure func(name)->weighted-usage, totalQuota
// is the weighted (or effective) pool size, and the callback takes the limit as
// a float64 (the C++ truncates it to int). The tree must already be prepared
// (PrepareForMatchmaking / AssignSubmitters+AssignQuotas).
//
// The RR inner loop over "round robin rate" (cfg.RoundRobinRate, default +Inf =
// a single pass) is preserved. Between rounds, usage is re-read; a round that
// filled all allocations (usage_total >= allocated_total) ends the driver early.
func NegotiateAllGroups(root *negotiator.GroupNode, totalQuota float64, cfg GroupConfig, usage func(name string) float64, negotiateGroup func(g *negotiator.GroupNode, allocation float64) error) error {
	groups := BreadthFirst(root)

	sortExprStr := cfg.GroupSortExpr
	if sortExprStr == "" {
		sortExprStr = DefaultGroupSortExpr
	}
	sortExpr, err := classad.ParseExpr(sortExprStr)
	if err != nil {
		return err
	}

	st := make(map[*negotiator.GroupNode]*nodeState, len(groups))
	for _, g := range groups {
		st[g] = &nodeState{}
	}
	a := &allocator{root: root, groups: groups, cfg: cfg, usage: usage, sortExpr: sortExpr, st: st}

	// global_accept_surplus: any non-root group (or the default) accepts surplus.
	globalAcceptSurplus := cfg.DefaultAcceptSurplus
	for _, g := range groups {
		if g != root && g.AcceptSurplus {
			globalAcceptSurplus = true
		}
	}
	autoregroup := root.Autoregroup

	maxrounds := cfg.MaxAllocationRounds
	if maxrounds < 1 {
		maxrounds = 1
	}

	// GROUP_QUOTA_ROUND_ROBIN_RATE: default +Inf (one pass), min 1.0.
	ninc := cfg.RoundRobinRate
	if !(ninc >= 1) { // catches 0, NaN, negatives
		ninc = math.Inf(1)
	}

	for iter := 0; iter < maxrounds; iter++ {
		for _, g := range groups {
			g.Allocated = 0
			st[g].subtreeRequested = 0
			st[g].rr = false
		}

		surplusQuota := a.fairshare(root)
		if !cfg.UsingWeightedSlots {
			surplusQuota += a.recoverRemainders(root)
		}
		_ = surplusQuota

		if autoregroup {
			root.Quota = totalQuota
			root.Allocated = totalQuota
		}

		var maxdelta, allocatedTotal float64
		for _, g := range groups {
			allocatedTotal += g.Allocated
			target := g.Quota
			if globalAcceptSurplus {
				target = g.Allocated
			}
			maxdelta = math.Max(maxdelta, math.Max(0, target-g.Usage))
		}

		for _, g := range groups {
			g.SortKey = a.computeSortKey(g)
		}
		negotiatingGroups := a.sortForNegotiation(autoregroup)

		n := 0.0
		for {
			n = math.Min(n+ninc, maxdelta)
			for _, g := range negotiatingGroups {
				if g.Allocated <= 0 {
					continue
				}
				if g.Usage >= g.Allocated && !cfg.ConsiderPreemption {
					continue
				}
				if len(g.Submitters) == 0 {
					continue
				}

				target := math.Max(g.Allocated, g.Quota)
				delta := math.Max(0, target-g.Usage)
				var slots float64
				if delta > 0 {
					slots = g.Usage + delta*(n/maxdelta)
				} else {
					slots = target
				}
				slots = math.Min(slots, target)
				if !cfg.UsingWeightedSlots {
					slots = math.Floor(slots)
				}
				slots = a.strictEnforce(g, slots)

				if err := negotiateGroup(g, slots); err != nil {
					return err
				}
			}
			if n >= maxdelta {
				break
			}
		}

		// Reassess against live usage.
		var usageTotal float64
		for _, g := range groups {
			g.Usage = usage(g.Name)
			usageTotal += math.Min(g.Usage, g.Allocated)
			if g.Usage < g.Allocated {
				g.Requested = g.Usage
			} else {
				g.Requested += g.Allocated
			}
		}

		if usageTotal >= allocatedTotal {
			break
		}
	}

	// Update rr_time after all rounds (cross-cycle RR fairness). Note: this
	// state is per-call here; see the GroupNode field-gap note.
	now := float64(time.Now().Unix())
	for _, g := range groups {
		if st[g].rr || g.Requested <= 0 {
			st[g].rrTime = now
		}
	}

	return nil
}
