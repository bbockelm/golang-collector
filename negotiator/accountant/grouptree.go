// Package accountant implements the HTCondor negotiator's accounting logic.
//
// This file implements the accounting-group tree: construction from
// configuration (GROUP_NAMES etc.), submitter->group assignment, and
// GROUP_SORT_EXPR evaluation for negotiation ordering. It is a faithful port of
// the C++ reference in src/condor_negotiator.V6/GroupEntry.cpp
// (hgq_construct_tree, GetAssignedGroup).
//
// All functions here are PURE: they operate on *negotiator.GroupNode trees, a
// GroupConfig value carrying the configuration knobs (design doc section 9),
// submitter ClassAds, and an injected usage lookup. They have no dependency on
// the sibling accountant store.
package accountant

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// RootGroupName is the reserved name of the always-present root group ("<none>"
// in the C++ negotiator). It always has AcceptSurplus=true.
const RootGroupName = "<none>"

// DefaultGroupSortExpr is the param-info default for GROUP_SORT_EXPR: sort by
// fraction of quota in use (ascending), with zero-quota groups and the root
// group pushed to the end.
const DefaultGroupSortExpr = `ifThenElse(AccountingGroup=?="<none>",3.4e+38, ifThenElse(GroupQuota>0, GroupResourcesInUse/GroupQuota, 3.3e+38))`

// GroupConfig carries the group/quota configuration knobs (design doc section
// 9). It is the pure-function equivalent of the C++ param() lookups. Callers
// should start from DefaultGroupConfig() and layer their parsed config on top.
//
// The GroupQuota/GroupQuotaDynamic/GroupAcceptSurplus/GroupAutoregroup maps are
// keyed by the (possibly dotted) group name exactly as it appears in
// GroupNames; lookups are case-insensitive (matching HTCondor param()).
// Presence of a name in GroupQuota marks it static (GROUP_QUOTA_<g>); otherwise
// presence in GroupQuotaDynamic marks it dynamic (GROUP_QUOTA_DYNAMIC_<g>).
type GroupConfig struct {
	// GroupNames is the ordered GROUP_NAMES list (dotted paths). Order does not
	// matter for correctness: the tree builder sorts case-insensitively so
	// parents precede children, exactly like the C++.
	GroupNames []string

	// GroupQuota holds static slot quotas (GROUP_QUOTA_<g>); presence => static.
	GroupQuota map[string]float64
	// GroupQuotaDynamic holds dynamic fractional quotas (GROUP_QUOTA_DYNAMIC_<g>,
	// 0..1); used only when the name is absent from GroupQuota.
	GroupQuotaDynamic map[string]float64
	// GroupAcceptSurplus / GroupAutoregroup are the per-group GROUP_ACCEPT_SURPLUS_<g>
	// / GROUP_AUTOREGROUP_<g> overrides. Absent => the Default* value.
	GroupAcceptSurplus map[string]bool
	GroupAutoregroup   map[string]bool

	// DefaultAcceptSurplus / DefaultAutoregroup are GROUP_ACCEPT_SURPLUS /
	// GROUP_AUTOREGROUP (both default false).
	DefaultAcceptSurplus bool
	DefaultAutoregroup   bool

	// GroupSortExpr is GROUP_SORT_EXPR; empty => DefaultGroupSortExpr.
	GroupSortExpr string

	// AllowQuotaOversubscription is NEGOTIATOR_ALLOW_QUOTA_OVERSUBSCRIPTION (false).
	AllowQuotaOversubscription bool
	// StrictEnforceQuota is NEGOTIATOR_STRICT_ENFORCE_QUOTA (default true).
	StrictEnforceQuota bool
	// MaxAllocationRounds is GROUP_QUOTA_MAX_ALLOCATION_ROUNDS (default 3, min 1).
	MaxAllocationRounds int
	// UseWeightedDemand is NEGOTIATOR_USE_WEIGHTED_DEMAND (default true): demand
	// = WeightedIdleJobs+WeightedRunningJobs, else IdleJobs+RunningJobs.
	UseWeightedDemand bool
	// RoundRobinRate is GROUP_QUOTA_ROUND_ROBIN_RATE (default +Inf = one pass,
	// min 1.0).
	RoundRobinRate float64
	// UsingWeightedSlots reports whether the pool uses non-unit slot weights
	// (accountant.UsingWeightedSlots()). When false, the allocator performs
	// fractional-remainder recovery and whole-slot round-robin.
	UsingWeightedSlots bool
	// ConsiderPreemption mirrors NEGOTIATOR_CONSIDER_PREEMPTION. The C++ default
	// is true, but the Go MVP defers preemption, so DefaultGroupConfig sets it
	// false. When false, groups already at/over their allocation are skipped in
	// the per-group negotiation loop (GroupEntry.cpp:468).
	ConsiderPreemption bool
}

// DefaultGroupConfig returns a GroupConfig populated with the design-doc
// section-9 defaults. Maps are non-nil and empty.
func DefaultGroupConfig() GroupConfig {
	return GroupConfig{
		GroupQuota:           map[string]float64{},
		GroupQuotaDynamic:    map[string]float64{},
		GroupAcceptSurplus:   map[string]bool{},
		GroupAutoregroup:     map[string]bool{},
		DefaultAcceptSurplus: false,
		DefaultAutoregroup:   false,
		GroupSortExpr:        DefaultGroupSortExpr,
		StrictEnforceQuota:   true,
		MaxAllocationRounds:  3,
		UseWeightedDemand:    true,
		RoundRobinRate:       math.Inf(1),
		UsingWeightedSlots:   false,
		ConsiderPreemption:   false,
	}
}

// ciEqual reports case-insensitive string equality (strcasecmp == 0).
func ciEqual(a, b string) bool { return strings.EqualFold(a, b) }

// ciLess reports strcasecmp(a,b) < 0 for ASCII group names.
func ciLess(a, b string) bool { return strings.ToLower(a) < strings.ToLower(b) }

// cfgFloat does a case-insensitive lookup in a name->float map.
func cfgFloat(m map[string]float64, name string) (float64, bool) {
	if v, ok := m[name]; ok {
		return v, true
	}
	for k, v := range m {
		if ciEqual(k, name) {
			return v, true
		}
	}
	return 0, false
}

// cfgBool does a case-insensitive lookup in a name->bool map.
func cfgBool(m map[string]bool, name string, def bool) bool {
	if v, ok := m[name]; ok {
		return v
	}
	for k, v := range m {
		if ciEqual(k, name) {
			return v
		}
	}
	return def
}

// parseGroupName splits a dotted group name into path components, mirroring the
// C++ parse_group_name (split on '.', empty components preserved).
func parseGroupName(gname string) []string {
	return strings.Split(gname, ".")
}

// findChild returns the child of parent whose leaf name matches name
// case-insensitively (the C++ chmap lookup), or nil.
func findChild(parent *negotiator.GroupNode, name string) *negotiator.GroupNode {
	for _, c := range parent.Children {
		if ciEqual(leafName(c.Name), name) {
			return c
		}
	}
	return nil
}

// leafName returns the last dotted component of a group node's full name.
func leafName(full string) string {
	if full == RootGroupName {
		return full
	}
	p := parseGroupName(full)
	return p[len(p)-1]
}

// BuildGroupTree constructs the accounting-group tree from cfg. The root group
// "<none>" is always present with AcceptSurplus=true. Group names are sorted
// case-insensitively so parents are created before children; names with a
// missing parent are warned-and-skipped, duplicates are warned-and-skipped, and
// the reserved name "<none>" in GroupNames is ignored. Per-group quota
// (static XOR dynamic), accept-surplus, and autoregroup flags are filled from
// cfg with the configured defaults. The root's Autoregroup is set to the
// effective global autoregroup value (true if any group has it), matching the
// C++.
//
// Warnings are returned as a slice of human-readable strings (the C++ dprintf
// D_ALWAYS warnings) so callers can log them; construction never fails on bad
// group definitions, only on an unparseable GROUP_SORT_EXPR.
func BuildGroupTree(cfg GroupConfig) (*negotiator.GroupNode, []string, error) {
	var warnings []string

	// Validate the sort expression up-front (C++ EXCEPTs on a bad expr).
	sortExpr := cfg.GroupSortExpr
	if sortExpr == "" {
		sortExpr = DefaultGroupSortExpr
	}
	if _, err := classad.ParseExpr(sortExpr); err != nil {
		return nil, warnings, fmt.Errorf("failed to parse GROUP_SORT_EXPR %q: %w", sortExpr, err)
	}

	// Collect group names, dropping the reserved root name.
	var groups []string
	for _, g := range cfg.GroupNames {
		if ciEqual(g, RootGroupName) {
			warnings = append(warnings, fmt.Sprintf("group name %q is reserved for root group -- ignoring this group", g))
			continue
		}
		groups = append(groups, g)
	}
	// Sort case-insensitively so a parent always precedes its children.
	sort.SliceStable(groups, func(i, j int) bool { return ciLess(groups[i], groups[j]) })

	root := &negotiator.GroupNode{Name: RootGroupName, AcceptSurplus: true}

	globalAutoregroup := cfg.DefaultAutoregroup

	for _, gname := range groups {
		gpath := parseGroupName(gname)

		// Walk to the parent, requiring each ancestor to already exist.
		group := root
		missingParent := false
		for k := 0; k < len(gpath)-1; k++ {
			child := findChild(group, gpath[k])
			if child == nil {
				warnings = append(warnings, fmt.Sprintf("ignoring group name %s with missing parent %s", gname, gpath[k]))
				missingParent = true
				break
			}
			group = child
		}
		if missingParent {
			continue
		}

		// Duplicate leaf?
		if findChild(group, gpath[len(gpath)-1]) != nil {
			warnings = append(warnings, fmt.Sprintf("ignoring duplicate group name %s", gname))
			continue
		}

		child := &negotiator.GroupNode{Name: gname, Parent: group}
		group.Children = append(group.Children, child)

		// Quota: static (GROUP_QUOTA_<g>) takes precedence over dynamic.
		if q, ok := cfgFloat(cfg.GroupQuota, gname); ok {
			if q < 0 {
				warnings = append(warnings, fmt.Sprintf("negative quota (%g) for group %s defaulting to zero", q, gname))
				q = 0
			}
			child.ConfigQuota = q
			child.StaticQuota = true
		} else if q, ok := cfgFloat(cfg.GroupQuotaDynamic, gname); ok {
			// Clamp to [0,1] like param_double's range.
			if q < 0 {
				q = 0
			} else if q > 1 {
				q = 1
			}
			child.ConfigQuota = q
			child.StaticQuota = false
		} else {
			warnings = append(warnings, fmt.Sprintf("no quota specified for group %q, defaulting to zero", gname))
			child.ConfigQuota = 0
			child.StaticQuota = false
		}

		child.AcceptSurplus = cfgBool(cfg.GroupAcceptSurplus, gname, cfg.DefaultAcceptSurplus)
		child.Autoregroup = cfgBool(cfg.GroupAutoregroup, gname, cfg.DefaultAutoregroup)
		if child.Autoregroup {
			globalAutoregroup = true
		}
	}

	// Root autoregroup mirrors the effective global value (used by the
	// accountant and to simplify the negotiator loops).
	root.Autoregroup = globalAutoregroup

	return root, warnings, nil
}

// BreadthFirst returns all nodes of the tree in breadth-first order (root
// first), matching the C++ hgq_groups ordering.
func BreadthFirst(root *negotiator.GroupNode) []*negotiator.GroupNode {
	var out []*negotiator.GroupNode
	queue := []*negotiator.GroupNode{root}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		out = append(out, n)
		queue = append(queue, n.Children...)
	}
	return out
}

// GetAssignedGroup maps a customer name (submitter or bare group name) to its
// accounting-group node, mirroring GroupEntry::GetAssignedGroup.
//
// Rules:
//   - A bare group name (no '@') that matches a defined group maps to itself
//     (the C++ pre-populated hgq_submitter_group_map); an unknown bare name
//     maps to the root.
//   - Otherwise strip everything from the last '@'; split the remainder on the
//     last '.'. No '.' => root. Walk the group path from the root, stopping at
//     the deepest matched ancestor if a component is unknown.
//
// nameMap is the seed map of all defined group names (lower-cased) to nodes;
// build it once with BuildNameMap.
func GetAssignedGroup(root *negotiator.GroupNode, nameMap map[string]*negotiator.GroupNode, customerName string) *negotiator.GroupNode {
	// Seeded lookup: defined group names (and any previously computed mapping)
	// resolve directly. This is what makes a bare group name map to itself.
	if n, ok := nameMap[strings.ToLower(customerName)]; ok {
		return n
	}

	atPos := strings.LastIndex(customerName, "@")
	isGroup := atPos < 0
	if isGroup {
		// Defunct group or malformed submitter name: root group.
		return root
	}

	// Strip '@' and everything after.
	gname := customerName[:atPos]

	// group/user separator?
	dotPos := strings.LastIndex(gname, ".")
	if dotPos < 0 {
		// No separator: "no group" -> root.
		return root
	}
	gname = gname[:dotPos]

	group := root
	for _, comp := range parseGroupName(gname) {
		child := findChild(group, comp)
		if child == nil {
			// Deepest matched ancestor.
			break
		}
		group = child
	}
	return group
}

// BuildNameMap returns the seed map used by GetAssignedGroup: every defined
// group name (lower-cased) mapped to its node, mirroring the C++
// GroupEntry::Initialize pre-population.
func BuildNameMap(root *negotiator.GroupNode) map[string]*negotiator.GroupNode {
	m := make(map[string]*negotiator.GroupNode)
	for _, n := range BreadthFirst(root) {
		m[strings.ToLower(n.Name)] = n
	}
	return m
}
