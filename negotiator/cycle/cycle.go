// Package cycle implements the negotiator's cycle orchestrator (Phase 5 of
// docs/NEGOTIATOR_DESIGN.md): one Run executes a full negotiation cycle --
// snapshot, accounting, significant attributes, the pie-spin fair-share loop
// (flat or hierarchical-group-quota dispatch), and accounting-ad publication.
//
// C++ behavioral reference: src/condor_negotiator.V6/matchmaker.cpp
// negotiationTime() :1861-2177, negotiateWithGroup() :2434-2845 and
// negotiate() :4127-4513 (the loop skeletons ported in spin.go).
//
// Concurrency contract ("concurrency for speed, determinism for decisions",
// design doc section 2): the fast mode overlaps I/O only -- RRL prefetch runs
// in a bounded worker pool and PERMISSION_AND_AD delivery streams from
// per-submitter ordered goroutines -- while every matchmaking decision is
// made on the single serial spine in submitter-sorted order. Compat mode
// (Config.CompatMode) executes the identical decision sequence with all I/O
// inline; the determinism test asserts the delivered match lists are equal.
package cycle

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/accountant"
	"github.com/bbockelm/golang-collector/negotiator/matchmaker"
)

// Cycle runs negotiation cycles. It implements negotiator.Cycle.
type Cycle struct {
	src  negotiator.AdSource
	acct negotiator.Accountant
	sf   negotiator.SessionFactory
	mm   negotiator.Matchmaker
	cfg  Config

	now func() time.Time // injectable clock (tests)
}

var _ negotiator.Cycle = (*Cycle)(nil)

// New builds a Cycle over the four collaborator interfaces. The matchmaker is
// constructed internally from the rank-expression passthrough (serial when
// CompatMode).
func New(src negotiator.AdSource, acct negotiator.Accountant, sf negotiator.SessionFactory, cfg Config) (*Cycle, error) {
	if src == nil || acct == nil || sf == nil {
		return nil, fmt.Errorf("cycle: nil AdSource, Accountant, or SessionFactory")
	}
	if cfg.ConsiderPreemption {
		return nil, fmt.Errorf("cycle: NEGOTIATOR_CONSIDER_PREEMPTION=true is not supported (preemption is deferred)")
	}
	cfg = cfg.withDefaults()
	mm, err := matchmaker.New(matchmaker.Config{
		PreJobRank:         cfg.PreJobRank,
		PostJobRank:        cfg.PostJobRank,
		Serial:             cfg.CompatMode,
		DisableSlotWeights: cfg.DisableSlotWeights,
	})
	if err != nil {
		return nil, fmt.Errorf("cycle: %w", err)
	}
	return &Cycle{
		src:  src,
		acct: acct,
		sf:   sf,
		mm:   mm,
		cfg:  cfg,
		now:  time.Now,
	}, nil
}

// runState is the per-cycle mutable state shared by the spin loops.
type runState struct {
	cycleStart time.Time
	view       *matchmaker.SlotView
	sigAttrs   string

	minSlotWeight  float64
	untrimmedTotal float64 // untrimmedSlotWeightTotal

	stats *negotiator.CycleStats

	// subs maps submitter ads to their cycle-lived state (sessions, budgets),
	// shared between the floor round and the main round.
	subs map[*classad.ClassAd]*subState

	// Group context (nil tree = flat pool).
	tree        *negotiator.GroupNode
	nameMap     map[string]*negotiator.GroupNode
	autoregroup bool
}

// Run executes one negotiation cycle (design doc 4.1).
func (c *Cycle) Run(ctx context.Context) (*negotiator.CycleStats, error) {
	start := c.now()
	stats := newStats(start)

	// Enforce the whole-cycle time budget through the context so even a
	// wedged peer cannot extend the cycle past MaxTimePerCycle.
	ctx, cancel := context.WithDeadline(ctx, start.Add(c.cfg.MaxTimePerCycle))
	defer cancel()

	// ---- Phase 1: obtain ads.
	snap, err := c.src.Snapshot(ctx)
	if err != nil {
		return stats, fmt.Errorf("cycle: snapshot: %w", err)
	}
	stats.Phase1Duration = c.now().Sub(start)
	stats.TotalSlots = len(snap.Slots)
	stats.Submitters = len(snap.Submitters)

	// ---- Phase 2: accounting.
	p2 := c.now()
	c.acct.UpdatePriorities(p2)
	c.acct.CheckMatches(snap.Slots, p2)

	// Significant attributes (compute_significant_attrs, matchmaker.cpp:1994).
	sig := computeSignificantAttrs(snap.Slots, c.cfg.PreJobRank, c.cfg.PostJobRank, c.cfg.JobConstraint)
	stats.Phase2Duration = c.now().Sub(p2)

	// Pool sizes over the UNTRIMMED slot set (matchmaker.cpp:1978-2016).
	untrimmedTotal, minSlotWeight := c.sumSlotWeights(snap.Slots)
	weightedPoolsize := untrimmedTotal
	if c.cfg.DisableSlotWeights {
		weightedPoolsize = float64(len(snap.Slots))
	}
	effectivePoolsize := countEffectiveSlots(snap.Slots)

	// Trim (preemption off): claimed/preempting non-pslot ads leave the
	// candidate set (trimStartdAds_PreemptionLogic, matchmaker.cpp:2986-3007).
	trimmed := trimSlots(snap.Slots)
	stats.TrimmedSlots = len(snap.Slots) - len(trimmed)
	stats.CandidateSlots = len(trimmed)

	trimSnap := &negotiator.PoolSnapshot{
		Slots:      trimmed,
		Submitters: snap.Submitters,
		ClaimIDs:   snap.ClaimIDs,
		Taken:      snap.Taken,
	}

	st := &runState{
		cycleStart:     start,
		view:           matchmaker.NewSlotView(trimSnap),
		sigAttrs:       sig,
		minSlotWeight:  minSlotWeight,
		untrimmedTotal: untrimmedTotal,
		stats:          stats,
		subs:           make(map[*classad.ClassAd]*subState, len(snap.Submitters)),
	}
	defer c.drainWorkers(st)

	// ---- Dispatch: flat vs HGQ (matchmaker.cpp:2052-2123).
	tree, _, err := accountant.BuildGroupTree(c.cfg.Group)
	if err != nil {
		return stats, fmt.Errorf("cycle: group tree: %w", err)
	}
	groups := accountant.BreadthFirst(tree)

	subs := c.wrapSubmitters(st, snap.Submitters)

	if len(groups) <= 1 {
		// Traditional flat pool: optional floor round, then the full round.
		if below := c.belowFloor(subs); len(below) > 0 {
			if err := c.negotiateWithGroup(ctx, st, roundInfo{isFloor: true, quota: math.Inf(1)}, below); err != nil {
				return stats, err
			}
		}
		if err := c.negotiateWithGroup(ctx, st, roundInfo{quota: math.Inf(1)}, subs); err != nil {
			return stats, err
		}
	} else {
		st.tree = tree
		st.nameMap = accountant.BuildNameMap(tree)
		st.autoregroup = tree.Autoregroup

		totalQuota := weightedPoolsize
		if c.cfg.DisableSlotWeights {
			totalQuota = float64(effectivePoolsize)
		}
		usage := c.acct.GetWeightedResourcesUsed
		accountant.PrepareForMatchmaking(tree, snap.Submitters, totalQuota, c.cfg.Group, usage)

		cb := func(g *negotiator.GroupNode, allocation float64) error {
			gsubs := c.wrapSubmitters(st, g.Submitters)
			// Autoregroup: the root negotiates with no group accounting name
			// (matchmaker.cpp:2084-2086).
			name := g.Name
			autoregroupRoot := false
			if st.autoregroup && g == tree {
				name = ""
				autoregroupRoot = true
			}
			ri := roundInfo{
				name:            name,
				node:            g,
				quota:           allocation,
				autoregroupRoot: autoregroupRoot,
			}
			if below := c.belowFloor(gsubs); len(below) > 0 {
				fri := ri
				fri.isFloor = true
				if err := c.negotiateWithGroup(ctx, st, fri, below); err != nil {
					return err
				}
			}
			return c.negotiateWithGroup(ctx, st, ri, gsubs)
		}
		// Persist per-group rr_time across cycles when the accountant supports
		// it (the concrete accountant does; a wrapping test stub may not).
		rrt, _ := c.acct.(accountant.RRTimeStore)
		if err := accountant.NegotiateAllGroups(tree, totalQuota, c.cfg.Group, usage, rrt, cb); err != nil {
			return stats, err
		}
	}

	// Flush async deliveries before publishing/stat-finalizing so the second
	// cycle's warm reuse (and the caller's view of the stats) is consistent.
	c.drainWorkers(st)

	// ---- Publish accounting ads.
	if !c.cfg.DisableAccountingAds {
		ads := c.acct.AccountingAds(c.cfg.NegotiatorName, c.now())
		if err := c.src.PublishAccountingAds(ctx, ads); err != nil && ctx.Err() == nil {
			return stats, fmt.Errorf("cycle: publishing accounting ads: %w", err)
		}
	}

	stats.End = c.now()
	return stats, nil
}

// wrapSubmitters returns the cycle-lived subState wrappers for ads, creating
// them on first sight (shared across floor/main rounds and HGQ callbacks).
// Ads missing Name/ScheddName/ScheddIpAddr are dropped, as in the C++
// per-submitter loop (matchmaker.cpp:2590-2599).
func (c *Cycle) wrapSubmitters(st *runState, ads []*classad.ClassAd) []*subState {
	out := make([]*subState, 0, len(ads))
	for _, ad := range ads {
		if sub, ok := st.subs[ad]; ok {
			out = append(out, sub)
			continue
		}
		name, ok1 := ad.EvaluateAttrString("Name")
		scheddName, ok2 := ad.EvaluateAttrString("ScheddName")
		scheddAddr, ok3 := ad.EvaluateAttrString("ScheddIpAddr")
		if !ok1 || !ok2 || !ok3 || name == "" || scheddAddr == "" {
			continue
		}
		sub := &subState{
			ad:         ad,
			name:       name,
			scheddName: scheddName,
			scheddAddr: scheddAddr,
			origIdx:    len(st.subs),
		}
		sub.tag, _ = ad.EvaluateAttrString("SubmitterTag")
		if v, ok := ad.EvaluateAttrInt("IdleJobs"); ok && v > 0 {
			sub.idleJobs = int(v)
		}
		st.stats.IdleJobs += sub.idleJobs
		sub.lastHeard, _ = ad.EvaluateAttrInt("LastHeardFrom")
		st.subs[ad] = sub
		out = append(out, sub)
	}
	return out
}

// belowFloor selects the submitters with a configured floor they are below
// (findBelowFloorSubmitters, matchmaker.cpp:5778-5793).
func (c *Cycle) belowFloor(subs []*subState) []*subState {
	var out []*subState
	for _, sub := range subs {
		floor := c.acct.GetFloor(sub.name)
		usage := c.acct.GetWeightedResourcesUsed(sub.name)
		if floor > 0 && usage < floor {
			out = append(out, sub)
		}
	}
	return out
}

// drainWorkers stops every per-submitter delivery worker and waits for the
// queued I/O to finish.
func (c *Cycle) drainWorkers(st *runState) {
	for _, sub := range st.subs {
		sub.stopWorker()
	}
}

// slotWeightOf mirrors Accountant::GetSlotWeight (Accountant.cpp:2082) with
// the matchmaker's Cpus fallback for ads that missed the AdSource fixup: the
// SlotWeight expression when it evaluates to a non-negative number, else
// Cpus, else 1.0. With slot weights disabled every slot costs 1.0.
func (c *Cycle) slotWeightOf(slot *classad.ClassAd) float64 {
	if c.cfg.DisableSlotWeights {
		return 1.0
	}
	if f, ok := evalNumber(slot, "SlotWeight"); ok && f >= 0 {
		return f
	}
	if f, ok := evalNumber(slot, "Cpus"); ok && f >= 0 {
		return f
	}
	return 1.0
}

// sumSlotWeights is the C++ sumSlotWeights (matchmaker.cpp:3062): the total
// and minimum slot weight over the (untrimmed) slot set.
func (c *Cycle) sumSlotWeights(slots []*classad.ClassAd) (total, min float64) {
	for _, s := range slots {
		w := c.slotWeightOf(s)
		total += w
		if min == 0 || w < min {
			min = w
		}
	}
	return total, min
}

// countEffectiveSlots is count_effective_slots (matchmaker.cpp:1821): a
// partitionable slot counts as its Cpus, everything else as 1.
func countEffectiveSlots(slots []*classad.ClassAd) int {
	sum := 0
	for _, s := range slots {
		if b, ok := s.EvaluateAttrBool("PartitionableSlot"); ok && b {
			if cpus, ok := s.EvaluateAttrInt("Cpus"); ok && cpus > 0 {
				sum += int(cpus)
				continue
			}
		}
		sum++
	}
	return sum
}

// trimSlots drops Claimed/Preempting non-partitionable slots, matching
// trimStartdAds_PreemptionLogic with ConsiderPreemption=false
// (matchmaker.cpp:2986-3007). Claimed pslots stay (they still accept jobs).
func trimSlots(slots []*classad.ClassAd) []*classad.ClassAd {
	out := make([]*classad.ClassAd, 0, len(slots))
	for _, s := range slots {
		state, _ := s.EvaluateAttrString("State")
		if state == "Claimed" || state == "Preempting" {
			if pslot, ok := s.EvaluateAttrBool("PartitionableSlot"); !ok || !pslot {
				continue
			}
		}
		out = append(out, s)
	}
	return out
}

// evalNumber evaluates attr to a float64, accepting integers and reals.
func evalNumber(ad *classad.ClassAd, attr string) (float64, bool) {
	v := ad.EvaluateAttr(attr)
	if v.IsReal() {
		f, err := v.RealValue()
		return f, err == nil
	}
	if v.IsInteger() {
		i, err := v.IntValue()
		return float64(i), err == nil
	}
	return 0, false
}
