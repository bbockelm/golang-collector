package cycle

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/accountant"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
)

// roundInfo describes one negotiateWithGroup invocation: the negotiating
// group (empty name = no group accounting, i.e. the flat pool or the
// autoregroup root), its quota/allocation, and whether this is a floor round.
type roundInfo struct {
	name            string // group accounting name; "" = none (C++ groupName==NULL)
	node            *negotiator.GroupNode
	quota           float64 // group quota / allocation; +Inf for the flat pool
	isFloor         bool
	autoregroupRoot bool
}

// mmResult mirrors the C++ MM_* result codes for one submitter negotiation.
type mmResult int

const (
	mmResume mmResult = iota // hit a limit; resume next spin
	mmDone                   // satisfied (or skipped for good); remove submitter
	mmError                  // protocol failure; invalidate socket + remove
)

// spinEnv carries the per-spin constants and the running pieLeft.
type spinEnv struct {
	spin            int
	groupusage      float64 // group usage sampled at spin top
	maxPrio         float64
	normalFactor    float64
	slotWeightTotal float64
	pieLeft         float64
	prios           map[string]float64 // per-spin effective-priority cache
	// ignoreSubmitterLimit is the C++ ignore_submitter_limit: on spin 1 with
	// preemption enabled, a submitter that exceeds its fair share keeps
	// negotiating but only for startd-rank-preferred (RANK) preemptions
	// (matchmaker.cpp:2484).
	ignoreSubmitterLimit bool
}

// negotiateWithGroup is the pie-spin do/while, a port of
// Matchmaker::negotiateWithGroup (matchmaker.cpp:2434-2845) with the
// preemption-off simplifications (ignore_submitter_limit always false).
func (c *Cycle) negotiateWithGroup(ctx context.Context, st *runState, ri roundInfo, submitters []*subState) error {
	subs := append([]*subState(nil), submitters...)

	spin := 0
	for {
		if err := ctx.Err(); err != nil {
			// Cycle deadline / cancellation: close out any open rounds.
			for _, sub := range subs {
				c.endRound(sub, true)
			}
			return err
		}
		spin++
		st.stats.PieSpins++

		// ignore_submitter_limit: spin 1 with preemption on lets over-limit
		// submitters keep negotiating for startd-rank preemptions, and (like
		// C++ :2487) suppresses the group-quota halt so those rank preemptions
		// are still considered.
		ignoreSubmitterLimit := spin == 1 && c.cfg.ConsiderPreemption

		// Group-quota halt check at the top of each spin (:2486-2491).
		groupusage := 0.0
		if !ignoreSubmitterLimit && ri.name != "" {
			groupusage = c.acct.GetWeightedResourcesUsed(ri.name)
			if groupusage >= ri.quota {
				break
			}
		} else if ri.name != "" {
			groupusage = c.acct.GetWeightedResourcesUsed(ri.name)
		}

		// Preemption off only: drop submitters with no idle jobs before the
		// normalization so they do not dilute fair share (:2499-2501). With
		// preemption on the C++ keeps them (they still count toward the
		// normalization factor); negotiateOne skips them via mmDone.
		if !c.cfg.ConsiderPreemption {
			subs = filterIdle(subs)
		}
		if len(subs) == 0 {
			break
		}

		env := &spinEnv{
			spin:                 spin,
			groupusage:           groupusage,
			prios:                make(map[string]float64, len(subs)),
			ignoreSubmitterLimit: ignoreSubmitterLimit,
		}
		c.calculateNormalizationFactor(subs, env)

		// slotWeightTotal = min(untrimmed total, group quota) (:2509-2512).
		env.slotWeightTotal = st.untrimmedTotal
		if env.slotWeightTotal > ri.quota {
			env.slotWeightTotal = ri.quota
		}

		env.pieLeft = c.calculatePieLeft(subs, ri, env)
		if !c.cfg.ConsiderPreemption && env.pieLeft <= 0 {
			// Preemption off: nothing left to hand out (:2527-2532). With
			// preemption on, startd-rank preemptions may still match even with
			// no fair-share pie; the outer progress check terminates the loop.
			break
		}

		if spin == 1 {
			p3 := c.now()
			c.sortSubmitters(subs, env)
			st.stats.Phase3Duration += c.now().Sub(p3)
		}

		// Prefetch this spin's sessions + first request batches (:2561).
		pf := c.now()
		c.prefetchSpin(ctx, st, subs)
		st.stats.PrefetchDuration += c.now().Sub(pf)

		pieLeftOrig := env.pieLeft
		origCount := len(subs)

		p4 := c.now()
		var next []*subState
		halted := false
		for _, sub := range subs {
			if halted || ctx.Err() != nil {
				// The round was halted; the remaining prefetched sessions
				// still need a clean END_NEGOTIATE.
				c.endRound(sub, false)
				next = append(next, sub)
				continue
			}
			// Group quota met mid-spin: halt (matchmaker.cpp:2584-2588).
			// Suppressed on spin 1 with preemption on (ignore_submitter_limit).
			if !env.ignoreSubmitterLimit && ri.name != "" && c.acct.GetWeightedResourcesUsed(ri.name) >= ri.quota {
				halted = true
				c.endRound(sub, false)
				next = append(next, sub)
				continue
			}
			switch c.negotiateOne(ctx, st, ri, sub, env) {
			case mmResume:
				next = append(next, sub)
			case mmDone, mmError:
				// removed from the list (:2795-2827)
			}
		}
		subs = next
		st.stats.Phase4Duration += c.now().Sub(p4)

		// Continue while progress was made and work remains; floor rounds
		// never spin more than once (:2831-2834).
		progressed := env.pieLeft < pieLeftOrig || len(subs) < origCount
		if !progressed || len(subs) == 0 || st.view.Len() == 0 || ri.isFloor {
			break
		}
	}
	return nil
}

// filterIdle drops submitters with no idle jobs, preserving order
// (filter_submitters_no_idle).
func filterIdle(subs []*subState) []*subState {
	out := subs[:0]
	for _, sub := range subs {
		if sub.idleJobs > 0 {
			out = append(out, sub)
		}
	}
	return out
}

// getPrio returns the (spin-cached) effective priority of a submitter.
func (c *Cycle) getPrio(env *spinEnv, name string) float64 {
	if p, ok := env.prios[name]; ok {
		return p
	}
	p := c.acct.GetPriority(name)
	env.prios[name] = p
	return p
}

// calculateNormalizationFactor is matchmaker.cpp:5633-5668: maxPrio is the
// numerically largest (i.e. worst) effective priority; normalFactor is the
// sum of maxPrio/prio over UNIQUE submitter names (the same submitter
// flocking from several schedds counts once).
func (c *Cycle) calculateNormalizationFactor(subs []*subState, env *spinEnv) {
	env.maxPrio = -math.MaxFloat64
	for _, sub := range subs {
		if p := c.getPrio(env, sub.name); p > env.maxPrio {
			env.maxPrio = p
		}
	}
	seen := make(map[string]bool, len(subs))
	env.normalFactor = 0
	for _, sub := range subs {
		if seen[sub.name] {
			continue
		}
		seen[sub.name] = true
		env.normalFactor += env.maxPrio / c.getPrio(env, sub.name)
	}
}

// calculateSubmitterLimit is Matchmaker::calculateSubmitterLimit
// (matchmaker.cpp:5513-5574). It returns both the full submitter limit and the
// "unclaimed" variant:
//
//   - limitUnclaimed = fair-share limit capped by the group-quota headroom
//     (quota - groupusage);
//   - limit          = fair-share limit; with preemption OFF it collapses to
//     limitUnclaimed (:5556), with preemption ON it stays uncapped by the group
//     headroom (claimed/prio-preemption matches may exceed it).
//
// Floor rounds additionally cap the full limit at the submitter's Floor (applied
// after the ConsiderPreemption collapse, matching :5568-5572).
func (c *Cycle) calculateSubmitterLimit(name string, ri roundInfo, env *spinEnv, isFloorRound bool) (limit, limitUnclaimed, share, usage, prio float64) {
	prio = c.getPrio(env, name)
	usage = c.acct.GetWeightedResourcesUsed(name)
	share = env.maxPrio / (prio * env.normalFactor)
	limit = share*env.slotWeightTotal - usage
	if limit < 0 {
		limit = 0
	}
	// Unclaimed variant: cap at the group-quota headroom (:5547-5556).
	limitUnclaimed = limit
	if ri.name != "" {
		maxAllowed := ri.quota - env.groupusage
		if maxAllowed < 0 {
			maxAllowed = 0
		}
		if limitUnclaimed > maxAllowed {
			limitUnclaimed = maxAllowed
		}
	}
	// Preemption off: the unclaimed variant IS the submitter limit (:5556).
	if !c.cfg.ConsiderPreemption {
		limit = limitUnclaimed
	}
	if isFloorRound {
		if floor := c.acct.GetFloor(name); floor < limit {
			limit = floor
		}
	}
	return limit, limitUnclaimed, share, usage, prio
}

// calculatePieLeft is matchmaker.cpp:5577-5630: the sum of every submitter's
// limit this spin (always computed with isFloorRound=false, as the C++ does),
// stamping each submitter's starvation ratio for the spin-1 sort.
func (c *Cycle) calculatePieLeft(subs []*subState, ri roundInfo, env *spinEnv) float64 {
	pie := 0.0
	for _, sub := range subs {
		limit, _, _, usage, _ := c.calculateSubmitterLimit(sub.name, ri, env, false)
		sub.starvation = starvationRatio(usage, usage+limit)
		pie += limit
	}
	return pie
}

// starvationRatio is matchmaker.cpp:1817-1819.
func starvationRatio(usage, allocated float64) float64 {
	if allocated > 0 {
		return usage / allocated
	}
	return math.MaxFloat32
}

// sortSubmitters orders the submitters for the whole group negotiation,
// porting struct submitterLessThan (matchmaker.cpp:332-381). Key order:
//
//  1. effective priority ascending (numerically lower = better priority
//     negotiates first);
//  2. [C++ optional secondary key on job priority when USE_GLOBAL_JOB_PRIOS
//     is set -- not implemented in the MVP];
//  3. SubmitterStarvation ascending (stamped by calculatePieLeft);
//  4. LastHeardFrom % 1009 ascending (the C++ pseudo-random tiebreak for
//     same-named submitters on different schedds);
//  5. original submitter-ad order (Go addition: sort.SliceStable + this key
//     makes the order fully deterministic where the C++ std::sort is
//     unspecified on full ties).
func (c *Cycle) sortSubmitters(subs []*subState, env *spinEnv) {
	sort.SliceStable(subs, func(i, j int) bool {
		a, b := subs[i], subs[j]
		pa, pb := c.getPrio(env, a.name), c.getPrio(env, b.name)
		if pa != pb {
			return pa < pb
		}
		if a.starvation != b.starvation {
			return a.starvation < b.starvation
		}
		ta, tb := a.lastHeard%1009, b.lastHeard%1009
		if ta != tb {
			return ta < tb
		}
		return a.origIdx < b.origIdx
	})
}

// negotiateOne computes one submitter's limit/ceiling and either skips it
// (the optimization ladder at matchmaker.cpp:2718-2768) or negotiates it.
func (c *Cycle) negotiateOne(ctx context.Context, st *runState, ri roundInfo, sub *subState, env *spinEnv) mmResult {
	limit, limitUnclaimed, share, _, prio := c.calculateSubmitterLimit(sub.name, ri, env, ri.isFloor)

	// Spin 1 only: save the fair-share figures on the submitter's accounting
	// record — SubmitterShare and SubmitterLimit = share x slotWeightTotal
	// (NOT the starved/capped limit) — for ReportState and the published
	// Accounting ads (matchmaker.cpp:2647-2655).
	if env.spin == 1 {
		_ = c.acct.SetSubmitterShare(sub.name, share, share*env.slotWeightTotal)
	}

	// Starvation cap: never hand a submitter more than what is left of the
	// pie this spin (:2657-2667).
	if limit > env.pieLeft {
		limit = env.pieLeft
	}

	// Ceiling headroom (:2669-2680); -1 = unlimited.
	headroom := math.MaxFloat64
	if ceiling := c.acct.GetCeiling(sub.name); ceiling >= 0 {
		headroom = ceiling - c.acct.GetWeightedResourcesUsed(sub.name)
		if headroom < 0 {
			headroom = 0
		}
	}

	now := c.now()
	remainingCycle := c.cfg.MaxTimePerCycle - now.Sub(st.cycleStart)
	remainingSubmitter := c.cfg.MaxTimePerSubmitter - sub.timeUsed

	var result mmResult
	switch {
	case sub.idleJobs <= 0:
		result = mmDone
	case remainingSubmitter <= 0:
		result = mmDone // out of per-submitter time (:2736-2744)
	case remainingCycle <= 0:
		result = mmDone // out of cycle time (:2754-2758)
	case env.spin > 1 && (limit < st.minSlotWeight || env.pieLeft < st.minSlotWeight):
		result = mmResume // skip to the next spin (:2759-2763)
	case headroom <= 0 || headroom < st.minSlotWeight:
		result = mmDone // ceiling exhausted (:2764-2768)
	default:
		deadline := now.Add(minDuration(c.cfg.MaxTimePerSpin, minDuration(remainingCycle, remainingSubmitter)))
		result = c.negotiateSubmitter(ctx, st, ri, sub, env, prio, limit, limitUnclaimed, headroom, deadline)
		sub.timeUsed += c.now().Sub(now)
	}

	// The skip paths never touched the session the prefetch opened; close the
	// round out either way.
	c.endRound(sub, result == mmError)
	return result
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// negotiateSubmitter is the per-submitter request loop, porting
// Matchmaker::negotiate (matchmaker.cpp:4127-4513):
//
//   - fetch requests through the (prefetched) session, honoring
//     NEGOTIATOR_RESOURCE_REQUEST_LIST_SIZE per batch;
//   - each request ad represents Request.Count identical jobs
//     (_condor_RESOURCE_COUNT): it is offered matches one at a time (the C++
//     resource_request_count / resource_request_offers cursor) until Count
//     matches, a rejection, or a limit stops it;
//   - a rejection marks the request's autocluster rejected: queued requests
//     with the same autocluster are silently skipped, exactly like the C++
//     m_rejected_auto_clusters set (matchmaker_negotiate.cpp:196-218);
//   - limit/deadline/ceiling breaks return MM_RESUME (the C++ falls out of
//     the loop to endNegotiate + MM_RESUME); running out of requests returns
//     MM_DONE unless the submitter was limited this round (:4244-4248).
func (c *Cycle) negotiateSubmitter(ctx context.Context, st *runState, ri roundInfo, sub *subState, env *spinEnv, prio, submitterLimit, submitterLimitUnclaimed, ceilingHeadroom float64, deadline time.Time) mmResult {
	if err := sub.err(); err != nil {
		return mmError // prefetch already failed
	}
	if !sub.sessOpen {
		return mmError
	}

	// The per-negotiation deadline governs the spine's own blocking calls
	// (request fetches, matchmaking); queued async deliveries run under the
	// cycle-wide context instead so a returned spine never cancels in-flight
	// PERMISSION_AND_AD writes.
	sctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	// Two match-cost accumulators (matchmaker.cpp:4145-4146): limitUsed counts
	// every match, limitUsedUnclaimed only unclaimed (NO_PREEMPTION) matches.
	limitUsed := 0.0
	limitUsedUnclaimed := 0.0
	limitedBySubmitterLimit := false
	onlyForStartdRank := false
	rejectedACs := make(map[int]bool)

	var cur *negotiator.Request
	curOffers := 0

	for {
		if err := sub.err(); err != nil {
			return mmError // async delivery marked the session broken
		}
		now := c.now()
		if !now.Before(deadline) {
			break // deadline reached (:4185-4193) -> MM_RESUME
		}
		// Over the submitter limit: normally stop, but with ignore_submitter_limit
		// (spin 1 + preemption) keep going for startd-rank preemptions only
		// (matchmaker.cpp:4196-4213).
		if limitUsed >= submitterLimit {
			if env.ignoreSubmitterLimit {
				onlyForStartdRank = true
			} else {
				break // submitter resource limit -> MM_RESUME
			}
		} else {
			onlyForStartdRank = false
		}
		if limitUsed >= ceilingHeadroom {
			break // ceiling (:4216-4219) -> MM_RESUME; next spin sees MM_DONE
		}

		// Next request: keep offering the current one while its Count lasts.
		if cur == nil || curOffers >= cur.Count {
			cur = nil
			curOffers = 0
			req, err := c.nextRequest(sctx, sub, rejectedACs)
			if err != nil {
				return mmError
			}
			if req == nil {
				// Out of requests: round over (:4235-4248).
				c.endRound(sub, false)
				if limitUsed >= submitterLimit || limitedBySubmitterLimit {
					return mmResume
				}
				return mmDone
			}
			cur = req
		}
		st.stats.JobsConsidered++

		c.enrichRequest(st, ri, sub, cur, prio, env)

		limits := &negotiator.MatchLimits{
			SubmitterLimit:          submitterLimit, // already pieLeft-capped by the caller
			LimitUsed:               limitUsed,
			PieLeft:                 env.pieLeft,
			Ceiling:                 ceilingHeadroom,
			SubmitterLimitUnclaimed: submitterLimitUnclaimed,
			LimitUsedUnclaimed:      limitUsedUnclaimed,
			OnlyForStartdRank:       onlyForStartdRank,
			SubmitterName:           sub.name,
		}
		cand, rej, err := c.mm.Match(sctx, cur, st.view, limits)
		if err != nil {
			if sctx.Err() != nil {
				break // deadline mid-scan -> MM_RESUME
			}
			return mmError
		}

		if cand == nil {
			// REJECTED_WITH_REASON (:4306-4390).
			st.stats.Rejections++
			reason := "no match found"
			if rej != nil && rej.Reason != "" {
				reason = rej.Reason
			}
			if !c.sendReject(ctx, sub, cur, reason) {
				return mmError
			}
			if rej != nil && rej.ForSubmitterLimit > 0 {
				limitedBySubmitterLimit = true
				if c.cfg.DisableSlotWeights && !c.cfg.ConsiderPreemption {
					// Unweighted + preemption-off: a submitter-limit reject
					// means we are done this spin (:4450-4459).
					break
				}
			}
			if cur.AutoClusterID >= 0 {
				rejectedACs[cur.AutoClusterID] = true
			}
			cur = nil
			continue
		}

		// A match: claim + enrich + deliver + account (:4465-4504 and
		// matchmakingProtocol).
		cost := c.slotWeightOf(cand.Slot)
		claim, ok := st.view.ClaimID(cand.Slot)
		if !ok {
			claim = "null" // claiming off / no private ad
		}
		enriched := protocol.EnrichMatchAd(cand.Slot, cur, c.matchContext(st, ri, sub, cur))
		mr := &negotiator.MatchResult{
			Request: cur,
			SlotAd:  enriched,
			ClaimID: claim,
			Cost:    cost,
		}
		if !c.sendMatch(ctx, sub, mr) {
			return mmError
		}
		c.acct.AddMatch(sub.name, cand.Slot, c.now())
		st.view.Consume(cand.ScanIndex)
		st.stats.Matches++
		limitUsed += cost
		// Only unclaimed (NO_PREEMPTION) matches count against the unclaimed
		// accumulator (matchmaker.cpp:4502: `if (remoteUser == "")`).
		if remoteUserOf(cand.Slot) == "" {
			limitUsedUnclaimed += cost
		}
		env.pieLeft -= cost
		curOffers++
	}

	// Broke out on a limit/deadline: round over, resume next spin
	// (:4508-4512).
	c.endRound(sub, false)
	return mmResume
}

// nextRequest returns the next usable request from the submitter's queue,
// fetching more batches from the schedd as needed and skipping rejected
// autoclusters. Returns (nil, nil) when the schedd is out of requests.
func (c *Cycle) nextRequest(ctx context.Context, sub *subState, rejectedACs map[int]bool) (*negotiator.Request, error) {
	for {
		for len(sub.queue) > 0 {
			r := sub.queue[0]
			sub.queue = sub.queue[1:]
			if r.AutoClusterID >= 0 && rejectedACs[r.AutoClusterID] {
				continue // skip rejected autocluster silently
			}
			return r, nil
		}
		if sub.exhausted {
			return nil, nil
		}
		var (
			reqs []*negotiator.Request
			err  error
		)
		sess := sub.sess
		sub.run(func() {
			reqs, err = sess.FetchRequests(ctx, c.cfg.RequestListSize)
		})
		if err != nil {
			sub.setErr(err)
			return nil, err
		}
		if len(reqs) == 0 {
			sub.exhausted = true
			return nil, nil
		}
		sub.queue = append(sub.queue, reqs...)
	}
}

// enrichRequest folds the submitter/group context into the request ad before
// matching (matchmaker.cpp:4256-4275).
func (c *Cycle) enrichRequest(st *runState, ri roundInfo, sub *subState, req *negotiator.Request, prio float64, env *spinEnv) {
	// The negotiating-group context is stamped on EVERY request, flat pool or
	// not (matchmaker.cpp:4257-4258); the SubmitterGroup* family only when the
	// submitter maps to a real accounting group.
	sc := protocol.SubmitterContext{
		UserPrio:           prio,
		UserResourcesInUse: c.acct.GetWeightedResourcesUsed(sub.name),
		NegotiatingGroup:   c.negotiatingGroupName(ri),
		Autoregroup:        ri.autoregroupRoot,
	}
	if g := c.assignedGroup(st, sub); g != nil {
		sc.Group = g.Name
		sc.GroupResourcesInUse = c.acct.GetWeightedResourcesUsed(g.Name)
		sc.GroupQuota = g.Quota
	}
	protocol.EnrichRequestAd(req.Ad, sc)
	_ = env
}

// matchContext builds the accounting-group context stamped on the offer ad
// before PERMISSION_AND_AD (design doc section 5).
func (c *Cycle) matchContext(st *runState, ri roundInfo, sub *subState, req *negotiator.Request) protocol.MatchContext {
	mc := protocol.MatchContext{}
	if cl, ok := req.Ad.EvaluateAttrString("ConcurrencyLimits"); ok && cl != "" {
		mc.ConcurrencyLimits = cl
	}
	if g := c.assignedGroup(st, sub); g != nil {
		mc.RemoteGroup = g.Name
		mc.RemoteNegotiatingGroup = c.negotiatingGroupName(ri)
		mc.RemoteAutoregroup = ri.autoregroupRoot
		mc.HasAutoregroup = true
	}
	return mc
}

// assignedGroup returns the submitter's accounting-group node, or nil when
// the pool is flat or the submitter maps to the root (the C++
// getGroupInfoFromUserId returns false in those cases).
func (c *Cycle) assignedGroup(st *runState, sub *subState) *negotiator.GroupNode {
	if st.tree == nil {
		return nil
	}
	g := accountant.GetAssignedGroup(st.tree, st.nameMap, sub.name)
	if g == nil || g == st.tree {
		return nil
	}
	return g
}

// negotiatingGroupName is the group name stamped as
// SubmitterNegotiatingGroup / RemoteNegotiatingGroup: the current round's
// group, or the root's name when negotiating without group accounting.
func (c *Cycle) negotiatingGroupName(ri roundInfo) string {
	if ri.name != "" {
		return ri.name
	}
	return accountant.RootGroupName
}
