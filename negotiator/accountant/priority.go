package accountant

import (
	"math"
	"strings"
	"time"
)

// GetPriority returns the EFFECTIVE priority (real x factor) of a submitter,
// initializing a new submitter's real priority at MinPriority.
//
// WRITE-ON-READ (Accountant.cpp:320): this read has side effects. It calls
// GetPriorityFactor (which persists the chosen factor for a new submitter), and
// if the stored real priority is below MinPriority it clamps and persists the
// floor. The returned value is real x factor.
func (a *Accountant) GetPriority(submitter string) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.getPriorityLocked(submitter)
}

func (a *Accountant) getPriorityLocked(name string) float64 {
	factor := a.getPriorityFactorLocked(name)
	priority := MinPriority
	if v, ok := a.store.getFloat(tableCustomer, name, attrPriority); ok {
		priority = v
	}
	if priority < MinPriority {
		priority = MinPriority
		// Read-side write: persist the floored priority (C++ SetPriority).
		a.store.setFloat(tableCustomer, name, attrPriority, priority)
	}
	return priority * factor
}

// GetPriorityFactor returns the priority factor of a submitter.
//
// WRITE-ON-READ (Accountant.cpp:372): when no factor >= 1.0 is stored, the
// factor is chosen by the selection order below and PERSISTED before returning.
// Selection order:
//
//  1. stored factor, if >= 1.0;
//  2. nice-user factor, if the name begins with "nice-user";
//  3. GROUP_PRIO_FACTOR_<group> hook, if it returns a non-zero value;
//  4. remote factor, if LocalDomain is set and the submitter's @domain differs;
//  5. the default factor.
func (a *Accountant) GetPriorityFactor(submitter string) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.getPriorityFactorLocked(submitter)
}

func (a *Accountant) getPriorityFactorLocked(name string) float64 {
	if v, ok := a.store.getFloat(tableCustomer, name, attrPriorityFactor); ok && v >= minPriorityFactor {
		return v
	}
	factor := a.cfg.DefaultPrioFactor
	switch {
	case strings.HasPrefix(name, niceUserPrefix):
		factor = a.cfg.NiceUserPrioFactor
	case a.groupPriorityFactor(name) != 0:
		factor = a.groupPriorityFactor(name)
	case a.cfg.LocalDomain != "" && a.cfg.LocalDomain != domainOf(name):
		factor = a.cfg.RemotePrioFactor
	}
	// Read-side write (C++ SetPriorityFactor), clamped to the minimum.
	if factor < minPriorityFactor {
		factor = minPriorityFactor
	}
	a.store.setFloat(tableCustomer, name, attrPriorityFactor, factor)
	return factor
}

// groupPriorityFactor consults the GROUP_PRIO_FACTOR_<group> hook for a
// submitter's assigned group (Accountant.cpp:360). Returns 0 when unset.
func (a *Accountant) groupPriorityFactor(name string) float64 {
	if a.cfg.GroupPrioFactor == nil {
		return 0
	}
	return a.cfg.GroupPrioFactor(AssignedGroupName(name))
}

// UpdatePriorities applies the elapsed-time half-life decay tick to every
// customer record (Accountant.cpp:1094). The first tick after construction
// merely establishes the LastUpdateTime baseline.
func (a *Accountant) UpdatePriorities(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	T := now.Unix()
	timePassed := T - a.lastUpdate
	if timePassed == 0 {
		return
	}
	if timePassed < 0 {
		// Clock went backwards; just re-baseline (C++ Accountant.cpp:1103).
		a.setLastUpdate(T)
		return
	}
	aging := math.Pow(0.5, float64(timePassed)/a.halfLife)
	a.setLastUpdate(T)

	// Collect keys first so we can delete GC'd records without mutating the map
	// mid-iteration.
	var names []string
	a.store.forEach(tableCustomer, func(name string, _ *record) bool {
		names = append(names, name)
		return true
	})
	for _, name := range names {
		a.updateOnePriority(T, float64(timePassed), aging, name)
	}
}

func (a *Accountant) setLastUpdate(T int64) {
	a.lastUpdate = T
	a.store.setInt(tableAcct, "", attrLastUpdateTime, T)
}

// updateOnePriority decays a single customer's priority and rolls its uncharged
// time into accumulated usage (Accountant.cpp:1151). Must hold a.mu.
func (a *Accountant) updateOnePriority(T int64, timePassed, aging float64, name string) {
	r, ok := a.store.getRecord(tableCustomer, name)
	if !ok {
		return
	}

	priority := 0.0
	if v, ok := r.getFloat(attrPriority); ok {
		priority = v
	}
	if priority < MinPriority {
		priority = MinPriority
	}

	// set_prio_factor tracks whether a factor was explicitly stored; such
	// records are never GC'd (they hold an admin setting).
	_, setPrioFactor := r.getFloat(attrPriorityFactor)

	unchargedTime, _ := r.getInt(attrUnchargedTime)
	weightedUnchargedTime, _ := r.getFloat(attrWeightedUnchargedTime)
	accumulatedUsage, _ := r.getFloat(attrAccumulatedUsage)
	weightedAccumulatedUsage, hasWAU := r.getFloat(attrWeightedAccumulatedUsage)
	if !hasWAU {
		weightedAccumulatedUsage = accumulatedUsage
	}
	beginUsageTime, _ := r.getInt(attrBeginUsageTime)
	resourcesUsed, _ := r.getInt(attrResourcesUsed)
	weightedResourcesUsed, _ := r.getFloat(attrWeightedResourcesUsed)

	recentUsage := float64(resourcesUsed) + float64(unchargedTime)/timePassed
	weightedRecentUsage := weightedResourcesUsed + weightedUnchargedTime/timePassed

	// Group nodes with rolled-up subtree usage decay on the hierarchical total.
	if hier, ok := r.getFloat(attrHierWeightedResourcesUsed); ok && hier > 0 {
		weightedRecentUsage = hier
	}

	// Age out the old priority and fold in the new usage.
	newPriority := priority*aging + weightedRecentUsage*(1-aging)
	// The pre-clamp value drives the GC decision below; the persisted value is
	// clamped to the floor.
	clamped := newPriority
	if clamped < MinPriority {
		clamped = MinPriority
	}

	newAccum := accumulatedUsage + float64(resourcesUsed)*timePassed + float64(unchargedTime)
	newWeightedAccum := weightedAccumulatedUsage + weightedResourcesUsed*timePassed + weightedUnchargedTime

	if clamped != priority {
		a.store.setFloat(tableCustomer, name, attrPriority, clamped)
	}
	if newAccum != accumulatedUsage {
		a.store.setFloat(tableCustomer, name, attrAccumulatedUsage, newAccum)
	}
	if newWeightedAccum != weightedAccumulatedUsage {
		a.store.setFloat(tableCustomer, name, attrWeightedAccumulatedUsage, newWeightedAccum)
	}
	if newAccum > 0 && beginUsageTime == 0 {
		a.store.setInt(tableCustomer, name, attrBeginUsageTime, T)
	}
	if recentUsage > 0 {
		a.store.setInt(tableCustomer, name, attrLastUsageTime, T)
	}
	// Zero the uncharged buckets after folding them in.
	if unchargedTime != 0 {
		a.store.setInt(tableCustomer, name, attrUnchargedTime, 0)
	}
	if weightedUnchargedTime != 0 {
		a.store.setFloat(tableCustomer, name, attrWeightedUnchargedTime, 0)
	}

	// GC: purge an idle record that decayed below the floor and was never given
	// an explicit factor (design doc 3.1). The comparison uses the pre-clamp
	// priority so the rule is reachable (the C++ clamp on line 1200 otherwise
	// makes it dead code).
	if newPriority < MinPriority && resourcesUsed == 0 && newAccum == 0 && !setPrioFactor {
		a.store.deleteRecord(tableCustomer, name)
		return
	}

	// Clear the transient per-cycle share/limit annotations.
	if r.has(attrSubmitterShare) {
		a.store.setFloat(tableCustomer, name, attrSubmitterShare, 0)
	}
	if r.has(attrSubmitterLimit) {
		a.store.setFloat(tableCustomer, name, attrSubmitterLimit, 0)
	}
}
