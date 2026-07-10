package accountant

import (
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// resource/state string values (condor_state.cpp).
const (
	stateClaimed    = "Claimed"
	statePreempting = "Preempting"
	stateMatched    = "Matched"
	activitySuspend = "Suspended"
)

// getResourceName reproduces Accountant::GetResourceName (Accountant.cpp:1772):
// the slot's Name attribute, an '@', and its StartdIpAddr (empty when absent).
func getResourceName(slot *classad.ClassAd) (string, bool) {
	name, ok := classad.GetAs[string](slot, slotName)
	if !ok {
		return "", false
	}
	addr, _ := classad.GetAs[string](slot, slotStartdIP)
	return name + "@" + addr, true
}

// remoteUserOf returns the effective remote user of a claimed slot: the
// AccountingGroup attribute if present, else RemoteUser (Accountant.cpp:1828).
func remoteUserOf(slot *classad.ClassAd) (string, bool) {
	if g, ok := classad.GetAs[string](slot, slotAcctGroup); ok && g != "" {
		return g, true
	}
	u, ok := classad.GetAs[string](slot, slotRemoteUser)
	return u, ok
}

// isClaimed reports whether a slot ad counts as an active claim and, if so, the
// customer it is charged to (Accountant.cpp:1818).
func (a *Accountant) isClaimed(slot *classad.ClassAd) (string, bool) {
	state, ok := classad.GetAs[string](slot, slotState)
	if !ok {
		return "", false
	}
	if !strings.EqualFold(state, stateClaimed) && !strings.EqualFold(state, statePreempting) {
		return "", false
	}
	user, ok := remoteUserOf(slot)
	if !ok {
		return "", false
	}
	if a.cfg.DiscountSuspended {
		if act, ok := classad.GetAs[string](slot, slotActivity); ok && strings.EqualFold(act, activitySuspend) {
			return "", false
		}
	}
	return user, true
}

// checkClaimedOrMatched reports whether a slot ad is still claimed/matched by
// the expected customer (Accountant.cpp:1856), used to reap stale Resource
// records. Preemption bookkeeping is omitted (deferred with preemption).
func (a *Accountant) checkClaimedOrMatched(slot *classad.ClassAd, customer string) bool {
	state, ok := classad.GetAs[string](slot, slotState)
	if !ok {
		return false
	}
	if strings.EqualFold(state, stateMatched) {
		return true
	}
	if !strings.EqualFold(state, stateClaimed) && !strings.EqualFold(state, statePreempting) {
		return false
	}
	user, ok := remoteUserOf(slot)
	if !ok {
		return false
	}
	if user != customer {
		return false
	}
	if a.cfg.DiscountSuspended {
		if act, ok := classad.GetAs[string](slot, slotActivity); ok && strings.EqualFold(act, activitySuspend) {
			return false
		}
	}
	return true
}

// AddMatch charges a new match to a submitter: bumps usage, pre-charges the
// uncharged-time bucket for the interval since the last decay tick, rolls the
// weighted usage up the assigned group's ancestor chain, and writes the
// Resource record (Accountant.cpp:816).
func (a *Accountant) AddMatch(submitter string, slotAd *classad.ClassAd, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	name, ok := getResourceName(slotAd)
	if !ok {
		return
	}
	a.addMatchLocked(submitter, name, a.getSlotWeight(slotAd), now.Unix())
}

func (a *Accountant) addMatchLocked(customer, resourceName string, weight float64, T int64) {
	delta := T - a.lastUpdate

	// Submitter record.
	ru, _ := a.store.getInt(tableCustomer, customer, attrResourcesUsed)
	wru, _ := a.store.getFloat(tableCustomer, customer, attrWeightedResourcesUsed)
	ut, _ := a.store.getInt(tableCustomer, customer, attrUnchargedTime)
	wut, _ := a.store.getFloat(tableCustomer, customer, attrWeightedUnchargedTime)
	a.store.setInt(tableCustomer, customer, attrResourcesUsed, ru+1)
	a.store.setFloat(tableCustomer, customer, attrWeightedResourcesUsed, wru+weight)
	a.store.setInt(tableCustomer, customer, attrUnchargedTime, ut-delta)
	a.store.setFloat(tableCustomer, customer, attrWeightedUnchargedTime, wut-float64(delta)*weight)

	// Assigned group record (same bookkeeping a second time).
	group := AssignedGroupName(customer)
	gru, _ := a.store.getInt(tableCustomer, group, attrResourcesUsed)
	gwru, _ := a.store.getFloat(tableCustomer, group, attrWeightedResourcesUsed)
	gut, _ := a.store.getInt(tableCustomer, group, attrUnchargedTime)
	gwut, _ := a.store.getFloat(tableCustomer, group, attrWeightedUnchargedTime)
	a.store.setInt(tableCustomer, group, attrResourcesUsed, gru+1)
	a.store.setFloat(tableCustomer, group, attrWeightedResourcesUsed, gwru+weight)
	a.store.setInt(tableCustomer, group, attrUnchargedTime, gut-delta)
	a.store.setFloat(tableCustomer, group, attrWeightedUnchargedTime, gwut-float64(delta)*weight)

	// Roll HierWeightedResourcesUsed up the group's ancestor chain.
	for _, part := range ancestorChain(group) {
		h, _ := a.store.getFloat(tableCustomer, part, attrHierWeightedResourcesUsed)
		a.store.setFloat(tableCustomer, part, attrHierWeightedResourcesUsed, h+weight)
	}

	// Resource record.
	a.store.setString(tableResource, resourceName, attrRemoteUser, customer)
	a.store.setFloat(tableResource, resourceName, attrSlotWeight, weight)
	a.store.setInt(tableResource, resourceName, attrStartTime, T)
}

// RemoveMatch settles and removes a match by resource name: charges the elapsed
// time since max(StartTime, LastUpdateTime) into the uncharged-time bucket,
// decrements usage (floored at zero), unwinds the group ancestor chain, and
// deletes the Resource record (Accountant.cpp:944).
func (a *Accountant) RemoveMatch(resourceName string, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.removeMatchLocked(resourceName, now.Unix())
}

func (a *Accountant) removeMatchLocked(resourceName string, T int64) {
	rec, ok := a.store.getRecord(tableResource, resourceName)
	if !ok {
		return
	}
	customer, ok := rec.getString(attrRemoteUser)
	if !ok {
		a.store.deleteRecord(tableResource, resourceName)
		return
	}
	startTime, _ := rec.getInt(attrStartTime)
	weight, ok := rec.getFloat(attrSlotWeight)
	if !ok {
		weight = 1.0
	}
	if startTime < a.lastUpdate {
		startTime = a.lastUpdate
	}
	charged := T - startTime

	// Submitter record.
	ru, _ := a.store.getInt(tableCustomer, customer, attrResourcesUsed)
	wru, _ := a.store.getFloat(tableCustomer, customer, attrWeightedResourcesUsed)
	ut, _ := a.store.getInt(tableCustomer, customer, attrUnchargedTime)
	wut, _ := a.store.getFloat(tableCustomer, customer, attrWeightedUnchargedTime)
	if ru > 0 {
		ru--
	}
	wru -= weight
	if wru < 0 {
		wru = 0
	}
	a.store.setInt(tableCustomer, customer, attrResourcesUsed, ru)
	a.store.setFloat(tableCustomer, customer, attrWeightedResourcesUsed, wru)
	a.store.setInt(tableCustomer, customer, attrUnchargedTime, ut+charged)
	a.store.setFloat(tableCustomer, customer, attrWeightedUnchargedTime, wut+float64(charged)*weight)

	// Assigned group record.
	group := AssignedGroupName(customer)
	gru, _ := a.store.getInt(tableCustomer, group, attrResourcesUsed)
	gwru, _ := a.store.getFloat(tableCustomer, group, attrWeightedResourcesUsed)
	gut, _ := a.store.getInt(tableCustomer, group, attrUnchargedTime)
	gwut, _ := a.store.getFloat(tableCustomer, group, attrWeightedUnchargedTime)
	if gru > 0 {
		gru--
	}
	gwru -= weight
	if gwru < 0 {
		gwru = 0
	}
	a.store.setInt(tableCustomer, group, attrResourcesUsed, gru)
	a.store.setFloat(tableCustomer, group, attrWeightedResourcesUsed, gwru)
	a.store.setInt(tableCustomer, group, attrUnchargedTime, gut+charged)
	a.store.setFloat(tableCustomer, group, attrWeightedUnchargedTime, gwut+float64(charged)*weight)

	for _, part := range ancestorChain(group) {
		h, _ := a.store.getFloat(tableCustomer, part, attrHierWeightedResourcesUsed)
		h -= weight
		if h < 0 {
			h = 0
		}
		a.store.setFloat(tableCustomer, part, attrHierWeightedResourcesUsed, h)
	}

	a.store.deleteRecord(tableResource, resourceName)
}

// CheckMatches is the per-cycle reconcile (Accountant.cpp:1260): reap Resource
// records that are no longer claimed by their recorded user, zero every
// customer's WeightedResourcesUsed, then re-AddMatch every currently-claimed
// slot so weighted usage is rebuilt authoritatively from the live pool. The
// integer ResourcesUsed stays incremental (fixed by the startup sanity check),
// matching the C++ semantics.
func (a *Accountant) CheckMatches(slotAds []*classad.ClassAd, now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Index live slots by resource name.
	live := make(map[string]*classad.ClassAd, len(slotAds))
	for _, ad := range slotAds {
		if name, ok := getResourceName(ad); ok {
			live[name] = ad
		}
	}

	// Reap stale Resource records (collect first, delete after).
	var stale []string
	a.store.forEach(tableResource, func(name string, r *record) bool {
		ad, present := live[name]
		if !present {
			stale = append(stale, name)
			return true
		}
		user, ok := r.getString(attrRemoteUser)
		if !ok || !a.checkClaimedOrMatched(ad, user) {
			stale = append(stale, name)
		}
		return true
	})
	for _, name := range stale {
		a.store.deleteRecord(tableResource, name)
	}

	// Zero all WeightedResourcesUsed; usage is rebuilt from scratch below.
	var customers []string
	a.store.forEach(tableCustomer, func(name string, _ *record) bool {
		customers = append(customers, name)
		return true
	})
	for _, name := range customers {
		a.store.setFloat(tableCustomer, name, attrWeightedResourcesUsed, 0)
	}

	// Re-add every currently-claimed slot.
	T := now.Unix()
	for _, ad := range slotAds {
		if cust, ok := a.isClaimed(ad); ok {
			if name, ok := getResourceName(ad); ok {
				a.addMatchLocked(cust, name, a.getSlotWeight(ad), T)
			}
		}
	}
}
