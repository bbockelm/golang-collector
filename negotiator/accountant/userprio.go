package accountant

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/golang-collector/negotiator"
)

// errNoName is returned by the mutators when handed an empty submitter name.
var errNoName = errors.New("accountant: empty submitter name")

// ReportState renders the condor_userprio reply ad (design doc 3.5): a flat
// ClassAd of numbered attributes (Name<i>, Priority<i>, ...). Accounting groups
// are numbered first, breadth-first, then submitters; when rollup is true,
// child usage is summed into ancestor entries. Priority<i> is the EFFECTIVE
// priority (real x factor) for submitters. This mirrors
// Accountant::ReportState(bool) (Accountant.cpp:1418).
func (a *Accountant) ReportState(rollup bool) *classad.ClassAd {
	a.mu.Lock()
	defer a.mu.Unlock()

	ad := classad.New()
	ad.InsertAttr("LastUpdate", a.lastUpdate)

	// Number the groups breadth-first.
	nodes := BreadthFirst(a.groupTreeLocked())
	gnum := map[string]int{}
	entry := 1
	for _, n := range nodes {
		gnum[n.Name] = entry
		entry++
	}
	for _, n := range nodes {
		a.reportGroup(ad, n, gnum, rollup)
	}

	// Then the submitters (customer records with an '@').
	a.store.forEach(tableCustomer, func(name string, r *record) bool {
		if isGroupName(name) {
			return true
		}
		i := entry
		entry++
		suf := fmt.Sprintf("%d", i)
		ad.InsertAttrString("Name"+suf, name)
		ad.InsertAttrBool("IsAccountingGroup"+suf, false)
		ad.InsertAttrString("AccountingGroup"+suf, AssignedGroupName(name))
		ad.InsertAttrFloat("Priority"+suf, a.getPriorityLocked(name))
		ad.InsertAttr("Ceiling"+suf, ceilingOf(r))
		ad.InsertAttr("Floor"+suf, floorOf(r))
		ad.InsertAttrFloat("PriorityFactor"+suf, floatOr(r, attrPriorityFactor, 0))
		ad.InsertAttr("ResourcesUsed"+suf, intOr(r, attrResourcesUsed, 0))
		ad.InsertAttrFloat("WeightedResourcesUsed"+suf, floatOr(r, attrWeightedResourcesUsed, 0))
		ad.InsertAttrFloat("AccumulatedUsage"+suf, floatOr(r, attrAccumulatedUsage, 0))
		ad.InsertAttrFloat("WeightedAccumulatedUsage"+suf, floatOr(r, attrWeightedAccumulatedUsage, 0))
		ad.InsertAttrFloat("SubmitterShare"+suf, floatOr(r, attrSubmitterShare, 0))
		ad.InsertAttrFloat("SubmitterLimit"+suf, floatOr(r, attrSubmitterLimit, 0))
		ad.InsertAttr("BeginUsageTime"+suf, intOr(r, attrBeginUsageTime, 0))
		ad.InsertAttr("LastUsageTime"+suf, intOr(r, attrLastUsageTime, 0))
		return true
	})

	ad.InsertAttr("NumSubmittors", int64(entry-1))
	return ad
}

// reportGroup emits the numbered attributes for one group node and, in rollup
// mode, folds its usage into its parent's entry (Accountant.cpp:1531).
func (a *Accountant) reportGroup(ad *classad.ClassAd, n *negotiator.GroupNode, gnum map[string]int, rollup bool) {
	r, ok := a.store.getRecord(tableCustomer, n.Name)
	if !ok {
		return
	}
	i := gnum[n.Name]
	suf := fmt.Sprintf("%d", i)

	acctGroup := n.Name
	if n.Parent != nil {
		acctGroup = n.Parent.Name
	}
	ad.InsertAttrString("Name"+suf, n.Name)
	ad.InsertAttrBool("IsAccountingGroup"+suf, true)
	ad.InsertAttrString("AccountingGroup"+suf, acctGroup)

	prio := 0.0
	if !rollup {
		prio = a.getPriorityLocked(n.Name)
	}
	ad.InsertAttrFloat("Priority"+suf, prio)

	pf := 0.0
	if !rollup {
		pf = a.groupPriorityFactor(n.Name)
	} else {
		pf = floatOr(r, attrPriorityFactor, 0)
	}
	ad.InsertAttrFloat("PriorityFactor"+suf, pf)

	ad.InsertAttrFloat("EffectiveQuota"+suf, n.Quota)
	ad.InsertAttrFloat("ConfigQuota"+suf, n.ConfigQuota)
	ad.InsertAttrFloat("SubtreeQuota"+suf, n.SubtreeQuota)
	ad.InsertAttrFloat("GroupSortKey"+suf, n.SortKey)
	ad.InsertAttrString("SurplusPolicy"+suf, surplusPolicy(n))
	ad.InsertAttrFloat("Requested"+suf, n.Requested)

	ad.InsertAttr("ResourcesUsed"+suf, intOr(r, attrResourcesUsed, 0))
	ad.InsertAttrFloat("WeightedResourcesUsed"+suf, floatOr(r, attrWeightedResourcesUsed, 0))
	ad.InsertAttrFloat("AccumulatedUsage"+suf, floatOr(r, attrAccumulatedUsage, 0))
	ad.InsertAttrFloat("HierWeightedResourcesUsed"+suf, floatOr(r, attrHierWeightedResourcesUsed, 0))
	ad.InsertAttrFloat("WeightedAccumulatedUsage"+suf, floatOr(r, attrWeightedAccumulatedUsage, 0))
	ad.InsertAttr("BeginUsageTime"+suf, intOr(r, attrBeginUsageTime, 0))
	ad.InsertAttr("LastUsageTime"+suf, intOr(r, attrLastUsageTime, 0))

	if !rollup || n.Parent == nil {
		return
	}
	// Roll this group's values up into its parent's numbered entry.
	pnum := fmt.Sprintf("%d", gnum[n.Parent.Name])
	addInto(ad, "ResourcesUsed"+pnum, intOr(r, attrResourcesUsed, 0))
	addIntoFloat(ad, "WeightedResourcesUsed"+pnum, floatOr(r, attrWeightedResourcesUsed, 0))
	addIntoFloat(ad, "AccumulatedUsage"+pnum, floatOr(r, attrAccumulatedUsage, 0))
	addIntoFloat(ad, "WeightedAccumulatedUsage"+pnum, floatOr(r, attrWeightedAccumulatedUsage, 0))
	minInto(ad, "BeginUsageTime"+pnum, intOr(r, attrBeginUsageTime, 0))
	maxInto(ad, "LastUsageTime"+pnum, intOr(r, attrLastUsageTime, 0))
}

// AccountingAds renders the per-submitter / per-group Accounting ads for the
// collector publish (design doc 3.6). Each ad is the full Customer record plus
// Name, NegotiatorName, effective Priority, Ceiling, Floor, IsAccountingGroup,
// AccountingGroup, LastUpdate (and, for groups, the quota fields). Submitters
// with no recorded usage are skipped so idle records do not spam the collector.
func (a *Accountant) AccountingAds(negotiatorName string, now time.Time) []*classad.ClassAd {
	return a.accountingAds(negotiatorName, now, true)
}

// ReportStateAds renders one Accounting ad per Customer record with NO usage
// filter — the set the C++ Accountant::ReportState(queryAd, ads) returns for a
// direct QUERY_ACCOUNTING_ADS (Accountant.cpp:1701 iterates every customer).
// This is what a modern condor_userprio -modular reads straight from the
// negotiator, so it must include submitters seeded via SET_PRIORITY /
// SET_PRIORITYFACTOR that have not yet accrued any usage.
func (a *Accountant) ReportStateAds(negotiatorName string, now time.Time) []*classad.ClassAd {
	return a.accountingAds(negotiatorName, now, false)
}

func (a *Accountant) accountingAds(negotiatorName string, now time.Time, onlyWithUsage bool) []*classad.ClassAd {
	a.mu.Lock()
	defer a.mu.Unlock()

	nameToNode := map[string]*negotiator.GroupNode{}
	for _, n := range BreadthFirst(a.groupTreeLocked()) {
		nameToNode[n.Name] = n
	}

	var ads []*classad.ClassAd
	a.store.forEach(tableCustomer, func(name string, r *record) bool {
		group := isGroupName(name)
		if onlyWithUsage && !group && !r.has(attrResourcesUsed) {
			// Collector publish only: skip submitters with no accrued usage.
			return true
		}
		ad := recordToAd(r)
		ad.InsertAttrString("MyType", "Accounting")
		ad.InsertAttrString(slotName, name)
		if negotiatorName != "" {
			ad.InsertAttrString("NegotiatorName", negotiatorName)
		}
		ad.InsertAttr("LastUpdate", now.Unix())
		ad.InsertAttrFloat(attrPriority, a.getPriorityLocked(name))
		// recordToAd copied the RAW stored factor (0 for a submitter seeded with
		// only SET_PRIORITY); overwrite with the effective/write-on-read factor so
		// the ad matches C++, whose ReportState builds the ad AFTER GetPriority has
		// persisted the default factor (Accountant.cpp:1712-1717).
		ad.InsertAttrFloat(attrPriorityFactor, a.getPriorityFactorLocked(name))
		ad.InsertAttr(attrCeiling, ceilingOf(r))
		ad.InsertAttr(attrFloor, floorOf(r))
		ad.InsertAttrBool("IsAccountingGroup", group)
		ad.InsertAttrString("AccountingGroup", AssignedGroupName(name))
		// Uncharged buckets are internal bookkeeping; drop them from the ad
		// (matches the C++ comparison hack).
		ad.Delete(attrUnchargedTime)
		ad.Delete(attrWeightedUnchargedTime)
		if group {
			if n, ok := nameToNode[name]; ok {
				ad.InsertAttrString("AccountingGroup", n.Name)
				ad.InsertAttrFloat("EffectiveQuota", n.Quota)
				ad.InsertAttrFloat("ConfigQuota", n.ConfigQuota)
				ad.InsertAttrFloat("SubtreeQuota", n.SubtreeQuota)
				ad.InsertAttrFloat("GroupSortKey", n.SortKey)
				ad.InsertAttrString("SurplusPolicy", surplusPolicy(n))
				ad.InsertAttrFloat("Requested", n.Requested)
			}
		}
		ads = append(ads, ad)
		return true
	})
	return ads
}

// ResList renders the per-resource list for one submitter (GET_RESLIST /
// condor_userprio -getreslist): Name<i>/StartTime<i> for every Resource record
// currently charged to the submitter. Mirrors Accountant::ReportState(customer)
// (Accountant.cpp:1344-1382): a traditional submitter matches Resource records
// by RemoteUser; a group name matches by assigned group (and, as in C++, only
// advances the resource index for a group -- no Name<i> is emitted).
func (a *Accountant) ResList(submitter string) *classad.ClassAd {
	a.mu.Lock()
	defer a.mu.Unlock()

	ad := classad.New()
	group := isGroupName(submitter)
	n := 1
	a.store.forEach(tableResource, func(key string, r *record) bool {
		rname, ok := r.getString(attrRemoteUser)
		if !ok || rname == "" {
			return true
		}
		if group {
			if AssignedGroupName(rname) != submitter {
				return true
			}
		} else {
			if rname != submitter {
				return true
			}
			suf := strconv.Itoa(n)
			ad.InsertAttrString("Name"+suf, key)
			ad.InsertAttr("StartTime"+suf, intOr(r, attrStartTime, 0))
		}
		n++
		return true
	})
	return ad
}

// ---- Userprio mutators (SET_* / RESET_* / DELETE_USER handlers) ----

// SetPriorityFactor sets a submitter's priority factor (SET_PRIORITYFACTOR),
// clamping to the legal minimum like Accountant.cpp:451.
func (a *Accountant) SetPriorityFactor(submitter string, factor float64) error {
	if submitter == "" {
		return errNoName
	}
	if factor < minPriorityFactor {
		factor = minPriorityFactor
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setFloat(tableCustomer, submitter, attrPriorityFactor, factor)
	return nil
}

// SetPriority sets a submitter's real priority (SET_PRIORITY,
// Accountant.cpp:466).
func (a *Accountant) SetPriority(submitter string, priority float64) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setFloat(tableCustomer, submitter, attrPriority, priority)
	return nil
}

// SetAccumUsage sets a submitter's WeightedAccumulatedUsage (SET_ACCUMUSAGE).
// Note the C++ (Accountant.cpp:784) writes the WEIGHTED accumulated-usage
// attribute, which this faithfully reproduces.
func (a *Accountant) SetAccumUsage(submitter string, accumUsage float64) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setFloat(tableCustomer, submitter, attrWeightedAccumulatedUsage, accumUsage)
	return nil
}

// SetCeiling sets a submitter's weighted-usage ceiling (SET_CEILING). A
// negative value clears the cap (GetCeiling then reports -1 = unlimited).
func (a *Accountant) SetCeiling(submitter string, ceiling int64) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setInt(tableCustomer, submitter, attrCeiling, ceiling)
	return nil
}

// SetFloor sets a submitter's weighted-usage floor (SET_FLOOR). A negative
// value clears it (GetFloor then reports 0 = none).
func (a *Accountant) SetFloor(submitter string, floor int64) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setInt(tableCustomer, submitter, attrFloor, floor)
	return nil
}

// SetSubmitterShare stamps the spin-1 fair-share figures on the submitter's
// Customer record. The C++ writes SubmitterShare/SubmitterLimit onto the
// in-memory accounting ad on the first pie spin (matchmaker.cpp:2647-2655);
// they are transient — the next decay tick zeroes them (priority.go) — and
// surface in ReportState and the published Accounting ads.
func (a *Accountant) SetSubmitterShare(submitter string, share, limit float64) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setFloat(tableCustomer, submitter, attrSubmitterShare, share)
	a.store.setFloat(tableCustomer, submitter, attrSubmitterLimit, limit)
	return nil
}

// SetBeginTime sets a submitter's BeginUsageTime (SET_BEGINTIME,
// Accountant.cpp:794).
func (a *Accountant) SetBeginTime(submitter string, t time.Time) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setInt(tableCustomer, submitter, attrBeginUsageTime, t.Unix())
	return nil
}

// SetLastTime sets a submitter's LastUsageTime (SET_LASTTIME,
// Accountant.cpp:804).
func (a *Accountant) SetLastTime(submitter string, t time.Time) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setInt(tableCustomer, submitter, attrLastUsageTime, t.Unix())
	return nil
}

// ResetUsage clears a submitter's accumulated usage and restamps BeginUsageTime
// (RESET_USAGE / Accountant::ResetAccumulatedUsage, Accountant.cpp:425).
func (a *Accountant) ResetUsage(submitter string) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.resetUsageLocked(submitter, time.Now().Unix())
	return nil
}

// ResetAllUsage clears accumulated usage for every customer (RESET_ALL_USAGE,
// Accountant.cpp:406).
func (a *Accountant) ResetAllUsage() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	T := time.Now().Unix()
	var names []string
	a.store.forEach(tableCustomer, func(name string, _ *record) bool {
		names = append(names, name)
		return true
	})
	for _, name := range names {
		a.resetUsageLocked(name, T)
	}
	return nil
}

func (a *Accountant) resetUsageLocked(name string, T int64) {
	a.store.setFloat(tableCustomer, name, attrAccumulatedUsage, 0)
	a.store.setFloat(tableCustomer, name, attrWeightedAccumulatedUsage, 0)
	a.store.setInt(tableCustomer, name, attrBeginUsageTime, T)
}

// DeleteRecord removes a submitter's Customer record (DELETE_USER,
// Accountant.cpp:439).
func (a *Accountant) DeleteRecord(submitter string) error {
	if submitter == "" {
		return errNoName
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.deleteRecord(tableCustomer, submitter)
	return nil
}

// ---- small helpers ----

func surplusPolicy(n *negotiator.GroupNode) string {
	switch {
	case n.Autoregroup:
		return "regroup"
	case n.AcceptSurplus:
		return "byquota"
	default:
		return "no"
	}
}

func ceilingOf(r *record) int64 {
	if v, ok := r.getInt(attrCeiling); ok && v >= 0 {
		return v
	}
	return -1
}

func floorOf(r *record) int64 {
	if v, ok := r.getInt(attrFloor); ok && v >= 0 {
		return v
	}
	return 0
}

func floatOr(r *record, attr string, def float64) float64 {
	if v, ok := r.getFloat(attr); ok {
		return v
	}
	return def
}

func intOr(r *record, attr string, def int64) int64 {
	if v, ok := r.getInt(attr); ok {
		return v
	}
	return def
}

// recordToAd materializes a stored record into a ClassAd, preserving attribute
// kinds.
func recordToAd(r *record) *classad.ClassAd {
	ad := classad.New()
	for k, v := range r.attrs {
		switch x := v.(type) {
		case int64:
			ad.InsertAttr(k, x)
		case float64:
			ad.InsertAttrFloat(k, x)
		case string:
			ad.InsertAttrString(k, x)
		}
	}
	return ad
}

func addInto(ad *classad.ClassAd, attr string, delta int64) {
	cur, _ := classad.GetAs[int64](ad, attr)
	ad.InsertAttr(attr, cur+delta)
}

func addIntoFloat(ad *classad.ClassAd, attr string, delta float64) {
	cur, _ := classad.GetAs[float64](ad, attr)
	ad.InsertAttrFloat(attr, cur+delta)
}

func minInto(ad *classad.ClassAd, attr string, v int64) {
	cur, ok := classad.GetAs[int64](ad, attr)
	if !ok || v < cur {
		ad.InsertAttr(attr, v)
	}
}

func maxInto(ad *classad.ClassAd, attr string, v int64) {
	cur, ok := classad.GetAs[int64](ad, attr)
	if !ok || v > cur {
		ad.InsertAttr(attr, v)
	}
}
