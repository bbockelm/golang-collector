// Package accountant implements the HTCondor negotiator's accounting core in
// Go: user priorities with half-life decay, weighted usage tabulation, the
// native persistent state store, and the condor_userprio protocol surface.
//
// It is a faithful port of src/condor_negotiator.V6/Accountant.cpp; the
// normative specification is docs/NEGOTIATOR_DESIGN.md section 3. The exported
// type satisfies negotiator.Accountant.
//
// The hierarchical group-quota algorithms (surplus/scarcity allocation, quota
// normalization, GROUP_SORT_EXPR ordering) live in a sibling set of files
// (group*.go) owned by another workstream. This file provides only a
// flat-pool default GroupTree() and the submitter->group name-mapping helper
// (AssignedGroupName) that both workstreams share.
package accountant

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/golang-collector/negotiator"
)

// Constants mirroring the C++ Accountant (Accountant.cpp:40, :80-89).
const (
	// MinPriority is both the priority floor and the new-submitter initial
	// real priority (C++ MinPriority=0.5).
	MinPriority = 0.5
	// minPriorityFactor is the smallest legal priority factor
	// (C++ MIN_PRIORITY_FACTOR=1.0).
	minPriorityFactor = 1.0
	// RootGroupName ("<none>") is declared in grouptree.go (owned by the
	// hierarchical-group workstream); this package shares that single const.
	//
	// niceUserPrefix marks nice-user submitters (C++ NiceUserName="nice-user").
	niceUserPrefix = "nice-user"

	// defaultHalfLife is PRIORITY_HALFLIFE's default (seconds).
	defaultHalfLife = 86400.0
)

// ClassAd attribute names, matching the C++ ClassAdLog keys exactly (see
// Accountant.cpp:42-67 and design doc 3.4) so a future Accountantnew.log
// importer is a pure format adapter.
const (
	attrPriority                  = "Priority"
	attrPriorityFactor            = "PriorityFactor"
	attrResourcesUsed             = "ResourcesUsed"
	attrWeightedResourcesUsed     = "WeightedResourcesUsed"
	attrHierWeightedResourcesUsed = "HierWeightedResourcesUsed"
	attrUnchargedTime             = "UnchargedTime"
	attrWeightedUnchargedTime     = "WeightedUnchargedTime"
	attrAccumulatedUsage          = "AccumulatedUsage"
	attrWeightedAccumulatedUsage  = "WeightedAccumulatedUsage"
	attrBeginUsageTime            = "BeginUsageTime"
	attrLastUsageTime             = "LastUsageTime"
	attrCeiling                   = "Ceiling"
	attrFloor                     = "Floor"
	attrLastUpdateTime            = "LastUpdateTime"
	attrRemoteUser                = "RemoteUser"
	attrStartTime                 = "StartTime"
	attrSlotWeight                = "SlotWeight"

	// transient, not persisted across a decay tick (cleared each cycle).
	attrSubmitterShare = "SubmitterShare"
	attrSubmitterLimit = "SubmitterLimit"

	// attrRoundRobinTime persists a group's round-robin timestamp (the C++
	// GroupEntry rr_time) on the group's Customer record. Go-native attr: the
	// C++ keeps rr_time in its in-memory group tree (which lives as long as
	// the process), while our tree is rebuilt every cycle, so the value
	// round-trips through the state store instead (see RRTimeStore).
	attrRoundRobinTime = "RoundRobinTime"
)

// slot ad attribute names.
const (
	slotName       = "Name"
	slotStartdIP   = "StartdIpAddr"
	slotState      = "State"
	slotActivity   = "Activity"
	slotRemoteUser = "RemoteUser"
	slotAcctGroup  = "AccountingGroup"
	slotSlotWeight = "SlotWeight"
)

// Config holds the accountant's tunables. Zero-valued numeric fields are filled
// with their HTCondor defaults by New; DefaultConfig returns a fully-populated
// Config (with UseSlotWeights=true) as a convenient starting point.
type Config struct {
	// HalfLife is PRIORITY_HALFLIFE (default 86400s). A zero value means the
	// default.
	HalfLife time.Duration
	// DefaultPrioFactor is DEFAULT_PRIO_FACTOR (default 1000).
	DefaultPrioFactor float64
	// NiceUserPrioFactor is NICE_USER_PRIO_FACTOR (default 1e10).
	NiceUserPrioFactor float64
	// RemotePrioFactor is REMOTE_PRIO_FACTOR (default 1e7); only applied when
	// LocalDomain is non-empty and the submitter's @domain differs.
	RemotePrioFactor float64
	// LocalDomain is ACCOUNTANT_LOCAL_DOMAIN (""); empty means every user is
	// local (remote factor never applies).
	LocalDomain string
	// UseSlotWeights is NEGOTIATOR_USE_SLOT_WEIGHTS (default true).
	UseSlotWeights bool
	// DiscountSuspended is NEGOTIATOR_DISCOUNT_SUSPENDED_RESOURCES (default
	// false): exclude suspended claims from usage.
	DiscountSuspended bool
	// GroupPrioFactor is the GROUP_PRIO_FACTOR_<group> hook (nil ok). It is
	// consulted for a submitter's assigned group; a non-zero result overrides
	// the default factor. Mirrors Accountant::getGroupPriorityFactor.
	GroupPrioFactor func(group string) float64
	// LogFile is ACCOUNTANT_DATABASE_FILE; "" means memory-only (tests).
	LogFile string
	// ImportFrom, when non-empty, is the path to a C++ negotiator
	// Accountantnew.log (ClassAdLog journal) whose accumulated priority/usage
	// is imported ONCE into a fresh native store, so a Go negotiator can take
	// over a running C++ pool in place (roadmap #4). The import is a no-op when
	// the native store already holds Customer records (an earlier run already
	// imported), so it is safe to leave configured. See import.go.
	ImportFrom string
}

// DefaultConfig returns a Config populated with the HTCondor default values,
// including UseSlotWeights=true (which cannot be expressed as a Go zero value).
func DefaultConfig() Config {
	return Config{
		HalfLife:           time.Duration(defaultHalfLife) * time.Second,
		DefaultPrioFactor:  1000,
		NiceUserPrioFactor: 1e10,
		RemotePrioFactor:   1e7,
		UseSlotWeights:     true,
	}
}

// Accountant is the concurrency-safe implementation of negotiator.Accountant.
// A single sync.Mutex serializes every interface method, making each compound
// store sequence (AddMatch, UpdatePriorities, ...) atomic.
type Accountant struct {
	mu    sync.Mutex
	store *Store
	cfg   Config

	halfLife   float64 // seconds
	lastUpdate int64   // cached mirror of the Accountant singleton LastUpdateTime
}

var _ negotiator.Accountant = (*Accountant)(nil)

// New constructs an Accountant, opening (or creating) its state store and
// replaying any existing transaction log.
func New(cfg Config) (*Accountant, error) {
	if cfg.HalfLife <= 0 {
		cfg.HalfLife = time.Duration(defaultHalfLife) * time.Second
	}
	if cfg.DefaultPrioFactor <= 0 {
		cfg.DefaultPrioFactor = 1000
	}
	if cfg.NiceUserPrioFactor <= 0 {
		cfg.NiceUserPrioFactor = 1e10
	}
	if cfg.RemotePrioFactor <= 0 {
		cfg.RemotePrioFactor = 1e7
	}
	store, err := OpenStore(cfg.LogFile)
	if err != nil {
		return nil, fmt.Errorf("accountant: %w", err)
	}
	// One-shot import of a C++ Accountantnew.log. IDEMPOTENCY GUARD: only when
	// the freshly replayed native store holds no Customer records yet (a fresh
	// native log). A later run replays the native log we wrote below, finds
	// Customer records, and skips -- so ImportFrom may stay configured
	// permanently (NEGOTIATOR_CPP_DIFFERENCES.md §3). Must run before the root
	// group / reconcile bootstrap below, which itself writes Customer records.
	if cfg.ImportFrom != "" && store.count(tableCustomer) == 0 {
		n, err := store.loadCppAccountantLog(cfg.ImportFrom)
		if err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("accountant: importing %q: %w", cfg.ImportFrom, err)
		}
		slog.Info("accountant: imported C++ Accountantnew.log into native store",
			"path", cfg.ImportFrom, "records", n)
	}
	a := &Accountant{
		store:    store,
		cfg:      cfg,
		halfLife: cfg.HalfLife.Seconds(),
	}
	if v, ok := store.getInt(tableAcct, "", attrLastUpdateTime); ok {
		a.lastUpdate = v
	}
	// Ensure the root group's Customer record exists, mirroring the C++
	// Initialize step that instantiates a table entry (and priority factor)
	// for every configured accounting group (Accountant.cpp:273-289).
	a.mu.Lock()
	a.getPriorityLocked(RootGroupName)
	a.reconcileResourcesUsed()
	a.mu.Unlock()
	return a, nil
}

// Close releases the state store.
func (a *Accountant) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.Close()
}

// AssignedGroupName maps a submitter (or bare group) name to the name of its
// assigned accounting group using the C++ name-mapping rule
// (GroupEntry::GetAssignedGroup, GroupEntry.cpp:1046):
//
//   - strip the trailing "@domain";
//   - split the remainder on its LAST '.': the part before the dot is the
//     group name;
//   - a name with no '.' (and any name with no '@', i.e. a bare group) maps to
//     the root group RootGroupName ("<none>").
//
// This is the pure, tree-independent mapping shared with the group-quota
// workstream. When an actual GROUP_NAMES tree is configured, that workstream
// resolves an undefined dotted prefix to its deepest matched ancestor; this
// helper returns the full dotted prefix, which for a flat pool (no configured
// groups) is what the rollup keys on.
func AssignedGroupName(name string) string {
	at := strings.LastIndex(name, "@")
	if at < 0 {
		// No '@' -> this is itself a (defunct) group name; it maps to root.
		return RootGroupName
	}
	local := name[:at]
	if dot := strings.LastIndex(local, "."); dot >= 0 {
		return local[:dot]
	}
	return RootGroupName
}

// isGroupName reports whether a customer key names an accounting group (no '@')
// rather than a submitter, matching the C++ IsGroup test.
func isGroupName(name string) bool {
	return !strings.Contains(name, "@")
}

// domainOf returns the part after the last '@' (C++ Accountant::GetDomain).
func domainOf(name string) string {
	if at := strings.LastIndex(name, "@"); at >= 0 {
		return name[at+1:]
	}
	return ""
}

// ancestorChain returns a group name and all of its dotted ancestors, deepest
// first, matching the C++ "strip trailing .segment" walk used to roll
// HierWeightedResourcesUsed up the tree (Accountant.cpp:903-916).
func ancestorChain(group string) []string {
	chain := []string{}
	part := group
	for part != "" {
		chain = append(chain, part)
		if dot := strings.LastIndex(part, "."); dot >= 0 {
			part = part[:dot]
		} else {
			part = ""
		}
	}
	return chain
}

// GroupTree returns the accounting-group hierarchy for the cycle. This is the
// FLAT-POOL DEFAULT: a single root node named RootGroupName with
// accept_surplus=true and no children. The hierarchical-group workstream
// replaces this with a real GROUP_NAMES-derived tree (with quotas assigned);
// callers must treat the result as read-only.
func (a *Accountant) GroupTree() *negotiator.GroupNode {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.groupTreeLocked()
}

// groupTreeLocked builds the flat-pool group tree; callers hold a.mu. The
// hierarchical-group workstream will supply a richer implementation behind this
// seam.
func (a *Accountant) groupTreeLocked() *negotiator.GroupNode {
	root := &negotiator.GroupNode{
		Name:          RootGroupName,
		AcceptSurplus: true,
	}
	if u, ok := a.store.getFloat(tableCustomer, RootGroupName, attrWeightedResourcesUsed); ok {
		root.Usage = u
	}
	return root
}

// GetGroupRRTime returns a group's persisted round-robin timestamp (Unix
// seconds), or (0, false) when the group has never been stamped. Part of the
// RRTimeStore surface the group allocator uses for cross-cycle RR fairness.
func (a *Accountant) GetGroupRRTime(group string) (float64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.getFloat(tableCustomer, group, attrRoundRobinTime)
}

// SetGroupRRTime persists a group's round-robin timestamp (see GetGroupRRTime).
func (a *Accountant) SetGroupRRTime(group string, t float64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.store.setFloat(tableCustomer, group, attrRoundRobinTime, t)
}

// GetWeightedResourcesUsed returns the weighted usage for a submitter or bare
// group name (Accountant.cpp:309).
func (a *Accountant) GetWeightedResourcesUsed(name string) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, _ := a.store.getFloat(tableCustomer, name, attrWeightedResourcesUsed)
	return v
}

// GetCeiling returns the configured ceiling (-1 = unlimited),
// matching Accountant.cpp:335.
func (a *Accountant) GetCeiling(submitter string) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.store.getInt(tableCustomer, submitter, attrCeiling)
	if !ok || v < 0 {
		return -1
	}
	return float64(v)
}

// GetFloor returns the configured floor (0 = no floor), matching
// Accountant.cpp:345.
func (a *Accountant) GetFloor(submitter string) float64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.store.getInt(tableCustomer, submitter, attrFloor)
	if !ok || v < 0 {
		return 0
	}
	return float64(v)
}

// getSlotWeight extracts a slot's weight, honoring UseSlotWeights and defaulting
// to 1.0 when disabled, missing, or negative (Accountant.cpp:2082).
func (a *Accountant) getSlotWeight(slot *classad.ClassAd) float64 {
	if !a.cfg.UseSlotWeights {
		return 1.0
	}
	if w, ok := classad.GetAs[float64](slot, slotSlotWeight); ok && w >= 0 {
		return w
	}
	return 1.0
}

// reconcileResourcesUsed is the startup sanity check (Accountant.cpp:195-271):
// after loading the log, fix each submitter's ResourcesUsed / WeightedResourcesUsed
// to match the Resource records actually attributed to it. Only exact-match
// (submitter) customers are reconciled here; group aggregates are rebuilt by
// the group workstream. Must be called with a.mu held.
func (a *Accountant) reconcileResourcesUsed() {
	type agg struct {
		n int64
		w float64
	}
	byUser := map[string]*agg{}
	a.store.forEach(tableResource, func(_ string, r *record) bool {
		user, ok := r.getString(attrRemoteUser)
		if !ok {
			return true
		}
		w, ok := r.getFloat(attrSlotWeight)
		if !ok {
			w = 1.0
		}
		g := byUser[user]
		if g == nil {
			g = &agg{}
			byUser[user] = g
		}
		g.n++
		g.w += w
		return true
	})
	a.store.forEach(tableCustomer, func(name string, r *record) bool {
		if isGroupName(name) {
			return true
		}
		g := byUser[name]
		var n int64
		var w float64
		if g != nil {
			n, w = g.n, g.w
		}
		if cur, _ := r.getInt(attrResourcesUsed); cur != n {
			a.store.setInt(tableCustomer, name, attrResourcesUsed, n)
		}
		if cur, _ := r.getFloat(attrWeightedResourcesUsed); cur != w {
			a.store.setFloat(tableCustomer, name, attrWeightedResourcesUsed, w)
		}
		return true
	})
}
