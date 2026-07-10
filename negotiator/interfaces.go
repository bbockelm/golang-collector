package negotiator

import (
	"context"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// AdSource yields the negotiator's view of the pool for one cycle and accepts
// the ads the negotiator publishes back. Implementations: embedded (direct
// read of the collector's store, in-process) and remote (CEDAR queries to a
// collector, like the C++ negotiator).
type AdSource interface {
	// Snapshot gathers slot, submitter, and startd-private ads, applying the
	// per-ad fixups and constraint filters from design doc 4.1. Gathering MAY
	// be internally concurrent; the returned snapshot is immutable.
	Snapshot(ctx context.Context) (*PoolSnapshot, error)
	// PublishNegotiatorAd pushes the negotiator's own daemon ad.
	PublishNegotiatorAd(ctx context.Context, ad *classad.ClassAd) error
	// PublishAccountingAds pushes the per-submitter/per-group accounting ads
	// emitted at the end of each cycle.
	PublishAccountingAds(ctx context.Context, ads []*classad.ClassAd) error
}

// Accountant owns priorities, usage tabulation, and (MVP) the hierarchical
// group-quota tree. All methods are safe for concurrent use; the semantics --
// including the write-on-read side effects of GetPriority/GetPriorityFactor --
// are specified in design doc section 3.
type Accountant interface {
	// UpdatePriorities applies the elapsed-time half-life decay tick
	// (Priority = Priority*aging + WeightedRecentUsage*(1-aging)).
	UpdatePriorities(now time.Time)
	// GetPriority returns the EFFECTIVE priority (real x factor), initializing
	// a new submitter at MinPriority (write-on-read).
	GetPriority(submitter string) float64
	GetPriorityFactor(submitter string) float64
	// GetWeightedResourcesUsed returns the current weighted usage for a
	// submitter or (bare, no '@') group name.
	GetWeightedResourcesUsed(name string) float64
	// Ceiling/Floor return the configured caps (-1 = none).
	GetCeiling(submitter string) float64
	GetFloor(submitter string) float64
	// AddMatch charges a new match (usage + Resource record + group rollup).
	AddMatch(submitter string, slotAd *classad.ClassAd, now time.Time)
	// RemoveMatch settles and removes a match by resource name.
	RemoveMatch(resourceName string, now time.Time)
	// CheckMatches is the per-cycle reconcile: rebuild weighted usage from the
	// currently-claimed slot ads (zero + re-add), reap stale Resource records.
	CheckMatches(slotAds []*classad.ClassAd, now time.Time)
	// GroupTree returns the group hierarchy with quotas assigned for this
	// cycle (root "<none>" only on flat pools). Callers must treat it as
	// read-only outside the cycle's allocation phase.
	GroupTree() *GroupNode
	// ReportState renders the condor_userprio reply ad (numbered attrs;
	// rollup sums children into parents).
	ReportState(rollup bool) *classad.ClassAd
	// AccountingAds renders the per-submitter/per-group Accounting ads for
	// the collector.
	AccountingAds(negotiatorName string, now time.Time) []*classad.ClassAd
	// Userprio mutation surface (SET_* / RESET_* / DELETE_USER handlers).
	SetPriorityFactor(submitter string, factor float64) error
	SetPriority(submitter string, priority float64) error
	SetAccumUsage(submitter string, accumUsage float64) error
	SetBeginTime(submitter string, t time.Time) error
	SetLastTime(submitter string, t time.Time) error
	ResetUsage(submitter string) error
	ResetAllUsage() error
	DeleteRecord(submitter string) error
}

// GroupNode is one node of the accounting-group tree (design doc 3.3). The
// accountant constructs it from GROUP_NAMES/GROUP_QUOTA_* and assigns
// per-cycle quotas; the cycle orchestrator walks it to drive per-group
// negotiation.
type GroupNode struct {
	Name          string  // dotted path; root is "<none>"
	ConfigQuota   float64 // raw configured quota
	StaticQuota   bool    // static slots vs dynamic fraction
	Quota         float64 // assigned this cycle (normalized)
	SubtreeQuota  float64
	Requested     float64 // demand (weighted idle+running)
	Allocated     float64
	Usage         float64 // weighted resources in use (node-local)
	AcceptSurplus bool
	Autoregroup   bool
	SortKey       float64 // GROUP_SORT_EXPR result for negotiation order
	Parent        *GroupNode
	Children      []*GroupNode
	// Submitters are the submitter ads assigned to this group this cycle.
	Submitters []*classad.ClassAd
}

// SlotView is the cycle's mutable view over a snapshot's slots: consumed slots
// drop out, p-slots persist across matches, and the canonical scan order is
// fixed once so sharded scans stay deterministic.
type SlotView interface {
	// Len returns the number of live candidates.
	Len() int
	// Scan calls fn(scanIndex, slotAd) for every live candidate. Callers may
	// shard by index range; scanIndex is stable for the cycle.
	Scan(fn func(i int, slot *classad.ClassAd) bool)
	// Consume removes a matched slot from the view (p-slots stay; see design
	// doc 4.4).
	Consume(i int)
	// ClaimID looks up the claim secret for a slot (from the snapshot's
	// private ads); the returned id is single-use.
	ClaimID(slot *classad.ClassAd) (string, bool)
}

// Matchmaker finds the best candidate for one request against the cycle's slot
// view, honoring the full ranking order. Deterministic: identical inputs yield
// the identical winner regardless of internal parallelism.
type Matchmaker interface {
	Match(ctx context.Context, req *Request, view SlotView, limits *MatchLimits) (*Candidate, *RejectInfo, error)
}

// MatchLimits carries the pie-spin bookkeeping the matchmaker's submitter-limit
// gate needs (design doc 4.2/4.3).
type MatchLimits struct {
	SubmitterLimit float64
	LimitUsed      float64
	PieLeft        float64
	Ceiling        float64 // remaining ceiling headroom; MaxFloat64 = none
}

// RejectInfo explains a no-match for REJECTED_WITH_REASON.
type RejectInfo struct {
	Reason            string
	ForSubmitterLimit int
	ForConcurrencyLim int
	ForNetworkShare   int
}

// ScheddSession is the negotiator side of one NEGOTIATE conversation with a
// schedd on behalf of one submitter, over a cached warm cedar session
// (design doc section 5).
type ScheddSession interface {
	// Begin opens (or reuses) the session and sends the NEGOTIATE header.
	Begin(ctx context.Context, hdr *NegotiateHeader) error
	// FetchRequests pulls up to n requests (SEND_RESOURCE_REQUEST_LIST, or
	// SEND_JOB_INFO singles for old schedds). Returns an empty slice at
	// NO_MORE_JOBS.
	FetchRequests(ctx context.Context, n int) ([]*Request, error)
	// SendMatch delivers PERMISSION_AND_AD (claim secret + enriched slot ad).
	SendMatch(ctx context.Context, m *MatchResult) error
	// Reject delivers REJECTED_WITH_REASON for a request.
	Reject(ctx context.Context, req *Request, reason string) error
	// End sends END_NEGOTIATE and returns the socket to the session cache.
	End(ctx context.Context) error
	// Close discards the session (protocol error path).
	Close() error
}

// NegotiateHeader is the ClassAd sent after the NEGOTIATE command int.
type NegotiateHeader struct {
	Owner            string // submitter name (OriginalName if present)
	AutoClusterAttrs string // comma-joined significant attributes
	SubmitterTag     string
	NegotiatorName   string
	JobConstraint    string // NEGOTIATOR_JOB_CONSTRAINT, optional
}

// SessionFactory mints ScheddSessions, hiding the socket cache and the
// match-password security session derivation.
type SessionFactory interface {
	Session(submitter, scheddName, scheddAddr string, submitterAd *classad.ClassAd) ScheddSession
}

// Cycle runs one full negotiation cycle (gather -> accounting -> quotas ->
// pie spin -> publish) and reports its stats.
type Cycle interface {
	Run(ctx context.Context) (*CycleStats, error)
}
