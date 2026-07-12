package negotiator

import (
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// PoolSnapshot is the negotiator's view of the pool for one cycle: the slot
// (startd public) ads, the submitter ads, and the claim ids extracted from the
// startd private ads. Snapshots are immutable once returned by an AdSource;
// the cycle mutates only its own derived SlotView.
type PoolSnapshot struct {
	// Slots are the machine ads after the standard per-ad fixups (see design
	// doc 4.1: NegotiatorRequirements swap, MachineMatchCount reset, default
	// SlotWeight).
	Slots []*classad.ClassAd
	// Submitters are the submitter ads after filtering (valid name+addr,
	// idle+running > 0, SkipMatchmaking removed).
	Submitters []*classad.ClassAd
	// ClaimIDs maps "slotName + slotAddr" to the claim id from the startd
	// private ad, exactly like the C++ claimIds hash. Claim ids are secrets:
	// they ride only PERMISSION_AND_AD's encrypted string and must never be
	// republished.
	ClaimIDs map[string]string
	// Taken is when the snapshot was gathered.
	Taken time.Time
}

// Request is one resource request from a schedd's RRL: a representative job ad
// plus the group bookkeeping the schedd needs echoed back.
type Request struct {
	// Ad is the flattened representative job ad (includes ClusterId/ProcId,
	// AutoClusterId, and the _condor_RESOURCE_COUNT group size).
	Ad *classad.ClassAd
	// Cluster and Proc identify the representative job (echoed into the match
	// ad as ResourceRequestCluster/Proc).
	Cluster, Proc int
	// AutoClusterID is the schedd-assigned autocluster for reject bookkeeping.
	AutoClusterID int
	// Count is the group size (_condor_RESOURCE_COUNT): up to this many
	// matches may be returned against this one request.
	Count int
}

// Candidate is a matchable slot with its computed rank tuple. Ordering is the
// C++ lexicographic "more is better": PreJobRank, Rank, PostJobRank,
// PreemptTier, PreemptRank -- with the deterministic first-seen tie-break
// (ScanIndex ascending) so sharded scans reduce identically to a serial scan.
type Candidate struct {
	Slot        *classad.ClassAd
	PreJobRank  float64
	Rank        float64
	PostJobRank float64
	// PreemptTier is 2 (NO_PREEMPTION) until preemption support lands
	// (RANK=1, PRIO=0 reserved).
	PreemptTier int
	PreemptRank float64
	// ScanIndex is the candidate's position in the cycle's canonical slot
	// order; the tie-break key.
	ScanIndex int
}

// Better reports whether c strictly beats o under the C++ comparison order.
// Equal tuples are NOT better (first-seen incumbent wins).
func (c *Candidate) Better(o *Candidate) bool {
	if c.PreJobRank != o.PreJobRank {
		return c.PreJobRank > o.PreJobRank
	}
	if c.Rank != o.Rank {
		return c.Rank > o.Rank
	}
	if c.PostJobRank != o.PostJobRank {
		return c.PostJobRank > o.PostJobRank
	}
	if c.PreemptTier != o.PreemptTier {
		return c.PreemptTier > o.PreemptTier
	}
	if c.PreemptRank != o.PreemptRank {
		return c.PreemptRank > o.PreemptRank
	}
	return c.ScanIndex < o.ScanIndex
}

// MatchResult is a completed match ready for PERMISSION_AND_AD delivery.
type MatchResult struct {
	Request *Request
	// SlotAd is the offer ad enriched per design doc section 5 (Requirements
	// restored, ResourceRequestCluster/Proc, Remote* group attrs, ...).
	SlotAd *classad.ClassAd
	// ClaimID is the secret claim string ("null" when claiming is off);
	// ExtraClaims are appended space-separated (pslot preemption, deferred).
	ClaimID     string
	ExtraClaims []string
	// Cost is the accounting weight consumed (SlotWeight of the offer).
	Cost float64
}

// CycleStats summarizes one negotiation cycle (published on the NegotiatorAd).
type CycleStats struct {
	Start, End       time.Time
	TotalSlots       int
	TrimmedSlots     int
	CandidateSlots   int
	Submitters       int
	IdleJobs         int
	JobsConsidered   int
	Matches          int
	Rejections       int
	PieSpins         int
	PrefetchDuration time.Duration
	Phase1Duration   time.Duration // obtain ads
	Phase2Duration   time.Duration // accounting
	Phase3Duration   time.Duration // sort submitters
	Phase4Duration   time.Duration // matchmaking
}
