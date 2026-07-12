package matchmaker

import (
	"sync"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// SlotView is the cycle's mutable view over a PoolSnapshot's slots, implementing
// negotiator.SlotView (design doc 4.4):
//
//   - The canonical scan order is fixed once (snapshot order); each slot's
//     ScanIndex is its position in that order and never changes for the cycle.
//   - Consume removes a matched static slot from the view. A partitionable slot
//     (PartitionableSlot == true) is NOT removed: it stays a candidate for
//     further requests this cycle (the negotiator never splits a p-slot; the EP
//     carves dslots via claim-leftovers).
//   - ClaimID resolves the single-use claim secret for a slot from the
//     snapshot's private-ad map.
//
// Concurrency contract: Scan is safe to call concurrently with other Scan calls
// during a single Match (it takes a read lock). Consume takes the write lock and
// MUST only be called between Match calls, never concurrently with a Scan --
// which is exactly how the cycle drives it (scan the whole request, then consume
// the winner serially before the next request).
type SlotView struct {
	snap  *negotiator.PoolSnapshot
	slots []*classad.ClassAd

	mu    sync.RWMutex
	live  []bool
	pslot []bool
	nlive int
}

var _ negotiator.SlotView = (*SlotView)(nil)

// NewSlotView builds a view over snap. All slots start live; each slot is
// classified once as partitionable or not (so Consume can honor p-slot
// persistence without re-evaluating the ad).
func NewSlotView(snap *negotiator.PoolSnapshot) *SlotView {
	n := len(snap.Slots)
	sv := &SlotView{
		snap:  snap,
		slots: snap.Slots,
		live:  make([]bool, n),
		pslot: make([]bool, n),
		nlive: n,
	}
	for i, s := range snap.Slots {
		sv.live[i] = true
		if b, ok := s.EvaluateAttrBool("PartitionableSlot"); ok && b {
			sv.pslot[i] = true
		}
	}
	return sv
}

// Len returns the number of live candidates.
func (sv *SlotView) Len() int {
	sv.mu.RLock()
	defer sv.mu.RUnlock()
	return sv.nlive
}

// Scan calls fn(scanIndex, slot) for every live candidate in canonical order,
// stopping early if fn returns false.
func (sv *SlotView) Scan(fn func(i int, slot *classad.ClassAd) bool) {
	sv.mu.RLock()
	defer sv.mu.RUnlock()
	for i := range sv.slots {
		if !sv.live[i] {
			continue
		}
		if !fn(i, sv.slots[i]) {
			return
		}
	}
}

// Consume removes candidate i from the view. Partitionable slots persist
// (design doc 4.4); out-of-range or already-consumed indices are no-ops.
func (sv *SlotView) Consume(i int) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if i < 0 || i >= len(sv.live) || !sv.live[i] {
		return
	}
	if sv.pslot[i] {
		// p-slot: stays in the candidate set for further matches this cycle.
		return
	}
	sv.live[i] = false
	sv.nlive--
}

// ClaimID looks up the claim secret for slot. The key is the slot's Name
// concatenated with its StartdIpAddr, with no separator -- byte-identical to the
// C++ claimIds hash key (matchmaker.cpp:3589-3591 builds it as name+MyAddress
// from the private ad; matchmakingProtocol:5316-5337 looks it up as
// Name+StartdIpAddr from the public offer, which is the same startd address).
// The returned id is a single-use secret and must never be republished.
func (sv *SlotView) ClaimID(slot *classad.ClassAd) (string, bool) {
	if sv.snap == nil || sv.snap.ClaimIDs == nil || slot == nil {
		return "", false
	}
	name, ok := slot.EvaluateAttrString("Name")
	if !ok {
		return "", false
	}
	addr, ok := slot.EvaluateAttrString("StartdIpAddr")
	if !ok {
		return "", false
	}
	id, ok := sv.snap.ClaimIDs[name+addr]
	return id, ok
}
