// Package source implements the negotiator's AdSource: gathering the pool
// snapshot (slot, submitter, and startd-private ads) for a cycle and publishing
// the negotiator's own ads back. Two modes share the per-ad processing in this
// file: EmbeddedSource reads a collector store.Store directly, RemoteSource
// queries a collector over CEDAR. See docs/NEGOTIATOR_DESIGN.md sections 4.1
// (step 1) and 6.
package source

import (
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
)

// Attribute names used by the fixups, matching condor_attributes.h. Kept as
// literals (this package must not depend on the C++ headers) with the ATTR_
// symbol noted alongside.
const (
	attrName         = "Name"                              // ATTR_NAME
	attrMyAddress    = "MyAddress"                         // ATTR_MY_ADDRESS
	attrStartdIPAddr = "StartdIpAddr"                      // ATTR_STARTD_IP_ADDR
	attrScheddIPAddr = "ScheddIpAddr"                      // ATTR_SCHEDD_IP_ADDR
	attrRequirements = "Requirements"                      // ATTR_REQUIREMENTS
	attrNegReqs      = "NegotiatorRequirements"            // ATTR_NEGOTIATOR_REQUIREMENTS
	attrSavedReqs    = "SavedRequirements"                 // "Saved" + ATTR_REQUIREMENTS
	attrSlotWeight   = "SlotWeight"                        // ATTR_SLOT_WEIGHT
	attrRunningJobs  = "RunningJobs"                       // ATTR_RUNNING_JOBS
	attrIdleJobs     = "IdleJobs"                          // ATTR_IDLE_JOBS
	attrSkipMatch    = "SkipMatchmaking"                   // ATTR_SKIP_MATCHMAKING
	attrClaimID      = "ClaimId"                           // ATTR_CLAIM_ID
	attrCapability   = "Capability"                        // ATTR_CAPABILITY
	attrClaimIDList  = "ClaimIdList"                       // ATTR_CLAIM_ID_LIST
	attrRemoteAdmin  = "_condor_PrivRemoteAdminCapability" // ATTR_REMOTE_ADMIN_CAPABILITY
	attrMachMatchCnt = "MachineMatchCount"                 // literal (not ATTR_-defined)
	attrOfflineMatch = "OfflineMatches"                    // literal
)

// defaultSlotWeightExpr is the SlotWeight expression the C++ negotiator inserts
// when a slot ad has none (SLOT_WEIGHT config, default "Cpus"); matchmaker.cpp
// slotWeightStr (line ~904). MVP uses the "Cpus" default unconditionally.
const defaultSlotWeightExpr = "Cpus"

// FixupSlot applies the per-slot transforms from design doc 4.1 step 1 to a
// public startd (machine) ad, in place, mirroring the C++ negotiator's
// obtainAdsFromCollector loop (matchmaker.cpp:3252-3406):
//
//   - delete the admin capability the collector may have included;
//   - if NegotiatorRequirements is present, save the current Requirements as
//     SavedRequirements and replace Requirements with NegotiatorRequirements;
//   - reset MachineMatchCount to 0 (and drop OfflineMatches);
//   - default SlotWeight to the "Cpus" expression when the ad has no usable
//     SlotWeight.
//
// The ad must be a freshly-owned copy (embedded mode gets fresh decodes from
// the store; remote mode gets fresh ads off the wire) -- these mutations must
// never touch a live store entry.
//
// defaultWeight is the pre-parsed SLOT_WEIGHT cost expression (from
// ParseSlotWeight) inserted as the slot's SlotWeight when it has no usable one.
func FixupSlot(ad *classad.ClassAd, defaultWeight *classad.Expr) {
	// Drop the admin capability (matchmaker.cpp:3255). Delete the exact ATTR_
	// name and the short spelling defensively.
	ad.Delete(attrRemoteAdmin)
	ad.Delete("RemoteAdminCapability")

	// NegotiatorRequirements -> Requirements, saving the old Requirements.
	if neg, ok := ad.Lookup(attrNegReqs); ok {
		if req, ok := ad.Lookup(attrRequirements); ok {
			// Stringify + re-parse so SavedRequirements is an independent node
			// (mirrors the C++ ExprTreeToString/AssignExpr round-trip).
			if e, err := classad.ParseExpr(req.String()); err == nil {
				ad.InsertExpr(attrSavedReqs, e)
			}
		}
		if e, err := classad.ParseExpr(neg.String()); err == nil {
			ad.InsertExpr(attrRequirements, e)
		}
	}

	// Reset the per-cycle match bookkeeping (matchmaker.cpp:3399-3400).
	ad.InsertAttr(attrMachMatchCnt, 0)
	ad.Delete(attrOfflineMatch)

	// Default SlotWeight when the ad has no usable weight. The C++ code uses
	// LookupFloat (literal-only); we treat a SlotWeight that fails to evaluate
	// to a number as "missing", which additionally keeps a valid expression
	// weight (e.g. SlotWeight = Cpus) intact rather than overwriting it. When
	// truly absent, insert the configured SLOT_WEIGHT default expression.
	if _, ok := ad.EvaluateAttrNumber(attrSlotWeight); !ok && defaultWeight != nil {
		ad.InsertExpr(attrSlotWeight, defaultWeight)
	}
}

// ParseSlotWeight compiles the negotiator's default cost expression (the
// SLOT_WEIGHT knob): the expression a slot's weight defaults to when the ad has
// none. An empty or unparseable value falls back to the C++ default, "Cpus".
// The returned *Expr is shared read-only across every slot the source fixes up
// (ClassAd expressions are immutable), like the C++ negotiator's single parsed
// slotWeightStr.
func ParseSlotWeight(expr string) *classad.Expr {
	if s := strings.TrimSpace(expr); s != "" {
		if e, err := classad.ParseExpr(s); err == nil {
			return e
		}
	}
	e, _ := classad.ParseExpr(defaultSlotWeightExpr)
	return e
}

// KeepSubmitter reports whether a submitter ad survives the design-doc-4.1
// step-1 filters (matchmaker.cpp:3411-3443 + the SkipMatchmaking erase at
// :1948-1961): it must carry a non-empty Name and a ScheddIpAddr, must have
// RunningJobs+IdleJobs > 0, and must not set SkipMatchmaking = true.
func KeepSubmitter(ad *classad.ClassAd) bool {
	name, ok := ad.EvaluateAttrString(attrName)
	if !ok || name == "" {
		return false
	}
	if addr, ok := ad.EvaluateAttrString(attrScheddIPAddr); !ok || addr == "" {
		return false
	}
	if skip, ok := ad.EvaluateAttrBool(attrSkipMatch); ok && skip {
		return false
	}
	running, _ := ad.EvaluateAttrInt(attrRunningJobs)
	idle, _ := ad.EvaluateAttrInt(attrIdleJobs)
	return running+idle > 0
}

// ClaimKey is the key under which a slot's claim id is stored in a
// PoolSnapshot's ClaimIDs map, and the key SlotView.ClaimID uses to look it up.
// It is the offer-side key the C++ matchmaker builds in matchmakingProtocol
// (matchmaker.cpp:5335-5337): ATTR_NAME concatenated with ATTR_STARTD_IP_ADDR,
// no separator.
//
// NOTE: the producer side, MakeClaimIdHash (matchmaker.cpp:3572-3591), builds
// the SAME key from the PRIVATE ad using ATTR_NAME + ATTR_MY_ADDRESS (see
// claimKeyPvt). The two agree because the startd advertises StartdIpAddr ==
// MyAddress and the collector copies MyAddress (not StartdIpAddr) into the
// private ad (store.UpdatePvt). ClaimKey is therefore the public/offer spelling
// and claimKeyPvt the private spelling of one identity.
func ClaimKey(slot *classad.ClassAd) string {
	name, _ := slot.EvaluateAttrString(attrName)
	addr, _ := slot.EvaluateAttrString(attrStartdIPAddr)
	return name + addr
}

// claimKeyPvt builds the ClaimIDs key from a startd PRIVATE ad, using
// ATTR_NAME + ATTR_MY_ADDRESS exactly as MakeClaimIdHash does -- the private ad
// carries MyAddress (copied from its public ad by the collector), not
// StartdIpAddr.
func claimKeyPvt(pvt *classad.ClassAd) (string, bool) {
	name, ok := pvt.EvaluateAttrString(attrName)
	if !ok || name == "" {
		return "", false
	}
	addr, ok := pvt.EvaluateAttrString(attrMyAddress)
	if !ok || addr == "" {
		return "", false
	}
	return name + addr, true
}

// claimIDFromPvt extracts a slot's claim id from its private ad, following the
// C++ precedence in MakeClaimIdHash (matchmaker.cpp:3581-3607): ClaimId, then
// Capability, then the first entry of ClaimIdList. It returns false when the ad
// carries no claim secret.
func claimIDFromPvt(pvt *classad.ClassAd) (string, bool) {
	if id, ok := pvt.EvaluateAttrString(attrClaimID); ok && id != "" {
		return id, true
	}
	if id, ok := pvt.EvaluateAttrString(attrCapability); ok && id != "" {
		return id, true
	}
	if list, ok := pvt.EvaluateAttrString(attrClaimIDList); ok && list != "" {
		// ClaimIdList is a comma/space token list; the primary claim is first.
		for _, tok := range splitTokens(list) {
			if tok != "" {
				return tok, true
			}
		}
	}
	return "", false
}

// splitTokens splits a StringTokenIterator-style list (comma or whitespace
// separated) into its non-empty tokens.
func splitTokens(s string) []string {
	var out []string
	start := -1
	for i, r := range s {
		if r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if start >= 0 {
				out = append(out, s[start:i])
				start = -1
			}
			continue
		}
		if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		out = append(out, s[start:])
	}
	return out
}

// BuildClaimIDs constructs the ClaimIDs map from a slice of startd private ads,
// keyed by claimKeyPvt (Name + MyAddress). Ads missing a name/address or a
// claim secret are skipped (as the C++ MakeClaimIdHash does with `continue`).
// A duplicate key keeps the last ad's claim id, matching the C++ overwrite.
func BuildClaimIDs(pvtAds []*classad.ClassAd) map[string]string {
	out := make(map[string]string, len(pvtAds))
	for _, pvt := range pvtAds {
		key, ok := claimKeyPvt(pvt)
		if !ok {
			continue
		}
		id, ok := claimIDFromPvt(pvt)
		if !ok {
			continue
		}
		out[key] = id
	}
	return out
}

// compileConstraint compiles a NEGOTIATOR_*_CONSTRAINT expression into a query
// matcher for query-time filtering. An empty string yields (nil, nil) meaning
// "match everything".
func compileConstraint(expr string) (*vm.Query, error) {
	if expr == "" {
		return nil, nil
	}
	return vm.Parse(expr)
}
