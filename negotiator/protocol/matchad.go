package protocol

import (
	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// HTCondor negotiate-protocol attribute names. Two spellings of the
// resource-request cluster/proc are stamped on every match ad: C++ negotiators
// write the un-prefixed ResourceRequestCluster/ResourceRequestProc, while the
// golang-ap schedd's readMatch reads the _condor_-prefixed aliases
// (_condor_RESOURCE_CLUSTER/_condor_RESOURCE_PROC, negotiate.go:57-58). We set
// BOTH so the enriched match ad interoperates with either peer.
const (
	attrResourceRequestCluster     = "ResourceRequestCluster"
	attrResourceRequestProc        = "ResourceRequestProc"
	attrCondorResourceReqCluster   = "_condor_RESOURCE_CLUSTER"
	attrCondorResourceReqProc      = "_condor_RESOURCE_PROC"
	attrSavedRequirements          = "SavedRequirements"
	attrRequirements               = "Requirements"
	attrMatchedConcurrencyLimits   = "MatchedConcurrencyLimits"
	attrRemoteGroup                = "RemoteGroup"
	attrRemoteNegotiatingGroup     = "RemoteNegotiatingGroup"
	attrRemoteAutoregroup          = "RemoteAutoregroup"
	attrName                       = "Name"
	attrSubmitterUserPrio          = "SubmitterUserPrio"
	attrSubmitterUserResourcesUsed = "SubmitterUserResourcesInUse"
	attrSubmitterGroup             = "SubmitterGroup"
	attrSubmitterGroupResourcesUse = "SubmitterGroupResourcesInUse"
	attrSubmitterGroupQuota        = "SubmitterGroupQuota"
	attrSubmitterNegotiatingGroup  = "SubmitterNegotiatingGroup"
	attrSubmitterAutoregroup       = "SubmitterAutoregroup"
)

// rootGroupName is the fixed name of the root accounting group (the same value
// as accountant.RootGroupName, duplicated here so protocol does not depend on
// the accountant package). It is the C++ hgq_root_group->name.
const rootGroupName = "<none>"

// MatchExpr is one NEGOTIATOR_MATCH_EXPRS entry: an attribute Name (already
// carrying the NegotiatorMatchExpr prefix) and its unevaluated expression Expr.
// The negotiator stamps it onto every match ad; the schedd propagates it into
// the job ad it hands the startd (matchmaker.cpp:5268-5274).
type MatchExpr struct {
	Name string
	Expr string
}

// MatchContext carries the per-request group context the negotiator folds into
// the offer ad before delivering PERMISSION_AND_AD (design doc section 5).
type MatchContext struct {
	// ConcurrencyLimits is copied to MatchedConcurrencyLimits (passthrough).
	ConcurrencyLimits string
	// Remote* describe the accounting group the match is charged against; empty
	// values are omitted so a flat (single-root) pool produces no group attrs.
	RemoteGroup            string
	RemoteNegotiatingGroup string
	RemoteAutoregroup      bool
	// HasAutoregroup gates emission of RemoteAutoregroup (so a flat pool need
	// not stamp a spurious false).
	HasAutoregroup bool
	// MatchExprs are the NEGOTIATOR_MATCH_EXPRS to inject as NegotiatorMatchExpr<name>
	// attributes on the outgoing match ad (matchmaker.cpp:2044 insertNegotiatorMatchExprs).
	MatchExprs []MatchExpr
}

// EnrichMatchAd produces the PERMISSION_AND_AD payload ad from an offer/slot ad
// and the matched request. It copies the slot ad (carrying any
// NegotiatorMatchExprXXX attributes already stamped on it during the cycle's
// trim phase), restores Requirements from SavedRequirements when present, stamps
// the representative job id in both attribute spellings, and folds in the
// concurrency-limit and accounting-group context. The input ad is not mutated.
func EnrichMatchAd(slot *classad.ClassAd, req *negotiator.Request, mc MatchContext) *classad.ClassAd {
	out := copyAd(slot)

	// Restore the negotiator's matching Requirements from the SavedRequirements
	// the cycle stashed during the per-slot fixups (design doc 4.1). The schedd
	// and startd want the original Requirements back, not the negotiator's
	// swapped-in NegotiatorRequirements.
	if expr, ok := out.Lookup(attrSavedRequirements); ok {
		out.InsertExpr(attrRequirements, expr)
	}

	stampResourceRequest(out, req)

	if mc.ConcurrencyLimits != "" {
		_ = out.Set(attrMatchedConcurrencyLimits, mc.ConcurrencyLimits)
	}
	if mc.RemoteGroup != "" {
		_ = out.Set(attrRemoteGroup, mc.RemoteGroup)
	}
	if mc.RemoteNegotiatingGroup != "" {
		_ = out.Set(attrRemoteNegotiatingGroup, mc.RemoteNegotiatingGroup)
	}
	if mc.HasAutoregroup {
		_ = out.Set(attrRemoteAutoregroup, mc.RemoteAutoregroup)
	}
	insertMatchExprs(out, mc.MatchExprs)
	return out
}

// insertMatchExprs stamps each NEGOTIATOR_MATCH_EXPRS entry onto ad as an
// unevaluated expression (C++ AssignExpr, matchmaker.cpp:5272). An entry whose
// expression fails to parse falls back to a string literal so a misconfigured
// expr never drops the whole match ad.
func insertMatchExprs(ad *classad.ClassAd, exprs []MatchExpr) {
	for _, e := range exprs {
		if parsed, err := classad.ParseExpr(e.Expr); err == nil {
			ad.InsertExpr(e.Name, parsed)
		} else {
			ad.InsertAttrString(e.Name, e.Expr)
		}
	}
}

// stampResourceRequest writes the representative job id (and the slot Name, if
// the offer carries one) that the schedd echoes back to locate the request
// group. Both the C++ and _condor_-prefixed spellings are set (see the const
// block). Used both by EnrichMatchAd and defensively by session.SendMatch so the
// wire contract holds even if the cycle hands over an un-enriched ad.
func stampResourceRequest(ad *classad.ClassAd, req *negotiator.Request) {
	if req == nil {
		return
	}
	_ = ad.Set(attrResourceRequestCluster, int64(req.Cluster))
	_ = ad.Set(attrResourceRequestProc, int64(req.Proc))
	_ = ad.Set(attrCondorResourceReqCluster, int64(req.Cluster))
	_ = ad.Set(attrCondorResourceReqProc, int64(req.Proc))
}

// SubmitterContext carries the per-submitter priority and (optional) group
// figures the negotiator inserts into a request ad before matching, so job and
// machine expressions may reference them (design doc section 5).
type SubmitterContext struct {
	UserPrio           float64
	UserResourcesInUse float64
	// Group is empty on a flat pool; when set the group figures are stamped too.
	Group               string
	GroupResourcesInUse float64
	GroupQuota          float64
	NegotiatingGroup    string
	Autoregroup         bool
}

// EnrichRequestAd folds the submitter/group context into a request ad in place,
// before the matchmaker evaluates it. Mirrors the C++ negotiator's insertion
// of SubmitterUserPrio / SubmitterUserResourcesInUse and — UNCONDITIONALLY,
// even on a flat pool (matchmaker.cpp:4257-4258) — SubmitterNegotiatingGroup
// and SubmitterAutoregroup; the SubmitterGroup* family is added only when the
// submitter belongs to an accounting group.
func EnrichRequestAd(reqAd *classad.ClassAd, sc SubmitterContext) {
	if reqAd == nil {
		return
	}
	// The C++ negGroupName is never empty: it falls back to the root group's
	// name when negotiating without group accounting.
	negGroup := sc.NegotiatingGroup
	if negGroup == "" {
		negGroup = rootGroupName
	}
	_ = reqAd.Set(attrSubmitterNegotiatingGroup, negGroup)
	_ = reqAd.Set(attrSubmitterAutoregroup, sc.Autoregroup)
	_ = reqAd.Set(attrSubmitterUserPrio, sc.UserPrio)
	_ = reqAd.Set(attrSubmitterUserResourcesUsed, sc.UserResourcesInUse)
	if sc.Group != "" {
		_ = reqAd.Set(attrSubmitterGroup, sc.Group)
		_ = reqAd.Set(attrSubmitterGroupResourcesUse, sc.GroupResourcesInUse)
		_ = reqAd.Set(attrSubmitterGroupQuota, sc.GroupQuota)
	}
}

// copyAd returns a shallow attribute-by-attribute copy of ad (expressions are
// shared, but the attribute map is independent so mutation of the copy does not
// disturb the snapshot's slot ad). Mirrors golang-ap's buildRequestAd copy.
func copyAd(ad *classad.ClassAd) *classad.ClassAd {
	out := classad.New()
	if ad == nil {
		return out
	}
	for _, name := range ad.GetAttributes() {
		if expr, ok := ad.Lookup(name); ok {
			out.InsertExpr(name, expr)
		}
	}
	return out
}
