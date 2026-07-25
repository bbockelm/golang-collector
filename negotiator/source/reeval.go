package source

import (
	"context"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

const (
	attrWantAdRevaluate = "WantAdRevaluate" // C++ ATTR_WANT_AD_REVAULATE (note the C++ typo)
	// defaultReevalExpr is the C++ default STARTD_AD_REEVAL_EXPR: keep the stashed
	// ad unless the new one's sequence number advanced. Old ad = my, new ad = target.
	defaultReevalExpr = "target.UpdateSequenceNumber > my.UpdateSequenceNumber"
)

// ReevalSource decorates an AdSource with STARTD_AD_REEVAL_EXPR support
// (matchmaker.cpp:3304-3345). A machine ad flagged WantAdRevaluate is replaced by
// a newer snapshot's version only when the configured expression holds -- default
// "target.UpdateSequenceNumber > my.UpdateSequenceNumber" with the stashed ad as
// "my" and the new ad as "target"; otherwise the previously stashed ad is reused
// for this cycle. This lets a startd suppress a stale or lower-sequence update
// (e.g. a slow collector re-send) rather than have the negotiator match against
// it. Ads without WantAdRevaluate -- and a snapshot containing none -- pass
// through untouched, so the decorator is a zero-cost no-op on a pool that does not
// use the feature.
type ReevalSource struct {
	inner negotiator.AdSource
	expr  *classad.Expr // nil (empty/uncompilable) => always replace (C++ "treat as TRUE")
	stash map[string]*classad.ClassAd
}

var _ negotiator.AdSource = (*ReevalSource)(nil)

// NewReevalSource wraps inner. An empty exprStr uses the C++ default; an
// uncompilable expression is treated as always-replace, matching the C++
// "Can't compile STARTD_AD_REEVAL_EXPR ..., treating as TRUE".
func NewReevalSource(inner negotiator.AdSource, exprStr string) *ReevalSource {
	if exprStr == "" {
		exprStr = defaultReevalExpr
	}
	expr, _ := classad.ParseExpr(exprStr)
	return &ReevalSource{inner: inner, expr: expr, stash: map[string]*classad.ClassAd{}}
}

// Snapshot delegates to the inner source, then applies the reeval-replace gate to
// any slot carrying WantAdRevaluate. It copies the slot slice only when it
// actually substitutes a stashed ad (copy-on-write), so the common no-reeval path
// returns the inner snapshot unchanged.
func (r *ReevalSource) Snapshot(ctx context.Context) (*negotiator.PoolSnapshot, error) {
	snap, err := r.inner.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	var out []*classad.ClassAd // nil until a replacement forces the copy
	seen := make(map[string]struct{})
	for i, ad := range snap.Slots {
		if b, ok := ad.EvaluateAttrBool(attrWantAdRevaluate); !ok || !b {
			continue
		}
		id := machineAdID(ad)
		seen[id] = struct{}{}
		if old, have := r.stash[id]; have && !r.shouldReplace(old, ad) {
			if out == nil {
				out = append(out, snap.Slots...) // copy-on-write; keep the inner snapshot immutable
			}
			out[i] = old // reuse the previously stashed ad this cycle
			continue
		}
		r.stash[id] = ad
	}
	// Keep the stash bounded to the current pool: forget reeval machines that are
	// no longer advertising (a machine that reappears is treated as first-seen).
	for id := range r.stash {
		if _, ok := seen[id]; !ok {
			delete(r.stash, id)
		}
	}
	if out == nil {
		return snap, nil // pure passthrough
	}
	cp := *snap
	cp.Slots = out
	return &cp, nil
}

// shouldReplace evaluates the reeval expression with the stashed ad as "my" and
// the new ad as "target". A nil expression, a non-boolean result, or an
// evaluation error all mean replace (the C++ "treating as TRUE" fallbacks).
func (r *ReevalSource) shouldReplace(old, fresh *classad.ClassAd) bool {
	if r.expr == nil {
		return true
	}
	v := old.EvaluateExprWithTarget(r.expr, fresh)
	if !v.IsBool() {
		return true
	}
	b, berr := v.BoolValue()
	if berr != nil {
		return true
	}
	return b
}

func (r *ReevalSource) PublishNegotiatorAd(ctx context.Context, ad *classad.ClassAd) error {
	return r.inner.PublishNegotiatorAd(ctx, ad)
}

func (r *ReevalSource) PublishAccountingAds(ctx context.Context, ads []*classad.ClassAd) error {
	return r.inner.PublishAccountingAds(ctx, ads)
}

// machineAdID keys a machine ad by StartdIpAddr + Name, the C++ MachineAdID
// (matchmaker.cpp:154). It is an internal stash key, never sent on the wire.
func machineAdID(ad *classad.ClassAd) string {
	addr, ok := ad.EvaluateAttrString(attrStartdIPAddr)
	if !ok {
		addr = "<No Address>"
	}
	name, _ := ad.EvaluateAttrString("Name")
	return addr + name
}
