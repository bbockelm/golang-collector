package server

import (
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
)

// Query-ad attribute names (a subset of condor_attributes.h). The C++ tools use
// "Projection" (whitespace-separated); the golang-htcondor client uses
// "ProjectionAttributes" (comma-separated). We accept either.
const (
	attrRequirements         = "Requirements"
	attrProjection           = "Projection"
	attrProjectionAttributes = "ProjectionAttributes"
	attrLimitResults         = "LimitResults"
)

// parseQuery extracts the constraint, projection and result limit from a query
// ad. A nil *vm.Query means "match everything" (an absent or literally-true
// Requirements), which the store serves with a plain scan.
func parseQuery(queryAd *classad.ClassAd) (q *vm.Query, projection []string, limit int, err error) {
	if expr, ok := queryAd.Lookup(attrRequirements); ok {
		s := strings.TrimSpace(expr.String())
		if s != "" && !strings.EqualFold(s, "true") {
			q, err = vm.Parse(s)
			if err != nil {
				return nil, nil, 0, err
			}
		}
	}
	projection = parseProjection(queryAd)
	if l, ok := queryAd.EvaluateAttrInt(attrLimitResults); ok && l > 0 {
		limit = int(l)
	}
	return q, projection, limit, nil
}

// parseProjection returns the attribute whitelist a query requests, or nil for
// "return the whole ad". It accepts either the whitespace-separated Projection
// attribute or the comma-separated ProjectionAttributes attribute.
func parseProjection(queryAd *classad.ClassAd) []string {
	if s, ok := queryAd.EvaluateAttrString(attrProjection); ok && strings.TrimSpace(s) != "" {
		return splitAttrs(s)
	}
	if s, ok := queryAd.EvaluateAttrString(attrProjectionAttributes); ok && strings.TrimSpace(s) != "" {
		return splitAttrs(s)
	}
	return nil
}

func splitAttrs(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// project returns a copy of ad containing only the whitelisted attributes that
// are present. With an empty whitelist the ad is returned unchanged.
func project(ad *classad.ClassAd, attrs []string) *classad.ClassAd {
	if len(attrs) == 0 {
		return ad
	}
	out := classad.New()
	for _, a := range attrs {
		if e, ok := ad.Lookup(a); ok {
			out.InsertExpr(a, e)
		}
	}
	return out
}
