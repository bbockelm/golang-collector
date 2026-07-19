package store

import (
	"iter"
	"strings"

	"github.com/PelicanPlatform/classad/collections"
)

// rawExprAttrName extracts the attribute name from a rendered "Name = value"
// expression line (leading whitespace trimmed, name up to the first '=' or
// space). It does not allocate beyond the returned name.
func rawExprAttrName(expr []byte) string {
	i := 0
	for i < len(expr) && (expr[i] == ' ' || expr[i] == '\t') {
		i++
	}
	start := i
	for i < len(expr) && expr[i] != '=' && expr[i] != ' ' && expr[i] != '\t' {
		i++
	}
	return string(expr[start:i])
}

// projectRawSeq wraps a RawAd sequence, restricting each ad's expressions to the
// attributes named in projection (matched case-insensitively). MyType and
// TargetType are RawAd fields, not expressions, so they always pass through --
// the ad stays identifiable. An empty projection is a no-op (all attributes
// pass). This is the local (no-pushdown) projection: a backend that cannot push
// the projection to its storage fetches whole ads and trims here, matching the
// behavior of the remote database's server-side projection.
func projectRawSeq(seq iter.Seq[collections.RawAd], projection []string) iter.Seq[collections.RawAd] {
	if len(projection) == 0 {
		return seq
	}
	keep := make(map[string]struct{}, len(projection))
	for _, a := range projection {
		keep[strings.ToLower(a)] = struct{}{}
	}
	return func(yield func(collections.RawAd) bool) {
		for ra := range seq {
			ra.Exprs = projectRawExprs(ra.Exprs, keep)
			if !yield(ra) {
				return
			}
		}
	}
}

// projectRawExprs returns exprs with only the attributes whose lowercased name is
// in keep. When every expression is kept (the projection covers the whole ad) the
// original slice is returned unchanged, so the common case stays allocation-free.
func projectRawExprs(exprs [][]byte, keep map[string]struct{}) [][]byte {
	drop := -1
	for i, e := range exprs {
		if _, ok := keep[strings.ToLower(rawExprAttrName(e))]; !ok {
			drop = i
			break
		}
	}
	if drop < 0 {
		return exprs // everything kept
	}
	out := make([][]byte, drop, len(exprs)-1)
	copy(out, exprs[:drop])
	for _, e := range exprs[drop+1:] {
		if _, ok := keep[strings.ToLower(rawExprAttrName(e))]; ok {
			out = append(out, e)
		}
	}
	return out
}
