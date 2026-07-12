package cycle

import (
	"sort"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/parser"

	"github.com/bbockelm/golang-collector/negotiator"
)

// newStats initializes a CycleStats for a cycle starting now.
func newStats(now time.Time) *negotiator.CycleStats {
	return &negotiator.CycleStats{Start: now}
}

// alwaysSignificant is the attribute set every AutoClusterAttrs header
// includes, matching the schedd-side minimal set golang-ap folds into the RRL
// projection regardless (golang-ap internal/negotiate/negotiate.go
// alwaysSignificant): the C++ MinimalSigAttrs (Requirements, Rank,
// ConcurrencyLimits) plus the Request* resource asks.
var alwaysSignificant = []string{
	"Requirements", "Rank", "ConcurrencyLimits",
	"RequestCpus", "RequestMemory", "RequestDisk",
}

// bannedSignificant reproduces the C++ compute_significant_attrs ban list
// (matchmaker.cpp:1728-1757): attributes that are always erased from the
// external-reference union (volatile, per-negotiation, or known startd attrs
// that give false positives). Lower-cased for case-insensitive matching.
var bannedSignificant = map[string]bool{}

func init() {
	for _, a := range []string{
		"CurrentTime", "LastHeardFrom",
		"RemoteUser", "RemoteOwner", "RemoteUserFloor", "RemoteUserPrio",
		"RemoteUserResourcesInUse", "RemoteGroupResourcesInUse",
		"SubmittorPrio", "SubmitterUserPrio", "SubmitterUserResourcesInUse",
		"SubmitterGroupResourcesInUse",
		"TotalJobRunTime", "MachineLastMatchTime", "JobCurrentStartDate",
		"JobState",
		"Mips", "Kflops", "SlotID", "DSlotId", "PartitionableSlot",
		"DynamicSlot", "Offline",
	} {
		bannedSignificant[strings.ToLower(a)] = true
	}
}

// computeSignificantAttrs derives the AutoClusterAttrs string sent in every
// NEGOTIATE header: the union of external (job-side) attribute references
// found in the slot ads, plus the negotiator's own rank/constraint
// expressions, plus the always-significant set -- minus the C++ ban list and
// the Slot<N>_ cross-slot attributes.
//
// This is a simplified but faithful port of compute_significant_attrs
// (matchmaker.cpp:1604-1786). The C++ walks EVERY attribute expression of
// every slot ad; we walk the two expressions whose external references are
// what matchmaking actually evaluates against the job -- Requirements and
// Rank -- which is where a correctly-written slot ad keeps its job
// references. The always-significant set guarantees the schedd never
// collapses jobs that differ in resource asks even if a slot ad hides a job
// reference somewhere exotic (the schedd unions this set in regardless; see
// golang-ap negotiate.go buildProjection).
//
// The result is sorted and comma-joined so identical pools always produce an
// identical header (the C++ set iteration is sorted too).
func computeSignificantAttrs(slots []*classad.ClassAd, exprs ...string) string {
	seen := map[string]string{} // lower-cased -> first-seen spelling

	add := func(ref string) {
		// TrimReferenceNames: strip scope prefixes (TARGET./MY.) and keep the
		// final attribute component.
		if i := strings.LastIndexByte(ref, '.'); i >= 0 {
			ref = ref[i+1:]
		}
		if ref == "" {
			return
		}
		lc := strings.ToLower(ref)
		if bannedSignificant[lc] {
			return
		}
		// REMOVE_SIGNIFICANT_ATTRIBUTES_REGEX default "^Slot[0-9]*_": ban the
		// cross-slot Slot<N>_ attribute family.
		if isSlotPrefixed(lc) {
			return
		}
		if _, ok := seen[lc]; !ok {
			seen[lc] = ref
		}
	}

	for _, slot := range slots {
		for _, attr := range []string{"Requirements", "Rank"} {
			expr, ok := slot.Lookup(attr)
			if !ok {
				continue
			}
			collectJobRefs(slot, expr.String(), add)
		}
	}

	// Rank expressions + job constraint: their references are evaluated
	// against MY=slot, TARGET=job, so external refs relative to a sample slot
	// ad are job attributes (matchmaker.cpp:1689-1723).
	if len(slots) > 0 {
		sample := slots[0]
		for _, s := range exprs {
			if strings.TrimSpace(s) == "" {
				continue
			}
			collectJobRefs(sample, s, add)
		}
	}

	for _, a := range alwaysSignificant {
		add(a)
	}

	out := make([]string, 0, len(seen))
	for _, spelling := range seen {
		out = append(out, spelling)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// collectJobRefs parses exprText and reports every job-side (external)
// attribute reference to add: TARGET.-scoped references always, unscoped
// references only when the machine ad does not define them (the C++
// GetExternalReferences resolution rule). MY./PARENT.-scoped references are
// internal by definition. classad.ClassAd.ExternalRefs is NOT usable here: it
// deliberately skips scoped references, and TARGET.attr is exactly what a
// machine ad's Requirements says about the job.
func collectJobRefs(machineAd *classad.ClassAd, exprText string, add func(string)) {
	expr, err := parser.ParseExpr(exprText)
	if err != nil {
		return
	}
	var walk func(e ast.Expr)
	walk = func(e ast.Expr) {
		switch v := e.(type) {
		case *ast.AttributeReference:
			switch v.Scope {
			case ast.TargetScope:
				add(v.Name)
			case ast.NoScope:
				if _, ok := machineAd.Lookup(v.Name); !ok {
					add(v.Name)
				}
			}
		case *ast.ParenExpr:
			walk(v.Inner)
		case *ast.BinaryOp:
			walk(v.Left)
			walk(v.Right)
		case *ast.UnaryOp:
			walk(v.Expr)
		case *ast.ConditionalExpr:
			walk(v.Condition)
			walk(v.TrueExpr)
			walk(v.FalseExpr)
		case *ast.ElvisExpr:
			walk(v.Left)
			walk(v.Right)
		case *ast.FunctionCall:
			for _, a := range v.Args {
				walk(a)
			}
		case *ast.ListLiteral:
			for _, el := range v.Elements {
				walk(el)
			}
		case *ast.SelectExpr:
			walk(v.Record)
		case *ast.SubscriptExpr:
			walk(v.Container)
			walk(v.Index)
		}
	}
	walk(expr)
}

// isSlotPrefixed reports whether a lower-cased attribute matches ^slot[0-9]*_.
func isSlotPrefixed(lc string) bool {
	if !strings.HasPrefix(lc, "slot") {
		return false
	}
	rest := lc[4:]
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	return i < len(rest) && rest[i] == '_'
}
