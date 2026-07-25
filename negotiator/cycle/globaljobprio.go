package cycle

import (
	"math"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// USE_GLOBAL_JOB_PRIOS (matchmaker.cpp:817, gt#3218). A schedd running with the
// knob set advertises ATTR_JOB_PRIO_ARRAY -- the distinct job priorities it has
// idle jobs at, high-to-low -- on its submitter ad. The negotiator fans that one
// submitter ad out into one negotiation round per priority (fanOutJobPrios), each
// round carrying a single ATTR_JOB_PRIO; the submitter sort then uses job priority
// as a secondary key (sortSubmitters) so a high-priority round of one submitter
// negotiates before a low-priority round of another submitter at the same user
// priority. After the sort, adjacent rounds for the same (name, schedd) that did
// not interleave with another submitter are consolidated back into a single round
// spanning [JOBPRIO_MIN, JOBPRIO_MAX] (consolidateJobPrios) so the schedd is not
// re-contacted redundantly. The whole path is gated on Config.WantGlobalJobPrio;
// off, the submitter set is untouched.
const (
	attrJobPrio      = "JobPrio"
	attrJobPrioArray = "JobPrioArray"
)

// fanOutJobPrios expands each submitter ad into one ad per distinct job priority
// in its ATTR_JOB_PRIO_ARRAY (matchmaker.cpp:3455-3472). A submitter with no
// array (a schedd that does not, or cannot, play the game) yields a single ad
// stamped with the worst possible priority -- INT_MIN, as in the C++ -- which
// keeps it present but unranked by the secondary key and, lacking a JobPrioArray,
// excluded from consolidation and the JOBPRIO_MIN/MAX header. Input order is
// preserved (StringTokenIterator walks the array left to right); the returned ads
// are shallow copies sharing the original's immutable attribute expressions.
func fanOutJobPrios(ads []*classad.ClassAd) []*classad.ClassAd {
	out := make([]*classad.ClassAd, 0, len(ads))
	for _, ad := range ads {
		arr, ok := ad.EvaluateAttrString(attrJobPrioArray)
		if !ok || strings.TrimSpace(arr) == "" {
			// No array: a single round at the worst priority (INT_MIN).
			cp := copyAd(ad)
			cp.InsertAttr(attrJobPrio, math.MinInt32)
			out = append(out, cp)
			continue
		}
		for _, tok := range strings.Split(arr, ",") {
			tok = strings.TrimSpace(tok)
			if tok == "" {
				continue
			}
			prio, err := strconv.Atoi(tok)
			if err != nil {
				continue
			}
			cp := copyAd(ad)
			cp.InsertAttr(attrJobPrio, int64(prio))
			out = append(out, cp)
		}
	}
	return out
}

// consolidateJobPrios collapses each maximal run of adjacent subStates for the
// same (name, schedd address) in the already-sorted slice into a single round
// whose [jobPrioMin, jobPrioMax] spans the run, dropping the rest
// (consolidate_globaljobprio_submitter_ads, matchmaker.cpp:2353-2432). A run is
// adjacent only when no other submitter sorted between its rounds, so an
// interleaved high/low priority pattern survives as separate rounds while a
// contiguous block is merged. Submitters whose ad carried no JobPrioArray
// (hasJobPrioArray false) are never assigned a range and never consolidated. The
// input order is otherwise preserved; the survivor of each run keeps its sorted
// position.
func consolidateJobPrios(subs []*subState) []*subState {
	out := subs[:0:0]
	var prev *subState
	for _, sub := range subs {
		if !sub.hasJobPrioArray {
			out = append(out, sub)
			prev = nil
			continue
		}
		if prev != nil && prev.name == sub.name && prev.scheddAddr == sub.scheddAddr {
			// Extend the survivor's range and drop this round.
			if sub.jobPrioMin < prev.jobPrioMin {
				prev.jobPrioMin = sub.jobPrioMin
			}
			if sub.jobPrioMax > prev.jobPrioMax {
				prev.jobPrioMax = sub.jobPrioMax
			}
			continue
		}
		out = append(out, sub)
		prev = sub
	}
	return out
}

// copyAd returns a new ClassAd carrying the same attributes as src. It is a
// shallow copy: the attribute expressions (immutable ASTs) are shared, so it is
// cheap, and a later InsertAttr on the copy replaces only the copy's map entry
// and never mutates src.
func copyAd(src *classad.ClassAd) *classad.ClassAd {
	dst := classad.New()
	for _, name := range src.GetAttributes() {
		if e, ok := src.Lookup(name); ok {
			dst.InsertExpr(name, e)
		}
	}
	return dst
}
