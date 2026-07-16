package negtest

import (
	"fmt"
	"math/rand"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// This file holds a deterministic synthetic-pool generator for the negotiator
// performance benchmarks (roadmap item #5). It builds a negotiator.PoolSnapshot
// (a mix of static and partitionable Machine ads) plus per-submitter resource-
// request lists drawn from a small set of autoclusters, at a configurable scale.
//
// It is deliberately deterministic: everything is driven off a single seed via a
// local *rand.Rand (never the math/rand global, never time), so a given
// GenParams yields byte-identical ads on every run. That is what lets a
// benchmark compare allocations before/after a change without the pool itself
// shifting underneath it, and lets the sharded-vs-serial determinism tests reuse
// the same fixtures.
//
// The ads are realistic enough that the matchmaker's real work runs: Machine
// ads carry Requirements/Rank/Cpus/Memory/OpSys/Arch and job ads carry
// Requirements referencing the slot attributes plus RequestCpus/RequestMemory
// and a job Rank -- so MatchClassAd bilateral Symmetry and the three rank evals
// (PreJobRank/Rank/PostJobRank) all execute per candidate. Requirements are
// tuned so a realistic fraction of (job, slot) pairs fail to match (wrong
// OpSys/Arch or too small), rather than every pair matching.

// GenParams configures the synthetic pool's scale. The zero value is not useful;
// start from DefaultGenParams and override.
type GenParams struct {
	// Slots is the total number of Machine ads. PartitionableFrac of them are
	// partitionable slots (PartitionableSlot = true); the rest are static.
	Slots             int
	PartitionableFrac float64

	// Submitters is the number of Submitter ads (and per-submitter RRLs).
	// Schedds is how many distinct schedds those submitters are spread across
	// (each submitter is homed on one schedd, round-robin). Schedds is clamped
	// to [1, Submitters].
	Submitters int
	Schedds    int

	// RRLDepth is the number of resource requests in each submitter's request
	// list. AutoClusters is the number of distinct job templates those requests
	// are drawn from -- a request's Ad is one of the AutoClusters templates with
	// its own AutoClusterID, so a deep RRL still collapses onto a few
	// autoclusters (as a real schedd's RRL does). AutoClusters is clamped to
	// [1, RRLDepth].
	RRLDepth     int
	AutoClusters int

	// Seed drives the single local RNG. Same Seed + same fields => same ads.
	Seed int64
}

// DefaultGenParams returns a small, balanced pool suitable as a starting point
// for benchmarks and tests (1k slots, 20 submitters over 5 schedds, RRLs of
// depth 50 across 8 autoclusters).
func DefaultGenParams() GenParams {
	return GenParams{
		Slots:             1000,
		PartitionableFrac: 0.25,
		Submitters:        20,
		Schedds:           5,
		RRLDepth:          50,
		AutoClusters:      8,
		Seed:              1,
	}
}

// Pool is a generated synthetic pool: the immutable snapshot the matchmaker /
// cycle scan, plus the per-submitter request lists to feed them.
type Pool struct {
	// Snapshot is the pool view (Slots + Submitters + ClaimIDs) for one cycle.
	Snapshot *negotiator.PoolSnapshot
	// SubmitterNames lists the submitter accounting names in a stable order.
	SubmitterNames []string
	// Requests maps a submitter name to its ordered resource-request list.
	Requests map[string][]*negotiator.Request
}

// AllRequests flattens the per-submitter request lists into one slice, in
// submitter order then RRL order -- convenient for a benchmark that just needs a
// stream of requests to match.
func (p *Pool) AllRequests() []*negotiator.Request {
	var out []*negotiator.Request
	for _, name := range p.SubmitterNames {
		out = append(out, p.Requests[name]...)
	}
	return out
}

// opSysDist / archDist are the realistic OS/arch mixes drawn per slot and per
// autocluster template (LINUX/x86_64 dominant, with a Windows and an ARM tail).
// Each entry's weight is its cumulative share out of the final total.
var (
	opSysDist = []weighted{{"LINUX", 80}, {"WINDOWS", 20}}
	archDist  = []weighted{{"X86_64", 85}, {"AARCH64", 15}}
)

type weighted struct {
	value  string
	weight int
}

// pick draws a value from a weighted distribution using rng.
func pick(rng *rand.Rand, dist []weighted) string {
	total := 0
	for _, w := range dist {
		total += w.weight
	}
	r := rng.Intn(total)
	for _, w := range dist {
		if r < w.weight {
			return w.value
		}
		r -= w.weight
	}
	return dist[len(dist)-1].value
}

// acTemplate is one autocluster: a job shape shared by every request assigned to
// it. The parsed Requirements/Rank expression trees are read-only during
// evaluation, so all requests in the autocluster share the same *classad.Expr
// pointers (exactly as the matchmaker's cloneAd shares attribute trees).
type acTemplate struct {
	id          int
	requestCpus int64
	requestMem  int64
	opSys       string
	arch        string
	req         *classad.Expr
	rank        *classad.Expr
}

// Generate builds a synthetic Pool from params. It panics only on an internal
// expression-parse bug (the expressions are generator-controlled constants), so
// callers need no error handling; this keeps benchmark setup terse.
func Generate(params GenParams) *Pool {
	params = normalizeGenParams(params)
	rng := rand.New(rand.NewSource(params.Seed))

	// Slot-side Requirements/Rank are shared across every Machine ad (the
	// per-slot variation lives in the Cpus/Memory/OpSys/Arch literals). The slot
	// requires the job to fit; the slot Rank prefers memory-heavier jobs.
	slotReq := mustExpr("TARGET.RequestCpus <= MY.Cpus && TARGET.RequestMemory <= MY.Memory")
	slotRank := mustExpr("TARGET.RequestMemory")

	slots, claimIDs := genSlots(rng, params, slotReq, slotRank)
	templates := genTemplates(rng, params)
	submitters, names, requests := genSubmitters(rng, params, templates)

	return &Pool{
		Snapshot: &negotiator.PoolSnapshot{
			Slots:      slots,
			Submitters: submitters,
			ClaimIDs:   claimIDs,
		},
		SubmitterNames: names,
		Requests:       requests,
	}
}

// SlotView-friendly helpers: Slots exposes the generated Machine ads, so a
// benchmark can build a matchmaker.SlotView without reaching into Snapshot.
func (p *Pool) Slots() []*classad.ClassAd { return p.Snapshot.Slots }

func normalizeGenParams(p GenParams) GenParams {
	if p.Slots < 1 {
		p.Slots = 1
	}
	if p.PartitionableFrac < 0 {
		p.PartitionableFrac = 0
	}
	if p.PartitionableFrac > 1 {
		p.PartitionableFrac = 1
	}
	if p.Submitters < 1 {
		p.Submitters = 1
	}
	if p.Schedds < 1 {
		p.Schedds = 1
	}
	if p.Schedds > p.Submitters {
		p.Schedds = p.Submitters
	}
	if p.RRLDepth < 1 {
		p.RRLDepth = 1
	}
	if p.AutoClusters < 1 {
		p.AutoClusters = 1
	}
	if p.AutoClusters > p.RRLDepth {
		p.AutoClusters = p.RRLDepth
	}
	return p
}

// genSlots builds the Machine ads and the parallel claim-id map. cpuChoices are
// realistic core counts; memory scales with cores at 2 or 4 GiB/core.
func genSlots(rng *rand.Rand, params GenParams, slotReq, slotRank *classad.Expr) ([]*classad.ClassAd, map[string]string) {
	cpuChoices := []int64{1, 2, 4, 8, 16, 32, 64}
	memPerCore := []int64{2048, 4096}

	slots := make([]*classad.ClassAd, params.Slots)
	claimIDs := make(map[string]string, params.Slots)
	npslot := int(float64(params.Slots) * params.PartitionableFrac)

	for i := 0; i < params.Slots; i++ {
		cpus := cpuChoices[rng.Intn(len(cpuChoices))]
		mem := cpus * memPerCore[rng.Intn(len(memPerCore))]
		opSys := pick(rng, opSysDist)
		arch := pick(rng, archDist)
		partitionable := i < npslot

		name := fmt.Sprintf("slot%d@host%d.pool", i, i)
		addr := fmt.Sprintf("<10.%d.%d.%d:9618>", (i>>16)&0xff, (i>>8)&0xff, i&0xff)

		ad := classad.New()
		ad.InsertAttrString("MyType", "Machine")
		ad.InsertAttrString("Name", name)
		ad.InsertAttrString("Machine", fmt.Sprintf("host%d.pool", i))
		ad.InsertAttrString("MyAddress", addr)
		ad.InsertAttrString("StartdIpAddr", addr)
		ad.InsertAttrString("OpSys", opSys)
		ad.InsertAttrString("Arch", arch)
		ad.InsertAttr("Cpus", cpus)
		ad.InsertAttr("Memory", mem)
		ad.InsertAttr("Disk", cpus*(1<<20))
		ad.InsertAttrFloat("SlotWeight", float64(cpus))
		ad.InsertAttrString("State", "Unclaimed")
		ad.InsertAttrString("Activity", "Idle")
		ad.InsertAttrBool("PartitionableSlot", partitionable)
		ad.InsertExpr("Requirements", slotReq)
		ad.InsertExpr("Rank", slotRank)

		slots[i] = ad
		claimIDs[name+addr] = fmt.Sprintf("claim-%d-secret", i)
	}
	return slots, claimIDs
}

// genTemplates builds the autocluster job shapes. Each requires a specific
// OpSys/Arch and a Cpus/Memory floor, so a request only matches the subset of
// slots that satisfy it -- giving the scan a realistic mix of matches and
// requirements-failures rather than a match on every candidate.
func genTemplates(rng *rand.Rand, params GenParams) []acTemplate {
	rcChoices := []int64{1, 2, 4, 8, 16}
	templates := make([]acTemplate, params.AutoClusters)
	for i := range templates {
		rc := rcChoices[rng.Intn(len(rcChoices))]
		rm := rc * 2048
		opSys := pick(rng, opSysDist)
		arch := pick(rng, archDist)
		// Job Requirements reference the slot (TARGET) attributes; the OpSys/Arch
		// literals are baked in per template (as condor_submit expands them). Job
		// Rank prefers roomier machines.
		reqStr := fmt.Sprintf(
			"TARGET.OpSys == %q && TARGET.Arch == %q && TARGET.Cpus >= MY.RequestCpus && TARGET.Memory >= MY.RequestMemory",
			opSys, arch)
		templates[i] = acTemplate{
			id:          1000 + i,
			requestCpus: rc,
			requestMem:  rm,
			opSys:       opSys,
			arch:        arch,
			req:         mustExpr(reqStr),
			rank:        mustExpr("TARGET.Cpus"),
		}
	}
	return templates
}

// genSubmitters builds the Submitter ads and each submitter's RRL. Requests are
// drawn from the autocluster templates; a few carry a Count > 1 (a request group
// the schedd would fill with several identical jobs).
func genSubmitters(rng *rand.Rand, params GenParams, templates []acTemplate) ([]*classad.ClassAd, []string, map[string][]*negotiator.Request) {
	submitters := make([]*classad.ClassAd, params.Submitters)
	names := make([]string, params.Submitters)
	requests := make(map[string][]*negotiator.Request, params.Submitters)

	for s := 0; s < params.Submitters; s++ {
		schedd := s % params.Schedds
		name := fmt.Sprintf("user%d@pool", s)
		scheddName := fmt.Sprintf("schedd%d@pool", schedd)
		scheddAddr := fmt.Sprintf("<10.128.%d.%d:9618>", (schedd>>8)&0xff, schedd&0xff)
		names[s] = name

		rrl := genRRL(rng, params, templates, name)
		requests[name] = rrl

		sub := classad.New()
		sub.InsertAttrString("MyType", "Submitter")
		sub.InsertAttrString("Name", name)
		sub.InsertAttrString("ScheddName", scheddName)
		sub.InsertAttrString("ScheddIpAddr", scheddAddr)
		sub.InsertAttrString("SubmitterTag", "")
		sub.InsertAttr("IdleJobs", int64(len(rrl)))
		sub.InsertAttr("RunningJobs", 0)
		sub.InsertAttr("LastHeardFrom", 1000)
		submitters[s] = sub
	}
	return submitters, names, requests
}

// genRRL builds one submitter's resource-request list of params.RRLDepth
// requests, each cloned from an autocluster template and stamped with a unique
// (cluster, proc) id.
func genRRL(rng *rand.Rand, params GenParams, templates []acTemplate, owner string) []*negotiator.Request {
	rrl := make([]*negotiator.Request, params.RRLDepth)
	for j := 0; j < params.RRLDepth; j++ {
		tpl := templates[rng.Intn(len(templates))]
		count := 1
		// ~1 in 5 requests is a small group, up to 4 identical jobs.
		if rng.Intn(5) == 0 {
			count = 1 + rng.Intn(4)
		}
		rrl[j] = &negotiator.Request{
			Ad:            jobAdFromTemplate(tpl, owner),
			Cluster:       j + 1,
			Proc:          0,
			AutoClusterID: tpl.id,
			Count:         count,
		}
	}
	return rrl
}

// jobAdFromTemplate materializes one representative job ad for an autocluster.
// The Requirements/Rank expression trees are shared with the template (read-only
// during evaluation); only the scalar Request* literals are per-ad.
func jobAdFromTemplate(tpl acTemplate, owner string) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("Owner", owner)
	ad.InsertAttr("RequestCpus", tpl.requestCpus)
	ad.InsertAttr("RequestMemory", tpl.requestMem)
	ad.InsertAttr("RequestDisk", 1024)
	ad.InsertAttrString("DesiredOpSys", tpl.opSys)
	ad.InsertAttrString("DesiredArch", tpl.arch)
	ad.InsertExpr("Requirements", tpl.req)
	ad.InsertExpr("Rank", tpl.rank)
	return ad
}

// mustExpr parses a generator-controlled expression string, panicking on error
// (the inputs are constants, so a failure is a programming bug, not a runtime
// condition).
func mustExpr(s string) *classad.Expr {
	e, err := classad.ParseExpr(s)
	if err != nil {
		panic(fmt.Sprintf("negtest: parse generator expr %q: %v", s, err))
	}
	return e
}
