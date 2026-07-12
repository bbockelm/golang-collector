package cycle

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/accountant"
	"github.com/bbockelm/golang-collector/negotiator/negtest"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
	"github.com/bbockelm/golang-collector/negotiator/source"
	"github.com/bbockelm/golang-collector/store"
)

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// machineAd builds a Machine (slot) ad. The slot-side Requirements enforce the
// job's resource ask, so unsatisfiable requests reject cleanly.
func machineAd(t *testing.T, name string, cpus int64) *classad.ClassAd {
	t.Helper()
	addr := "<192.168.0.1:9618?slot=" + name + ">"
	ad := classad.New()
	ad.InsertAttrString("MyType", "Machine")
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("MyAddress", addr)
	ad.InsertAttrString("StartdIpAddr", addr)
	ad.InsertAttrString("State", "Unclaimed")
	ad.InsertAttrString("Activity", "Idle")
	ad.InsertAttr("Cpus", cpus)
	ad.InsertAttr("Memory", 4096)
	ad.InsertAttr("Disk", 1<<22)
	req, err := classad.ParseExpr("TARGET.RequestCpus <= MY.Cpus && TARGET.RequestMemory <= MY.Memory")
	if err != nil {
		t.Fatalf("parse slot requirements: %v", err)
	}
	ad.InsertExpr("Requirements", req)
	return ad
}

// pvtAd builds the matching startd-private ad carrying the claim secret.
func pvtAd(name, claim string) *classad.ClassAd {
	addr := "<192.168.0.1:9618?slot=" + name + ">"
	ad := classad.New()
	ad.InsertAttrBool("_forcePvt", true)
	ad.InsertAttrString("MyType", "Machine")
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("MyAddress", addr)
	ad.InsertAttrString("ClaimId", claim)
	return ad
}

// submitterAd builds a Submitter ad pointing at a (loopback) schedd address.
func submitterAd(name, scheddName, scheddAddr string, idle int64) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("MyType", "Submitter")
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("ScheddName", scheddName)
	ad.InsertAttrString("ScheddIpAddr", scheddAddr)
	ad.InsertAttr("IdleJobs", idle)
	ad.InsertAttr("RunningJobs", 0)
	ad.InsertAttrString("SubmitterTag", "")
	ad.InsertAttr("LastHeardFrom", 1000)
	return ad
}

// jobAd builds the representative job ad offered inside a request group.
func jobAd(t *testing.T, requestCpus int64, extraReq string) *classad.ClassAd {
	t.Helper()
	ad := classad.New()
	ad.InsertAttr("RequestCpus", requestCpus)
	ad.InsertAttr("RequestMemory", 512)
	ad.InsertAttr("RequestDisk", 1024)
	reqStr := "TARGET.Cpus >= MY.RequestCpus && TARGET.Memory >= MY.RequestMemory"
	if extraReq != "" {
		reqStr = "(" + reqStr + ") && (" + extraReq + ")"
	}
	req, err := classad.ParseExpr(reqStr)
	if err != nil {
		t.Fatalf("parse job requirements: %v", err)
	}
	ad.InsertExpr("Requirements", req)
	return ad
}

// group is a shorthand for a negtest request group of count identical jobs.
func group(t *testing.T, cluster, autocluster, count int, requestCpus int64, extraReq string) negtest.Group {
	t.Helper()
	members := make([]negtest.Job, count)
	for i := range members {
		members[i] = negtest.J(cluster, i)
	}
	return negtest.Group{
		RepCluster:    cluster,
		RepProc:       0,
		AutoClusterID: autocluster,
		Members:       members,
		RepAd:         jobAd(t, requestCpus, extraReq),
	}
}

// newAccountant builds a fresh in-memory accountant with the HTCondor default
// config and a baselined LastUpdateTime (so the cycle's decay tick does not
// wipe test-assigned priorities: a first-ever tick spans "all of history" and
// floors every priority, exactly as a fresh C++ accountant would).
func newAccountant(t *testing.T) *accountant.Accountant {
	t.Helper()
	acct, err := accountant.New(accountant.DefaultConfig())
	if err != nil {
		t.Fatalf("accountant.New: %v", err)
	}
	t.Cleanup(func() { _ = acct.Close() })
	acct.UpdatePriorities(time.Now())
	return acct
}

// capAcct wraps an Accountant, overriding ceilings/floors for chosen
// submitters. The accountant package does not export a ceiling/floor setter
// (the SET_CEILING/SET_FLOOR userprio handlers are Phase 6), so cycle tests
// inject caps at the interface seam instead.
type capAcct struct {
	negotiator.Accountant
	ceil  map[string]float64
	floor map[string]float64
}

func (c *capAcct) GetCeiling(submitter string) float64 {
	if v, ok := c.ceil[submitter]; ok {
		return v
	}
	return c.Accountant.GetCeiling(submitter)
}

func (c *capAcct) GetFloor(submitter string) float64 {
	if v, ok := c.floor[submitter]; ok {
		return v
	}
	return c.Accountant.GetFloor(submitter)
}

// seedStore stores machine + submitter + private ads into a fresh store.
func seedStore(t *testing.T, ads ...*classad.ClassAd) *store.Store {
	t.Helper()
	st := store.New()
	negtest.SeedStore(t, st, ads)
	return st
}

// embeddedSource wraps a store in the embedded AdSource.
func embeddedSource(t *testing.T, st *store.Store) *source.EmbeddedSource {
	t.Helper()
	src, err := source.NewEmbedded(st, source.Config{})
	if err != nil {
		t.Fatalf("NewEmbedded: %v", err)
	}
	return src
}

// fixedSource is an AdSource returning a pre-built snapshot in a FIXED slot
// order. The determinism test uses it (instead of the embedded store source)
// so both compat and fast runs see byte-identical candidate scan orders; the
// store's map-backed iteration order is not stable across instances, and slot
// order legitimately affects tie-breaks (ScanIndex), exactly as collector
// reply order does for the C++ negotiator.
type fixedSource struct {
	snap *negotiator.PoolSnapshot

	mu        sync.Mutex
	published []*classad.ClassAd
}

func newFixedSource(t *testing.T, slots []*classad.ClassAd, submitters []*classad.ClassAd, pvts []*classad.ClassAd) *fixedSource {
	t.Helper()
	for _, s := range slots {
		source.FixupSlot(s)
	}
	kept := make([]*classad.ClassAd, 0, len(submitters))
	for _, s := range submitters {
		if source.KeepSubmitter(s) {
			kept = append(kept, s)
		}
	}
	return &fixedSource{
		snap: &negotiator.PoolSnapshot{
			Slots:      slots,
			Submitters: kept,
			ClaimIDs:   source.BuildClaimIDs(pvts),
			Taken:      time.Now(),
		},
	}
}

func (f *fixedSource) Snapshot(ctx context.Context) (*negotiator.PoolSnapshot, error) {
	return f.snap, nil
}

func (f *fixedSource) PublishNegotiatorAd(ctx context.Context, ad *classad.ClassAd) error {
	return nil
}

func (f *fixedSource) PublishAccountingAds(ctx context.Context, ads []*classad.ClassAd) error {
	f.mu.Lock()
	f.published = append(f.published, ads...)
	f.mu.Unlock()
	return nil
}

// startSchedd launches a loopback schedd serving the given per-round plans.
func startSchedd(t *testing.T, ctx context.Context, rounds [][]negtest.Group, opts ...negtest.Option) *negtest.LoopbackSchedd {
	t.Helper()
	sched, err := negtest.Start(ctx, rounds, opts...)
	if err != nil {
		t.Fatalf("negtest.Start: %v", err)
	}
	return sched
}

func newFactory() *protocol.Factory {
	return protocol.NewFactory(negtest.ClientSecurity(), protocol.WithNegotiatorName("negotiator@test"))
}

// matchesByOwner flattens a schedd's round logs into the ordered slot names
// delivered per owner.
func matchesByOwner(sched *negtest.LoopbackSchedd) map[string][]string {
	out := map[string][]string{}
	for _, rl := range sched.Logs() {
		for _, m := range rl.Matches {
			out[rl.Owner] = append(out[rl.Owner], m.SlotName)
		}
	}
	return out
}

func totalMatches(sched *negtest.LoopbackSchedd) int {
	n := 0
	for _, rl := range sched.Logs() {
		n += len(rl.Matches)
	}
	return n
}

func totalRejects(sched *negtest.LoopbackSchedd) int {
	n := 0
	for _, rl := range sched.Logs() {
		n += len(rl.Rejects)
	}
	return n
}

// claimForSlot is the claim id the fixtures seed for a slot name.
func claimForSlot(name string) string {
	return fmt.Sprintf("claim-%s-secret", name)
}
