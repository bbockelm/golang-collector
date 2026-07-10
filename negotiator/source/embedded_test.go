package source

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
	"github.com/bbockelm/golang-collector/store"
)

func TestEmbeddedSnapshot_FlatPool(t *testing.T) {
	st := store.New()
	negtest.SeedStore(t, st, negtest.LoadAds(t, "../negtest/testdata/flatpool.ads"))

	src, err := NewEmbedded(st, Config{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Slots) != 2 {
		t.Errorf("slots = %d, want 2", len(snap.Slots))
	}
	if len(snap.Submitters) != 2 {
		t.Errorf("submitters = %d, want 2", len(snap.Submitters))
	}
	if len(snap.ClaimIDs) != 0 {
		t.Errorf("claim ids = %d, want 0 (no private ads seeded)", len(snap.ClaimIDs))
	}
	// SlotWeight = Cpus preserved and evaluable.
	for _, s := range snap.Slots {
		if _, ok := s.EvaluateAttrNumber("SlotWeight"); !ok {
			name, _ := s.EvaluateAttrString("Name")
			t.Errorf("slot %q SlotWeight not evaluable after fixup", name)
		}
		if v, ok := s.EvaluateAttrInt("MachineMatchCount"); !ok || v != 0 {
			t.Errorf("slot MachineMatchCount = %d (ok=%v), want 0", v, ok)
		}
	}
	if snap.Taken.IsZero() {
		t.Error("snapshot Taken time not set")
	}
}

const edgeAds = `MyType = "Machine"
Name = "slot1@edge"
Machine = "edge.pool.test"
StartdIpAddr = "<10.9.9.9:9618>"
MyAddress = "<10.9.9.9:9618>"
Cpus = 16
Memory = 8192
Requirements = Cpus > 0
NegotiatorRequirements = Memory > 999999
MachineMatchCount = 7
_condor_PrivRemoteAdminCapability = "leak-me-not"

MyType = "Machine"
Name = "slot1@edge"
MyAddress = "<10.9.9.9:9618>"
ClaimId = "edge-claim-42"
_forcePvt = true

MyType = "Submitter"
Name = "empty@pool.test"
ScheddName = "ap9.pool.test"
ScheddIpAddr = "<10.0.0.99:9618>"
IdleJobs = 0
RunningJobs = 0

MyType = "Submitter"
Name = "skip@pool.test"
ScheddName = "ap9.pool.test"
ScheddIpAddr = "<10.0.0.99:9618>"
IdleJobs = 4
SkipMatchmaking = true

MyType = "Submitter"
Name = "carol@pool.test"
ScheddName = "ap9.pool.test"
ScheddIpAddr = "<10.0.0.99:9618>"
IdleJobs = 2
RunningJobs = 1`

func TestEmbeddedSnapshot_FixupsFiltersClaims(t *testing.T) {
	ads, err := negtest.ParseAds(edgeAds)
	if err != nil {
		t.Fatal(err)
	}
	st := store.New()
	negtest.SeedStore(t, st, ads)

	// Sanity: the private ad landed in the private table, not the public one.
	if n := st.Len(store.StartdAd); n != 1 {
		t.Fatalf("public startd table = %d, want 1", n)
	}
	if n := st.Len(store.StartdPvtAd); n != 1 {
		t.Fatalf("private startd table = %d, want 1", n)
	}

	src, err := NewEmbedded(st, Config{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Only carol survives the submitter filters (empty=no jobs, skip=SkipMatchmaking).
	if len(snap.Submitters) != 1 {
		t.Fatalf("submitters = %d, want 1 (carol)", len(snap.Submitters))
	}
	if name, _ := snap.Submitters[0].EvaluateAttrString("Name"); name != "carol@pool.test" {
		t.Errorf("surviving submitter = %q, want carol@pool.test", name)
	}

	// Slot fixups applied.
	if len(snap.Slots) != 1 {
		t.Fatalf("slots = %d, want 1", len(snap.Slots))
	}
	slot := snap.Slots[0]
	if _, ok := slot.Lookup("_condor_PrivRemoteAdminCapability"); ok {
		t.Error("admin capability not dropped")
	}
	if _, ok := slot.Lookup("SavedRequirements"); !ok {
		t.Error("SavedRequirements not set (NegotiatorRequirements swap)")
	}
	if got, ok := slot.EvaluateAttrBool("Requirements"); !ok || got {
		t.Errorf("Requirements = %v (ok=%v), want false (NegotiatorRequirements Memory>999999)", got, ok)
	}
	if v, ok := slot.EvaluateAttrInt("MachineMatchCount"); !ok || v != 0 {
		t.Errorf("MachineMatchCount = %d, want 0", v)
	}

	// Claim map built from the private ad, keyed by ClaimKey(publicSlot).
	if len(snap.ClaimIDs) != 1 {
		t.Fatalf("claim ids = %d, want 1", len(snap.ClaimIDs))
	}
	key := ClaimKey(slot)
	if snap.ClaimIDs[key] != "edge-claim-42" {
		t.Errorf("claim[%q] = %q, want edge-claim-42", key, snap.ClaimIDs[key])
	}

	// The claim secret must never appear on a public slot ad.
	if _, leaked := slot.EvaluateAttrString("ClaimId"); leaked {
		t.Error("public slot ad leaked ClaimId")
	}
}

func TestEmbeddedSnapshot_MutationIsolation(t *testing.T) {
	st := store.New()
	negtest.SeedStore(t, st, negtest.LoadAds(t, "../negtest/testdata/flatpool.ads"))

	src, err := NewEmbedded(st, Config{})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := src.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Mutate a snapshot slot ad aggressively.
	victim := snap.Slots[0]
	vname, _ := victim.EvaluateAttrString("Name")
	victim.InsertAttr("Cpus", 99999)
	victim.Delete("Memory")
	victim.InsertAttrString("Injected", "boom")

	// Re-read the store directly: the stored ad must be untouched.
	found := false
	for ad := range st.Query(store.StartdAd, nil) {
		name, _ := ad.EvaluateAttrString("Name")
		if name != vname {
			continue
		}
		found = true
		if _, ok := ad.Lookup("Injected"); ok {
			t.Error("store ad gained the injected attribute (mutation leaked into store)")
		}
		if _, ok := ad.Lookup("Memory"); !ok {
			t.Error("store ad lost Memory (mutation leaked into store)")
		}
		if v, ok := ad.EvaluateAttrInt("Cpus"); !ok || v == 99999 {
			t.Errorf("store ad Cpus = %d, want original (mutation leaked)", v)
		}
	}
	if !found {
		t.Fatalf("slot %q not found back in store", vname)
	}
}

func TestEmbeddedPublish(t *testing.T) {
	st := store.New()
	src, err := NewEmbedded(st, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	neg := mustOld(t, `MyType="Negotiator"
Name="neg@pool.test"
MyAddress="<10.0.0.1:9618>"`)
	if err := src.PublishNegotiatorAd(ctx, neg); err != nil {
		t.Fatal(err)
	}
	if n := st.Len(store.NegotiatorAd); n != 1 {
		t.Errorf("negotiator table = %d, want 1", n)
	}

	acct := []*classad.ClassAd{
		mustOld(t, `MyType="Accounting"
Name="alice@pool.test"
MyAddress="<10.0.0.1:9618>"
Priority=1.0`),
		mustOld(t, `MyType="Accounting"
Name="bob@pool.test"
MyAddress="<10.0.0.1:9618>"
Priority=2.0`),
	}
	if err := src.PublishAccountingAds(ctx, acct); err != nil {
		t.Fatal(err)
	}
	if n := st.Len(store.AccountingAd); n != 2 {
		t.Errorf("accounting table = %d, want 2", n)
	}
}
