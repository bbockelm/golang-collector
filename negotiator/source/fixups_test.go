package source

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

func mustOld(t *testing.T, s string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.ParseOld(s)
	if err != nil {
		t.Fatalf("parse ad: %v\n%s", err, s)
	}
	return ad
}

func TestFixupSlot_DropRemoteAdminCapability(t *testing.T) {
	ad := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 4
_condor_PrivRemoteAdminCapability = "topsecret"
RemoteAdminCapability = "alsosecret"`)
	FixupSlot(ad, ParseSlotWeight(""))
	if _, ok := ad.Lookup("_condor_PrivRemoteAdminCapability"); ok {
		t.Error("admin capability (_condor_ spelling) not dropped")
	}
	if _, ok := ad.Lookup("RemoteAdminCapability"); ok {
		t.Error("admin capability (short spelling) not dropped")
	}
}

func TestFixupSlot_NegotiatorRequirementsSwap(t *testing.T) {
	// Old Requirements: Cpus > 2 (true here). NegotiatorRequirements: Memory >
	// 1000 (false here). After the swap, Requirements must evaluate to the
	// NegotiatorRequirements (false) and SavedRequirements to the old one (true).
	ad := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 8
Memory = 500
Requirements = Cpus > 2
NegotiatorRequirements = Memory > 1000`)
	FixupSlot(ad, ParseSlotWeight(""))

	if _, ok := ad.Lookup("SavedRequirements"); !ok {
		t.Fatal("SavedRequirements not set")
	}
	if got, ok := ad.EvaluateAttrBool("Requirements"); !ok || got {
		t.Errorf("Requirements after swap = %v (ok=%v), want false (the NegotiatorRequirements)", got, ok)
	}
	if got, ok := ad.EvaluateAttrBool("SavedRequirements"); !ok || !got {
		t.Errorf("SavedRequirements = %v (ok=%v), want true (the old Requirements)", got, ok)
	}
}

func TestFixupSlot_NoNegotiatorRequirements_LeavesRequirements(t *testing.T) {
	ad := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 8
Requirements = Cpus > 2`)
	FixupSlot(ad, ParseSlotWeight(""))
	if _, ok := ad.Lookup("SavedRequirements"); ok {
		t.Error("SavedRequirements should not be set without NegotiatorRequirements")
	}
	if got, ok := ad.EvaluateAttrBool("Requirements"); !ok || !got {
		t.Errorf("Requirements = %v (ok=%v), want unchanged true", got, ok)
	}
}

func TestFixupSlot_MachineMatchCountReset(t *testing.T) {
	ad := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 4
MachineMatchCount = 5
OfflineMatches = "x"`)
	FixupSlot(ad, ParseSlotWeight(""))
	if v, ok := ad.EvaluateAttrInt("MachineMatchCount"); !ok || v != 0 {
		t.Errorf("MachineMatchCount = %d (ok=%v), want 0", v, ok)
	}
	if _, ok := ad.Lookup("OfflineMatches"); ok {
		t.Error("OfflineMatches not dropped")
	}
}

func TestFixupSlot_SlotWeightDefaulting(t *testing.T) {
	t.Run("missing defaults to Cpus", func(t *testing.T) {
		ad := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 8`)
		FixupSlot(ad, ParseSlotWeight(""))
		if v, ok := ad.EvaluateAttrNumber("SlotWeight"); !ok || v != 8 {
			t.Errorf("defaulted SlotWeight = %v (ok=%v), want 8 (Cpus)", v, ok)
		}
	})
	t.Run("literal weight preserved", func(t *testing.T) {
		ad := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 8
SlotWeight = 2.5`)
		FixupSlot(ad, ParseSlotWeight(""))
		if v, ok := ad.EvaluateAttrNumber("SlotWeight"); !ok || v != 2.5 {
			t.Errorf("SlotWeight = %v (ok=%v), want 2.5 preserved", v, ok)
		}
	})
	t.Run("expression weight preserved", func(t *testing.T) {
		ad := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 3
SlotWeight = Cpus`)
		FixupSlot(ad, ParseSlotWeight(""))
		if v, ok := ad.EvaluateAttrNumber("SlotWeight"); !ok || v != 3 {
			t.Errorf("SlotWeight = %v (ok=%v), want 3 (Cpus expr preserved)", v, ok)
		}
	})
}

func TestKeepSubmitter(t *testing.T) {
	cases := []struct {
		name string
		ad   string
		keep bool
	}{
		{"valid", `MyType="Submitter"
Name="alice@p"
ScheddIpAddr="<1.2.3.4:5>"
IdleJobs=5
RunningJobs=0`, true},
		{"valid running only", `MyType="Submitter"
Name="alice@p"
ScheddIpAddr="<1.2.3.4:5>"
IdleJobs=0
RunningJobs=2`, true},
		{"no name", `MyType="Submitter"
ScheddIpAddr="<1.2.3.4:5>"
IdleJobs=5`, false},
		{"no schedd addr", `MyType="Submitter"
Name="alice@p"
IdleJobs=5`, false},
		{"no jobs", `MyType="Submitter"
Name="alice@p"
ScheddIpAddr="<1.2.3.4:5>"
IdleJobs=0
RunningJobs=0`, false},
		{"skip matchmaking", `MyType="Submitter"
Name="alice@p"
ScheddIpAddr="<1.2.3.4:5>"
IdleJobs=5
SkipMatchmaking=true`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := KeepSubmitter(mustOld(t, tc.ad)); got != tc.keep {
				t.Errorf("KeepSubmitter = %v, want %v", got, tc.keep)
			}
		})
	}
}

func TestClaimKeyExactness(t *testing.T) {
	slot := mustOld(t, `MyType="Machine"
Name="slot1@ep1.pool.test"
StartdIpAddr="<10.0.0.11:9618>"
MyAddress="<10.0.0.11:9618>"`)
	got := ClaimKey(slot)
	want := "slot1@ep1.pool.test<10.0.0.11:9618>"
	if got != want {
		t.Errorf("ClaimKey = %q, want %q", got, want)
	}
}

func TestBuildClaimIDs(t *testing.T) {
	pvt := []*classad.ClassAd{
		// ClaimId-carrying, keyed by Name+MyAddress.
		mustOld(t, `Name="slot1@ep1"
MyAddress="<10.0.0.11:9618>"
ClaimId="claim-A"`),
		// Capability fallback (older startds).
		mustOld(t, `Name="slot1@ep2"
MyAddress="<10.0.0.12:9618>"
Capability="cap-B"`),
		// ClaimIdList: first token wins.
		mustOld(t, `Name="slot1@ep3"
MyAddress="<10.0.0.13:9618>"
ClaimIdList="list-C1, list-C2"`),
		// No address -> skipped.
		mustOld(t, `Name="slot1@ep4"
ClaimId="orphan"`),
		// No claim secret -> skipped.
		mustOld(t, `Name="slot1@ep5"
MyAddress="<10.0.0.15:9618>"`),
	}
	got := BuildClaimIDs(pvt)

	want := map[string]string{
		"slot1@ep1<10.0.0.11:9618>": "claim-A",
		"slot1@ep2<10.0.0.12:9618>": "cap-B",
		"slot1@ep3<10.0.0.13:9618>": "list-C1",
	}
	if len(got) != len(want) {
		t.Fatalf("claim map size = %d, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("claim[%q] = %q, want %q", k, got[k], v)
		}
	}

	// The producer key (Name+MyAddress) must equal the offer-side ClaimKey
	// (Name+StartdIpAddr) when the addresses coincide -- the runtime invariant.
	slot := mustOld(t, `Name="slot1@ep1"
StartdIpAddr="<10.0.0.11:9618>"`)
	if _, ok := got[ClaimKey(slot)]; !ok {
		t.Errorf("ClaimKey(slot)=%q not found in claim map built from private ads", ClaimKey(slot))
	}
}

func TestBuildClaimIDs_DuplicateKeyLastWins(t *testing.T) {
	pvt := []*classad.ClassAd{
		mustOld(t, `Name="slot1@ep1"
MyAddress="<10.0.0.11:9618>"
ClaimId="old"`),
		mustOld(t, `Name="slot1@ep1"
MyAddress="<10.0.0.11:9618>"
ClaimId="new"`),
	}
	got := BuildClaimIDs(pvt)
	if got["slot1@ep1<10.0.0.11:9618>"] != "new" {
		t.Errorf("duplicate key kept %q, want last (new)", got["slot1@ep1<10.0.0.11:9618>"])
	}
}

// TestFixupSlot_ConfigurableDefaultWeight verifies the SLOT_WEIGHT knob: a slot
// with no SlotWeight of its own receives the CONFIGURED default cost expression
// (not the hardcoded "Cpus"), while a slot that already advertises its own
// SlotWeight is left untouched.
func TestFixupSlot_ConfigurableDefaultWeight(t *testing.T) {
	weight := ParseSlotWeight("Cpus + Memory / 1024")

	// No SlotWeight -> gets the configured expression, evaluating over this ad.
	noWeight := mustOld(t, `MyType = "Machine"
Name = "slot1@a"
Cpus = 4
Memory = 2048`)
	FixupSlot(noWeight, weight)
	if got, ok := noWeight.EvaluateAttrNumber("SlotWeight"); !ok || got != 6 { // 4 + 2048/1024
		t.Errorf("default SlotWeight = %v (ok=%v), want 6 (Cpus + Memory/1024)", got, ok)
	}

	// A slot with its own SlotWeight keeps it.
	ownWeight := mustOld(t, `MyType = "Machine"
Name = "slot2@a"
Cpus = 4
Memory = 2048
SlotWeight = 3`)
	FixupSlot(ownWeight, weight)
	if got, ok := ownWeight.EvaluateAttrNumber("SlotWeight"); !ok || got != 3 {
		t.Errorf("existing SlotWeight = %v (ok=%v), want 3 (untouched)", got, ok)
	}

	// Empty/invalid config falls back to the C++ "Cpus" default.
	if fallback := ParseSlotWeight(""); fallback == nil {
		t.Error("ParseSlotWeight(\"\") returned nil, want the Cpus default")
	}
}
