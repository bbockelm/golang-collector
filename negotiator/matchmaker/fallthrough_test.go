package matchmaker

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// TestUnqualifiedRequirementsFallthrough verifies the old-ClassAd MY->TARGET
// fallthrough (classad v0.5.1) through the full Match path: real HTCondor ads
// use unqualified cross-ad references constantly -- a job's Requirements names
// machine attrs ("Memory >= 1024") and a startd START expression names job
// attrs ("Owner != ...") -- and C++ matchmaking resolves them via the
// alternateScope mechanism. Without the fallthrough these were UNDEFINED and
// every such match failed.
func TestUnqualifiedRequirementsFallthrough(t *testing.T) {
	mm, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	snap := &negotiator.PoolSnapshot{
		Slots: []*classad.ClassAd{
			// Fails the job side (Memory 512 < 1024).
			mustAd(t, `[ Name = "small@ep"; StartdIpAddr = "<10.0.0.9:9618>"; Cpus = 1; Memory = 512; Requirements = Owner != "smith" ]`),
			// Fails the machine side (Owner == jones).
			mustAd(t, `[ Name = "banned@ep"; StartdIpAddr = "<10.0.0.9:9618>"; Cpus = 1; Memory = 4096; Requirements = Owner != "jones" ]`),
			// Both sides pass.
			mustAd(t, `[ Name = "good@ep"; StartdIpAddr = "<10.0.0.9:9618>"; Cpus = 1; Memory = 2048; Requirements = Owner != "smith" ]`),
		},
		ClaimIDs: map[string]string{},
	}
	view := NewSlotView(snap)

	// Job requirements reference an UNQUALIFIED machine attr; the machine
	// requirements above reference the job's Owner unqualified (START-style).
	job := mustAd(t, `[ Owner = "jones"; ClusterId = 1; ProcId = 0; Requirements = Memory >= 1024 ]`)
	req := &negotiator.Request{Ad: job, Cluster: 1, Proc: 0, Count: 1}
	limits := &negotiator.MatchLimits{SubmitterLimit: 100, PieLeft: 100}

	cand, rej, err := mm.Match(context.Background(), req, view, limits)
	if err != nil {
		t.Fatalf("Match: %v", err)
	}
	if cand == nil {
		t.Fatalf("expected a match via unqualified cross-ad refs, got reject: %+v", rej)
	}
	if name, _ := cand.Slot.EvaluateAttrString("Name"); name != "good@ep" {
		t.Fatalf("matched %q, want good@ep (Memory + Owner fallthrough both honored)", name)
	}
}
