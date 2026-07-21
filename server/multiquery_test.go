package server

import (
	"context"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestMultiQueryFlatSequence drives QUERY_MULTIPLE_ADS (the command the negotiator uses
// every cycle to fetch several ad types at once): a single query ad names several target
// types, and the collector streams the matches from every named table back as one flat
// PutInt32(1)+ad sequence with a single PutInt32(0) terminator. It also exercises the
// shared relayMatches fast path (the in-memory store is a RawQueryer) and per-target
// projection / constraints.
func TestMultiQueryFlatSequence(t *testing.T) {
	_, addr, stop := startCollector(t)
	defer stop()

	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 10*time.Second)
	defer cancel()

	col := htcondor.NewCollector(addr)
	// Two Machine ads (one matches the per-target constraint, one does not) and one
	// Submitter ad, so the multi-query must span two tables and honor a per-target filter.
	adverts := []struct {
		cmd commands.CommandType
		ad  string
	}{
		{commands.UPDATE_STARTD_AD, `[MyType="Machine"; Name="slot1@a"; Cpus=8; Memory=4096; MyAddress="<1.2.3.4:5>"]`},
		{commands.UPDATE_STARTD_AD, `[MyType="Machine"; Name="slot2@a"; Cpus=2; Memory=2048; MyAddress="<1.2.3.4:6>"]`},
		{commands.UPDATE_SUBMITTOR_AD, `[MyType="Submitter"; Name="user@a"; RunningJobs=3; MyAddress="<1.2.3.4:7>"]`},
	}
	for _, a := range adverts {
		if err := col.Advertise(ctx, mustAd(t, a.ad), &htcondor.AdvertiseOptions{Command: a.cmd}); err != nil {
			t.Fatalf("advertise (cmd %d): %v", a.cmd, err)
		}
	}

	// TargetType lists both tables. Machine has a per-target constraint (Cpus > 5, so only
	// slot1) and a per-target projection to Name only; Submitter is unfiltered.
	queryAd := mustAd(t, `[MyType="Query"; TargetType="Submitter,Machine";`+
		` MachineRequirements = Cpus > 5; MachineProjection="Name" ]`)

	// Updates are fire-and-forget, so poll until the whole expected set is visible.
	var machines, submitters []*classad.ClassAd
	ok := waitFor(5*time.Second, func() bool {
		got := runQuery(t, addr, commands.QUERY_MULTIPLE_ADS, queryAd)
		machines, submitters = nil, nil
		for _, ad := range got {
			switch mt, _ := ad.EvaluateAttrString("MyType"); mt {
			case "Machine":
				machines = append(machines, ad)
			case "Submitter":
				submitters = append(submitters, ad)
			}
		}
		return len(machines) == 1 && len(submitters) == 1
	})
	if !ok {
		t.Fatalf("multi-query returned %d machines, %d submitters; want 1 and 1", len(machines), len(submitters))
	}

	// The Machine match is slot1 (Cpus>5), projected to Name only.
	m := machines[0]
	if name, _ := m.EvaluateAttrString("Name"); name != "slot1@a" {
		t.Errorf("Machine match Name = %q, want slot1@a (per-target constraint Cpus>5)", name)
	}
	if _, leaked := m.EvaluateAttrInt("Memory"); leaked {
		t.Error("Machine ad leaked Memory; per-target projection to [Name] was not applied")
	}
	// The Submitter ad has no projection, so its attributes are intact.
	if name, _ := submitters[0].EvaluateAttrString("Name"); name != "user@a" {
		t.Errorf("Submitter match Name = %q, want user@a", name)
	}
	if rj, _ := submitters[0].EvaluateAttrInt("RunningJobs"); rj != 3 {
		t.Errorf("Submitter RunningJobs = %d, want 3 (unprojected)", rj)
	}
}
