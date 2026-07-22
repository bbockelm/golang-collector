package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/cedar/commands"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// advertiseMultiQueryFixture publishes two Machine ads (one matches the per-target
// constraint used below, one does not) and one Submitter ad, so a multi-target query must
// span two tables and honor a per-target filter.
func advertiseMultiQueryFixture(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 10*time.Second)
	defer cancel()
	col := htcondor.NewCollector(addr)
	fixture := []struct {
		cmd commands.CommandType
		ad  string
	}{
		{commands.UPDATE_STARTD_AD, `[MyType="Machine"; Name="slot1@a"; Cpus=8; Memory=4096; MyAddress="<1.2.3.4:5>"]`},
		{commands.UPDATE_STARTD_AD, `[MyType="Machine"; Name="slot2@a"; Cpus=2; Memory=2048; MyAddress="<1.2.3.4:6>"]`},
		{commands.UPDATE_SUBMITTOR_AD, `[MyType="Submitter"; Name="user@a"; RunningJobs=3; MyAddress="<1.2.3.4:7>"]`},
	}
	for _, f := range fixture {
		if err := col.Advertise(ctx, mustAd(t, f.ad), &htcondor.AdvertiseOptions{Command: f.cmd}); err != nil {
			t.Fatalf("advertise (cmd %d): %v", f.cmd, err)
		}
	}
}

// checkMultiQuery runs the multi-target query and asserts the negotiator-style result: one
// Machine (slot1, matched by the per-target Cpus>5 constraint, projected to Name+MyType so
// Memory is gone) and one full Submitter. Shared across the in-memory and remote-db
// backends so both relayMatches paths (buffered iterator/raw-relay) are covered by the same
// assertions. Bucketing by MyType also proves MyType survives as an attribute end to end.
func checkMultiQuery(t *testing.T, addr string) {
	t.Helper()
	queryAd := mustAd(t, `[MyType="Query"; TargetType="Submitter,Machine";`+
		` MachineRequirements = Cpus > 5; MachineProjection="Name MyType" ]`)

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
	if name, _ := machines[0].EvaluateAttrString("Name"); name != "slot1@a" {
		t.Errorf("Machine match Name = %q, want slot1@a (per-target constraint Cpus>5)", name)
	}
	if _, leaked := machines[0].EvaluateAttrInt("Memory"); leaked {
		t.Error("Machine ad leaked Memory; per-target projection to [Name MyType] was not applied")
	}
	if name, _ := submitters[0].EvaluateAttrString("Name"); name != "user@a" {
		t.Errorf("Submitter match Name = %q, want user@a", name)
	}
	if rj, _ := submitters[0].EvaluateAttrInt("RunningJobs"); rj != 3 {
		t.Errorf("Submitter RunningJobs = %d, want 3 (unprojected)", rj)
	}
}

// TestMultiQueryInMemory covers multiQueryHandler over the in-memory store (relayMatches'
// buffered RawQueryer path): a multi-target query with per-target constraint + projection.
func TestMultiQueryInMemory(t *testing.T) {
	_, addr, stop := startCollector(t)
	defer stop()
	advertiseMultiQueryFixture(t, addr)
	checkMultiQuery(t, addr)
}

// TestMultiQueryRemoteDB covers multiQueryHandler over the remote-database backend, which
// exercises relayMatches' streaming raw relay (RawStreamer / ProjectedRawStreamer) -- each
// target's ads are streamed and relayed as they arrive. Same assertions as the in-memory
// case, so the two delivery paths are proven equivalent. This is the path that regressed
// real matchmaking when MyType was not conveyed as an attribute.
func TestMultiQueryRemoteDB(t *testing.T) {
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := dbrpc.NewServerCatalog(cat)
	dial := func(context.Context) (dbrpc.MsgConn, error) {
		sc, cc := net.Pipe()
		go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{IncludePrivate: true}) }()
		return dbrpc.NewStreamConn(cc), nil
	}
	rpc := store.NewRPCBackend(context.Background(), dial, store.RetryPolicy{
		Initial: time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2, MaxElapsed: time.Second,
	})
	buf, err := store.NewBufferedBackend(rpc, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}

	cs := New(buf, plaintextSec(), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sctx, scancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = cs.ServeConn(sctx, conn) }()
		}
	}()
	addr := ln.Addr().String()
	defer func() { scancel(); _ = ln.Close(); _ = buf.Close(); srv.Close(); _ = cat.Close() }()

	advertiseMultiQueryFixture(t, addr)
	checkMultiQuery(t, addr)
}
