package protocol

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// TestHeaderCarriesJobPrioBand verifies the USE_GLOBAL_JOB_PRIOS band travels on
// the NEGOTIATE header: with HasJobPrio set the schedd sees JOBPRIO_MIN/MAX
// (schedd.cpp:8980); without it the attributes are absent, so a schedd that does
// not play the game is unaffected.
func TestHeaderCarriesJobPrioBand(t *testing.T) {
	ctx := testCtx(t)

	round := []negtest.Group{
		{RepCluster: 10, RepProc: 0, AutoClusterID: 100, Members: []negtest.Job{{Cluster: 10, Proc: 0}}},
	}

	begin := func(t *testing.T, h *negotiator.NegotiateHeader) *classad.ClassAd {
		sched, err := negtest.Start(ctx, [][]negtest.Group{round})
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		f := NewFactory(negtest.ClientSecurity())
		t.Cleanup(f.CloseAll)

		s := f.Session(h.Owner, "schedd1", sched.Addr(), nil)
		if err := s.Begin(ctx, h); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if _, err := s.FetchRequests(ctx, 10); err != nil {
			t.Fatalf("FetchRequests: %v", err)
		}
		if err := s.End(ctx); err != nil {
			t.Fatalf("End: %v", err)
		}
		if err := sched.WaitRounds(ctx, 1); err != nil {
			t.Fatalf("WaitRounds: %v", err)
		}
		return sched.Logs()[0].Header
	}

	t.Run("band present", func(t *testing.T) {
		hdr := &negotiator.NegotiateHeader{
			Owner: "alice", AutoClusterAttrs: "RequestCpus",
			HasJobPrio: true, JobPrioMin: 3, JobPrioMax: 7,
		}
		got := begin(t, hdr)
		jmin, ok1 := got.EvaluateAttrInt("JOBPRIO_MIN")
		jmax, ok2 := got.EvaluateAttrInt("JOBPRIO_MAX")
		if !ok1 || !ok2 || jmin != 3 || jmax != 7 {
			t.Errorf("received band = {%d(%v),%d(%v)}, want {3,7}", jmin, ok1, jmax, ok2)
		}
	})

	t.Run("band absent when off", func(t *testing.T) {
		hdr := &negotiator.NegotiateHeader{Owner: "bob", AutoClusterAttrs: "RequestCpus"}
		got := begin(t, hdr)
		if _, ok := got.EvaluateAttrInt("JOBPRIO_MIN"); ok {
			t.Error("JOBPRIO_MIN should be absent when HasJobPrio is false")
		}
		if _, ok := got.EvaluateAttrInt("JOBPRIO_MAX"); ok {
			t.Error("JOBPRIO_MAX should be absent when HasJobPrio is false")
		}
	})
}
