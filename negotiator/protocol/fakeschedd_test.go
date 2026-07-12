package protocol

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// slotAd builds a minimal offer/slot ad with a saved-Requirements stash and a
// NegotiatorMatchExpr attr, so EnrichMatchAd's restore + passthrough are exercised.
func slotAd(name string) *classad.ClassAd {
	ad := classad.New()
	_ = ad.Set("Name", name)
	_ = ad.Set("SavedRequirements", true)
	_ = ad.Set("Requirements", false) // negotiator's swapped-in value; must be restored
	_ = ad.Set("NegotiatorMatchExprFoo", "bar")
	_ = ad.Set("SlotWeight", 1.0)
	return ad
}

// sendMatches delivers cnt matches for req, each with a distinct claim id built
// from prefix, using EnrichMatchAd to build the payload ad.
func sendMatches(ctx context.Context, t *testing.T, s negotiator.ScheddSession, req *negotiator.Request, prefix string, cnt int) {
	t.Helper()
	for i := 0; i < cnt; i++ {
		enriched := EnrichMatchAd(slotAd(fmt.Sprintf("slot%d@host", i)), req, MatchContext{})
		mr := &negotiator.MatchResult{
			Request: req,
			SlotAd:  enriched,
			ClaimID: fmt.Sprintf("%s-%d", prefix, i),
			Cost:    1.0,
		}
		if err := s.SendMatch(ctx, mr); err != nil {
			t.Fatalf("SendMatch: %v", err)
		}
	}
}

func hdr(owner, tag string) *negotiator.NegotiateHeader {
	return &negotiator.NegotiateHeader{
		Owner:            owner,
		AutoClusterAttrs: "RequestCpus,RequestMemory",
		SubmitterTag:     tag,
		NegotiatorName:   "neg@pool",
	}
}

// TestFullRoundAndWarmReuse drives a complete NEGOTIATE round (batched RRL,
// batched matches assigned to the right group members with both id spellings, a
// reasoned reject), then a SECOND round on the SAME warm socket.
func TestFullRoundAndWarmReuse(t *testing.T) {
	ctx := testCtx(t)

	round0 := []negtest.Group{
		{RepCluster: 10, RepProc: 0, AutoClusterID: 100, Members: []negtest.Job{{Cluster: 10, Proc: 0}}},
		{RepCluster: 20, RepProc: 0, AutoClusterID: 200, Members: []negtest.Job{{Cluster: 20, Proc: 0}, {Cluster: 20, Proc: 1}}},
		{RepCluster: 30, RepProc: 0, AutoClusterID: 300, Members: []negtest.Job{{Cluster: 30, Proc: 0}, {Cluster: 30, Proc: 1}, {Cluster: 30, Proc: 2}, {Cluster: 30, Proc: 3}, {Cluster: 30, Proc: 4}}},
	}
	round1 := []negtest.Group{
		{RepCluster: 40, RepProc: 0, AutoClusterID: 400, Members: []negtest.Job{{Cluster: 40, Proc: 0}, {Cluster: 40, Proc: 1}}},
	}
	sched, err := negtest.Start(ctx, [][]negtest.Group{round0, round1})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	f := NewFactory(negtest.ClientSecurity())
	t.Cleanup(f.CloseAll)

	// ---- Round 0 ----
	s := f.Session("alice", "schedd1", sched.Addr(), nil)
	if err := s.Begin(ctx, hdr("alice", "tag1")); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	reqs, err := s.FetchRequests(ctx, 200)
	if err != nil {
		t.Fatalf("FetchRequests: %v", err)
	}
	if len(reqs) != 3 {
		t.Fatalf("got %d requests, want 3", len(reqs))
	}
	wantCounts := []int{1, 2, 5}
	wantClusters := []int{10, 20, 30}
	for i, r := range reqs {
		if r.Count != wantCounts[i] || r.Cluster != wantClusters[i] {
			t.Errorf("req[%d] = {Cluster:%d Count:%d}, want {Cluster:%d Count:%d}",
				i, r.Cluster, r.Count, wantClusters[i], wantCounts[i])
		}
	}

	sendMatches(ctx, t, s, reqs[0], "claimA", 1)
	sendMatches(ctx, t, s, reqs[1], "claimB", 2)
	if err := s.Reject(ctx, reqs[2], "no slots available"); err != nil {
		t.Fatalf("Reject: %v", err)
	}
	if err := s.End(ctx); err != nil {
		t.Fatalf("End: %v", err)
	}

	if err := sched.WaitRounds(ctx, 1); err != nil {
		t.Fatalf("WaitRounds(1): %v", err)
	}
	logs := sched.Logs()
	if len(logs) != 1 {
		t.Fatalf("got %d round logs, want 1", len(logs))
	}
	r0 := logs[0]
	if r0.Owner != "alice" {
		t.Errorf("round0 owner = %q, want alice", r0.Owner)
	}
	if len(r0.Matches) != 3 {
		t.Fatalf("round0 matches = %d, want 3", len(r0.Matches))
	}
	// Group members assigned in order: 10.0, then 20.0, 20.1.
	wantAssign := [][2]int{{10, 0}, {20, 0}, {20, 1}}
	for i, m := range r0.Matches {
		if !m.HasCondorSpelling || !m.HasPlainSpelling {
			t.Errorf("match[%d] spellings: condor=%v plain=%v, want both true", i, m.HasCondorSpelling, m.HasPlainSpelling)
		}
		if m.AssignedCluster != wantAssign[i][0] || m.AssignedProc != wantAssign[i][1] {
			t.Errorf("match[%d] assigned %d.%d, want %d.%d", i, m.AssignedCluster, m.AssignedProc, wantAssign[i][0], wantAssign[i][1])
		}
		// The enriched ad's restored Requirements must be the SavedRequirements value (true).
		if v, ok := m.MatchAd.EvaluateAttrBool("Requirements"); !ok || !v {
			t.Errorf("match[%d] Requirements not restored from SavedRequirements (got %v ok=%v)", i, v, ok)
		}
		if v, ok := m.MatchAd.EvaluateAttrString("NegotiatorMatchExprFoo"); !ok || v != "bar" {
			t.Errorf("match[%d] NegotiatorMatchExpr passthrough lost", i)
		}
	}
	if len(r0.Rejects) != 1 {
		t.Fatalf("round0 rejects = %d, want 1", len(r0.Rejects))
	}
	rej := r0.Rejects[0]
	if !rej.HasRep || rej.RepCluster != 30 || rej.RepProc != 0 {
		t.Errorf("reject suffix parsed to %d.%d hasRep=%v, want 30.0 true", rej.RepCluster, rej.RepProc, rej.HasRep)
	}
	if rej.Reason != "no slots available" {
		t.Errorf("reject reason = %q, want %q", rej.Reason, "no slots available")
	}

	if got := sched.Conns(); got != 1 {
		t.Fatalf("after round0, conns = %d, want 1", got)
	}

	// ---- Round 1 on the warm socket (fresh session object, same key) ----
	s2 := f.Session("alice", "schedd1", sched.Addr(), nil)
	if err := s2.Begin(ctx, hdr("alice", "tag1")); err != nil {
		t.Fatalf("Begin round1: %v", err)
	}
	reqs2, err := s2.FetchRequests(ctx, 200)
	if err != nil {
		t.Fatalf("FetchRequests round1: %v", err)
	}
	if len(reqs2) != 1 || reqs2[0].Count != 2 {
		t.Fatalf("round1 requests = %+v, want one group of 2", reqs2)
	}
	sendMatches(ctx, t, s2, reqs2[0], "claimC", 2)
	if err := s2.End(ctx); err != nil {
		t.Fatalf("End round1: %v", err)
	}

	if got := sched.Conns(); got != 1 {
		t.Errorf("after round1, conns = %d, want 1 (warm reuse, no new dial)", got)
	}
	if err := sched.WaitRounds(ctx, 2); err != nil {
		t.Fatalf("WaitRounds(2): %v", err)
	}
	logs = sched.Logs()
	if len(logs) != 2 || len(logs[1].Matches) != 2 {
		t.Fatalf("round1 log = %+v, want 2 matches", logs[1])
	}
	wantAssign1 := [][2]int{{40, 0}, {40, 1}}
	for i, m := range logs[1].Matches {
		if m.AssignedCluster != wantAssign1[i][0] || m.AssignedProc != wantAssign1[i][1] {
			t.Errorf("round1 match[%d] assigned %d.%d, want %d.%d", i, m.AssignedCluster, m.AssignedProc, wantAssign1[i][0], wantAssign1[i][1])
		}
	}
}

// TestNoMoreJobsMidBatch verifies a SEND_RESOURCE_REQUEST_LIST(N) that runs off
// the schedd's cursor mid-batch returns the partial batch, and the next fetch is
// empty (NO_MORE_JOBS).
func TestNoMoreJobsMidBatch(t *testing.T) {
	ctx := testCtx(t)
	round0 := []negtest.Group{
		{RepCluster: 1, RepProc: 0, AutoClusterID: 1, Members: []negtest.Job{{Cluster: 1, Proc: 0}}},
		{RepCluster: 2, RepProc: 0, AutoClusterID: 2, Members: []negtest.Job{{Cluster: 2, Proc: 0}}},
	}
	sched, err := negtest.Start(ctx, [][]negtest.Group{round0})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	f := NewFactory(negtest.ClientSecurity())
	t.Cleanup(f.CloseAll)

	s := f.Session("bob", "schedd1", sched.Addr(), nil)
	if err := s.Begin(ctx, hdr("bob", "t")); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	reqs, err := s.FetchRequests(ctx, 200)
	if err != nil {
		t.Fatalf("FetchRequests: %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("first fetch = %d requests, want 2", len(reqs))
	}
	// Cursor exhausted (NO_MORE_JOBS already consumed): next fetch is empty.
	more, err := s.FetchRequests(ctx, 200)
	if err != nil {
		t.Fatalf("second FetchRequests: %v", err)
	}
	if len(more) != 0 {
		t.Fatalf("second fetch = %d requests, want 0 (NO_MORE_JOBS)", len(more))
	}
	if err := s.End(ctx); err != nil {
		t.Fatalf("End: %v", err)
	}
}

// TestLegacySendJobInfo verifies the pre-8.3.0 one-at-a-time SEND_JOB_INFO path.
func TestLegacySendJobInfo(t *testing.T) {
	ctx := testCtx(t)
	round0 := []negtest.Group{
		{RepCluster: 5, RepProc: 0, AutoClusterID: 5, Members: []negtest.Job{{Cluster: 5, Proc: 0}}},
		{RepCluster: 6, RepProc: 0, AutoClusterID: 6, Members: []negtest.Job{{Cluster: 6, Proc: 0}}},
	}
	sched, err := negtest.Start(ctx, [][]negtest.Group{round0})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	f := NewFactory(negtest.ClientSecurity(), WithLegacyFetch())
	t.Cleanup(f.CloseAll)

	s := f.Session("carol", "schedd1", sched.Addr(), nil)
	if err := s.Begin(ctx, hdr("carol", "t")); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	reqs, err := s.FetchRequests(ctx, 200)
	if err != nil {
		t.Fatalf("FetchRequests (legacy): %v", err)
	}
	if len(reqs) != 2 {
		t.Fatalf("legacy fetch = %d requests, want 2", len(reqs))
	}
	if reqs[0].Cluster != 5 || reqs[1].Cluster != 6 {
		t.Errorf("legacy requests clusters = %d,%d, want 5,6", reqs[0].Cluster, reqs[1].Cluster)
	}
	if err := s.End(ctx); err != nil {
		t.Fatalf("End: %v", err)
	}
}

// TestClaimSecrecy asserts the PERMISSION_AND_AD claim string (and the header's
// plaintext) never appear in cleartext on the wire when the session is
// AES-encrypted.
func TestClaimSecrecy(t *testing.T) {
	ctx := testCtx(t)
	const secret = "SECRET-CLAIM-ID-do-not-leak-42"
	round0 := []negtest.Group{
		{RepCluster: 7, RepProc: 0, AutoClusterID: 7, Members: []negtest.Job{{Cluster: 7, Proc: 0}}},
	}
	sched, err := negtest.Start(ctx, [][]negtest.Group{round0}, negtest.WithTap())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	f := NewFactory(negtest.ClientSecurity())
	t.Cleanup(f.CloseAll)

	s := f.Session("dave", "schedd1", sched.Addr(), nil)
	if err := s.Begin(ctx, hdr("dave-secret-owner", "tag-secret")); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	reqs, err := s.FetchRequests(ctx, 200)
	if err != nil {
		t.Fatalf("FetchRequests: %v", err)
	}
	if len(reqs) != 1 {
		t.Fatalf("fetch = %d, want 1", len(reqs))
	}
	mr := &negotiator.MatchResult{
		Request: reqs[0],
		SlotAd:  EnrichMatchAd(slotAd("slot0@host"), reqs[0], MatchContext{}),
		ClaimID: secret,
	}
	if err := s.SendMatch(ctx, mr); err != nil {
		t.Fatalf("SendMatch: %v", err)
	}
	if err := s.End(ctx); err != nil {
		t.Fatalf("End: %v", err)
	}

	wire := sched.WireBytes()
	if len(wire) == 0 {
		t.Fatal("wire tap captured nothing")
	}
	if bytes.Contains(wire, []byte(secret)) {
		t.Error("claim id appeared in cleartext on the wire")
	}
	// The header Owner is also cleartext-sensitive; if it leaks, encryption is off.
	if bytes.Contains(wire, []byte("dave-secret-owner")) {
		t.Error("header Owner appeared in cleartext; session is not encrypted")
	}
}

// TestCacheRedialOnDroppedWarmSocket verifies transparent re-dial when the far
// side dropped the kept-alive socket between cycles.
func TestCacheRedialOnDroppedWarmSocket(t *testing.T) {
	ctx := testCtx(t)
	round0 := []negtest.Group{{RepCluster: 1, RepProc: 0, AutoClusterID: 1, Members: []negtest.Job{{Cluster: 1, Proc: 0}}}}
	round1 := []negtest.Group{{RepCluster: 2, RepProc: 0, AutoClusterID: 2, Members: []negtest.Job{{Cluster: 2, Proc: 0}}}}
	sched, err := negtest.Start(ctx, [][]negtest.Group{round0, round1})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	sched.DropWarmAfter(1) // close (no keepalive) after the first round on a conn

	f := NewFactory(negtest.ClientSecurity())
	t.Cleanup(f.CloseAll)

	// Round 0 -> cached, then server drops the socket.
	s := f.Session("erin", "schedd1", sched.Addr(), nil)
	if err := s.Begin(ctx, hdr("erin", "t")); err != nil {
		t.Fatalf("Begin r0: %v", err)
	}
	if _, err := s.FetchRequests(ctx, 200); err != nil {
		t.Fatalf("Fetch r0: %v", err)
	}
	if err := s.End(ctx); err != nil {
		t.Fatalf("End r0: %v", err)
	}
	if got := sched.Conns(); got != 1 {
		t.Fatalf("after r0 conns = %d, want 1", got)
	}

	// Round 1: warm socket is dead; FetchRequests must transparently re-dial.
	s2 := f.Session("erin", "schedd1", sched.Addr(), nil)
	if err := s2.Begin(ctx, hdr("erin", "t")); err != nil {
		t.Fatalf("Begin r1: %v", err)
	}
	reqs, err := s2.FetchRequests(ctx, 200)
	if err != nil {
		t.Fatalf("Fetch r1 (should re-dial transparently): %v", err)
	}
	if len(reqs) != 1 || reqs[0].Cluster != 2 {
		t.Fatalf("r1 requests = %+v, want one group cluster 2", reqs)
	}
	if err := s2.End(ctx); err != nil {
		t.Fatalf("End r1: %v", err)
	}
	if got := sched.Conns(); got != 2 {
		t.Errorf("after re-dial conns = %d, want 2", got)
	}
}

// TestCloseDoesNotCache verifies the protocol-error path (Close) discards the
// socket instead of returning it to the warm cache, so the next round re-dials.
func TestCloseDoesNotCache(t *testing.T) {
	ctx := testCtx(t)
	round0 := []negtest.Group{{RepCluster: 1, RepProc: 0, AutoClusterID: 1, Members: []negtest.Job{{Cluster: 1, Proc: 0}}}}
	sched, err := negtest.Start(ctx, [][]negtest.Group{round0, {}, {}})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	f := NewFactory(negtest.ClientSecurity())
	t.Cleanup(f.CloseAll)

	// Round 0: normal End -> socket cached.
	s := f.Session("frank", "schedd1", sched.Addr(), nil)
	if err := s.Begin(ctx, hdr("frank", "t")); err != nil {
		t.Fatalf("Begin r0: %v", err)
	}
	if _, err := s.FetchRequests(ctx, 200); err != nil {
		t.Fatalf("Fetch r0: %v", err)
	}
	if err := s.End(ctx); err != nil {
		t.Fatalf("End r0: %v", err)
	}
	f.mu.Lock()
	cached := len(f.warm)
	f.mu.Unlock()
	if cached != 1 {
		t.Fatalf("after End, cached sockets = %d, want 1", cached)
	}

	// Round 1: reuse the warm socket, then Close (protocol-error path).
	s2 := f.Session("frank", "schedd1", sched.Addr(), nil)
	if err := s2.Begin(ctx, hdr("frank", "t")); err != nil {
		t.Fatalf("Begin r1: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f.mu.Lock()
	cached = len(f.warm)
	f.mu.Unlock()
	if cached != 0 {
		t.Errorf("after Close, cached sockets = %d, want 0 (invalidated)", cached)
	}

	// Round 2: cache empty -> Begin must re-dial.
	s3 := f.Session("frank", "schedd1", sched.Addr(), nil)
	if err := s3.Begin(ctx, hdr("frank", "t")); err != nil {
		t.Fatalf("Begin r2: %v", err)
	}
	if got := sched.Conns(); got != 2 {
		t.Errorf("conns = %d, want 2 (one for r0/r1 warm, one fresh dial after Close)", got)
	}
	_ = s3.Close()
}

// TestEnrichHelpers unit-tests the match/request enrichment helpers.
func TestEnrichHelpers(t *testing.T) {
	req := &negotiator.Request{Cluster: 12, Proc: 3, AutoClusterID: 99, Count: 4}
	slot := slotAd("slotX@host")
	out := EnrichMatchAd(slot, req, MatchContext{
		ConcurrencyLimits:      "licA,licB",
		RemoteGroup:            "physics",
		RemoteNegotiatingGroup: "physics",
		RemoteAutoregroup:      true,
		HasAutoregroup:         true,
	})
	// Input ad not mutated.
	if _, ok := slot.EvaluateAttrInt("ResourceRequestCluster"); ok {
		t.Error("EnrichMatchAd mutated the input slot ad")
	}
	for _, attr := range []string{"ResourceRequestCluster", "_condor_RESOURCE_CLUSTER"} {
		if v, ok := out.EvaluateAttrInt(attr); !ok || v != 12 {
			t.Errorf("%s = %d ok=%v, want 12", attr, v, ok)
		}
	}
	for _, attr := range []string{"ResourceRequestProc", "_condor_RESOURCE_PROC"} {
		if v, ok := out.EvaluateAttrInt(attr); !ok || v != 3 {
			t.Errorf("%s = %d ok=%v, want 3", attr, v, ok)
		}
	}
	if v, ok := out.EvaluateAttrBool("Requirements"); !ok || !v {
		t.Errorf("Requirements not restored from SavedRequirements")
	}
	if v, ok := out.EvaluateAttrString("MatchedConcurrencyLimits"); !ok || v != "licA,licB" {
		t.Errorf("MatchedConcurrencyLimits = %q ok=%v", v, ok)
	}
	if v, ok := out.EvaluateAttrString("RemoteGroup"); !ok || v != "physics" {
		t.Errorf("RemoteGroup = %q ok=%v", v, ok)
	}
	if v, ok := out.EvaluateAttrBool("RemoteAutoregroup"); !ok || !v {
		t.Errorf("RemoteAutoregroup = %v ok=%v", v, ok)
	}

	reqAd := classad.New()
	EnrichRequestAd(reqAd, SubmitterContext{
		UserPrio:           10.5,
		UserResourcesInUse: 2,
		Group:              "physics",
		GroupQuota:         100,
		NegotiatingGroup:   "physics",
		Autoregroup:        true,
	})
	if v, ok := reqAd.EvaluateAttrReal("SubmitterUserPrio"); !ok || v != 10.5 {
		t.Errorf("SubmitterUserPrio = %v ok=%v", v, ok)
	}
	if v, ok := reqAd.EvaluateAttrString("SubmitterGroup"); !ok || v != "physics" {
		t.Errorf("SubmitterGroup = %q ok=%v", v, ok)
	}
	if v, ok := reqAd.EvaluateAttrString("SubmitterNegotiatingGroup"); !ok || v != "physics" {
		t.Errorf("SubmitterNegotiatingGroup = %q ok=%v", v, ok)
	}
	if v, ok := reqAd.EvaluateAttrBool("SubmitterAutoregroup"); !ok || !v {
		t.Errorf("SubmitterAutoregroup = %v ok=%v", v, ok)
	}

	// Flat pool (no group): the negotiating-group context is STILL stamped
	// (C++ matchmaker.cpp:4257-4258), defaulting to the root group name; the
	// SubmitterGroup* family is not.
	flatAd := classad.New()
	EnrichRequestAd(flatAd, SubmitterContext{UserPrio: 1.0})
	if v, ok := flatAd.EvaluateAttrString("SubmitterNegotiatingGroup"); !ok || v != "<none>" {
		t.Errorf("flat SubmitterNegotiatingGroup = %q ok=%v, want \"<none>\"", v, ok)
	}
	if v, ok := flatAd.EvaluateAttrBool("SubmitterAutoregroup"); !ok || v {
		t.Errorf("flat SubmitterAutoregroup = %v ok=%v, want false", v, ok)
	}
	if _, ok := flatAd.Lookup("SubmitterGroup"); ok {
		t.Error("flat pool must not stamp SubmitterGroup")
	}
}
