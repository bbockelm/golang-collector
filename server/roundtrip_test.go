package server

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// plaintextSec is an un-authenticated, un-encrypted security policy so the
// in-process handshake completes without credentials.
func plaintextSec() *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{},
		Authentication: security.SecurityNever,
		Encryption:     security.SecurityNever,
		Integrity:      security.SecurityNever,
	}
}

func mustAd(tb testing.TB, s string) *classad.ClassAd {
	tb.Helper()
	ad, err := classad.Parse(s)
	if err != nil {
		tb.Fatalf("parse %q: %v", s, err)
	}
	return ad
}

// mustLen returns st.Len, failing the test on error.
func mustLen(tb testing.TB, st *store.Store, at store.AdType) int {
	tb.Helper()
	n, err := st.Len(context.Background(), at)
	if err != nil {
		tb.Fatalf("Len(%v): %v", at, err)
	}
	return n
}

// startCollector stands up the collector server on a random localhost port.
func startCollector(t *testing.T) (*store.Store, string, func()) {
	return startCollectorFwd(t, nil)
}

// startCollectorFwd is startCollector with an optional view-forwarder attached.
func startCollectorFwd(t *testing.T, fwd *Forwarder) (*store.Store, string, func()) {
	t.Helper()
	st := store.New()
	srv := New(st, plaintextSec(), fwd)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				// context.Canceled is the expected shutdown path (stop() cancels ctx);
				// don't log it -- and never log after stop() returns, or the test may
				// have completed, turning t.Logf into a panic.
				if err := srv.ServeConn(ctx, conn); err != nil && ctx.Err() == nil {
					t.Logf("SERVECONN ERR: %v", err)
				}
			}()
		}
	}()
	// stop() cancels the server context, closes the listener, then waits for the
	// accept loop and all in-flight connection goroutines to exit -- so nothing logs
	// against the test after it returns.
	return st, ln.Addr().String(), func() { cancel(); ln.Close(); wg.Wait() }
}

// TestUpdateWithAck verifies UPDATE_STARTD_AD_WITH_ACK: the client blocks for
// the collector's acknowledgment, so Advertise(WithAck) returning nil means the
// ad was stored and acked.
func TestUpdateWithAck(t *testing.T) {
	_, addr, stop := startCollector(t)
	defer stop()
	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 8*time.Second)
	defer cancel()

	col := htcondor.NewCollector(addr)
	ad := mustAd(t, `[MyType="Machine"; Name="ack@host"; MyAddress="<1.2.3.4:5>"; Cpus=8]`)
	if err := col.Advertise(ctx, ad, &htcondor.AdvertiseOptions{
		Command: commands.UPDATE_STARTD_AD_WITH_ACK, WithAck: true,
	}); err != nil {
		t.Fatalf("advertise with ack: %v", err)
	}
	got, err := col.QueryAds(ctx, "Machine", `Name == "ack@host"`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("after acked update got %d ads, want 1", len(got))
	}
}

// TestStartdPrivateAd verifies that a startd's second (private) ad on an
// UPDATE_STARTD_AD connection is stored in the StartdPvt table keyed like the
// public ad -- and does not leak into the public table. The htcondor client does
// not send private ads, so drive the two-ad wire form with the low-level client.
func TestStartdPrivateAd(t *testing.T) {
	st, addr, stop := startCollector(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	sec := plaintextSec()
	sec.Command = commands.UPDATE_STARTD_AD
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	stream := cl.GetStream()

	// Public ad and private ad in ONE message under a single end_of_message,
	// exactly as the C++ startd's finishUpdate frames them.
	m := message.NewMessageForStream(stream)
	if err := m.PutClassAd(ctx, mustAd(t, `[MyType="Machine"; Name="slot1@a"; MyAddress="<1.2.3.4:5>"; Cpus=8]`)); err != nil {
		t.Fatalf("put public ad: %v", err)
	}
	// The startd's raw private ad carries only its claim secret -- no identifying
	// Name/MyAddress. The collector must enrich it from the public ad so the
	// negotiator can correlate it. The startd deliberately sends its secret here, so
	// opt in past the redact-by-default serialization (PutClassAdIncludePrivate).
	if err := m.PutClassAdWithOptions(ctx, mustAd(t, `[MyType="Machine"; ClaimId="secret-claim-123"]`),
		&message.PutClassAdConfig{Options: message.PutClassAdIncludePrivate}); err != nil {
		t.Fatalf("put private ad: %v", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatalf("finish: %v", err)
	}
	cl.Close()

	// Give the server a moment to process both frames.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mustLen(t, st, store.StartdPvtAd) == 0 {
		time.Sleep(20 * time.Millisecond)
	}

	if got := mustLen(t, st, store.StartdAd); got != 1 {
		t.Fatalf("public table has %d ads, want 1", got)
	}
	if got := mustLen(t, st, store.StartdPvtAd); got != 1 {
		t.Fatalf("private table has %d ads, want 1", got)
	}
	keyAd := mustAd(t, `[Name="slot1@a"; MyAddress="<1.2.3.4:5>"]`)
	pub, ok := st.Get(context.Background(), store.StartdAd, keyAd)
	if !ok {
		t.Fatal("public ad not found")
	}
	if _, leaked := pub.EvaluateAttrString("ClaimId"); leaked {
		t.Error("public startd ad leaked the private ClaimId")
	}
	pvt, ok := st.Get(context.Background(), store.StartdPvtAd, keyAd)
	if !ok {
		t.Fatal("private ad not found under the public ad's key")
	}
	if cid, _ := pvt.EvaluateAttrString("ClaimId"); cid != "secret-claim-123" {
		t.Errorf("private ad ClaimId = %q, want secret-claim-123", cid)
	}

	// And it must come back over the wire via QUERY_STARTD_PVT_ADS -- the command
	// the negotiator uses to fetch claim capabilities. This is exactly the query
	// that returned "0 private" in the pool, so assert it returns the ad here.
	qsec := plaintextSec()
	qsec.Command = commands.QUERY_STARTD_PVT_ADS
	qcl, err := client.ConnectAndAuthenticate(ctx, addr, qsec)
	if err != nil {
		t.Fatalf("connect for private query: %v", err)
	}
	qs := qcl.GetStream()
	qm := message.NewMessageForStream(qs)
	if err := qm.PutClassAd(ctx, mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements = true]`)); err != nil {
		t.Fatalf("put query ad: %v", err)
	}
	if err := qm.FinishMessage(ctx); err != nil {
		t.Fatalf("finish query: %v", err)
	}
	rm := message.NewMessageFromStream(qs)
	var results []*classad.ClassAd
	for {
		more, err := rm.GetInt(ctx)
		if err != nil {
			t.Fatalf("read result marker: %v", err)
		}
		if more == 0 {
			break
		}
		ad, err := rm.GetClassAd(ctx)
		if err != nil {
			t.Fatalf("read result ad: %v", err)
		}
		results = append(results, ad)
	}
	qcl.Close()
	if len(results) != 1 {
		t.Fatalf("QUERY_STARTD_PVT_ADS returned %d ads, want 1", len(results))
	}
	// The returned private ad must carry the claim secret AND the Name/MyAddress
	// copied from the public ad -- the (Name, MyAddress) key the negotiator uses
	// to correlate it back to the public slot ad (MakeClaimIdHash).
	got := results[0]
	if cid, _ := got.EvaluateAttrString("ClaimId"); cid != "secret-claim-123" {
		t.Errorf("queried private ad ClaimId = %q, want secret-claim-123", cid)
	}
	if name, _ := got.EvaluateAttrString("Name"); name != "slot1@a" {
		t.Errorf("queried private ad Name = %q, want slot1@a (copied from public ad)", name)
	}
	if _, ok := got.EvaluateAttrString("MyAddress"); !ok {
		t.Error("queried private ad missing MyAddress (should be copied from public ad)")
	}
}

// TestViewForwarding verifies CONDOR_VIEW_HOST forwarding: an update advertised
// to the primary collector is relayed to the view collector, and an invalidation
// is relayed too. Forwarding is asynchronous, so we poll the view collector.
func TestViewForwarding(t *testing.T) {
	// View collector (the forwarding target).
	viewStore, viewAddr, stopView := startCollector(t)
	defer stopView()

	// Primary collector forwarding to the view collector.
	fwd := NewForwarder([]string{viewAddr}, plaintextSec())
	_, primaryAddr, stopPrimary := startCollectorFwd(t, fwd)
	defer stopPrimary()

	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 10*time.Second)
	defer cancel()

	col := htcondor.NewCollector(primaryAddr)
	ad := mustAd(t, `[MyType="Machine"; Name="fwd@host"; MyAddress="<1.2.3.4:5>"; Cpus=8]`)
	if err := col.Advertise(ctx, ad, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
		t.Fatalf("advertise to primary: %v", err)
	}

	// The ad should propagate to the view collector.
	if !waitFor(2*time.Second, func() bool { return mustLen(t, viewStore, store.StartdAd) == 1 }) {
		t.Fatalf("update not forwarded to view collector (view has %d ads)", mustLen(t, viewStore, store.StartdAd))
	}

	// Now invalidate on the primary; the view collector should drop it too.
	inv := mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements = Name == "fwd@host"]`)
	if err := col.Advertise(ctx, inv, &htcondor.AdvertiseOptions{Command: commands.INVALIDATE_STARTD_ADS}); err != nil {
		t.Fatalf("invalidate on primary: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return mustLen(t, viewStore, store.StartdAd) == 0 }) {
		t.Fatalf("invalidation not forwarded to view collector (view still has %d ads)", mustLen(t, viewStore, store.StartdAd))
	}
}

// waitFor polls cond until it is true or the timeout elapses.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// TestRoundTripAdvertiseQuery drives the real htcondor.Collector client against
// the Go collector: advertise startd ads, then query them back (all, by
// constraint, with projection) and invalidate one -- proving the wire protocol
// end to end.
func TestRoundTripAdvertiseQuery(t *testing.T) {
	_, addr, stop := startCollector(t)
	defer stop()

	// Inject the plaintext policy into the context so the client negotiates
	// no-auth with our plaintext server (no HTCondor config needed).
	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 10*time.Second)
	defer cancel()

	col := htcondor.NewCollector(addr)

	ads := []*classad.ClassAd{
		mustAd(t, `[MyType="Machine"; Name="slot1@a"; State="Unclaimed"; Cpus=8; MyAddress="<1.2.3.4:5>"]`),
		mustAd(t, `[MyType="Machine"; Name="slot1@b"; State="Claimed"; Cpus=4; MyAddress="<1.2.3.4:6>"]`),
	}
	for _, ad := range ads {
		if err := col.Advertise(ctx, ad, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
			t.Fatalf("advertise: %v", err)
		}
	}

	// Query all. UPDATE_STARTD_AD is fire-and-forget (no ack), so the server may
	// still be committing the second ad when the query races in; poll until both
	// are visible.
	var got []*classad.ClassAd
	var err error
	if !waitFor(5*time.Second, func() bool {
		got, err = col.QueryAds(ctx, "Machine", "")
		return err == nil && len(got) == 2
	}) {
		t.Fatalf("query all returned %d ads (err=%v), want 2", len(got), err)
	}

	// Query by constraint.
	got, err = col.QueryAds(ctx, "Machine", "Cpus > 5")
	if err != nil {
		t.Fatalf("query constraint: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Cpus>5 returned %d ads, want 1", len(got))
	}
	if name, _ := got[0].EvaluateAttrString("Name"); name != "slot1@a" {
		t.Errorf("Cpus>5 matched %q, want slot1@a", name)
	}

	// Projection: ask for Name only.
	got, err = col.QueryAdsWithProjection(ctx, "Machine", "Cpus > 5", []string{"Name"})
	if err != nil {
		t.Fatalf("projection query: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("projection returned %d ads, want 1", len(got))
	}
	if _, ok := got[0].EvaluateAttrString("State"); ok {
		t.Error("projection to [Name] leaked State")
	}
	if name, _ := got[0].EvaluateAttrString("Name"); name != "slot1@a" {
		t.Errorf("projected Name=%q, want slot1@a", name)
	}

	// Invalidate the Claimed one, then confirm it's gone.
	inv := mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements = State == "Claimed"]`)
	if err := col.Advertise(ctx, inv, &htcondor.AdvertiseOptions{Command: commands.INVALIDATE_STARTD_ADS}); err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	// INVALIDATE_STARTD_ADS is fire-and-forget (no ack), like UPDATE above, so the
	// server may not have applied the invalidation when the query races in; poll
	// until only the survivor remains.
	if !waitFor(5*time.Second, func() bool {
		got, err = col.QueryAds(ctx, "Machine", "")
		return err == nil && len(got) == 1
	}) {
		t.Fatalf("after invalidate returned %d ads (err=%v), want 1", len(got), err)
	}
	if name, _ := got[0].EvaluateAttrString("Name"); name != "slot1@a" {
		t.Errorf("survivor is %q, want slot1@a", name)
	}
}
