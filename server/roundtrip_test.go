package server

import (
	"context"
	"net"
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
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				if err := srv.ServeConn(ctx, conn); err != nil {
					t.Logf("SERVECONN ERR: %v", err)
				}
			}()
		}
	}()
	return st, ln.Addr().String(), func() { cancel(); ln.Close() }
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

	// public ad, then private ad, on the same connection.
	for _, ad := range []*classad.ClassAd{
		mustAd(t, `[MyType="Machine"; Name="slot1@a"; MyAddress="<1.2.3.4:5>"; Cpus=8]`),
		mustAd(t, `[MyType="Machine"; Name="slot1@a"; MyAddress="<1.2.3.4:5>"; ClaimId="secret-claim-123"]`),
	} {
		m := message.NewMessageForStream(stream)
		if err := m.PutClassAd(ctx, ad); err != nil {
			t.Fatalf("put ad: %v", err)
		}
		if err := m.FinishMessage(ctx); err != nil {
			t.Fatalf("finish: %v", err)
		}
	}
	cl.Close()

	// Give the server a moment to process both frames.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && st.Len(store.StartdPvtAd) == 0 {
		time.Sleep(20 * time.Millisecond)
	}

	if got := st.Len(store.StartdAd); got != 1 {
		t.Fatalf("public table has %d ads, want 1", got)
	}
	if got := st.Len(store.StartdPvtAd); got != 1 {
		t.Fatalf("private table has %d ads, want 1", got)
	}
	keyAd := mustAd(t, `[Name="slot1@a"; MyAddress="<1.2.3.4:5>"]`)
	pub, ok := st.Get(store.StartdAd, keyAd)
	if !ok {
		t.Fatal("public ad not found")
	}
	if _, leaked := pub.EvaluateAttrString("ClaimId"); leaked {
		t.Error("public startd ad leaked the private ClaimId")
	}
	pvt, ok := st.Get(store.StartdPvtAd, keyAd)
	if !ok {
		t.Fatal("private ad not found under the public ad's key")
	}
	if cid, _ := pvt.EvaluateAttrString("ClaimId"); cid != "secret-claim-123" {
		t.Errorf("private ad ClaimId = %q, want secret-claim-123", cid)
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
	if !waitFor(2*time.Second, func() bool { return viewStore.Len(store.StartdAd) == 1 }) {
		t.Fatalf("update not forwarded to view collector (view has %d ads)", viewStore.Len(store.StartdAd))
	}

	// Now invalidate on the primary; the view collector should drop it too.
	inv := mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements = Name == "fwd@host"]`)
	if err := col.Advertise(ctx, inv, &htcondor.AdvertiseOptions{Command: commands.INVALIDATE_STARTD_ADS}); err != nil {
		t.Fatalf("invalidate on primary: %v", err)
	}
	if !waitFor(2*time.Second, func() bool { return viewStore.Len(store.StartdAd) == 0 }) {
		t.Fatalf("invalidation not forwarded to view collector (view still has %d ads)", viewStore.Len(store.StartdAd))
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

	// Query all.
	got, err := col.QueryAds(ctx, "Machine", "")
	if err != nil {
		t.Fatalf("query all: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("query all returned %d ads, want 2", len(got))
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
	got, err = col.QueryAds(ctx, "Machine", "")
	if err != nil {
		t.Fatalf("query after invalidate: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("after invalidate returned %d ads, want 1", len(got))
	}
	if name, _ := got[0].EvaluateAttrString("Name"); name != "slot1@a" {
		t.Errorf("survivor is %q, want slot1@a", name)
	}
}
