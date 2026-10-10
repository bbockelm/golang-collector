package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/stream"

	"github.com/bbockelm/golang-collector/store"
)

// TestInvalidateKeepsConnectionOpen is a regression test for a persistent
// forwarding socket that dropped a few ads after an INVALIDATE.
//
// A C++ collector forwards UPDATE_* and INVALIDATE_* interleaved down ONE
// persistent command socket (its view-collector feed). The invalidate handler
// must (a) keep the socket alive and (b) read the ad from the follow-on message
// frame that already carried the command int. If it closes the socket (no
// KeepAlive) or reads a fresh frame (desyncing the stream), the UPDATE that
// follows the invalidate is lost and the sender sees "condor_write(): Socket
// closed" on a later buffered write.
//
// The test drives exactly that sequence on one authenticated connection and
// asserts the post-invalidate UPDATE reaches the store.
func TestInvalidateKeepsConnectionOpen(t *testing.T) {
	st, addr, stop := startCollector(t)
	defer stop()

	ctx := context.Background()
	sec := plaintextSec()
	sec.Command = commands.UPDATE_STARTD_AD // first command is carried by the handshake
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect+authenticate: %v", err)
	}
	defer func() { _ = cl.Close() }()
	strm := cl.GetStream()

	adA := benchAd(1) // stored via the first (handshake) command, then invalidated
	adC := benchAd(3) // stored AFTER the invalidate -- the ad that vanishes on the bug

	// First command's ad: a fresh message with no command-int prefix (the command
	// was carried by the DC_AUTHENTICATE handshake), matching the C++ reused
	// update socket and the golang-htcondor client's AdvertiseMultiple.
	first := message.NewMessageForStream(strm)
	if err := first.PutClassAd(ctx, adA); err != nil {
		t.Fatalf("send adA: %v", err)
	}
	if err := first.FinishMessage(ctx); err != nil {
		t.Fatalf("finish adA: %v", err)
	}

	// Follow-on INVALIDATE targeting adA, then a follow-on UPDATE of adC -- both on
	// the same socket, each framed as [cmd int][ad][end_of_message].
	sendFollowOn(t, ctx, strm, commands.INVALIDATE_STARTD_ADS, adA)
	sendFollowOn(t, ctx, strm, commands.UPDATE_STARTD_AD, adC)

	// The connection must have stayed open and aligned: adA invalidated, adC stored.
	// Check the SPECIFIC ads, not just the count -- a bare count of 1 can't tell the
	// correct {adC} from the buggy {adA} (invalidate dropped and adC lost, but adA
	// survived), which both total 1.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, haveA := st.Get(ctx, store.StartdAd, adA)
		_, haveC := st.Get(ctx, store.StartdAd, adC)
		if !haveA && haveC {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("persistent socket after INVALIDATE: haveA=%v haveC=%v (want adA gone, adC present). "+
				"adC missing => the follow-on UPDATE was dropped (socket closed after the invalidate); "+
				"adA present => the invalidate desynced the follow-on frame.", haveA, haveC)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// sendFollowOn writes one follow-on command on a kept-alive collector socket:
// the command integer, the ad, then one end_of_message -- the framing the
// server's keep-alive read loop expects.
func sendFollowOn(t *testing.T, ctx context.Context, strm *stream.Stream, cmd int, ad *classad.ClassAd) {
	t.Helper()
	msg := message.NewMessageForStream(strm)
	if err := msg.PutInt(ctx, cmd); err != nil {
		t.Fatalf("put cmd %d: %v", cmd, err)
	}
	if err := msg.PutClassAd(ctx, ad); err != nil {
		t.Fatalf("put ad for cmd %d: %v", cmd, err)
	}
	if err := msg.FinishMessage(ctx); err != nil {
		t.Fatalf("finish cmd %d: %v", cmd, err)
	}
}

// TestAckUpdateKeepsConnectionOpen is the WITH_ACK analogue of
// TestInvalidateKeepsConnectionOpen: a startd may stream several
// UPDATE_STARTD_AD_WITH_ACK updates down one persistent socket, reading the ack
// between each. ackUpdateHandler must keep the socket open and read the follow-on
// frame; otherwise the socket closes after the first ack and the second update's
// ack read fails / its ad is lost.
func TestAckUpdateKeepsConnectionOpen(t *testing.T) {
	st, addr, stop := startCollector(t)
	defer stop()

	ctx := context.Background()
	sec := plaintextSec()
	sec.Command = commands.UPDATE_STARTD_AD_WITH_ACK // first command carried by the handshake
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect+authenticate: %v", err)
	}
	defer func() { _ = cl.Close() }()
	strm := cl.GetStream()

	adA := benchAd(5) // acked via the first (handshake) command
	adB := benchAd(7) // acked as a follow-on -- lost on the bug

	sendAckUpdate(t, ctx, strm, adA, true)
	sendAckUpdate(t, ctx, strm, adB, false)

	// Both durable updates must be stored (DurableUpdate commits before the ack).
	// If the socket closed after adA's ack or the follow-on frame desynced, adB's
	// ack read above already fails; this confirms both ads actually landed.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, haveA := st.Get(ctx, store.StartdAd, adA)
		_, haveB := st.Get(ctx, store.StartdAd, adB)
		if haveA && haveB {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("WITH_ACK on a persistent socket: haveA=%v haveB=%v (want both stored). "+
				"The follow-on WITH_ACK update was lost -- the socket closed after the first ack "+
				"or the follow-on frame desynced.", haveA, haveB)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// sendAckUpdate sends one UPDATE_STARTD_AD_WITH_ACK on a persistent socket and
// waits for the collector's one-int acknowledgment. firstOnConn omits the
// command-int prefix (the command was carried by the handshake).
func sendAckUpdate(t *testing.T, ctx context.Context, strm *stream.Stream, ad *classad.ClassAd, firstOnConn bool) {
	t.Helper()
	msg := message.NewMessageForStream(strm)
	if !firstOnConn {
		if err := msg.PutInt(ctx, commands.UPDATE_STARTD_AD_WITH_ACK); err != nil {
			t.Fatalf("put WITH_ACK cmd: %v", err)
		}
	}
	if err := msg.PutClassAd(ctx, ad); err != nil {
		t.Fatalf("put ad: %v", err)
	}
	if err := msg.FinishMessage(ctx); err != nil {
		t.Fatalf("finish ad: %v", err)
	}
	resp := message.NewMessageFromStream(strm)
	ok, err := resp.GetInt32(ctx)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ok != 1 {
		t.Fatalf("ack = %d, want 1", ok)
	}
}

// TestAckUpdatePrivateAd: a startd sends its private ad after the public one
// on UPDATE_STARTD_AD_WITH_ACK too. As in the C++ collector
// (receive_update_expect_ack), the public ad is stored and the private ad is
// not -- the private table keeps what the last UPDATE_STARTD_AD stored -- and
// the private ad is consumed, so a follow-on update on the same connection is
// read intact even when the private ad spans more than one frame.
func TestAckUpdatePrivateAd(t *testing.T) {
	st, addr, stop := startCollector(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const pub = `[MyType="Machine"; Name="slot1@ack"; MyAddress="<1.2.3.4:5>"; Cpus=%d]`
	key := mustAd(t, fmt.Sprintf(pub, 8))
	sec := plaintextSec()
	sec.Command = commands.UPDATE_STARTD_AD
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	m := message.NewMessageForStream(cl.GetStream())
	for _, ad := range []string{fmt.Sprintf(pub, 8), `[MyType="Machine"; Name="slot1@ack"; MyAddress="<1.2.3.4:5>"; PvtTag="OLD"]`} {
		if err := m.PutClassAd(ctx, mustAd(t, ad)); err != nil {
			t.Fatalf("put ad: %v", err)
		}
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatalf("finish: %v", err)
	}
	_ = cl.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := st.Get(ctx, store.StartdPvtAd, key); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("UPDATE_STARTD_AD did not store the private ad")
		}
		time.Sleep(10 * time.Millisecond)
	}

	sec = plaintextSec()
	sec.Command = commands.UPDATE_STARTD_AD_WITH_ACK
	cl, err = client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = cl.Close() }()
	strm := cl.GetStream()
	m = message.NewMessageForStream(strm)
	if err := m.PutClassAd(ctx, mustAd(t, fmt.Sprintf(pub, 16))); err != nil {
		t.Fatalf("put public ad: %v", err)
	}
	// Larger than a frame, so it does not end with the public ad's frame.
	pvt := fmt.Sprintf(`[MyType="Machine"; Name="slot1@ack"; MyAddress="<1.2.3.4:5>"; PvtTag="NEW"; Filler=%q]`,
		strings.Repeat("x", 3*message.TargetFrameSize))
	if err := m.PutClassAd(ctx, mustAd(t, pvt)); err != nil {
		t.Fatalf("put private ad: %v", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatalf("finish: %v", err)
	}
	ack, err := message.NewMessageFromStream(strm).GetInt32(ctx)
	if err != nil || ack != 1 {
		t.Fatalf("ack = %d, %v; want 1", ack, err)
	}
	next := benchAd(3)
	sendAckUpdate(t, ctx, strm, next, false)

	got, ok := st.Get(ctx, store.StartdAd, key)
	if !ok {
		t.Fatal("WITH_ACK public ad not stored")
	}
	if cpus, _ := got.EvaluateAttrInt("Cpus"); cpus != 16 {
		t.Errorf("public ad Cpus = %d, want 16 (the WITH_ACK update)", cpus)
	}
	if _, ok := st.Get(ctx, store.StartdAd, next); !ok {
		t.Error("the follow-on WITH_ACK update was not stored")
	}
	p, ok := st.Get(ctx, store.StartdPvtAd, key)
	if !ok {
		t.Fatal("WITH_ACK removed the private ad UPDATE_STARTD_AD stored")
	}
	if tag, _ := p.EvaluateAttrString("PvtTag"); tag != "OLD" {
		t.Errorf("private ad PvtTag = %q, want OLD (WITH_ACK does not store its private ad)", tag)
	}
}
