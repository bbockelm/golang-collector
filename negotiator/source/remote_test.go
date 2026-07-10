package source

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/security"

	"github.com/bbockelm/golang-collector/server"
	"github.com/bbockelm/golang-collector/store"
)

func plaintextSec() *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{},
		Authentication: security.SecurityNever,
		Encryption:     security.SecurityNever,
		Integrity:      security.SecurityNever,
	}
}

// startTestCollector stands up the real collector server over the given store
// on a random localhost port and returns its address plus a stop func.
//
// Each accepted connection gets its OWN server instance (a fresh SecurityConfig)
// sharing the one store. The cedar server mutates its SecurityConfig in place
// during the handshake (security/auth.go stores the peer's ECDH public key on
// the shared config), so a single server instance handling the negotiator's
// three concurrent connections trips the race detector -- an in-process-only
// artifact (a real collector and negotiator are separate processes). Per-conn
// server instances isolate that per-connection state; the store is
// concurrency-safe and stays shared.
func startTestCollector(t *testing.T, st *store.Store) (string, func()) {
	t.Helper()
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
			srv := server.New(st, plaintextSec(), nil)
			go func() { _ = srv.ServeConn(ctx, conn) }()
		}
	}()
	return ln.Addr().String(), func() { cancel(); ln.Close() }
}

// seedRemotePool seeds a store with two slots (one 8-cpu with a private claim,
// one 2-cpu), plus alice (idle jobs) and bob (no jobs).
func seedRemotePool(t *testing.T, st *store.Store) {
	t.Helper()
	seed := func(tbl store.AdType, s string) {
		if err := st.Update(tbl, mustOld(t, s)); err != nil {
			t.Fatal(err)
		}
	}
	seed(store.StartdAd, `MyType="Machine"
Name="slot1@big"
Machine="big.pool.test"
StartdIpAddr="<10.0.0.11:9618>"
MyAddress="<10.0.0.11:9618>"
Cpus=8
Memory=16384
State="Unclaimed"
Requirements=true`)
	seed(store.StartdAd, `MyType="Machine"
Name="slot1@small"
Machine="small.pool.test"
StartdIpAddr="<10.0.0.12:9618>"
MyAddress="<10.0.0.12:9618>"
Cpus=2
Memory=2048
State="Unclaimed"
Requirements=true`)
	seed(store.StartdPvtAd, `MyType="Machine"
Name="slot1@big"
MyAddress="<10.0.0.11:9618>"
ClaimId="remote-claim-big"`)
	seed(store.SubmitterAd, `MyType="Submitter"
Name="alice@pool.test"
ScheddName="ap1.pool.test"
ScheddIpAddr="<10.0.0.21:9618>"
IdleJobs=5
RunningJobs=0`)
	seed(store.SubmitterAd, `MyType="Submitter"
Name="bob@pool.test"
ScheddName="ap1.pool.test"
ScheddIpAddr="<10.0.0.21:9618>"
IdleJobs=0
RunningJobs=0`)
}

func TestRemoteSnapshot_ThreeWayGather(t *testing.T) {
	st := store.New()
	seedRemotePool(t, st)
	addr, stop := startTestCollector(t, st)
	defer stop()

	src, err := NewRemote(Config{CollectorAddr: addr, Security: plaintextSec()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	snap, err := src.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	if len(snap.Slots) != 2 {
		t.Errorf("slots = %d, want 2", len(snap.Slots))
	}
	// bob has no jobs -> dropped by the submitter filter.
	if len(snap.Submitters) != 1 {
		t.Fatalf("submitters = %d, want 1 (alice)", len(snap.Submitters))
	}
	if name, _ := snap.Submitters[0].EvaluateAttrString("Name"); name != "alice@pool.test" {
		t.Errorf("submitter = %q, want alice@pool.test", name)
	}

	// Private-ad claim map keyed by ClaimKey of the public slot.
	if len(snap.ClaimIDs) != 1 {
		t.Fatalf("claim ids = %d, want 1", len(snap.ClaimIDs))
	}
	var bigSlot = snap.Slots[0]
	for _, s := range snap.Slots {
		if n, _ := s.EvaluateAttrString("Name"); n == "slot1@big" {
			bigSlot = s
		}
	}
	if got := snap.ClaimIDs[ClaimKey(bigSlot)]; got != "remote-claim-big" {
		t.Errorf("claim[%q] = %q, want remote-claim-big", ClaimKey(bigSlot), got)
	}

	// Slot fixups applied over the wire path too.
	for _, s := range snap.Slots {
		if v, ok := s.EvaluateAttrInt("MachineMatchCount"); !ok || v != 0 {
			t.Errorf("MachineMatchCount = %d, want 0", v)
		}
		if _, ok := s.EvaluateAttrNumber("SlotWeight"); !ok {
			t.Error("SlotWeight not defaulted/evaluable")
		}
		if _, leaked := s.EvaluateAttrString("ClaimId"); leaked {
			t.Error("public slot ad leaked ClaimId over the wire")
		}
	}
}

func TestRemoteSnapshot_ConstraintPushdown(t *testing.T) {
	st := store.New()
	seedRemotePool(t, st)
	addr, stop := startTestCollector(t, st)
	defer stop()

	src, err := NewRemote(Config{
		CollectorAddr:       addr,
		Security:            plaintextSec(),
		SlotConstraint:      "Cpus >= 8",
		SubmitterConstraint: "IdleJobs >= 5",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	snap, err := src.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Slot constraint pushed to the collector: only the 8-cpu slot returns,
	// and the private-ad query honors the same constraint (still one claim).
	if len(snap.Slots) != 1 {
		t.Fatalf("slots = %d, want 1 (Cpus>=8 pushdown)", len(snap.Slots))
	}
	if name, _ := snap.Slots[0].EvaluateAttrString("Name"); name != "slot1@big" {
		t.Errorf("slot = %q, want slot1@big", name)
	}
	if len(snap.Submitters) != 1 {
		t.Fatalf("submitters = %d, want 1 (IdleJobs>=5 pushdown)", len(snap.Submitters))
	}
	if len(snap.ClaimIDs) != 1 {
		t.Errorf("claim ids = %d, want 1", len(snap.ClaimIDs))
	}
}

func TestRemotePublish(t *testing.T) {
	st := store.New()
	addr, stop := startTestCollector(t, st)
	defer stop()

	src, err := NewRemote(Config{CollectorAddr: addr, Security: plaintextSec()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	neg := mustOld(t, `MyType="Negotiator"
Name="neg@pool.test"
MyAddress="<10.0.0.1:9618>"`)
	if err := src.PublishNegotiatorAd(ctx, neg); err != nil {
		t.Fatalf("publish negotiator ad: %v", err)
	}
	if !waitForLen(st, store.NegotiatorAd, 1) {
		t.Errorf("negotiator table = %d, want 1", st.Len(store.NegotiatorAd))
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
		t.Fatalf("publish accounting ads: %v", err)
	}
	if !waitForLen(st, store.AccountingAd, 2) {
		t.Errorf("accounting table = %d, want 2", st.Len(store.AccountingAd))
	}
}

func waitForLen(st *store.Store, t store.AdType, want int) bool {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st.Len(t) == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return st.Len(t) == want
}
