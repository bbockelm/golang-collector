package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// serveCollector starts the collector server over the given backend on a loopback
// listener and returns its address plus a stop func.
func serveCollector(t *testing.T, st store.Backend) (string, func()) {
	t.Helper()
	srv := New(st, plaintextSec(), nil)
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
			go func() { _ = srv.ServeConn(ctx, conn) }()
		}
	}()
	return ln.Addr().String(), func() { cancel(); _ = ln.Close() }
}

// advertisePubPvtAndCheck advertises a startd public+private pair on one
// UPDATE_STARTD_AD message (as a startd sends and the C++ collector forwards to a
// CONDOR_VIEW_HOST) against addr, then asserts both the public ad (Startd) and the
// private ad (StartdPvt) came back.
func advertisePubPvtAndCheck(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 8*time.Second)
	defer cancel()

	col := htcondor.NewCollector(addr)
	pub := mustAd(t, `[MyType="Machine"; Name="slot1@h"; MyAddress="<1.2.3.4:5>"; Cpus=8]`)
	pvt := mustAd(t, `[MyType="Machine"; Name="slot1@h"; MyAddress="<1.2.3.4:5>"; Capability="CAP-XYZ"]`)
	if err := col.Advertise(ctx, pub, &htcondor.AdvertiseOptions{
		Command:   commands.UPDATE_STARTD_AD,
		PrivateAd: pvt,
	}); err != nil {
		t.Fatalf("advertise pub+pvt: %v", err)
	}

	q := mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements=true]`)
	if got := runQuery(t, addr, commands.QUERY_STARTD_ADS, q); len(got) != 1 {
		t.Fatalf("public query: got %d ads, want 1", len(got))
	}
	pvtAds := runQuery(t, addr, commands.QUERY_STARTD_PVT_ADS, q)
	if len(pvtAds) != 1 {
		t.Fatalf("private query: got %d ads, want 1 (the private ad was not stored on ingest)", len(pvtAds))
	}
	if cap, _ := pvtAds[0].EvaluateAttrString("Capability"); cap != "CAP-XYZ" {
		t.Errorf("private ad missing Capability: got %q", cap)
	}
}

// realisticClaimID is a claim id shaped exactly like a live startd's: a sinful
// with query params, the #time#pid# stamps, and a nested session ClassAd whose
// attribute values carry escaped quotes. This is the value shape whose mishandling
// desynced the wire read and broke the old-ClassAd parser in the field, so the
// collector's ingest -> store -> private-query path must round-trip it byte-exact.
const realisticClaimID = `<128.104.100.17:9618?addrs=128.104.100.17-9618&noUDP&sock=startd_11788_2a7e>#1783747566#19094#[Integrity="YES";Encryption="YES";CryptoMethods="AES";]409352b86b31e6daefdd45a87283a835ecdf67761744e547503010216396883a`

// advertisePvtWithClaimIDAndCheck advertises a startd public+private pair whose
// private ad carries a realistic ClaimId, then asserts the private ad round-trips
// through StartdPvt with the claim id intact (not dropped, truncated, or mangled).
func advertisePvtWithClaimIDAndCheck(t *testing.T, addr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 8*time.Second)
	defer cancel()

	col := htcondor.NewCollector(addr)
	pub := mustAd(t, `[MyType="Machine"; Name="slot1@h"; MyAddress="<1.2.3.4:5>"; Cpus=8]`)
	// New-ClassAd source: the inner quotes of the nested session ad are escaped.
	pvt := mustAd(t, `[MyType="Machine"; Name="slot1@h"; MyAddress="<1.2.3.4:5>"; ClaimId="`+
		`<128.104.100.17:9618?addrs=128.104.100.17-9618&noUDP&sock=startd_11788_2a7e>#1783747566#19094#`+
		`[Integrity=\"YES\";Encryption=\"YES\";CryptoMethods=\"AES\";]409352b86b31e6daefdd45a87283a835ecdf67761744e547503010216396883a`+
		`"]`)
	if err := col.Advertise(ctx, pub, &htcondor.AdvertiseOptions{
		Command:   commands.UPDATE_STARTD_AD,
		PrivateAd: pvt,
	}); err != nil {
		t.Fatalf("advertise pub+pvt: %v", err)
	}

	// A collector advertise is not acked, so the private ad appears after the
	// server processes the update -- poll rather than race the store.
	q := mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements=true]`)
	var pvtAds []*classad.ClassAd
	deadline := time.Now().Add(3 * time.Second)
	for {
		pvtAds = runQuery(t, addr, commands.QUERY_STARTD_PVT_ADS, q)
		if len(pvtAds) == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(pvtAds) != 1 {
		t.Fatalf("private query: got %d ads, want 1", len(pvtAds))
	}
	got, ok := pvtAds[0].EvaluateAttrString("ClaimId")
	if !ok {
		t.Fatal("private ad has no ClaimId after round-trip")
	}
	if got != realisticClaimID {
		t.Errorf("ClaimId corrupted on round-trip:\n got  %q\n want %q", got, realisticClaimID)
	}
}

// TestAdvertiseStartdStoresPublicAndPrivateMemory exercises the ingest path for
// the in-memory backend.
func TestAdvertiseStartdStoresPublicAndPrivateMemory(t *testing.T) {
	addr, stop := serveCollector(t, store.New())
	defer stop()
	advertisePubPvtAndCheck(t, addr)
}

// TestAdvertiseStartdClaimIDRoundTripMemory / ...DB guard that a realistic claim id
// (nested escaped quotes, the shape that broke the field parser) survives the
// collector's private-ad ingest -> store -> QUERY_STARTD_PVT_ADS path byte-exact.
func TestAdvertiseStartdClaimIDRoundTripMemory(t *testing.T) {
	addr, stop := serveCollector(t, store.New())
	defer stop()
	advertisePvtWithClaimIDAndCheck(t, addr)
}

func TestAdvertiseStartdClaimIDRoundTripDB(t *testing.T) {
	dbb, err := store.NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buf, err := store.NewBufferedBackend(dbb, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, stop := serveCollector(t, buf)
	defer stop()
	advertisePvtWithClaimIDAndCheck(t, addr)
}

// TestAdvertiseStartdStoresPublicAndPrivateDB exercises the same ingest path for
// the db backend (buffered over an embedded catalog) -- the shape a view collector
// backed by htcondordb runs, where StartdPvt was observed empty.
func TestAdvertiseStartdStoresPublicAndPrivateDB(t *testing.T) {
	dbb, err := store.NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	buf, err := store.NewBufferedBackend(dbb, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	addr, stop := serveCollector(t, buf)
	defer stop()
	advertisePubPvtAndCheck(t, addr)
}
