package server

import (
	"context"
	"net"
	"testing"
	"time"

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

// TestAdvertiseStartdStoresPublicAndPrivateMemory exercises the ingest path for
// the in-memory backend.
func TestAdvertiseStartdStoresPublicAndPrivateMemory(t *testing.T) {
	addr, stop := serveCollector(t, store.New())
	defer stop()
	advertisePubPvtAndCheck(t, addr)
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
