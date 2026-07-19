package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// TestQueryProjectionPushdownRPC drives a projected QUERY_STARTD_ADS against a
// collector backed by a remote database (RPCBackend). The handler must take the
// projected wire-form fast path (ProjectedRawQueryer), pushing the projection to
// the db so only the requested attributes come back.
func TestQueryProjectionPushdownRPC(t *testing.T) {
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

	cctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 8*time.Second)
	defer cancel()
	col := htcondor.NewCollector(addr)
	ad := mustAd(t, `[MyType="Machine"; Name="slot1@h"; MyAddress="<1.2.3.4:5>"; Cpus=8; Memory=4096; State="Idle"]`)
	if err := col.Advertise(cctx, ad, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
		t.Fatalf("advertise: %v", err)
	}

	got := runQuery(t, addr, commands.QUERY_STARTD_ADS,
		mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements=true; Projection="Name Cpus"]`))
	if len(got) != 1 {
		t.Fatalf("got %d ads, want 1", len(got))
	}
	if c, _ := got[0].EvaluateAttrInt("Cpus"); c != 8 {
		t.Errorf("Cpus = %d, want 8 (projected attr missing)", c)
	}
	if _, ok := got[0].Lookup("Name"); !ok {
		t.Error("Name missing (projected attr)")
	}
	if _, ok := got[0].Lookup("Memory"); ok {
		t.Error("Memory returned but was not projected (projection not pushed down)")
	}
	if _, ok := got[0].Lookup("State"); ok {
		t.Error("State returned but was not projected (projection not pushed down)")
	}
}
