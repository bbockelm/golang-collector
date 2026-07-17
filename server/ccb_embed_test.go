package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bbockelm/cedar/ccb"
	"github.com/bbockelm/cedar/commands"
	ccbserver "github.com/bbockelm/golang-ccb"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// TestEmbeddedCCB verifies the collector can host a CCB server on its own command
// socket (ENABLE_CCB_SERVER), exactly as main.go wires it: a CCB listener
// registers through the collector's port and is assigned a CCBID, while ordinary
// collector advertise/query keep working on the same socket.
func TestEmbeddedCCB(t *testing.T) {
	st := store.New()
	srv := New(st, plaintextSec(), nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	addr := ln.Addr().String()

	// Embed the CCB server onto the collector's cedar server (as main.go does via
	// maybeStartEmbeddedCCB: ccbserver.New + RegisterOn + StartBackground).
	ccbSrv, err := ccbserver.New(ccbserver.Config{PublicAddress: addr, Security: plaintextSec()})
	if err != nil {
		t.Fatalf("build embedded CCB: %v", err)
	}
	ccbSrv.RegisterOn(srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ccbSrv.StartBackground(ctx)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = srv.ServeConn(ctx, conn) }()
		}
	}()

	// A CCB listener (a NAT'd daemon) registers through the collector's socket.
	lis := ccb.NewListener(ccb.ListenerConfig{
		BrokerAddr:        addr,
		Security:          plaintextSec(),
		Name:              "embed-target",
		HeartbeatInterval: 30 * time.Second,
		ReconnectInterval: 100 * time.Millisecond,
		Handler:           func(c net.Conn, _ ccb.InboundMeta) { _ = c.Close() },
	})
	lctx, lcancel := context.WithCancel(context.Background())
	defer lcancel()
	go func() { _ = lis.Run(lctx) }()

	// Registration should yield a CCB contact "<addr>#<id>" from the embedded CCB.
	contact := ""
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c := lis.Contact(); c != "" {
			contact = c
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if contact == "" {
		t.Fatal("CCB listener never registered through the embedded collector-CCB")
	}
	t.Logf("registered CCB contact through the collector: %s", contact)

	// The collector still serves ordinary advertise + query on the same socket.
	cctx := htcondor.WithSecurityConfig(context.Background(), plaintextSec())
	col := htcondor.NewCollector(addr)
	ad := mustAd(t, `[MyType="Machine"; Name="ccb@host"; MyAddress="<1.2.3.4:5>"; Cpus=2]`)
	if err := col.Advertise(cctx, ad, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
		t.Fatalf("advertise via CCB-hosting collector: %v", err)
	}
	got, err := col.QueryAds(cctx, "Machine", `Name == "ccb@host"`)
	if err != nil {
		t.Fatalf("query via CCB-hosting collector: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("query via CCB-hosting collector returned %d ads, want 1", len(got))
	}
}
