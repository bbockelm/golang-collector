package store

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// newStreamTestBackend returns an RPCBackend wired to an in-process db server, plus a
// cleanup. Shared by the streaming read-path tests.
func newStreamTestBackend(t *testing.T) *RPCBackend {
	t.Helper()
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
	b := NewRPCBackend(context.Background(), dial, RetryPolicy{
		Initial: time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2, MaxElapsed: time.Second,
	})
	t.Cleanup(func() { _ = b.Close(); srv.Close(); _ = cat.Close() })
	return b
}

// TestRPCBackendQueryRawStream verifies the streaming read path (store.RawStreamer):
// every matching ad is delivered to the yield callback, and a consumer that stops early
// (yield returns false) receives no further ads.
func TestRPCBackendQueryRawStream(t *testing.T) {
	b := newStreamTestBackend(t)
	ctx := context.Background()

	batch := []PendingUpdate{
		{Type: StartdAd, Text: `Name = "slot1@a"` + "\n" + `State = "Idle"`},
		{Type: StartdAd, Text: `Name = "slot2@a"` + "\n" + `State = "Claimed"`},
		{Type: StartdAd, Text: `Name = "slot3@a"` + "\n" + `State = "Idle"`},
	}
	if err := b.UpdateBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}

	// Full delivery.
	var got []collections.RawAd
	if err := b.QueryRawStream(ctx, StartdAd, "true", 0, func(ra collections.RawAd) bool {
		got = append(got, ra)
		return true
	}); err != nil {
		t.Fatalf("QueryRawStream: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("streamed %d ads, want 3", len(got))
	}

	// Constraint pushdown: only the Idle slots.
	idle := 0
	if err := b.QueryRawStream(ctx, StartdAd, `State == "Idle"`, 0, func(collections.RawAd) bool {
		idle++
		return true
	}); err != nil {
		t.Fatalf("QueryRawStream(Idle): %v", err)
	}
	if idle != 2 {
		t.Fatalf("streamed %d Idle ads, want 2", idle)
	}

	// Early stop: the consumer returns false after the first ad and must get no more.
	seen := 0
	if err := b.QueryRawStream(ctx, StartdAd, "true", 0, func(collections.RawAd) bool {
		seen++
		return false // stop immediately
	}); err != nil {
		t.Fatalf("QueryRawStream(early-stop): %v", err)
	}
	if seen != 1 {
		t.Fatalf("consumer saw %d ads after stopping, want 1", seen)
	}
}

// TestRPCBackendQueryRawProjectStream verifies the projected streaming path delivers each
// ad projected to the requested attributes (plus the RawAd type fields).
func TestRPCBackendQueryRawProjectStream(t *testing.T) {
	b := newStreamTestBackend(t)
	ctx := context.Background()

	if err := b.UpdateBatch(ctx, []PendingUpdate{
		{Type: StartdAd, Text: `MyType = "Machine"` + "\n" + `Name = "slot1@a"` + "\n" + `Cpus = 8` + "\n" + `Memory = 4096`},
	}); err != nil {
		t.Fatal(err)
	}

	var got []collections.RawAd
	if err := b.QueryRawProjectStream(ctx, StartdAd, "true", []string{"Name", "Cpus"}, 0, func(ra collections.RawAd) bool {
		got = append(got, ra)
		return true
	}); err != nil {
		t.Fatalf("QueryRawProjectStream: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("streamed %d ads, want 1", len(got))
	}
	var body string
	for _, e := range got[0].Exprs {
		body += string(e) + "\n"
	}
	// Memory was not requested, so the pushdown must not send it.
	if strings.Contains(body, "Memory") {
		t.Errorf("projected ad carries Memory; projection was not pushed down: %q", body)
	}
}
