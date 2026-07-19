package store

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestRPCBackendPartialCommitSkipsBadAd verifies that a single ad the db server
// cannot parse does not abort the whole batch: the good ads in the same
// transaction still commit, and only the bad ad is dropped (a partial commit via
// the open dbrpc transaction, per putBatchTx).
func TestRPCBackendPartialCommitSkipsBadAd(t *testing.T) {
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
	defer func() { _ = b.Close(); srv.Close(); _ = cat.Close() }()

	batch := []PendingUpdate{
		{Type: StartdAd, Text: `Name = "slot1@a"` + "\n" + `State = "Idle"`},
		// Unterminated string -> the server's ParseOld rejects this ad.
		{Type: StartdAd, Text: `Name = "slot2@a"` + "\n" + `Bad = "unterminated`},
		{Type: StartdAd, Text: `Name = "slot3@a"` + "\n" + `State = "Claimed"`},
	}
	if err := b.UpdateBatch(context.Background(), batch); err != nil {
		t.Fatalf("UpdateBatch returned %v; want nil (bad ad skipped, good ads committed)", err)
	}

	if n := countAds(t, b, StartdAd, "true"); n != 2 {
		t.Fatalf("stored %d startd ads, want 2 (slot1 + slot3 committed, slot2 dropped)", n)
	}
	if _, ok := b.Get(context.Background(), StartdAd, mustParse(t, `Name = "slot1@a"`)); !ok {
		t.Error("slot1 missing (a good ad in the batch was lost)")
	}
	if _, ok := b.Get(context.Background(), StartdAd, mustParse(t, `Name = "slot3@a"`)); !ok {
		t.Error("slot3 missing (a good ad after the bad one was lost)")
	}
}
