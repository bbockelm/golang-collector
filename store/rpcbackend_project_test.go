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

// TestRPCBackendQueryRawProject verifies the projection is pushed to the database:
// the returned RawAds carry only the requested attributes (plus MyType, a RawAd
// field), so a projected query does not pull every attribute over the wire.
func TestRPCBackendQueryRawProject(t *testing.T) {
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

	ctx := context.Background()
	batch := []PendingUpdate{
		{Type: StartdAd, Text: `MyType = "Machine"` + "\n" + `Name = "slot1@a"` + "\n" + `Cpus = 8` + "\n" + `Memory = 4096` + "\n" + `State = "Idle"`},
	}
	if err := b.UpdateBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}

	seq, err := b.QueryRawProject(ctx, StartdAd, "true", []string{"Name", "Cpus"}, 0)
	if err != nil {
		t.Fatalf("QueryRawProject: %v", err)
	}
	var got []collections.RawAd
	for ra := range seq {
		got = append(got, ra)
	}
	if len(got) != 1 {
		t.Fatalf("got %d ads, want 1", len(got))
	}
	if got[0].MyType != "Machine" {
		t.Errorf("MyType = %q, want Machine", got[0].MyType)
	}
	var body string
	for _, e := range got[0].Exprs {
		body += string(e) + "\n"
	}
	for _, want := range []string{"Name", "Cpus"} {
		if !strings.Contains(body, want) {
			t.Errorf("projected ad missing %s: %q", want, body)
		}
	}
	for _, drop := range []string{"Memory", "State"} {
		if strings.Contains(body, drop) {
			t.Errorf("projected ad should not carry %s: %q", drop, body)
		}
	}
}
