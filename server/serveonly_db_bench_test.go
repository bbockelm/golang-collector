package server

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/stream"

	"github.com/bbockelm/golang-collector/store"
)

// BenchmarkServeOnlyDBBacked is BenchmarkServeOnly for the two-hop topology the
// htcondordb-backed collector runs: relayMatches -> BufferedBackend ->
// RPCBackend -> (in-process dbrpc server over a pipe) -> persistent db catalog.
// The response is rendered at the database, shipped as old-ClassAd text rows,
// re-parsed into raw expressions at the collector, and re-framed for the client
// -- so this measures both hops' CPU in one profile. The pipe is synchronous
// (no socket buffering), which understates pipelining slightly; the shape of
// the costs is what matters.
func BenchmarkServeOnlyDBBacked(b *testing.B) {
	const n = 2000
	ads := loadLargeMachineAds(b, n)
	ctx := context.Background()

	// WIRE_BATCH_BUDGET overrides the dbrpc batch-frame budget for size-
	// sensitivity runs (the memory-vs-throughput trade is measured, not assumed).
	if v := os.Getenv("WIRE_BATCH_BUDGET"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			dbrpc.WireBatchBudget = n
		}
	}
	cat, err := db.OpenCatalog(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer cat.Close()
	srv := dbrpc.NewServerCatalog(cat)
	defer srv.Close()
	// Real TCP loopback between the hops (production runs TCP, whose socket
	// buffering pipelines frames; a net.Pipe's synchronous rendezvous does not).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			sc, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			// The collector's database connection is privileged (it applies
			// per-client redaction itself), matching production.
			go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{IncludePrivate: true}) }()
		}
	}()
	dial := func(context.Context) (dbrpc.MsgConn, error) {
		cc, derr := net.Dial("tcp", ln.Addr().String())
		if derr != nil {
			return nil, derr
		}
		return dbrpc.NewStreamConn(cc), nil
	}
	rpc := store.NewRPCBackend(ctx, dial, store.RetryPolicy{
		Initial: time.Millisecond, Max: 50 * time.Millisecond, Multiplier: 2, MaxElapsed: 5 * time.Second,
	})
	defer rpc.Close()
	st, err := store.NewBufferedBackend(rpc, 100*time.Millisecond, 2048, nil)
	if err != nil {
		b.Fatal(err)
	}

	for _, ad := range ads {
		if err := st.Update(ctx, store.StartdAd, ad); err != nil {
			b.Fatal(err)
		}
	}

	strm := stream.NewStream(discardConn{})
	strm.FinalizeDigests()
	projection := []string{
		"Name", "Machine", "OpSys", "Arch", "State", "Activity",
		"LoadAvg", "Memory", "Cpus", "EnteredCurrentActivity", "MyCurrentTime", "TotalSlots",
	}

	run := func(b *testing.B, proj []string) {
		for i := 0; i < b.N; i++ {
			resp := message.NewMessageForStream(strm)
			if err := relayMatches(ctx, resp, st, store.StartdAd, "", proj, 0, true); err != nil {
				b.Fatal(err)
			}
			if err := resp.FinishMessage(ctx); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "ads/sec")
	}

	b.Run("unprojected", func(b *testing.B) { run(b, nil) })
	b.Run("projected-cold", func(b *testing.B) { run(b, projection) })

	// Converge: the database-side table refreshes its hot set from the projection
	// demand recorded above, and the ads are rewritten (production: daemons
	// re-advertise on their usual cadence).
	tbl, ok := cat.Table(store.StartdAd.String())
	if !ok {
		b.Fatal("Startd table missing from catalog")
	}
	tbl.RefreshHotSet(n, 32)
	for _, ad := range ads {
		if err := st.Update(ctx, store.StartdAd, ad); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("projected-hot", func(b *testing.B) { run(b, projection) })
}
