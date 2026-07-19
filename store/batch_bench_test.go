package store

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// laggyConn adds a fixed delay to each client request and counts requests, so a
// benchmark reflects what the batching buffer actually saves: round trips /
// commits to the database (the dominant cost on a real remote store), not CPU.
type laggyConn struct {
	dbrpc.MsgConn
	lag  time.Duration
	reqs *atomic.Int64
}

func (c *laggyConn) WriteMsg(b []byte) error {
	c.reqs.Add(1)
	if c.lag > 0 {
		time.Sleep(c.lag)
	}
	return c.MsgConn.WriteMsg(b)
}

// newLaggyRPCBackend wires an RPCBackend to an in-process dbrpc server over a pipe
// with lag added per request; reqs counts the client requests (round trips).
func newLaggyRPCBackend(tb testing.TB, lag time.Duration, reqs *atomic.Int64) *RPCBackend {
	cat, err := db.OpenCatalog(tb.TempDir())
	if err != nil {
		tb.Fatal(err)
	}
	srv := dbrpc.NewServerCatalog(cat)
	tb.Cleanup(func() { srv.Close(); _ = cat.Close() })
	dial := func(context.Context) (dbrpc.MsgConn, error) {
		sc, cc := net.Pipe()
		go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{IncludePrivate: true}) }()
		return &laggyConn{MsgConn: dbrpc.NewStreamConn(cc), lag: lag, reqs: reqs}, nil
	}
	return NewRPCBackend(context.Background(), dial, RetryPolicy{
		Initial: time.Millisecond, Max: 10 * time.Millisecond, Multiplier: 2, MaxElapsed: time.Second,
	})
}

// startdText builds a distinct startd ad for slot key.
func startdText(key int) string {
	return fmt.Sprintf("MyType = \"Machine\"\nName = \"slot%d@host\"\nState = \"Unclaimed\"\nCpus = 8\nMemory = 16384", key)
}

// BenchmarkBatchedIngest ingests a re-advertise burst through the remote backend
// directly vs through the batching buffer, under simulated per-request latency on
// a single connection. Reports updates/sec and round trips per update: the buffer
// turns each ad's begin/insert/commit into one shared batch commit and collapses
// re-advertises, so both wall clock and round trips drop sharply.
func BenchmarkBatchedIngest(b *testing.B) {
	const (
		lag      = 100 * time.Microsecond
		nKeys    = 50
		nUpdates = 500
	)
	run := func(b *testing.B, buffered bool) {
		var reqs atomic.Int64
		base := newLaggyRPCBackend(b, lag, &reqs)
		var backend Backend = base
		if buffered {
			buf, err := NewBufferedBackend(base, 0, nUpdates+1, nil)
			if err != nil {
				b.Fatal(err)
			}
			backend = buf
		}
		b.Cleanup(func() { _ = backend.Close() })

		b.ResetTimer()
		start := reqs.Load()
		for i := 0; i < b.N; i++ {
			for j := 0; j < nUpdates; j++ {
				if err := backend.UpdateOldText(context.Background(), StartdAd, startdText(j%nKeys)); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := backend.Len(context.Background(), StartdAd); err != nil { // flush buffered
				b.Fatal(err)
			}
		}
		b.StopTimer()
		total := float64(nUpdates * b.N)
		b.ReportMetric(total/b.Elapsed().Seconds(), "updates/sec")
		b.ReportMetric(float64(reqs.Load()-start)/total, "roundtrips/update")
	}
	b.Run("unbuffered", func(b *testing.B) { run(b, false) })
	b.Run("buffered", func(b *testing.B) { run(b, true) })
}

// BenchmarkBatchedIngestConcurrent models the real collector load: many daemons
// advertising at once over the collector's single database connection. The
// headline metric is roundtrips/update -- the load the shared connection and the
// database actually carry. Without batching every concurrent update is its own
// begin/insert/commit; with batching a background flush aggregates all the
// concurrent updates into one transaction per table, so the round-trip (and
// commit) count drops by roughly the batch size. (Wall clock here is dominated by
// the mock's zero-cost, perfectly-pipelined server, which flatters the
// unbuffered path; a real database pays per commit, where fewer round trips win.)
func BenchmarkBatchedIngestConcurrent(b *testing.B) {
	const (
		lag         = 50 * time.Microsecond
		window      = 2 * time.Millisecond
		producers   = 16
		perProducer = 100 // 1600 updates/iteration
		nKeys       = 400
	)
	run := func(b *testing.B, buffered bool) {
		var reqs atomic.Int64
		base := newLaggyRPCBackend(b, lag, &reqs)
		var backend Backend = base
		if buffered {
			buf, err := NewBufferedBackend(base, window, 8192, nil)
			if err != nil {
				b.Fatal(err)
			}
			backend = buf
		}
		b.Cleanup(func() { _ = backend.Close() })

		b.ResetTimer()
		start := reqs.Load()
		for iter := 0; iter < b.N; iter++ {
			var wg sync.WaitGroup
			for p := 0; p < producers; p++ {
				wg.Add(1)
				go func(p int) {
					defer wg.Done()
					for j := 0; j < perProducer; j++ {
						if err := backend.UpdateOldText(context.Background(), StartdAd, startdText((p*perProducer+j)%nKeys)); err != nil {
							b.Error(err)
							return
						}
					}
				}(p)
			}
			wg.Wait()
			if _, err := backend.Len(context.Background(), StartdAd); err != nil { // flush remainder
				b.Fatal(err)
			}
		}
		b.StopTimer()
		total := float64(producers * perProducer * b.N)
		b.ReportMetric(total/b.Elapsed().Seconds(), "updates/sec")
		b.ReportMetric(float64(reqs.Load()-start)/total, "roundtrips/update")
	}
	b.Run("unbuffered", func(b *testing.B) { run(b, false) })
	b.Run("buffered", func(b *testing.B) { run(b, true) })
}
