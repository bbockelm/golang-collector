package store

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/stream"
)

// stripLastHeardFrom removes a LastHeardFrom line from old-ClassAd text. The
// collector stamps LastHeardFrom on receipt, so a startd never sends one; corpus
// ads captured via `condor_status -l` already carry it, which -- combined with the
// store's unconditional re-stamp -- makes markSeen see a duplicate and fall back to
// a full classad.ParseOld re-parse (~6x slower, ~8x more allocs than the streaming
// fast path a real update takes). Stripping it makes the benchmark measure
// production ingest.
func stripLastHeardFrom(text string) string {
	var b strings.Builder
	b.Grow(len(text))
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "LastHeardFrom ") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// BenchmarkIngestScaling measures how concurrent ad ingest scales across cores with
// NO network: g goroutines each re-advertise their own set of distinct-keyed ads
// into the shared store (Update = serialize + intern + shard insert). Reveals any
// write-path contention (shard write locks, intern-table locking on encode).
//
//	go test ./store/ -run=^$ -bench=BenchmarkIngestScaling -benchtime=200000x
func BenchmarkIngestScaling(b *testing.B) {
	sample := loadStartdCorpus(b)
	for _, g := range []int{1, 2, 4, 6, 8, 12} {
		b.Run(fmt.Sprintf("g=%02d", g), func(b *testing.B) {
			st := New()
			// Pre-render each goroutine's ads to old-ClassAd text (the form the
			// server feeds UpdateOldText -- the raw ingest path, no AST) and warm the
			// store + intern table so the measured loop hits steady state.
			perG := 256
			textByG := make([][]string, g)
			rng := rand.New(rand.NewSource(1))
			for w := 0; w < g; w++ {
				texts := make([]string, perG)
				for i := range texts {
					ad := mutate(sample[i%len(sample)], w*perG+i, rng)
					texts[i] = stripLastHeardFrom(ad.MarshalOld())
					_ = st.UpdateOldText(StartdAd, texts[i])
				}
				textByG[w] = texts
			}
			var claimed int64
			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < g; w++ {
				wg.Add(1)
				go func(texts []string) {
					defer wg.Done()
					i := 0
					for atomic.AddInt64(&claimed, 1) <= int64(b.N) {
						if err := st.UpdateOldText(StartdAd, texts[i%len(texts)]); err != nil {
							b.Error(err)
							return
						}
						i++
					}
				}(textByG[w])
			}
			wg.Wait()
			b.StopTimer()
			if secs := b.Elapsed().Seconds(); secs > 0 {
				b.ReportMetric(float64(b.N)/secs, "updates/sec")
			}
		})
	}
}

// BenchmarkSerializeScaling measures how the raw serialization path scales across
// cores with NO network and NO client: g goroutines each repeatedly scan the shared
// store and serialize every ad to its own discard stream (PutClassAdRawBytes). This
// isolates store-scan + serialization CPU scaling from all I/O -- if it scales ~g
// then the server itself is contention-free and any lower scaling in the networked
// benchmarks is I/O/client, not the collector.
//
//	go test ./store/ -run=^$ -bench=BenchmarkSerializeScaling -benchtime=100x
func BenchmarkSerializeScaling(b *testing.B) {
	sample := loadStartdCorpus(b)
	st := New()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		_ = st.Update(StartdAd, mutate(sample[i%len(sample)], i, rng))
	}

	for _, g := range []int{1, 2, 4, 6, 8, 12} {
		b.Run(fmt.Sprintf("g=%02d", g), func(b *testing.B) {
			var ads, claimed int64
			b.ResetTimer()
			var wg sync.WaitGroup
			for w := 0; w < g; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx := context.Background()
					st2 := stream.NewStream(discardConn{})
					var local int64
					for atomic.AddInt64(&claimed, 1) <= int64(b.N) {
						msg := message.NewMessageForStream(st2)
						for ra := range st.QueryRaw(StartdAd, nil) {
							if err := msg.PutClassAdRawBytes(ctx, ra.Exprs, ra.MyType, ra.TargetType); err != nil {
								b.Error(err)
								return
							}
							local++
						}
						_ = msg.FlushFrame(ctx, true)
					}
					atomic.AddInt64(&ads, local)
				}()
			}
			wg.Wait()
			b.StopTimer()
			if secs := b.Elapsed().Seconds(); secs > 0 {
				b.ReportMetric(float64(ads)/secs, "ads/sec")
			}
		})
	}
}

// discardConn is a net.Conn whose writes are dropped -- lets us drive cedar's
// PutClassAd (the real server serialization) at full speed without a socket.
type discardConn struct{}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "pipe" }
func (fakeAddr) String() string  { return "discard" }

func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (discardConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

// BenchmarkQuerySerialize measures the collector's *serialization* cost in
// isolation (no network): iterate a match-all query and render each result to
// old-ClassAd text, exactly the per-ad work the query handler does
// (decodeAd -> *classad.ClassAd, then format every attribute). Run on both small
// synthetic ads and real ~21 KB OSPool ads to see when this CPU cost -- the thing
// a raw wire-out send path would cut -- actually dominates.
//
//	go test ./store/ -run=^$ -bench=BenchmarkQuerySerialize -benchmem
func BenchmarkQuerySerialize(b *testing.B) {
	b.Run("small", func(b *testing.B) {
		st := New()
		for i := 0; i < 2000; i++ {
			ad, _ := classad.Parse("[MyType=\"Machine\"; Name=\"slot" + strconv.Itoa(i) +
				"@h\"; MyAddress=\"<1.2.3.4:9618>\"; State=\"Unclaimed\"; Activity=\"Idle\"; " +
				"Cpus=8; Memory=2048; Disk=1000000; Arch=\"X86_64\"; OpSys=\"LINUX\"]")
			_ = st.Update(StartdAd, ad)
		}
		benchSerialize(b, st)
	})
	b.Run("large-ospool", func(b *testing.B) {
		sample := loadStartdCorpus(b)
		st := New()
		rng := rand.New(rand.NewSource(1))
		for i := 0; i < 2000; i++ {
			_ = st.Update(StartdAd, mutate(sample[i%len(sample)], i, rng))
		}
		benchSerialize(b, st)
	})
}

func benchSerialize(b *testing.B, st *Store) {
	// "ast" is the legacy path: decode each stored ad to a *classad.ClassAd, then
	// PutClassAd. "raw" is the current query-handler path: QueryRaw renders the wire
	// straight to "Name = Value" byte slices (no AST) and PutClassAdRawBytes streams
	// them. Both write to a discard conn, so this is pure server serialization -- no
	// client, no socket -- which is what a CPU/alloc profile should attribute.
	ctx := context.Background()
	st2 := stream.NewStream(discardConn{})

	b.Run("ast", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var ads int64
		for i := 0; i < b.N; i++ {
			msg := message.NewMessageForStream(st2)
			for ad := range st.Query(StartdAd, nil) { // nil == match all
				if err := msg.PutClassAd(ctx, ad); err != nil {
					b.Fatal(err)
				}
				ads++
			}
			_ = msg.FlushFrame(ctx, true)
		}
		b.StopTimer()
		if secs := b.Elapsed().Seconds(); secs > 0 {
			b.ReportMetric(float64(ads)/secs, "ads/sec")
		}
	})

	b.Run("raw", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		var ads int64
		for i := 0; i < b.N; i++ {
			msg := message.NewMessageForStream(st2)
			for ra := range st.QueryRaw(StartdAd, nil) {
				if err := msg.PutClassAdRawBytes(ctx, ra.Exprs, ra.MyType, ra.TargetType); err != nil {
					b.Fatal(err)
				}
				ads++
			}
			_ = msg.FlushFrame(ctx, true)
		}
		b.StopTimer()
		if secs := b.Elapsed().Seconds(); secs > 0 {
			b.ReportMetric(float64(ads)/secs, "ads/sec")
		}
	})
}
