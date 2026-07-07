package store

import (
	"context"
	"io"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/stream"
)

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
