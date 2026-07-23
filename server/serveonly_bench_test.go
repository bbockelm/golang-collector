package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/stream"

	"github.com/bbockelm/golang-collector/store"
)

// discardConn is a net.Conn whose Write discards instantly (never blocks) so a
// server-side serialize benchmark measures CPU, not socket backpressure.
type discardConn struct{ net.Conn }

func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) Read(p []byte) (int, error)       { return 0, nil }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (discardConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

// BenchmarkServeOnly measures ONLY the server-side match-all serialize path
// (relayMatches -> store raw scan -> cedar frame encode -> discard), with no
// client, no socket blocking, no loopback. It isolates serialize+GC CPU.
func BenchmarkServeOnly(b *testing.B) {
	const n = 2000
	ads := loadLargeMachineAds(b, n)
	st := store.New()
	for _, ad := range ads {
		if err := st.Update(context.Background(), store.StartdAd, ad); err != nil {
			b.Fatal(err)
		}
	}
	strm := stream.NewStream(discardConn{})
	strm.FinalizeDigests() // simulate a finalized plaintext handshake (stop per-frame SHA256)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := message.NewMessageForStream(strm)
		if err := relayMatches(ctx, resp, st, store.StartdAd, "", nil, 0, true); err != nil {
			b.Fatal(err)
		}
		if err := resp.FinishMessage(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "ads/sec")
}

// BenchmarkServeOnlyProjected is BenchmarkServeOnly with a realistic
// condor_status-style projection (~12 of an OSPool ad's ~460 attributes),
// exercising the in-walk projection pushdown (redacted, like a public query).
func BenchmarkServeOnlyProjected(b *testing.B) {
	const n = 2000
	ads := loadLargeMachineAds(b, n)
	st := store.New()
	for _, ad := range ads {
		if err := st.Update(context.Background(), store.StartdAd, ad); err != nil {
			b.Fatal(err)
		}
	}
	strm := stream.NewStream(discardConn{})
	strm.FinalizeDigests()
	ctx := context.Background()
	projection := []string{
		"Name", "Machine", "OpSys", "Arch", "State", "Activity",
		"LoadAvg", "Memory", "Cpus", "EnteredCurrentActivity", "MyCurrentTime", "TotalSlots",
	}

	// Converge the hot set the way production does: a projected query records
	// demand, the periodic maintenance refreshes the hot set from it, and the
	// daemons' next re-advertise rewrites each ad with the new hot header.
	{
		resp := message.NewMessageForStream(strm)
		if err := relayMatches(ctx, resp, st, store.StartdAd, "", projection, 0, true); err != nil {
			b.Fatal(err)
		}
		if err := resp.FinishMessage(ctx); err != nil {
			b.Fatal(err)
		}
		st.RefreshHotSets(2000, 32)
		for _, ad := range ads {
			if err := st.Update(context.Background(), store.StartdAd, ad); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := message.NewMessageForStream(strm)
		if err := relayMatches(ctx, resp, st, store.StartdAd, "", projection, 0, true); err != nil {
			b.Fatal(err)
		}
		if err := resp.FinishMessage(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "ads/sec")
}
