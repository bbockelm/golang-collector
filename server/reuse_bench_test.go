package server

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/cedar/commands"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// reuseCtx returns a context carrying the plaintext security config the test
// collectors use, so the client's dial+authenticate negotiates no crypto.
func reuseCtx() context.Context {
	return htcondor.WithSecurityConfig(context.Background(), plaintextSec())
}

// advertiseReuse sends ads down one authenticated connection via the client's
// persistent-connection path (Collector.AdvertiseMultiple), which frames each
// follow-on update as [cmd + EOM][ad + EOM] -- the HTCondor-standard framing the
// collector's keep-alive loop reads.
func advertiseReuse(ctx context.Context, addr string, ads []*classad.ClassAd) []error {
	col := htcondor.NewCollector(addr)
	return col.AdvertiseMultiple(ctx, ads, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD})
}

// TestWriteReuseStoresAll confirms every ad sent down one reused connection is
// stored -- i.e. the client's command-int-per-ad framing matches the server's
// keep-alive read loop end to end.
func TestWriteReuseStoresAll(t *testing.T) {
	for _, n := range []int{1, 2, 6} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			st, addr, stop := startCollector(t)
			defer stop()

			ads := make([]*classad.ClassAd, n)
			for i := range ads {
				ads[i] = benchAd(i) // distinct Name/host per i -> distinct keys
			}
			for i, err := range advertiseReuse(reuseCtx(), addr, ads) {
				if err != nil {
					t.Fatalf("ad %d: %v", i, err)
				}
			}
			// The server processes ads asynchronously; poll briefly.
			var got int
			for i := 0; i < 200; i++ {
				if got = mustLen(t, st, store.StartdAd); got == n {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Fatalf("stored %d ads via one reused connection, want %d", got, n)
		})
	}
}

// BenchmarkWriteReuse measures per-ad advertise throughput as a function of how
// many ads share one connection (batch=1 is the current one-connection-per-ad
// model). It isolates the connection+handshake setup cost the write path is
// bounded by: the gap between batch=1 and larger batches is the amortization
// headroom.
func BenchmarkWriteReuse(b *testing.B) {
	addr, stop := startPlaintextGoCollector(b)
	defer stop()
	ctx := reuseCtx()

	for _, batch := range []int{1, 2, 4, 8, 32, 128} {
		b.Run(fmt.Sprintf("batch=%03d", batch), func(b *testing.B) {
			rng := rand.New(rand.NewSource(1))
			ads := make([]*classad.ClassAd, batch)
			b.ResetTimer()
			for sent := 0; sent < b.N; {
				n := batch
				if sent+n > b.N {
					n = b.N - sent
				}
				for k := 0; k < n; k++ {
					ads[k] = benchAd(rng.Intn(benchNumAds))
				}
				for _, err := range advertiseReuse(ctx, addr, ads[:n]) {
					if err != nil {
						b.Fatal(err)
					}
				}
				sent += n
			}
			b.StopTimer()
			if secs := b.Elapsed().Seconds(); secs > 0 {
				b.ReportMetric(float64(b.N)/secs, "ads/sec")
			}
		})
	}
}
