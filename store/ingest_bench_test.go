package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// nopWriter is a backend whose commits cost nothing, so a benchmark measures the
// ingest path itself -- what an ad update costs the command handler before anything
// touches storage -- rather than the storage underneath it.
type nopWriter struct {
	Backend
	batches atomic.Int64
	ads     atomic.Int64
}

func (w *nopWriter) UpdateBatch(_ context.Context, batch []PendingUpdate) error {
	w.batches.Add(1)
	w.ads.Add(int64(len(batch)))
	return nil
}

// benchAds builds n distinct startd ads of a realistic size, reused across arms so
// every arm measures the same work on the same data.
func benchAds(n int) []string {
	ads := make([]string, n)
	for i := range ads {
		ads[i] = fmt.Sprintf("Name = \"slot%d@node%d.example.org\"\nMyType = \"Machine\"\nState = \"Unclaimed\"\nActivity = \"Idle\"\nCpus = 8\nMemory = 32768\nDisk = 1048576\nArch = \"X86_64\"\nOpSys = \"LINUX\"\nMyAddress = \"<10.0.%d.%d:9618>\"\nStartdIpAddr = \"<10.0.%d.%d:9618>\"\nTotalSlots = 16\nLoadAvg = 0.9%d",
			i%16, i/16, (i/256)%256, i%256, (i/256)%256, i%256, i%10)
	}
	return ads
}

// BenchmarkIngestEnqueue measures one ad update through the buffered backend with the
// commit stubbed out: the per-ad cost the collector's command handler pays, and the
// number this change is allowed to move. Keys repeat (a pool re-advertising), so
// coalescing is exercised as it is in production.
func BenchmarkIngestEnqueue(b *testing.B) {
	ads := benchAds(20000)
	w := &nopWriter{Backend: New()}
	bb, err := NewBufferedBackend(w, 100*time.Millisecond, 2048, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = bb.Close() }()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := bb.UpdateOldText(ctx, StartdAd, ads[i%len(ads)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIngestEnqueueParallel is the same path under the concurrency a real
// collector sees: one goroutine per advertising daemon's command socket, all
// contending for the one buffer.
func BenchmarkIngestEnqueueParallel(b *testing.B) {
	ads := benchAds(20000)
	w := &nopWriter{Backend: New()}
	bb, err := NewBufferedBackend(w, 100*time.Millisecond, 2048, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = bb.Close() }()
	ctx := context.Background()
	var seq atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := seq.Add(1)
			if err := bb.UpdateOldText(ctx, StartdAd, ads[int(i)%len(ads)]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkIngestLatencyUnderStall is the number that matters operationally: with the
// backend wedged and never committing, what does one ad update cost the handler? It
// reports the worst single update, not just the mean, because a stalled sender is
// caused by one slow update, not an average.
func BenchmarkIngestLatencyUnderStall(b *testing.B) {
	ads := benchAds(20000)
	release := make(chan struct{})
	w := &wedgedWriter{Backend: New(), release: release}
	bb, err := NewBufferedBackend(w, 100*time.Millisecond, 2048, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { close(release); _ = bb.Close() }()
	ctx := context.Background()
	var worst time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		if err := bb.UpdateOldText(ctx, StartdAd, ads[i%len(ads)]); err != nil {
			b.Fatal(err)
		}
		if d := time.Since(start); d > worst {
			worst = d
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(worst.Nanoseconds()), "worst-ns/op")
}

// wedgedWriter never completes a commit until released.
type wedgedWriter struct {
	Backend
	release chan struct{}
	once    sync.Once
}

func (w *wedgedWriter) UpdateBatch(_ context.Context, _ []PendingUpdate) error {
	<-w.release
	return nil
}
