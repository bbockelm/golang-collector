package store

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// The three ways the in-memory store can sit behind the ingest path, measured on the
// same ads so the only variable is the guard. Raw is today's behavior (no guard at all);
// Guarded is DirectUntilStalled (straight through while healthy); Queued is
// StallThreshold=0 (every update buffered, nobody can ever block).
//
// The question these answer is whether the unconditional guarantee is affordable for the
// default backend, or whether it has to be bought with a detection window.

func BenchmarkMemoryIngestRaw(b *testing.B) {
	ads := benchAds(20000)
	st := New()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.UpdateOldText(ctx, StartdAd, ads[i%len(ads)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMemoryIngestGuarded(b *testing.B) {
	benchMemGuard(b, QueuePolicy{StallThreshold: time.Second, StallClear: time.Minute, DirectUntilStalled: true})
}

func BenchmarkMemoryIngestQueued(b *testing.B) {
	benchMemGuard(b, QueuePolicy{StallThreshold: 0, StallClear: time.Minute})
}

func benchMemGuard(b *testing.B, qp QueuePolicy) {
	ads := benchAds(20000)
	ct := &countingStore{Store: New()}
	bb, err := NewBufferedBackendPolicy(ct, 100*time.Millisecond, 2048, qp, nil)
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
	b.StopTimer()
	// How many updates actually reached the store per update offered. Below 1.0 is
	// coalescing -- real for a collector under a re-advertise storm, but it means the
	// per-op figure above is not purely the cost of one store write.
	b.ReportMetric(float64(ct.applied.Load())/float64(b.N), "applied/op")
}

// countingStore counts the updates that actually reach the in-memory store.
type countingStore struct {
	*Store
	applied atomic.Int64
}

func (c *countingStore) UpdateOldText(ctx context.Context, t AdType, text string) error {
	c.applied.Add(1)
	return c.Store.UpdateOldText(ctx, t, text)
}

func (c *countingStore) UpdateBatch(ctx context.Context, batch []PendingUpdate) error {
	c.applied.Add(int64(len(batch)))
	return c.Store.UpdateBatch(ctx, batch)
}

func (c *countingStore) Base() Backend { return c.Store }
