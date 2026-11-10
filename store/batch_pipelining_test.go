package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// gatedBatchWriter wraps a real backend but blocks every UpdateBatch until release is
// closed, so a test can hold a commit "in flight" and observe what the producer does
// meanwhile. It signals started on the first UpdateBatch entry.
type gatedBatchWriter struct {
	Backend
	startedOnce sync.Once
	started     chan struct{}
	release     chan struct{}
}

func (g *gatedBatchWriter) UpdateBatch(ctx context.Context, batch []PendingUpdate) error {
	g.startedOnce.Do(func() { g.started <- struct{}{} })
	<-g.release
	return g.Backend.(BatchWriter).UpdateBatch(ctx, batch)
}

// TestBufferedBackendPipelining verifies the fix for the jerky db-mode update stream:
// while a batch is committing, producers must keep buffering rather than block on the
// commit -- and past the high-water mark they must STILL not block, engaging the deep
// update queue instead (see TestIngestNeverBlocksOnStalledBackend for the full
// guarantee). Every ad here fits inside the queue's caps, so none is shed: pipelining
// must not cost correctness.
func TestBufferedBackendPipelining(t *testing.T) {
	under, err := NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g := &gatedBatchWriter{Backend: under, started: make(chan struct{}, 1), release: make(chan struct{})}
	// maxBuf 4 -> hardCap 16; a real window so the background writer runs.
	b, err := NewBufferedBackend(g, 50*time.Millisecond, 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	var relOnce sync.Once
	release := func() { relOnce.Do(func() { close(g.release) }) }
	defer func() { release(); _ = b.Close() }() // never leave the writer gated (Close would hang)

	ctx := context.Background()
	put := func(i int) {
		if err := b.UpdateOldText(ctx, StartdAd, fmt.Sprintf(`Name = "s%d@a"`+"\n"+`State = "Idle"`, i)); err != nil {
			t.Errorf("update %d: %v", i, err)
		}
	}

	// Fill to the high-water mark; the background writer wakes, enters UpdateBatch, and
	// gates there (commit "in flight").
	for i := 0; i < 4; i++ {
		put(i)
	}
	select {
	case <-g.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background writer never started the commit")
	}

	// CORE: with the commit gated, more updates must return promptly -- the producer keeps
	// buffering instead of blocking on the in-flight commit.
	done := make(chan struct{})
	go func() {
		for i := 4; i < 10; i++ {
			put(i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("producer blocked while a batch was committing -- no pipelining")
	}

	// Past the high-water mark: the buffer reaching hardCap with the writer still
	// committing is the signal that engages the deep update queue (counted). The producer
	// must sail straight through it -- the writer is single-threaded and gated, so it
	// cannot drain anything meanwhile, and anything that waited here would wait forever.
	before := testutil.ToFloat64(Metrics.backpressureTotal)
	bp := make(chan struct{})
	go func() {
		for i := 10; i < 40; i++ {
			put(i)
		}
		close(bp)
	}()
	select {
	case <-bp:
	case <-time.After(2 * time.Second):
		t.Fatal("producer blocked past the buffer's high-water mark -- the ingest path still applies backpressure")
	}
	if testutil.ToFloat64(Metrics.backpressureTotal) == before {
		t.Fatal("filling past the hard cap did not engage the deep update queue")
	}
	if !b.Shedding() {
		t.Fatal("deep update queue was not engaged after the buffer passed its hard cap")
	}

	// Release: every gated commit proceeds; all distinct ads must land.
	release()
	<-bp
	if !waitForLen(t, b, StartdAd, 40, 5*time.Second) {
		n, _ := b.Len(ctx, StartdAd)
		t.Fatalf("after release stored %d ads, want 40 (some updates were lost)", n)
	}
}

func waitForLen(t *testing.T, b *BufferedBackend, at AdType, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, _ := b.Len(context.Background(), at); n == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
