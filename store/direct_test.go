package store

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestDirectUntilStalledBypassesBufferWhenHealthy checks the fast path the in-memory
// store depends on: while the backend is healthy, an update goes straight through and
// nothing accumulates. If the buffer filled here, the default collector would have
// gained a buffer hop and a flush-before-read for nothing.
func TestDirectUntilStalledBypassesBufferWhenHealthy(t *testing.T) {
	b := newGuardedMemStore(t, QueuePolicy{
		StallThreshold: 50 * time.Millisecond,
		StallClear:     time.Minute,
		FlushTimeout:   -1,
	}, nil)
	defer func() { _ = b.Close() }()

	ctx := context.Background()
	for i := 0; i < 50; i++ {
		if err := b.UpdateOldText(ctx, StartdAd, adText(i)); err != nil {
			t.Fatal(err)
		}
	}
	b.mu.Lock()
	queued := len(b.buf)
	b.mu.Unlock()
	if queued != 0 {
		t.Fatalf("%d ads buffered while the store was healthy, want 0 (updates must go straight through)", queued)
	}
	// Straight through means visible immediately, with no flush in between.
	if n, _ := b.Backend.Len(ctx, StartdAd); n != 50 {
		t.Fatalf("underlying store holds %d ads, want 50 (updates were not applied directly)", n)
	}
	if b.Shedding() {
		t.Fatal("the queue engaged against a healthy store")
	}
}

// TestDirectModeDivertsProducersOnlyAfterDetection records the limit of the direct
// escape hatch, which is why it is not the default.
//
// A stall has to be OBSERVED before it can be reacted to, and in direct mode producers
// reach the backend themselves. So every producer that enters the backend before the
// stall is detected wedges with it -- for a pool advertising at a few hundred ads a
// second and a one-second threshold, that is hundreds of stuck connections. Only
// producers arriving after detection are diverted into the queue.
//
// The default (buffered) path has no such window: a producer never touches the backend,
// so there is nothing for it to wedge on. See TestIngestNeverBlocksOnStalledBackend.
func TestDirectModeDivertsProducersOnlyAfterDetection(t *testing.T) {
	const threshold = 100 * time.Millisecond
	gate := make(chan struct{})
	var once sync.Once
	entered := make(chan struct{})
	b := newGuardedMemStore(t, QueuePolicy{
		StallThreshold: threshold,
		StallClear:     time.Minute,
		FlushTimeout:   -1,
	}, func() { once.Do(func() { close(entered) }); <-gate })
	defer func() { close(gate); _ = b.Close() }()

	ctx := context.Background()
	go func() { _ = b.UpdateOldText(ctx, StartdAd, adText(0)) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the gated update never reached the store")
	}

	// A producer arriving during the detection window wedges with it: the documented
	// cost of this mode, asserted so it cannot change silently.
	early := make(chan struct{})
	go func() { _ = b.UpdateOldText(ctx, StartdAd, adText(1)); close(early) }()
	select {
	case <-early:
		t.Fatal("an update during the detection window returned; this mode's documented window is gone (good -- update the docs and this test)")
	case <-time.After(threshold / 2):
	}

	// Past the threshold the stall is visible, and every later producer is diverted.
	time.Sleep(threshold)
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		for i := 2; i < 100; i++ {
			if err := b.UpdateOldText(ctx, StartdAd, adText(i)); err != nil {
				t.Errorf("update %d: %v", i, err)
				break
			}
		}
		done <- time.Since(start)
	}()
	select {
	case d := <-done:
		if d > 2*time.Second {
			t.Fatalf("98 post-detection updates took %v; they are still reaching the wedged store", d)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("producers arriving after detection still blocked -- the guard never engaged")
	}
	if !b.Shedding() {
		t.Fatal("the queue never engaged, so those updates were applied directly by luck")
	}
}

// TestAlwaysQueueWhenStallThresholdZero: StallThreshold 0 trades the direct fast path
// for the unconditional guarantee -- nobody blocks, including the first producer.
func TestAlwaysQueueWhenStallThresholdZero(t *testing.T) {
	gate := make(chan struct{})
	var once sync.Once
	entered := make(chan struct{})
	b := newGuardedMemStore(t, QueuePolicy{StallThreshold: 0, StallClear: time.Minute, FlushTimeout: -1},
		func() { once.Do(func() { close(entered) }); <-gate })
	defer func() { close(gate); _ = b.Close() }()

	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			if err := b.UpdateOldText(ctx, StartdAd, adText(i)); err != nil {
				t.Errorf("update %d: %v", i, err)
				break
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a producer blocked with StallThreshold=0, where every update must be queued")
	}
}

// TestWrapperKeepsBackendCapabilities is the regression test for a trap that reports no
// error when sprung. BufferedBackend embeds the Backend *interface*, so only Backend's
// own methods are promoted: a plain type assertion for Retrainer or Statser against a
// wrapped in-memory store finds nothing, and dictionary retraining and the storage
// gauges silently stop existing. store.As must see through the wrapper.
func TestWrapperKeepsBackendCapabilities(t *testing.T) {
	b := newGuardedMemStore(t, QueuePolicy{StallThreshold: time.Second, FlushTimeout: -1}, nil)
	defer func() { _ = b.Close() }()

	if _, ok := As[Retrainer](b); !ok {
		t.Error("Retrainer lost through the wrapper: dictionary auto-retraining would silently stop")
	}
	if _, ok := As[Statser](b); !ok {
		t.Error("Statser lost through the wrapper: the per-table storage gauges would silently empty")
	}
	// And the forwarded Stats must be the wrapped store's, not an empty stand-in.
	if err := b.UpdateOldText(context.Background(), StartdAd, adText(1)); err != nil {
		t.Fatal(err)
	}
	st, ok := As[Statser](b)
	if !ok {
		t.Fatal("Statser missing")
	}
	if got := st.Stats()[StartdAd].Ads; got != 1 {
		t.Errorf("forwarded Stats reports %d startd ads, want 1 (the wrapper answered instead of delegating)", got)
	}
	// A backend with no such capability must still report that honestly.
	if _, ok := As[Retrainer](&bareBackend{}); ok {
		t.Error("As found Retrainer on a backend that does not implement it")
	}
}

// bareBackend implements nothing beyond the embedded nil interface; it exists only to
// check that As reports a missing capability as missing.
type bareBackend struct{ Backend }

// gatedStore is an in-memory Store whose UpdateOldText runs hook first, so a test can
// wedge a write inside the store the way a future locking bug would.
type gatedStore struct {
	*Store
	hook func()
}

func (g *gatedStore) UpdateOldText(ctx context.Context, t AdType, text string) error {
	if g.hook != nil {
		g.hook()
	}
	return g.Store.UpdateOldText(ctx, t, text)
}

func (g *gatedStore) Base() Backend { return g.Store }

// newGuardedMemStore wraps an in-memory Store in the stall guard, as the collector does
// for COLLECTOR_STORE=memory. hook, if non-nil, runs inside every direct update.
func newGuardedMemStore(t *testing.T, qp QueuePolicy, hook func()) *BufferedBackend {
	t.Helper()
	qp.DirectUntilStalled = true
	b, err := NewBufferedBackendPolicy(&gatedStore{Store: New(), hook: hook}, 20*time.Millisecond, 64, qp, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
