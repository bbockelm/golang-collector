package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// stalledWriter is a backend whose every UpdateBatch hangs until released -- the
// in-process stand-in for the failures this queue exists for: a disk that stops
// completing I/O, a SIGSTOPped database, a storage bug that never returns. It
// deliberately ignores ctx, like a blocking syscall, so a test cannot "pass" because a
// timeout rescued it.
type stalledWriter struct {
	Backend
	startedOnce sync.Once
	started     chan struct{}
	release     chan struct{}
	relOnce     sync.Once

	mu      sync.Mutex
	applied int
}

func (g *stalledWriter) UpdateBatch(ctx context.Context, batch []PendingUpdate) error {
	g.startedOnce.Do(func() { close(g.started) })
	<-g.release
	g.mu.Lock()
	g.applied += len(batch)
	g.mu.Unlock()
	return g.Backend.(BatchWriter).UpdateBatch(ctx, batch)
}

func (g *stalledWriter) Release() { g.relOnce.Do(func() { close(g.release) }) }

func newStalledWriter(t *testing.T) *stalledWriter {
	t.Helper()
	under, err := NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &stalledWriter{Backend: under, started: make(chan struct{}), release: make(chan struct{})}
}

func adText(i int) string {
	return fmt.Sprintf("Name = \"slot%d@host\"\nState = \"Idle\"", i)
}

// TestIngestNeverBlocksOnStalledBackend is the guarantee the deep queue exists for: with
// the backend wedged and never returning, every ad update must still be accepted
// promptly. A blocked update handler blocks the daemon that sent the ad in its TCP
// send -- for a primary collector feeding this one as a CONDOR_VIEW_HOST, that stalls
// the source pool's scheduling, which is strictly worse than losing a monitoring ad.
func TestIngestNeverBlocksOnStalledBackend(t *testing.T) {
	g := newStalledWriter(t)
	// maxBuf 4 -> hardCap 16; a 20-ad queue cap so shedding is reachable in a test.
	b, err := NewBufferedBackendPolicy(g, 20*time.Millisecond, 4, QueuePolicy{
		StallThreshold: 50 * time.Millisecond,
		StallClear:     time.Minute,
		MaxAds:         20,
		MaxBytes:       -1,
		FlushTimeout:   -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { g.Release(); _ = b.Close() }() // never leave the writer wedged (Close would hang)

	ctx := context.Background()
	if err := b.UpdateOldText(ctx, StartdAd, adText(0)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background writer never entered the commit")
	}

	// With the commit wedged, push an order of magnitude past every cap. Each update is
	// timed individually: one slow update is a stalled sender.
	const n = 500
	dropsBefore := testutil.ToFloat64(Metrics.queueDroppedTotal.WithLabelValues(dropAdsCap))
	var worst time.Duration
	for i := 1; i <= n; i++ {
		start := time.Now()
		if err := b.UpdateOldText(ctx, StartdAd, adText(i)); err != nil {
			t.Fatalf("update %d rejected: %v", i, err)
		}
		if d := time.Since(start); d > worst {
			worst = d
		}
	}
	// Generous: the point is "did not wait on the wedged commit", not a latency SLO.
	if worst > 250*time.Millisecond {
		t.Fatalf("slowest update took %v while the backend was wedged; the ingest path blocked on storage", worst)
	}

	if !b.Shedding() {
		t.Fatal("deep queue never engaged despite a permanently wedged backend")
	}
	b.mu.Lock()
	queued := len(b.buf)
	b.mu.Unlock()
	if queued > 20 {
		t.Fatalf("queue held %d ads, want <= the 20-ad cap (memory is not bounded)", queued)
	}
	if drops := testutil.ToFloat64(Metrics.queueDroppedTotal.WithLabelValues(dropAdsCap)) - dropsBefore; drops == 0 {
		t.Fatal("queue_dropped_total did not record the shed ads; the loss would be invisible to an operator")
	}

	// The ad accepted last must survive shedding: it is the freshest information held,
	// and shedLocked explicitly refuses to evict it.
	b.mu.Lock()
	_, kept := b.buf[dedupKey{t: StartdAd, key: mustKey(t, StartdAd, adText(n))}]
	b.mu.Unlock()
	if !kept {
		t.Fatal("the most recently accepted ad was shed; shedding must keep the newest")
	}
}

// mustKey returns the dedup key an ad text hashes to.
func mustKey(t *testing.T, at AdType, text string) string {
	t.Helper()
	k, ok := hashKeyFromText(at, text)
	if !ok {
		t.Fatalf("no hash key for %q", text)
	}
	return string(k)
}

// TestShedModeEngagesOnSlowFlushAndClears checks the dynamic part: the queue stays out
// of the way until the backend is actually slow, and steps back out once it recovers,
// so an operator sees a mode that tracks the backend rather than one permanently on.
func TestShedModeEngagesOnSlowFlushAndClears(t *testing.T) {
	under, err := NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	slow := &delayWriter{Backend: under}
	b, err := NewBufferedBackendPolicy(slow, 10*time.Millisecond, 1024, QueuePolicy{
		StallThreshold: 100 * time.Millisecond,
		StallClear:     300 * time.Millisecond,
		FlushTimeout:   -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	ctx := context.Background()
	if err := b.UpdateOldText(ctx, StartdAd, adText(1)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if b.Shedding() {
		t.Fatal("deep queue engaged against a healthy backend; it must stay dormant until a stall")
	}

	// One flush slower than the threshold is enough to engage it.
	slow.set(250 * time.Millisecond)
	if err := b.UpdateOldText(ctx, StartdAd, adText(2)); err != nil {
		t.Fatal(err)
	}
	if !waitFor(500*time.Millisecond, b.Shedding) {
		t.Fatal("a flush well past StallThreshold did not engage the deep queue")
	}

	// Recovered: after StallClear with no slow flush, it disengages again.
	slow.set(0)
	if !waitFor(3*time.Second, func() bool { b.shedding(); return !b.Shedding() }) {
		t.Fatal("deep queue stayed engaged after StallClear elapsed with a healthy backend")
	}
}

// delayWriter adds a settable delay to every commit, standing in for a backend that is
// slow rather than wedged.
type delayWriter struct {
	Backend
	mu sync.Mutex
	d  time.Duration
}

func (w *delayWriter) set(d time.Duration) { w.mu.Lock(); w.d = d; w.mu.Unlock() }

func (w *delayWriter) UpdateBatch(ctx context.Context, batch []PendingUpdate) error {
	w.mu.Lock()
	d := w.d
	w.mu.Unlock()
	time.Sleep(d)
	return w.Backend.(BatchWriter).UpdateBatch(ctx, batch)
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestInvalidateDeferredWhileStalled covers the half of the ingest path that is not an
// update. A view-collector feed interleaves UPDATE_* and INVALIDATE_* down one
// persistent command socket, so an invalidation that blocks on stalled storage stalls
// that socket exactly as an update would -- queueing only the updates would leave the
// hole wide open. The deferred invalidation must still be applied AFTER the queued
// updates, or the ad it removes would be resurrected by an update buffered alongside it.
func TestInvalidateDeferredWhileStalled(t *testing.T) {
	g := newStalledWriter(t)
	b, err := NewBufferedBackendPolicy(g, 20*time.Millisecond, 4, QueuePolicy{
		StallThreshold: 50 * time.Millisecond,
		StallClear:     time.Minute,
		FlushTimeout:   -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { g.Release(); _ = b.Close() }()

	ctx := context.Background()
	for i := 0; i < 8; i++ {
		if err := b.UpdateOldText(ctx, StartdAd, adText(i)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-g.started:
	case <-time.After(5 * time.Second):
		t.Fatal("background writer never entered the commit")
	}
	if !waitFor(2*time.Second, func() bool { b.shedding(); return b.Shedding() }) {
		t.Fatal("deep queue never engaged against a wedged backend")
	}

	// slot3 re-advertises and is then invalidated: the ordering case that matters.
	if err := b.UpdateOldText(ctx, StartdAd, adText(3)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := b.Invalidate(ctx, StartdAd, "", mustParse(t, `Name = "slot3@host"`))
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("deferred invalidate: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Invalidate blocked on the wedged backend; a view feed's command socket would be stalled")
	}

	// Let storage recover and settle. The settled count is the assertion, not slot3's
	// absence on its own: before the queue drains, slot3 is absent merely because it has
	// not been written yet, which would pass for the wrong reason.
	g.Release()
	if !waitFor(10*time.Second, func() bool { n, _ := b.Len(ctx, StartdAd); return n == 7 }) {
		n, _ := b.Len(ctx, StartdAd)
		t.Fatalf("settled at %d ads, want 7 (8 advertised, 1 invalidated) -- queued updates or the invalidation were lost", n)
	}
	if _, ok := b.Get(ctx, StartdAd, mustParse(t, `Name = "slot3@host"`)); ok {
		t.Fatal("slot3 survived: the deferred invalidation was applied before the update it must outrank, which resurrected the ad")
	}
}

// TestQueueByteCapBoundsMemory checks the bound that actually matters in production: a
// pool of large ads must be capped by memory, not by ad count. An 8 KiB startd ad times
// the 200k-ad default would be 1.6 GB of queue.
func TestQueueByteCapBoundsMemory(t *testing.T) {
	g := newStalledWriter(t)
	const maxBytes = 256 << 10
	b, err := NewBufferedBackendPolicy(g, 20*time.Millisecond, 4, QueuePolicy{
		StallThreshold: 50 * time.Millisecond,
		StallClear:     time.Minute,
		MaxAds:         -1, // only the byte cap applies
		MaxBytes:       maxBytes,
		FlushTimeout:   -1,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { g.Release(); _ = b.Close() }()

	ctx := context.Background()
	pad := strings.Repeat("x", 8<<10)
	before := testutil.ToFloat64(Metrics.queueDroppedTotal.WithLabelValues(dropBytesCap))
	for i := 0; i < 400; i++ {
		fat := fmt.Sprintf("Name = \"slot%d@host\"\nState = \"Idle\"\nPad = \"%s\"", i, pad)
		if err := b.UpdateOldText(ctx, StartdAd, fat); err != nil {
			t.Fatal(err)
		}
	}
	b.mu.Lock()
	bytes := b.bufBytes
	ads := len(b.buf)
	b.mu.Unlock()
	if bytes > maxBytes {
		t.Fatalf("queue holds %d bytes, over the %d-byte cap (%d ads)", bytes, maxBytes, ads)
	}
	if testutil.ToFloat64(Metrics.queueDroppedTotal.WithLabelValues(dropBytesCap)) <= before {
		t.Fatal("byte-cap shedding was not attributed to bytes_cap, so a memory-bound stall would be misdiagnosed as an ad-count one")
	}
}

// TestFlushTimeoutUnwedgesWriter covers the writer side: against a backend that honors
// cancellation but never answers (a TCP connection that is open but dead), the flush
// must give up so the writer can drop the connection and retry, instead of holding the
// queue forever. The ads it was carrying must survive for the retry.
func TestFlushTimeoutUnwedgesWriter(t *testing.T) {
	under, err := NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	hang := &ctxHangWriter{Backend: under, hang: make(chan struct{})}
	b, err := NewBufferedBackendPolicy(hang, 20*time.Millisecond, 4, QueuePolicy{
		StallThreshold: 50 * time.Millisecond,
		StallClear:     time.Minute,
		FlushTimeout:   150 * time.Millisecond,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { hang.unhang(); _ = b.Close() }()

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := b.UpdateOldText(ctx, StartdAd, adText(i)); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(5*time.Second, func() bool { return hang.attempts() >= 2 }) {
		t.Fatalf("flush was attempted %d time(s); the timeout did not release the wedged writer for a retry", hang.attempts())
	}
	// All four ads must survive every timed-out attempt: a bounded flush requeues what
	// it was carrying, it does not discard it. Checked by letting the backend recover
	// rather than by peeking at the buffer, which legitimately looks empty while an
	// attempt is in flight.
	hang.unhang()
	if !waitFor(10*time.Second, func() bool { n, _ := b.Len(context.Background(), StartdAd); return n == 4 }) {
		n, _ := b.Len(context.Background(), StartdAd)
		t.Fatalf("%d ads landed after the backend recovered, want 4 (a timed-out flush discarded ads)", n)
	}
}

// ctxHangWriter blocks until ctx ends -- a backend that is reachable and cancellable but
// never completes.
type ctxHangWriter struct {
	Backend
	hang    chan struct{}
	hangOne sync.Once
	mu      sync.Mutex
	n       int
}

func (w *ctxHangWriter) unhang() { w.hangOne.Do(func() { close(w.hang) }) }

func (w *ctxHangWriter) attempts() int { w.mu.Lock(); defer w.mu.Unlock(); return w.n }

func (w *ctxHangWriter) UpdateBatch(ctx context.Context, batch []PendingUpdate) error {
	w.mu.Lock()
	w.n++
	w.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.hang:
		return w.Backend.(BatchWriter).UpdateBatch(context.Background(), batch)
	}
}
