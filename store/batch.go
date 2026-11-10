package store

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
)

// BatchWriter is an optional Backend capability: apply many ad upserts in one
// transaction, so a burst of updates costs one commit / round trip instead of
// one per ad. A database backend implements it; the in-memory backend does not
// need it (BufferedBackend requires it).
type BatchWriter interface {
	UpdateBatch(ctx context.Context, batch []PendingUpdate) error
}

// PendingUpdate is one buffered ad upsert: an ad in old-ClassAd wire text for a
// table, or (Pvt) a startd public+private pair.
type PendingUpdate struct {
	Type    AdType // the ad's table; StartdAd for a private-ad update
	Text    string // old-ClassAd wire text (ordinary ad, or the public ad when Pvt)
	PvtText string // the startd private ad text (only when Pvt)
	Pvt     bool
}

// dedupKey identifies the ad a PendingUpdate targets, so repeated updates to the
// same ad within a window collapse to the latest.
type dedupKey struct {
	t   AdType
	key string
}

func (p PendingUpdate) dedupKey() (dedupKey, bool) {
	if p.Pvt {
		k, ok := hashKeyFromText(StartdAd, p.Text)
		return dedupKey{StartdPvtAd, string(k)}, ok
	}
	k, ok := hashKeyFromText(p.Type, p.Text)
	return dedupKey{p.Type, string(k)}, ok
}

// BufferedBackend wraps a BatchWriter backend with a short "Nagle" buffer:
// wire-text updates (UpdateOldText/UpdatePvt) accumulate, deduplicated by ad, and
// flush as one batched transaction on a size or time threshold. It collapses a
// daemon's rapid re-advertises and turns a burst of updates into one commit /
// round trip -- most valuable for a remote database where each commit is a round
// trip.
//
// It is also what keeps the ingest path from ever blocking on storage. An update
// handler that blocks blocks the daemon that sent the ad -- for a primary collector
// feeding this one as a CONDOR_VIEW_HOST, in its TCP send, which is why a storage
// problem here used to degrade the whole source pool. So a producer only ever takes
// the buffer lock long enough to store wire text: it never commits, never waits on a
// commit, and never waits for room. When the backend stalls the buffer grows into the
// deep update queue (see QueuePolicy), and at its cap it sheds ads rather than apply
// backpressure. Losing an ad costs one update interval -- every daemon re-advertises --
// while blocking the ingest socket costs the source pool its scheduling cycle.
//
// To keep results consistent, every read/invalidate/expire flushes the buffer
// first, so a query never misses a just-buffered ad and an invalidation is never
// overwritten by a stale buffered write. Buffered ads are therefore visible after
// at most one window (comparable to the C++ collector's update latency). The
// negotiator's own materialized Update and all reads pass straight through.
type BufferedBackend struct {
	Backend // reads and non-buffered writes pass through
	bw      BatchWriter
	window  time.Duration
	maxBuf  int // high-water mark: signal the writer to flush
	hardCap int // ordinary ceiling; reaching it means the writer is behind -> engage the deep queue
	tripAt  int // where that happens: hardCap, or the queue's own ad cap if it is smaller
	qp      QueuePolicy
	log     func(error)

	// flushMu serializes the apply side: only one flush may be snapshotting and
	// applying at a time. Without it a second flush can overtake the first, applying an
	// invalidation (or a newer ad) against the store before the batch it was supposed to
	// follow has landed -- the removed ad comes back, or an older value wins. mu, by
	// contrast, is held only for the buffer itself and never across an apply.
	flushMu sync.Mutex

	mu       sync.Mutex
	buf      map[dedupKey]PendingUpdate
	bufBytes int64               // approximate memory held by buf, charged against QueuePolicy.MaxBytes
	inval    []pendingInvalidate // FIFO of invalidations deferred while shedding
	flushSig chan struct{}       // buffered(1); wakes the background writer at the high-water mark
	stop     chan struct{}
	wg       sync.WaitGroup

	// Deep-queue state, read on every update, so it lives in atomics rather than
	// behind mu: stallUntil is when the mode lapses, shedOn is the current mode (for
	// edge-triggered logging and the gauge), flushStart is the start of the oldest
	// in-flight flush (0 when idle), which is how a stall is noticed while it is
	// still happening rather than only after it ends.
	stallUntil atomic.Int64
	shedOn     atomic.Bool
	flushStart atomic.Int64
}

// pendingInvalidate is one deferred invalidation: the arguments of a Backend.Invalidate
// call queued while the backend is stalled.
type pendingInvalidate struct {
	t          AdType
	constraint string
	keyAd      *classad.ClassAd
}

// Base returns the wrapped backend, so a caller can reach an optional capability the
// wrapper does not itself expose (e.g. the RPCBackend's DBDiagnostics for metrics).
func (b *BufferedBackend) Base() Backend { return b.Backend }

var (
	_ Backend              = (*BufferedBackend)(nil)
	_ RawQueryer           = (*BufferedBackend)(nil)
	_ ProjectedRawQueryer  = (*BufferedBackend)(nil)
	_ RawStreamer          = (*BufferedBackend)(nil)
	_ ProjectedRawStreamer = (*BufferedBackend)(nil)
	_ DurableUpdater       = (*BufferedBackend)(nil)
	_ Statser              = (*BufferedBackend)(nil)
	_ Retrainer            = (*BufferedBackend)(nil)
)

// DurableUpdater is an optional Backend capability: apply one ad update and return
// only once it is durably stored. The ACK-update path (UPDATE_*_WITH_ACK) uses it
// so a BufferedBackend -- which normally defers writes into its Nagle buffer --
// still acknowledges only after the write is committed, never merely buffered.
type DurableUpdater interface {
	UpdateOldTextDurable(ctx context.Context, t AdType, text string) error
}

// DurableUpdate applies text to st and returns only once it is durable: through
// DurableUpdater when the backend provides it, else a plain UpdateOldText (already
// synchronous for unbuffered backends).
func DurableUpdate(ctx context.Context, st Backend, t AdType, text string) error {
	if du, ok := st.(DurableUpdater); ok {
		return du.UpdateOldTextDurable(ctx, t, text)
	}
	return st.UpdateOldText(ctx, t, text)
}

// NewBufferedBackend wraps under (which must implement BatchWriter) with a buffer
// that flushes every window or once maxBuf ads accumulate, using DefaultQueuePolicy
// for its stall behavior. logErr, if non-nil, receives background-flush errors.
func NewBufferedBackend(under Backend, window time.Duration, maxBuf int, logErr func(error)) (*BufferedBackend, error) {
	return NewBufferedBackendPolicy(under, window, maxBuf, DefaultQueuePolicy, logErr)
}

// NewBufferedBackendPolicy is NewBufferedBackend with an explicit QueuePolicy, which
// governs when the deep update queue engages, how large it may grow, and how long it
// stays engaged after a stall. Unset fields take their DefaultQueuePolicy values.
func NewBufferedBackendPolicy(under Backend, window time.Duration, maxBuf int, qp QueuePolicy, logErr func(error)) (*BufferedBackend, error) {
	bw, ok := under.(BatchWriter)
	if !ok {
		return nil, fmt.Errorf("collector: backend %T does not support batched writes", under)
	}
	if maxBuf <= 0 {
		maxBuf = 2048
	}
	if logErr == nil {
		logErr = func(error) {}
	}
	b := &BufferedBackend{
		Backend: under,
		bw:      bw,
		window:  window,
		maxBuf:  maxBuf,
		// The writer may fall behind a burst; let the buffer grow to a multiple of the
		// high-water mark before treating it as a stall, so an ordinary slow commit does
		// not engage the deep queue.
		hardCap:  maxBuf * 4,
		qp:       qp.withDefaults(),
		log:      logErr,
		buf:      make(map[dedupKey]PendingUpdate),
		flushSig: make(chan struct{}, 1),
		stop:     make(chan struct{}),
	}
	// Engage the deep queue at whichever comes first: the ordinary ceiling, or the
	// queue's own ad cap if an operator configured one below it. Without this an
	// undersized cap would shed ads while still never deferring invalidations, which
	// would leave the ingest socket blocking on exactly the half of the traffic the ad
	// queue does not cover.
	b.tripAt = b.hardCap
	if b.qp.MaxAds > 0 && b.qp.MaxAds < b.tripAt {
		b.tripAt = b.qp.MaxAds
	}
	if window > 0 {
		b.wg.Add(1)
		go func() { defer b.wg.Done(); b.run() }()
	}
	return b, nil
}

// signal wakes the background writer without blocking. A coalesced pending signal is
// fine: the writer drains everything buffered regardless of how many signals it stands for.
func (b *BufferedBackend) signal() {
	select {
	case b.flushSig <- struct{}{}:
	default:
	}
}

// putLocked stores p under k, maintaining the byte accounting across a coalesced
// overwrite (the superseded ad's bytes are released).
func (b *BufferedBackend) putLocked(k dedupKey, p PendingUpdate) {
	if old, ok := b.buf[k]; ok {
		b.bufBytes -= pendingSize(old)
	}
	b.buf[k] = p
	b.bufBytes += pendingSize(p)
}

// shedLocked evicts queued updates until the queue is back inside its caps, never
// evicting keep (the ad just stored -- the freshest thing in the queue). It returns the
// number dropped and the cap that was exceeded.
//
// The victim is whichever entry a map iteration reaches first, which Go starts at a
// random bucket. That arbitrariness is the point. Every entry is a *different* daemon's
// latest ad, and a daemon that re-advertises often keeps refreshing its entry, so any
// age-ordered policy would evict precisely the daemons that advertise least often and
// starve them for the whole stall; uniform-ish eviction spreads the loss instead.
func (b *BufferedBackend) shedLocked(keep dedupKey) (int, string) {
	dropped, reason := 0, dropAdsCap
	for {
		overAds := b.qp.MaxAds > 0 && len(b.buf) > b.qp.MaxAds
		overBytes := b.qp.MaxBytes > 0 && b.bufBytes > b.qp.MaxBytes
		if !overAds && !overBytes {
			return dropped, reason
		}
		if overAds {
			reason = dropAdsCap
		} else {
			reason = dropBytesCap
		}
		victim, found := dedupKey{}, false
		for k := range b.buf {
			if k == keep {
				continue
			}
			victim, found = k, true
			break
		}
		if !found {
			return dropped, reason // nothing but the ad we just took; keep it
		}
		b.bufBytes -= pendingSize(b.buf[victim])
		delete(b.buf, victim)
		dropped++
	}
}

// observeDepth publishes the queue's occupancy. Called with mu held.
func (b *BufferedBackend) observeDepthLocked() {
	Metrics.queueAds.Set(float64(len(b.buf)))
	Metrics.queueBytes.Set(float64(b.bufBytes))
	Metrics.queueInvalidations.Set(float64(len(b.inval)))
}

// run is the single background writer: it drains and commits the buffer on the window
// ticker OR whenever a producer signals the high-water mark. Committing here (not on the
// producer's goroutine) is what lets producers keep buffering while a batch commits --
// the writer picks up everything that accumulated during the previous commit on its next
// loop, so commits pipeline back-to-back instead of stalling the update stream.
func (b *BufferedBackend) run() {
	t := time.NewTicker(b.window)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			_ = b.flush(context.Background()) // final drain so shutdown loses nothing buffered
			return
		case <-t.C:
			// The background flush is not tied to a request, so it uses a background
			// context: the underlying backend's own retry budget bounds it. A failure
			// here is already logged/handled by flush.
			_ = b.flush(context.Background())
		case <-b.flushSig:
			_ = b.flush(context.Background())
		}
	}
}

// enqueue buffers p, and is the whole of what an ad update costs the ingest path:
// hash the ad's key, take a mutex, store wire text, release it. With a background
// writer running (window > 0) it never commits on the caller's goroutine and never
// waits for one -- not while a batch is in flight, not when the buffer is full, not
// when the backend has stopped making progress entirely.
//
// Reaching hardCap means the writer is not keeping up, which is the same condition a
// slow flush reports, so it engages the deep queue too. Past the deep queue's caps the
// ad is stored and an arbitrary other queued ad is dropped (shedLocked), keeping the
// freshest information and bounding memory without ever making the sender wait.
//
// Without a background writer (window == 0, e.g. tests) it flushes inline when full,
// as before -- there is no one else to do the work.
func (b *BufferedBackend) enqueue(ctx context.Context, p PendingUpdate) error {
	// The direct path is decided BEFORE the ad is keyed. dedupKey hashes the ad's name
	// out of its wire text, which is most of what an in-memory update costs; computing
	// it only to hand the text to a backend that keys it again doubled the cost of the
	// default store's fast path. A direct update needs no dedup key -- nothing is being
	// deduplicated -- and the backend reports an unkeyable ad itself.
	if b.window > 0 && b.qp.DirectUntilStalled && !b.shedding() {
		return b.applyDirect(ctx, p)
	}

	k, ok := p.dedupKey()
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", p.Type)
	}
	if b.window <= 0 {
		b.mu.Lock()
		b.putLocked(k, p)
		n := len(b.buf)
		b.mu.Unlock()
		if n >= b.maxBuf {
			return b.flush(ctx)
		}
		return nil
	}

	deep := b.shedding()
	var dropped int
	var reason string
	var tripped bool

	b.mu.Lock()
	b.putLocked(k, p)
	n := len(b.buf)
	if !deep && n >= b.tripAt {
		deep, tripped = true, true
	}
	// The caps hold whether or not the deep queue is engaged, so the queue never grows
	// past what the operator configured. With the defaults they are far above the
	// ordinary ceiling, so this is two comparisons on the healthy path.
	dropped, reason = b.shedLocked(k)
	n = len(b.buf)
	queued, queuedBytes := len(b.buf), b.bufBytes
	if deep {
		// Occupancy is published per update only while the queue is engaged, when a
		// flush may be a long way off and the gauges are the only live view of how much
		// of the stall the buffer is still absorbing. On the healthy path the flush
		// publishes them, which is as often as they can meaningfully change.
		b.observeDepthLocked()
	}
	b.mu.Unlock()

	// Logging and metrics outside the lock: a stall must not serialize producers.
	if tripped {
		Metrics.backpressureTotal.Inc()
		b.markStall(fmt.Sprintf("the update buffer reached its high-water mark (%d ads) with the backend still committing", b.tripAt))
	}
	if dropped > 0 {
		Metrics.queueDroppedTotal.WithLabelValues(reason).Add(float64(dropped))
		logShed(dropped, reason, queued, queuedBytes)
	}
	if n >= b.maxBuf {
		b.signal()
	}
	return nil
}

// shedDropLogInterval rate-limits the shed WARN: under a sustained stall every single
// update drops one, and a per-ad log line would itself become the outage.
const shedDropLogInterval = 10 * time.Second

var shedLogLast atomic.Int64

// logShed reports dropped ads at most once per shedDropLogInterval, so the log records
// that shedding is happening without a line per lost ad.
func logShed(dropped int, reason string, queued int, queuedBytes int64) {
	now := time.Now().UnixNano()
	last := shedLogLast.Load()
	if now-last < int64(shedDropLogInterval) || !shedLogLast.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("collector: update queue full; shedding ads while the backend is stalled (senders are never blocked; daemons re-advertise)",
		"dropped_now", dropped, "reason", reason, "queued_ads", queued, "queued_bytes", queuedBytes)
}

// flush applies everything queued, in the order that keeps the result correct: the
// buffered ad updates first, then any invalidations deferred behind them (so an
// invalidation cannot be silently undone by an update that was sitting in the buffer
// when it arrived).
//
// It is also where a stall is detected. The flush's start is published for the whole
// time it runs -- that is how shedding() can notice an outage while it is still
// happening instead of only after it ends -- and a flush that takes at least
// StallThreshold engages the deep queue on the way out.
func (b *BufferedBackend) flush(ctx context.Context) error {
	b.mu.Lock()
	empty := len(b.buf) == 0 && len(b.inval) == 0
	b.mu.Unlock()
	if empty {
		return nil
	}
	b.flushMu.Lock()
	defer b.flushMu.Unlock()
	return b.flushLocked(ctx)
}

// tryFlush is flush for callers that must not wait: if another flush is already
// applying, it returns at once instead of queueing behind it.
//
// Every read path uses it. A read that waited would be waiting on exactly the storage
// that is stalled, which is how a storage problem reaches the query path -- and it buys
// nothing: an ad being flushed right now is not in the store yet either way, so a
// reader that skips sees precisely what a reader that waited for the previous flush
// would have seen. Only a flush actually in progress causes a skip; an idle buffer is
// flushed normally, so ordinary read-your-writes behavior is unchanged.
func (b *BufferedBackend) tryFlush(ctx context.Context) error {
	b.mu.Lock()
	empty := len(b.buf) == 0 && len(b.inval) == 0
	b.mu.Unlock()
	if empty {
		return nil
	}
	if !b.flushMu.TryLock() {
		return nil
	}
	defer b.flushMu.Unlock()
	return b.flushLocked(ctx)
}

// flushLocked is the body of a flush; the caller holds flushMu.
func (b *BufferedBackend) flushLocked(ctx context.Context) error {
	ctx, cancel := b.flushContext(ctx)
	defer cancel()

	start := time.Now()
	startNano := start.UnixNano()
	// Publish this flush's start for as long as it runs, so a producer can see that the
	// backend is stalling right now rather than learning it only once the flush ends.
	// flushMu makes this exclusive; the CAS pair is belt-and-braces against a future
	// caller that reaches flushLocked another way.
	b.flushStart.CompareAndSwap(0, startNano)
	defer func() {
		b.flushStart.CompareAndSwap(startNano, 0)
		if d := time.Since(start); b.qp.StallThreshold > 0 && d >= b.qp.StallThreshold {
			b.markStall("a backend flush took " + d.Round(time.Millisecond).String())
		}
	}()

	// Both queues are snapshotted under ONE lock acquisition, so an invalidation is
	// applied against exactly the updates that preceded it. Taking them separately lets
	// an invalidation ride out with an earlier flush's batch while the update it was
	// meant to outrank is still buffered -- the removed ad then reappears.
	b.mu.Lock()
	batch, keys := b.takeBufLocked()
	pend := b.inval
	b.inval = nil
	b.observeDepthLocked()
	b.mu.Unlock()

	err := b.applyBatch(ctx, batch, keys)
	if ierr := b.applyInvalidations(ctx, pend); ierr != nil && err == nil {
		err = ierr
	}
	return err
}

// takeBufLocked empties the ad buffer, returning its contents and their keys (parallel
// slices: keys[i] is batch[i]'s, so a failed apply can put ads back).
func (b *BufferedBackend) takeBufLocked() ([]PendingUpdate, []dedupKey) {
	if len(b.buf) == 0 {
		return nil, nil
	}
	batch := make([]PendingUpdate, 0, len(b.buf))
	keys := make([]dedupKey, 0, len(b.buf))
	for k, p := range b.buf {
		batch = append(batch, p)
		keys = append(keys, k)
	}
	b.buf = make(map[dedupKey]PendingUpdate)
	b.bufBytes = 0
	return batch, keys
}

// flushContext bounds one flush by QueuePolicy.FlushTimeout, unless ctx already carries
// an earlier deadline. The bound is what lets a writer wedged on a connection that is
// open but dead give up, drop it and redial; it cannot interrupt a blocking disk
// syscall, which is why the queue, not the timeout, is the actual protection.
func (b *BufferedBackend) flushContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if b.qp.FlushTimeout <= 0 {
		return ctx, func() {}
	}
	if dl, ok := ctx.Deadline(); ok && time.Until(dl) <= b.qp.FlushTimeout {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, b.qp.FlushTimeout)
}

// queueInvalidate defers iv for the background writer, dropping the oldest queued
// invalidation if the FIFO is full. Dropping one costs less than dropping an update:
// the ad it targeted simply lingers until its ATTR_CLASSAD_LIFETIME expires, which is
// the same path that already reaps a daemon that died without invalidating.
func (b *BufferedBackend) queueInvalidate(iv pendingInvalidate) {
	var evicted bool
	b.mu.Lock()
	if b.qp.MaxInvalidates > 0 && len(b.inval) >= b.qp.MaxInvalidates {
		copy(b.inval, b.inval[1:])
		b.inval = b.inval[:len(b.inval)-1]
		evicted = true
	}
	b.inval = append(b.inval, iv)
	b.observeDepthLocked()
	b.mu.Unlock()
	if evicted {
		Metrics.queueDroppedTotal.WithLabelValues(dropInvalidateCap).Inc()
	}
	b.signal()
}

// applyInvalidations applies deferred invalidations in arrival order. If ctx ends
// mid-drain the remainder goes back on the front of the FIFO (ahead of anything queued
// since) for the next flush. A failed invalidation is logged and dropped -- the ad it
// targeted expires on its own -- rather than blocking the ones behind it.
func (b *BufferedBackend) applyInvalidations(ctx context.Context, pend []pendingInvalidate) error {
	if len(pend) == 0 {
		return nil
	}
	for i, iv := range pend {
		if ctx.Err() != nil {
			b.requeueInvalidations(pend[i:])
			return ctx.Err()
		}
		if _, err := b.Backend.Invalidate(ctx, iv.t, iv.constraint, iv.keyAd); err != nil {
			if ctx.Err() != nil {
				b.requeueInvalidations(pend[i:])
				return err
			}
			b.log(fmt.Errorf("collector: deferred invalidation of %s ads failed (dropping it; the ads expire on their own lifetime): %w", iv.t, err))
		}
	}
	return nil
}

// requeueInvalidations puts an unapplied remainder back at the FRONT of the FIFO, ahead
// of anything queued while this flush ran, so arrival order survives a cut-short drain.
// The result is re-trimmed to the cap, oldest first, since the remainder plus what
// arrived meanwhile can exceed it.
func (b *BufferedBackend) requeueInvalidations(rest []pendingInvalidate) {
	b.mu.Lock()
	b.inval = append(append([]pendingInvalidate(nil), rest...), b.inval...)
	var evicted int
	if n := b.qp.MaxInvalidates; n > 0 && len(b.inval) > n {
		evicted = len(b.inval) - n
		b.inval = b.inval[evicted:]
	}
	b.observeDepthLocked()
	b.mu.Unlock()
	if evicted > 0 {
		Metrics.queueDroppedTotal.WithLabelValues(dropInvalidateCap).Add(float64(evicted))
	}
}

// applyBatch applies one drained batch under ctx. The buffer was swapped out before the
// call, so producers are not blocked during the (possibly remote) apply.
//
// On failure the ads are not silently lost. If ctx cut the apply short (a
// read-triggered flush carrying a caller's short deadline, or the flush timeout), the
// ads are re-enqueued so the background flusher retries them with a full budget -- a
// newer update for the same ad that arrived meanwhile wins. Any other failure means the
// underlying backend already exhausted its retry budget (or hit a permanent error), so
// the ads are dropped with a log, per the bounded-retry policy (daemons re-advertise).
func (b *BufferedBackend) applyBatch(ctx context.Context, batch []PendingUpdate, keys []dedupKey) error {
	if len(batch) == 0 {
		return nil
	}

	// Metrics: one flushed batch, its ad count (a batch touches ~all shards, so
	// this drives the store's lock contention), and its wall-clock time.
	Metrics.batchesTotal.Inc()
	Metrics.adsPerBatch.Observe(float64(len(batch)))
	flushStart := time.Now()
	err := b.bw.UpdateBatch(ctx, batch)
	flushDur := time.Since(flushStart)
	Metrics.batchSeconds.Observe(flushDur.Seconds())
	MaybeLogSlow("batch-flush", flushDur, "ads", len(batch))
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		// Caller's deadline cut this flush short; keep the ads for the background
		// flusher, without clobbering any newer update that arrived during the apply.
		b.mu.Lock()
		for i, k := range keys {
			if _, newer := b.buf[k]; !newer {
				b.putLocked(k, batch[i])
			}
		}
		b.observeDepthLocked()
		b.mu.Unlock()
		return err
	}
	// The batch failed as a unit, but the cause is usually ONE bad ad (e.g. an
	// unparseable value) among many good ones. Re-apply each update on its own so
	// a single culprit does not drop the rest: keep the ads that succeed, and log
	// + drop only those that still fail -- with the offending ad's text, so an
	// operator can see exactly which ad and value the sender forwarded (the wire
	// bytes are otherwise encrypted).
	var failed int
	for i := range batch {
		if e := b.bw.UpdateBatch(ctx, batch[i:i+1]); e != nil {
			if ctx.Err() != nil {
				// Deadline hit mid-isolation: re-enqueue the not-yet-applied
				// remainder for the background flusher (newer updates win).
				b.mu.Lock()
				for j := i; j < len(batch); j++ {
					if _, newer := b.buf[keys[j]]; !newer {
						b.putLocked(keys[j], batch[j])
					}
				}
				b.observeDepthLocked()
				b.mu.Unlock()
				return e
			}
			failed++
			b.log(fmt.Errorf("collector: rejected ad update name=%q type=%s (dropping just this one, keeping the rest): %w\n--- ad ---\n%s\n--- end ad ---",
				AdName(batch[i].Text), batch[i].Type, e, AdExcerpt(rejectedText(batch[i]))))
		}
	}
	if failed > 0 {
		b.log(fmt.Errorf("collector: dropped %d of %d buffered ad update(s); the rest were applied", failed, len(batch)))
	}
	// The buffer is fully drained (good ads applied; bad ones logged above), so
	// report success -- a rejected ad is an operator-visible drop, not a flush
	// failure to propagate to a flush-before-read.
	return nil
}

// rejectedText returns the ad text to show for a rejected PendingUpdate: the ad,
// plus the private ad appended when this is a startd public+private update (the
// parse failure may be in either).
func rejectedText(p PendingUpdate) string {
	if p.Pvt && p.PvtText != "" {
		return p.Text + "\n# --- private ad ---\n" + p.PvtText
	}
	return p.Text
}

// applyDirect writes p straight through to the backend on the caller's goroutine, which
// is what DirectUntilStalled does while the backend is healthy, and watches that call for
// the stall that switches the mode.
//
// It watches in two ways because one is not enough. The duration on the way out catches a
// backend that is slow; publishing the call's start for its whole lifetime catches one
// that has stopped returning at all, which a completed-duration check can never see. That
// published start is what every other producer reads in shedding(), so a single wedged
// update diverts everyone else into the queue instead of into the same wedged call.
func (b *BufferedBackend) applyDirect(ctx context.Context, p PendingUpdate) error {
	start := time.Now()
	nano := start.UnixNano()
	// Only the claimant clears the slot. A producer that does not get it is simply not
	// the one being timed; if it is the one that wedges, the next producer to claim an
	// empty slot starts the clock, so detection is delayed by at most one call.
	claimed := b.flushStart.CompareAndSwap(0, nano)

	var err error
	if p.Pvt {
		err = b.Backend.UpdatePvt(ctx, p.Text, p.PvtText)
	} else {
		err = b.Backend.UpdateOldText(ctx, p.Type, p.Text)
	}

	if claimed {
		b.flushStart.CompareAndSwap(nano, 0)
	}
	if d := time.Since(start); b.qp.StallThreshold > 0 && d >= b.qp.StallThreshold {
		b.markStall("an ad update took " + d.Round(time.Millisecond).String())
	}
	return err
}

// --- optional capabilities forwarded from the wrapped backend ---
//
// Without these the wrapper would ANSWER for the backend it hides: it embeds the Backend
// interface, so only Backend's methods are promoted and a capability the wrapped store
// implements becomes invisible. Reached through store.As, which also unwraps via Base(),
// so a caller gets the same answer wrapped or not.

// Stats forwards per-table introspection metrics, so wrapping a backend does not empty
// the collector's storage gauges.
func (b *BufferedBackend) Stats() map[AdType]collections.Stats {
	if s, ok := b.Backend.(Statser); ok {
		return s.Stats()
	}
	return nil
}

// RetrainDict forwards one dictionary-retraining pass.
func (b *BufferedBackend) RetrainDict(sampleMax int) {
	if r, ok := b.Backend.(Retrainer); ok {
		r.RetrainDict(sampleMax)
	}
}

// StartAutoRetrain forwards periodic dictionary retraining, so wrapping the in-memory
// store does not silently turn off its memory-footprint lever. It returns a no-op stop
// for a backend that manages its own storage.
func (b *BufferedBackend) StartAutoRetrain(interval time.Duration, sampleMax int) func() {
	if r, ok := b.Backend.(Retrainer); ok {
		return r.StartAutoRetrain(interval, sampleMax)
	}
	return func() {}
}

// --- buffered write paths ---

func (b *BufferedBackend) UpdateOldText(ctx context.Context, t AdType, text string) error {
	return b.enqueue(ctx, PendingUpdate{Type: t, Text: text})
}

func (b *BufferedBackend) UpdatePvt(ctx context.Context, publicText, pvtText string) error {
	return b.enqueue(ctx, PendingUpdate{Type: StartdAd, Text: publicText, PvtText: pvtText, Pvt: true})
}

// UpdateOldTextDurable flushes any buffered writes (so an earlier buffered update
// of the same ad cannot overwrite this one) and applies text synchronously through
// the underlying backend, returning only once it is committed. The ACK-update path
// uses it so the acknowledgment follows durability, not mere buffering.
func (b *BufferedBackend) UpdateOldTextDurable(ctx context.Context, t AdType, text string) error {
	if err := b.flush(ctx); err != nil {
		return err
	}
	return b.Backend.UpdateOldText(ctx, t, text)
}

// --- flush-then-passthrough paths (consistency) ---

func (b *BufferedBackend) Update(ctx context.Context, t AdType, ad *classad.ClassAd) error {
	if err := b.flush(ctx); err != nil {
		return err
	}
	return b.Backend.Update(ctx, t, ad)
}

func (b *BufferedBackend) Query(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[*classad.ClassAd], error) {
	if err := b.tryFlush(ctx); err != nil {
		return nil, err
	}
	return b.Backend.Query(ctx, t, constraint, limit)
}

func (b *BufferedBackend) QueryRaw(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	if err := b.tryFlush(ctx); err != nil {
		return nil, err
	}
	rq, ok := b.Backend.(RawQueryer)
	if !ok {
		return nil, fmt.Errorf("collector: backend %T has no raw query", b.Backend)
	}
	return rq.QueryRaw(ctx, t, constraint, limit)
}

// QueryRawRedacted flushes, then serves a source-redacted raw query. The buffer
// itself cannot strip private attributes, so this only delegates; when the
// underlying backend lacks the capability it returns ErrRedactionNotSupported
// and the caller redacts results itself (its type assertion on the wrapper
// necessarily succeeds, so the capability is probed at call time).
func (b *BufferedBackend) QueryRawRedacted(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	if err := b.tryFlush(ctx); err != nil {
		return nil, err
	}
	rq, ok := b.Backend.(RedactedRawQueryer)
	if !ok {
		return nil, ErrRedactionNotSupported
	}
	return rq.QueryRawRedacted(ctx, t, constraint, limit)
}

// QueryRawProjectRedacted is QueryRawRedacted with a projection (see
// QueryRawProject); it delegates or reports ErrRedactionNotSupported.
func (b *BufferedBackend) QueryRawProjectRedacted(ctx context.Context, t AdType, constraint string, projection []string, limit int) (iter.Seq[collections.RawAd], error) {
	if err := b.tryFlush(ctx); err != nil {
		return nil, err
	}
	prq, ok := b.Backend.(RedactedProjectedRawQueryer)
	if !ok {
		return nil, ErrRedactionNotSupported
	}
	return prq.QueryRawProjectRedacted(ctx, t, constraint, projection, limit)
}

// QueryRawProject flushes, then serves a projected raw query. If the underlying
// backend can push the projection down (a remote database), it delegates. If not,
// it falls back to a whole-ad QueryRaw and trims the projection locally -- so a
// projected query never fails just because the backend lacks native pushdown.
// BufferedBackend therefore satisfies ProjectedRawQueryer for any RawQueryer
// backend, which is what the server's projected fast path relies on.
func (b *BufferedBackend) QueryRawProject(ctx context.Context, t AdType, constraint string, projection []string, limit int) (iter.Seq[collections.RawAd], error) {
	if err := b.tryFlush(ctx); err != nil {
		return nil, err
	}
	if prq, ok := b.Backend.(ProjectedRawQueryer); ok {
		return prq.QueryRawProject(ctx, t, constraint, projection, limit)
	}
	rq, ok := b.Backend.(RawQueryer)
	if !ok {
		return nil, fmt.Errorf("collector: backend %T has no raw query", b.Backend)
	}
	raw, err := rq.QueryRaw(ctx, t, constraint, limit)
	if err != nil {
		return nil, err
	}
	return projectRawSeq(raw, projection), nil
}

// QueryRawStream flushes, then streams the raw query. A backend that streams
// natively (the remote-database backends) delegates -- the relay forwards each
// ad as it arrives instead of buffering the whole result set, which the wrapper
// was silently hiding (the server prefers RawStreamer, but the wrapper's method
// set did not include it, downgrading the DB-backed collector to fetch-then-
// relay). A backend without a native stream is adapted from its iterator, so
// the wrapper is safe to expose unconditionally.
func (b *BufferedBackend) QueryRawStream(ctx context.Context, t AdType, constraint string, limit int, yield func(collections.RawAd) bool) error {
	if err := b.tryFlush(ctx); err != nil {
		return err
	}
	if rs, ok := b.Backend.(RawStreamer); ok {
		return rs.QueryRawStream(ctx, t, constraint, limit, yield)
	}
	rq, ok := b.Backend.(RawQueryer)
	if !ok {
		return fmt.Errorf("collector: backend %T has no raw query", b.Backend)
	}
	raw, err := rq.QueryRaw(ctx, t, constraint, limit)
	if err != nil {
		return err
	}
	for ra := range raw {
		if !yield(ra) {
			break
		}
	}
	return nil
}

// QueryRawProjectStream is QueryRawStream with a projection (see
// QueryRawProject for the fallback ladder).
func (b *BufferedBackend) QueryRawProjectStream(ctx context.Context, t AdType, constraint string, projection []string, limit int, yield func(collections.RawAd) bool) error {
	if err := b.tryFlush(ctx); err != nil {
		return err
	}
	if prs, ok := b.Backend.(ProjectedRawStreamer); ok {
		return prs.QueryRawProjectStream(ctx, t, constraint, projection, limit, yield)
	}
	raw, err := b.QueryRawProject(ctx, t, constraint, projection, limit)
	if err != nil {
		return err
	}
	for ra := range raw {
		if !yield(ra) {
			break
		}
	}
	return nil
}

// QueryRawWireStream flushes, then delegates the wire-form row stream; the
// buffer itself cannot assemble rows, so a wrapped backend without the
// capability reports ErrWireRowsNotSupported (probed at call time) and the
// caller falls back to the text row streams.
func (b *BufferedBackend) QueryRawWireStream(ctx context.Context, t AdType, constraint string, projection []string, limit int, redact bool, yield func(row []byte) bool) error {
	if err := b.tryFlush(ctx); err != nil {
		return err
	}
	ws, ok := b.Backend.(WireRowStreamer)
	if !ok {
		return ErrWireRowsNotSupported
	}
	return ws.QueryRawWireStream(ctx, t, constraint, projection, limit, redact, yield)
}

func (b *BufferedBackend) Get(ctx context.Context, t AdType, keyAd *classad.ClassAd) (*classad.ClassAd, bool) {
	if err := b.tryFlush(ctx); err != nil {
		return nil, false
	}
	return b.Backend.Get(ctx, t, keyAd)
}

// Invalidate removes ads, flushing the buffer first so a buffered update cannot undo
// the removal.
//
// While the deep queue is engaged it instead defers the invalidation to the background
// writer and returns at once. This is not an optimization: a view-collector feed sends
// UPDATE_* and INVALIDATE_* interleaved down ONE persistent command socket, so an
// invalidation that blocks on stalled storage blocks that socket exactly as an update
// would -- and the whole point of the queue is that nothing on that socket waits for
// storage. The deferred invalidation is applied after the queued updates, so it still
// wins over anything buffered alongside it. The removed count is 0 in that case: it is
// not yet known, and no caller uses it.
func (b *BufferedBackend) Invalidate(ctx context.Context, t AdType, constraint string, keyAd *classad.ClassAd) (int, error) {
	if b.window > 0 && b.shedding() {
		b.queueInvalidate(pendingInvalidate{t: t, constraint: constraint, keyAd: keyAd})
		return 0, nil
	}
	if err := b.flush(ctx); err != nil {
		return 0, err
	}
	return b.Backend.Invalidate(ctx, t, constraint, keyAd)
}

func (b *BufferedBackend) Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error) {
	if err := b.tryFlush(ctx); err != nil {
		return nil, err
	}
	return b.Backend.Watch(ctx, t, cursor, constraint)
}

func (b *BufferedBackend) Expire(ctx context.Context) (int, error) {
	if err := b.tryFlush(ctx); err != nil {
		return 0, err
	}
	return b.Backend.Expire(ctx)
}

func (b *BufferedBackend) Len(ctx context.Context, t AdType) (int, error) {
	if err := b.tryFlush(ctx); err != nil {
		return 0, err
	}
	return b.Backend.Len(ctx, t)
}

// Close stops the flusher, drains the buffer, and closes the underlying backend.
func (b *BufferedBackend) Close() error {
	if b.window > 0 {
		close(b.stop)
		b.wg.Wait()
	}
	// Final drain on shutdown: a background context, bounded by the underlying
	// backend's own retry budget.
	ferr := b.flush(context.Background())
	cerr := b.Backend.Close()
	if ferr != nil {
		return ferr
	}
	return cerr
}
