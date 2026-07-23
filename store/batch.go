package store

import (
	"context"
	"fmt"
	"iter"
	"sync"
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
	hardCap int // absolute cap: block the producer (backpressure) when the writer falls behind
	log     func(error)

	mu       sync.Mutex
	buf      map[dedupKey]PendingUpdate
	flushSig chan struct{} // buffered(1); wakes the background writer at the high-water mark
	stop     chan struct{}
	wg       sync.WaitGroup
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
// that flushes every window or once maxBuf ads accumulate. logErr, if non-nil,
// receives background-flush errors.
func NewBufferedBackend(under Backend, window time.Duration, maxBuf int, logErr func(error)) (*BufferedBackend, error) {
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
		// high-water mark before applying backpressure, so a slow commit does not stall the
		// producer for anything short of a sustained overload.
		hardCap:  maxBuf * 4,
		log:      logErr,
		buf:      make(map[dedupKey]PendingUpdate),
		flushSig: make(chan struct{}, 1),
		stop:     make(chan struct{}),
	}
	if window > 0 {
		b.wg.Add(1)
		go func() { defer b.wg.Done(); b.run() }()
	}
	return b, nil
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

// enqueue buffers p. With a background writer running (window > 0) it does NOT commit on
// the caller's goroutine: at the high-water mark it just signals the writer (so the
// update stream keeps flowing while the previous batch commits), and only blocks -- doing
// an inline flush as backpressure -- if the buffer has grown past hardCap because the
// writer is not keeping up. Without a background writer (window == 0, e.g. tests) it
// flushes inline when full, as before.
func (b *BufferedBackend) enqueue(ctx context.Context, p PendingUpdate) error {
	k, ok := p.dedupKey()
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", p.Type)
	}
	b.mu.Lock()
	b.buf[k] = p
	n := len(b.buf)
	b.mu.Unlock()

	if b.window <= 0 {
		// No background writer to hand off to: commit inline when full.
		if n >= b.maxBuf {
			return b.flush(ctx)
		}
		return nil
	}
	if n >= b.hardCap {
		// The writer is falling behind and the buffer is at its cap; block this producer
		// on an inline flush to bound memory and push backpressure to the sender.
		Metrics.backpressureTotal.Inc()
		return b.flush(ctx)
	}
	if n >= b.maxBuf {
		// Wake the writer without blocking; a coalesced pending signal is fine (the writer
		// drains the whole buffer regardless of how many signals it represents).
		select {
		case b.flushSig <- struct{}{}:
		default:
		}
	}
	return nil
}

// flush drains the buffer and applies it as one batch under ctx. The map is swapped
// out under the lock so writers are not blocked during the (possibly remote) apply.
//
// On failure the ads are not silently lost. If ctx cut the apply short (a
// read-triggered flush carrying a caller's short deadline), the ads are re-enqueued
// so the background flusher retries them with a full budget -- a newer update for
// the same ad that arrived meanwhile wins. Any other failure means the underlying
// backend already exhausted its retry budget (or hit a permanent error), so the ads
// are dropped with a log, per the bounded-retry policy (daemons re-advertise).
func (b *BufferedBackend) flush(ctx context.Context) error {
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	batch := make([]PendingUpdate, 0, len(b.buf))
	keys := make([]dedupKey, 0, len(b.buf))
	for k, p := range b.buf {
		batch = append(batch, p)
		keys = append(keys, k)
	}
	b.buf = make(map[dedupKey]PendingUpdate)
	b.mu.Unlock()

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
				b.buf[k] = batch[i]
			}
		}
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
						b.buf[keys[j]] = batch[j]
					}
				}
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
	if err := b.flush(ctx); err != nil {
		return nil, err
	}
	return b.Backend.Query(ctx, t, constraint, limit)
}

func (b *BufferedBackend) QueryRaw(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	if err := b.flush(ctx); err != nil {
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
	if err := b.flush(ctx); err != nil {
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
	if err := b.flush(ctx); err != nil {
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
	if err := b.flush(ctx); err != nil {
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
	if err := b.flush(ctx); err != nil {
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
	if err := b.flush(ctx); err != nil {
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

func (b *BufferedBackend) Get(ctx context.Context, t AdType, keyAd *classad.ClassAd) (*classad.ClassAd, bool) {
	if err := b.flush(ctx); err != nil {
		return nil, false
	}
	return b.Backend.Get(ctx, t, keyAd)
}

func (b *BufferedBackend) Invalidate(ctx context.Context, t AdType, constraint string, keyAd *classad.ClassAd) (int, error) {
	if err := b.flush(ctx); err != nil {
		return 0, err
	}
	return b.Backend.Invalidate(ctx, t, constraint, keyAd)
}

func (b *BufferedBackend) Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error) {
	if err := b.flush(ctx); err != nil {
		return nil, err
	}
	return b.Backend.Watch(ctx, t, cursor, constraint)
}

func (b *BufferedBackend) Expire(ctx context.Context) (int, error) {
	if err := b.flush(ctx); err != nil {
		return 0, err
	}
	return b.Backend.Expire(ctx)
}

func (b *BufferedBackend) Len(ctx context.Context, t AdType) (int, error) {
	if err := b.flush(ctx); err != nil {
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
