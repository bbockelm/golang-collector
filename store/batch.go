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
	UpdateBatch(batch []PendingUpdate) error
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
	maxBuf  int
	log     func(error)

	mu   sync.Mutex
	buf  map[dedupKey]PendingUpdate
	stop chan struct{}
	wg   sync.WaitGroup
}

var (
	_ Backend    = (*BufferedBackend)(nil)
	_ RawQueryer = (*BufferedBackend)(nil)
)

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
		log:     logErr,
		buf:     make(map[dedupKey]PendingUpdate),
		stop:    make(chan struct{}),
	}
	if window > 0 {
		b.wg.Add(1)
		go func() { defer b.wg.Done(); b.run() }()
	}
	return b, nil
}

func (b *BufferedBackend) run() {
	t := time.NewTicker(b.window)
	defer t.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
			if err := b.flush(); err != nil {
				b.log(err)
			}
		}
	}
}

// enqueue buffers p, flushing synchronously if the buffer is full.
func (b *BufferedBackend) enqueue(p PendingUpdate) error {
	k, ok := p.dedupKey()
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", p.Type)
	}
	b.mu.Lock()
	b.buf[k] = p
	full := len(b.buf) >= b.maxBuf
	b.mu.Unlock()
	if full {
		return b.flush()
	}
	return nil
}

// flush drains the buffer and applies it as one batch. The map is swapped out
// under the lock so writers are not blocked during the (possibly remote) apply.
func (b *BufferedBackend) flush() error {
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	batch := make([]PendingUpdate, 0, len(b.buf))
	for _, p := range b.buf {
		batch = append(batch, p)
	}
	b.buf = make(map[dedupKey]PendingUpdate)
	b.mu.Unlock()
	return b.bw.UpdateBatch(batch)
}

// --- buffered write paths ---

func (b *BufferedBackend) UpdateOldText(t AdType, text string) error {
	return b.enqueue(PendingUpdate{Type: t, Text: text})
}

func (b *BufferedBackend) UpdatePvt(publicText, pvtText string) error {
	return b.enqueue(PendingUpdate{Type: StartdAd, Text: publicText, PvtText: pvtText, Pvt: true})
}

// --- flush-then-passthrough paths (consistency) ---

func (b *BufferedBackend) Update(t AdType, ad *classad.ClassAd) error {
	if err := b.flush(); err != nil {
		return err
	}
	return b.Backend.Update(t, ad)
}

func (b *BufferedBackend) Query(t AdType, constraint string, limit int) (iter.Seq[*classad.ClassAd], error) {
	if err := b.flush(); err != nil {
		return nil, err
	}
	return b.Backend.Query(t, constraint, limit)
}

func (b *BufferedBackend) QueryRaw(t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	if err := b.flush(); err != nil {
		return nil, err
	}
	rq, ok := b.Backend.(RawQueryer)
	if !ok {
		return nil, fmt.Errorf("collector: backend %T has no raw query", b.Backend)
	}
	return rq.QueryRaw(t, constraint, limit)
}

func (b *BufferedBackend) Get(t AdType, keyAd *classad.ClassAd) (*classad.ClassAd, bool) {
	if err := b.flush(); err != nil {
		return nil, false
	}
	return b.Backend.Get(t, keyAd)
}

func (b *BufferedBackend) Invalidate(t AdType, constraint string, keyAd *classad.ClassAd) (int, error) {
	if err := b.flush(); err != nil {
		return 0, err
	}
	return b.Backend.Invalidate(t, constraint, keyAd)
}

func (b *BufferedBackend) Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error) {
	if err := b.flush(); err != nil {
		return nil, err
	}
	return b.Backend.Watch(ctx, t, cursor, constraint)
}

func (b *BufferedBackend) Expire() (int, error) {
	if err := b.flush(); err != nil {
		return 0, err
	}
	return b.Backend.Expire()
}

func (b *BufferedBackend) Len(t AdType) (int, error) {
	if err := b.flush(); err != nil {
		return 0, err
	}
	return b.Backend.Len(t)
}

// Close stops the flusher, drains the buffer, and closes the underlying backend.
func (b *BufferedBackend) Close() error {
	if b.window > 0 {
		close(b.stop)
		b.wg.Wait()
	}
	ferr := b.flush()
	cerr := b.Backend.Close()
	if ferr != nil {
		return ferr
	}
	return cerr
}
