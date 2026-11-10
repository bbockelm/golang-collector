package store

import (
	"log/slog"
	"time"
)

// Shed reasons labeled onto queue_dropped_total.
const (
	dropAdsCap        = "ads_cap"
	dropBytesCap      = "bytes_cap"
	dropInvalidateCap = "invalidate_cap"
)

// QueuePolicy governs the deep update queue: the mode BufferedBackend enters when
// the backend stalls, in which ad updates are queued and shed rather than allowed to
// block the daemon that sent them.
//
// The problem it solves is specific. A collector's ingest path is a persistent CEDAR
// command socket, and the sender -- notably a primary C++ collector feeding a view
// collector via CONDOR_VIEW_HOST -- writes to it synchronously. If an update handler
// blocks, the sender blocks in its TCP send, and a storage problem on the *view*
// collector becomes a scheduling problem for the *whole pool*. So the ingest path must
// never block on storage, whatever storage is doing: a disk that stops completing I/O,
// a database bug, a SIGSTOPped database daemon. Queue what we cannot write yet, drop
// what will not fit, and keep reading the socket.
//
// Normal operation is unaffected. BufferedBackend already coalesces updates in wire
// form on a short Nagle window, so the queue is just that buffer allowed to grow; the
// deep mode only raises its cap and changes what happens at the cap (shed, not block).
type QueuePolicy struct {
	// StallThreshold is how slow a single backend flush must be to count as a stall and
	// engage the deep queue. 0 engages it permanently (always queue, never block).
	StallThreshold time.Duration
	// StallClear is how long the backend must go without a stall before the deep queue
	// disengages and the buffer returns to its ordinary high-water mark.
	StallClear time.Duration
	// MaxAds / MaxBytes bound the queue while it is engaged. Because entries are
	// coalesced per ad, MaxAds is effectively "how many distinct daemons we can hold an
	// update for"; MaxBytes is the real memory bound and the one worth tuning.
	MaxAds   int
	MaxBytes int64
	// MaxInvalidates bounds the queued-invalidation FIFO. Shedding an invalidation only
	// means the targeted ad lingers until its ATTR_CLASSAD_LIFETIME expires.
	MaxInvalidates int
	// FlushTimeout bounds one backend flush, so a writer wedged on a connection that is
	// open but dead eventually gives up, drops it and redials instead of never returning.
	// It cannot interrupt a blocking disk syscall -- that is what the queue is for.
	FlushTimeout time.Duration
}

// In every bound above, zero means "unset, take the default" (Go cannot tell an unset
// field from a deliberate 0) and a NEGATIVE value means "no bound at all". The one
// exception is StallThreshold, where 0 is itself meaningful: keep the queue engaged
// permanently, never waiting for a stall.

// DefaultQueuePolicy keeps the deep queue dormant in healthy operation and sized, when
// it does engage, to ride out a multi-minute stall for a large pool: 256 MiB of
// coalesced wire text is roughly 50k ads at a typical 5 KiB startd ad.
var DefaultQueuePolicy = QueuePolicy{
	StallThreshold: time.Second,
	StallClear:     5 * time.Minute,
	MaxAds:         200_000,
	MaxBytes:       256 << 20,
	MaxInvalidates: 4096,
	FlushTimeout:   5 * time.Minute,
}

// withDefaults fills unset fields from DefaultQueuePolicy, so a caller can override
// one knob without restating the rest. A negative value means "explicitly disabled"
// and survives; only the zero value is treated as unset. StallThreshold is excluded:
// zero there is meaningful (always shed), so it is normalized by the caller.
func (q QueuePolicy) withDefaults() QueuePolicy {
	if q.StallClear == 0 {
		q.StallClear = DefaultQueuePolicy.StallClear
	}
	if q.MaxAds == 0 {
		q.MaxAds = DefaultQueuePolicy.MaxAds
	}
	if q.MaxBytes == 0 {
		q.MaxBytes = DefaultQueuePolicy.MaxBytes
	}
	if q.MaxInvalidates == 0 {
		q.MaxInvalidates = DefaultQueuePolicy.MaxInvalidates
	}
	if q.FlushTimeout == 0 {
		q.FlushTimeout = DefaultQueuePolicy.FlushTimeout
	}
	return q
}

// pendingOverhead is the per-entry cost the queue charges on top of an ad's wire text:
// the map bucket, the dedup key string and the PendingUpdate struct. It is an estimate,
// deliberately generous, so the byte cap bounds real process memory rather than just
// the text it can account for exactly.
const pendingOverhead = 192

// pendingSize is the memory one queued update is charged against MaxBytes.
func pendingSize(p PendingUpdate) int64 {
	return int64(len(p.Text)+len(p.PvtText)) + pendingOverhead
}

// shedding reports whether the deep queue is currently engaged, and is called on every
// buffered update -- two monotonic clock reads and two atomic loads, no locking.
//
// It engages on either of two signals: a flush that already completed slowly (recorded
// by flush), or a flush that is still in flight and has already been running longer
// than StallThreshold. The second matters because the very first stall has not finished
// yet -- without it the collector would block through the whole of the first outage and
// only protect itself against the second.
//
// It disengages once StallClear has passed with neither signal, restoring the ordinary
// high-water mark so a buffer blow-up in healthy operation is still caught early.
func (b *BufferedBackend) shedding() bool {
	if b.qp.StallThreshold <= 0 {
		return true // permanently engaged
	}
	until, inflight := b.stallUntil.Load(), b.flushStart.Load()
	if until == 0 && inflight == 0 {
		// Nothing has ever stalled and no flush is running: the overwhelmingly common
		// case, and the one that must not even read the clock. shedOn cannot be set
		// here, because engaging always sets stallUntil to a future time and nothing
		// ever resets it to zero.
		return false
	}
	now := time.Now().UnixNano()
	if now < until {
		return true
	}
	if b.shedOn.Load() && b.shedOn.CompareAndSwap(true, false) {
		Metrics.queueShedMode.Set(0)
		slog.Info("collector: backend healthy again; deep update queue disengaged",
			"quiet_for", b.qp.StallClear)
	}
	if inflight != 0 && now-inflight >= int64(b.qp.StallThreshold) {
		b.markStall("a backend flush has been in flight for " + time.Duration(now-inflight).Round(time.Millisecond).String())
		return true
	}
	return false
}

// markStall engages the deep queue and holds it engaged until StallClear from now. It
// is idempotent: repeated calls during one stall extend the deadline but log and count
// only the healthy->stalled transition.
func (b *BufferedBackend) markStall(reason string) {
	b.stallUntil.Store(time.Now().Add(b.qp.StallClear).UnixNano())
	if !b.shedOn.CompareAndSwap(false, true) {
		return
	}
	Metrics.queueStallsTotal.Inc()
	Metrics.queueShedMode.Set(1)
	slog.Warn("collector: backend stalled; deep update queue engaged -- ads are queued and shed at the cap, never blocked",
		"reason", reason, "max_ads", b.qp.MaxAds, "max_bytes", b.qp.MaxBytes,
		"max_invalidations", b.qp.MaxInvalidates, "clears_after", b.qp.StallClear)
}

// Shedding reports whether the deep update queue is engaged. Exported for the metrics
// endpoint and for tests.
func (b *BufferedBackend) Shedding() bool { return b.shedOn.Load() }
