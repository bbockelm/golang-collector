package store

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// RetryPolicy governs how RPCBackend rides out a transient database outage: how it
// spaces reconnection/replay attempts and how long it keeps trying before giving up.
// The zero value is unusable; DefaultRetryPolicy supplies sane values.
type RetryPolicy struct {
	Initial    time.Duration // first backoff after a failure
	Max        time.Duration // ceiling on a single backoff interval
	Multiplier float64       // exponential growth factor per attempt
	Jitter     float64       // 0..1 fraction of random jitter applied to each backoff
	// MaxElapsed bounds the total time spent retrying one operation before giving up
	// (drop + surface the error). It is a duration, not a count. A write path uses a
	// long-lived context, so MaxElapsed is its effective bound; a read path is also
	// bound by the caller's context, whichever fires first. 0 means retry until the
	// context ends (no independent budget).
	MaxElapsed time.Duration
}

// DefaultRetryPolicy is a reasonable outage-riding default: quick first retries that
// back off to a few seconds, giving up on one operation after a minute.
var DefaultRetryPolicy = RetryPolicy{
	Initial:    100 * time.Millisecond,
	Max:        5 * time.Second,
	Multiplier: 2.0,
	Jitter:     0.2,
	MaxElapsed: 60 * time.Second,
}

// maxConflictRetries caps immediate retries of an optimistic write-write conflict
// (a healthy connection, so no backoff) before giving up -- a safety bound; real
// conflicts on distinct-keyed collector ads are rare and resolve at once.
const maxConflictRetries = 100

// RPCBackend is a store.Backend that keeps ads in an external database reached over
// the classad/dbrpc protocol -- typically an htcondordb daemon spoken to over CEDAR
// -- so storage is decoupled from the collector process and can be shared or made
// highly available independently of it. Ad text flows over the wire (dbrpc is
// text/constraint based), so this backend materializes and re-encodes ads that the
// in-memory backend would relay untouched; it trades throughput for external,
// restart-independent storage.
//
// It is production-hardened against transient outages: each connection is
// established by a SINGLE-FLIGHT dial (concurrent operations share one dial attempt
// rather than each hammering a down server -- the reconnect-storm guard), every
// operation is retried through withRetry with exponential backoff + jitter until it
// succeeds, its context ends, or the retry budget is exhausted, and because every
// operation is idempotent-by-key (keyed upserts, keyed/constraint deletes, reads) a
// replay after an ambiguous failure converges to the same state -- so no operation
// needs an idempotency token.
//
// Reads and writes ride SEPARATE connections. dbrpc multiplexes a connection with a
// single reader goroutine that demuxes response frames by request id; a streaming
// query whose consumer falls behind fills that stream's frame buffer and blocks the
// reader (head-of-line blocking), stalling every other in-flight call on the same
// connection -- including write-transaction commit responses, which then time out and
// surface as "no such transaction". So writes ride a dedicated lane, and reads ride a
// small round-robin pool of lanes, so a slow query can never stall a commit (nor,
// beyond its own pool slot, other queries).
type RPCBackend struct {
	writes  []*connLane // round-robin pool for write transactions
	writeRR atomic.Uint64
	reads   []*connLane // round-robin pool for queries/streams
	readRR  atomic.Uint64

	policy RetryPolicy

	now             func() int64
	defaultLifetime int64
}

// connLane is one reconnect-managed dbrpc connection: a single client (re)established
// on demand by a single-flight dial behind a shared backoff gate. Each lane owns an
// independent connection -- and therefore an independent dbrpc reader goroutine -- so
// a stall on one lane's connection does not affect any other lane.
type connLane struct {
	dial func(context.Context) (dbrpc.MsgConn, error)
	ctx  context.Context

	kind string // "write" or "read", for the rpc_inflight metric label

	policy RetryPolicy

	mu          sync.Mutex
	client      *dbrpc.Client
	dialing     chan struct{} // non-nil while a shared dial is in flight
	dialErr     error         // result of the last failed shared dial
	nextDialAt  time.Time     // shared backoff gate: no dial before this instant
	dialBackoff time.Duration // current dial backoff, grown per consecutive failure
	closed      bool
}

var (
	_ Backend              = (*RPCBackend)(nil)
	_ RawQueryer           = (*RPCBackend)(nil)
	_ ProjectedRawQueryer  = (*RPCBackend)(nil)
	_ RawStreamer          = (*RPCBackend)(nil)
	_ ProjectedRawStreamer = (*RPCBackend)(nil)
	_ WireRowStreamer      = (*RPCBackend)(nil)
	_ BatchWriter          = (*RPCBackend)(nil)
)

var errBackendClosed = errors.New("collector: rpc backend is closed")

// DefaultReadConns is the size of the read-connection pool when NewRPCBackend is used
// (or NewRPCBackendPool is given a non-positive count). A small pool spreads
// concurrent queries across connections so one slow consumer stalls only its slot.
const DefaultReadConns = 8

// DefaultWriteConns is the size of the write-connection pool. Writes still ride lanes
// SEPARATE from reads (so a slow query can never stall a commit), but a small pool lets
// concurrent flushes -- the background flusher, full-buffer flushes from many advertising
// goroutines, and read-triggered flushes -- overlap their commit round-trips instead of
// serializing on one connection, and lets independent per-table transactions commit
// concurrently server-side. Kept small: same-table commits still serialize on the
// server's per-collection lock, so a large pool mostly just adds connections.
const DefaultWriteConns = 8

// NewRPCBackend builds a remote-database backend with a DefaultWriteConns-sized write
// pool and a DefaultReadConns-sized read pool, each connection produced by dial (called
// to (re)establish a connection; each call returns a fresh MsgConn -- e.g. a freshly
// authenticated CEDAR DBSession stream wrapped with dbrpc.NewCedarConn). ctx bounds the
// backend's lifetime (and every dial). policy governs retry/backoff; a zero policy
// becomes DefaultRetryPolicy.
func NewRPCBackend(ctx context.Context, dial func(context.Context) (dbrpc.MsgConn, error), policy RetryPolicy) *RPCBackend {
	return NewRPCBackendPool(ctx, dial, policy, DefaultReadConns, DefaultWriteConns)
}

// NewRPCBackendPool is NewRPCBackend with explicit read- and write-pool sizes (each
// clamped to at least 1). Reads and writes ride disjoint pools so a slow query never
// stalls a commit.
func NewRPCBackendPool(ctx context.Context, dial func(context.Context) (dbrpc.MsgConn, error), policy RetryPolicy, readConns, writeConns int) *RPCBackend {
	if policy == (RetryPolicy{}) {
		policy = DefaultRetryPolicy
	}
	if readConns < 1 {
		readConns = 1
	}
	if writeConns < 1 {
		writeConns = 1
	}
	newLane := func(kind string) *connLane {
		return &connLane{dial: dial, ctx: ctx, policy: policy, kind: kind}
	}
	b := &RPCBackend{
		policy:          policy,
		now:             func() int64 { return time.Now().Unix() },
		defaultLifetime: DefaultLifetime,
	}
	for i := 0; i < writeConns; i++ {
		b.writes = append(b.writes, newLane("write"))
	}
	for i := 0; i < readConns; i++ {
		b.reads = append(b.reads, newLane("read"))
	}
	return b
}

// writeLane picks the next write-pool lane round-robin. Each write op (a Begin...Commit
// transaction) runs entirely on the one lane returned here; write ops are independent
// idempotent-by-key transactions, so successive ops need not share a lane.
func (b *RPCBackend) writeLane() *connLane {
	if len(b.writes) == 1 {
		return b.writes[0]
	}
	i := b.writeRR.Add(1) - 1
	return b.writes[int(i%uint64(len(b.writes)))]
}

// readLane picks the next read-pool lane round-robin. A transaction (Get) or query
// runs entirely on the one lane returned here, so its multiplexed calls stay coherent.
func (b *RPCBackend) readLane() *connLane {
	if len(b.reads) == 1 {
		return b.reads[0]
	}
	i := b.readRR.Add(1) - 1
	return b.reads[int(i%uint64(len(b.reads)))]
}

// conn returns the lane's current dbrpc client, establishing one with a SINGLE-FLIGHT
// dial if needed: at most one dial runs at a time on this lane, and every concurrent
// caller waits on that one attempt (bounded by its own ctx) instead of dialing
// independently -- so a burst of operations against a down database produces one
// reconnect attempt per lane, not a storm. The shared dial itself is bounded by the
// backend lifetime ctx (it serves all waiters, not any single caller).
func (lane *connLane) conn(ctx context.Context) (*dbrpc.Client, error) {
	for {
		lane.mu.Lock()
		if lane.closed {
			lane.mu.Unlock()
			return nil, errBackendClosed
		}
		if lane.client != nil {
			cl := lane.client
			lane.mu.Unlock()
			return cl, nil
		}
		// Shared backoff gate: after a failed dial, hold every caller off until
		// nextDialAt, so a burst of operations against a down database cannot drive a
		// reconnect storm even when individual dials fail too fast to overlap (which
		// single-flight alone would dedup). We LOOP on the gate -- a caller that wakes
		// re-checks under the lock rather than committing to a dial -- so a batch of
		// callers whose waits expire together does not each fire a serial dial;
		// aggregate dial rate stays ~one attempt per backoff interval regardless of
		// how many operations are waiting.
		if wait := time.Until(lane.nextDialAt); wait > 0 {
			lane.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		// Gate open: single-flight the dial (concurrent callers share this one).
		if lane.dialing == nil {
			lane.dialing = make(chan struct{})
			go lane.dialShared()
		}
		done := lane.dialing
		lane.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
		}
		lane.mu.Lock()
		if lane.closed {
			lane.mu.Unlock()
			return nil, errBackendClosed
		}
		if lane.client != nil {
			cl := lane.client
			lane.mu.Unlock()
			return cl, nil
		}
		err := lane.dialErr
		lane.mu.Unlock()
		return nil, err
	}
}

// dialShared performs the one shared dial for this lane and wakes every waiter.
func (lane *connLane) dialShared() {
	mc, err := lane.dial(lane.ctx)
	var cl *dbrpc.Client
	if err == nil {
		cl = dbrpc.NewClient(mc)
		// Localize where each write-path round-trip spends its time (client write-lock /
		// socket send / server wait) so a flush stall can be attributed without a server
		// profile. Cheap: three Observe calls per unary RPC.
		cl.Observer = func(s dbrpc.CallStats) {
			op := dbrpc.OpName(s.Op)
			Metrics.rpcCallPhaseSeconds.WithLabelValues(op, "write_wait").Observe(s.WriteWait.Seconds())
			Metrics.rpcCallPhaseSeconds.WithLabelValues(op, "send").Observe(s.Send.Seconds())
			Metrics.rpcCallPhaseSeconds.WithLabelValues(op, "wait").Observe(s.Wait.Seconds())
		}
		// The server does not auto-create tables on first write; ensure each AdType's
		// table exists (idempotent). Done on every lane -- including read lanes -- so a
		// query that reaches a fresh database before any write does not fail on a
		// missing table. Ignore errors (a genuinely dead connection surfaces on the
		// first real operation).
		for t := AnyAd + 1; t < numAdTypes; t++ {
			_ = cl.CreateTable(lane.ctx, t.String())
		}
	}
	lane.mu.Lock()
	if err == nil {
		lane.client = cl
		lane.dialErr = nil
		lane.dialBackoff = 0
		lane.nextDialAt = time.Time{}
	} else {
		lane.dialErr = fmt.Errorf("collector: connect to ad database: %w", err)
		lane.dialBackoff = lane.grow(lane.dialBackoff) // 0 -> Initial on the first failure
		lane.nextDialAt = time.Now().Add(lane.backoff(lane.dialBackoff))
	}
	close(lane.dialing)
	lane.dialing = nil
	lane.mu.Unlock()
}

// drop discards cl so the next operation on this lane redials, but only if cl is still
// the current client (a concurrent reconnect may already have replaced it).
func (lane *connLane) drop(cl *dbrpc.Client) {
	lane.mu.Lock()
	if lane.client != nil && lane.client == cl {
		_ = lane.client.Close()
		lane.client = nil
	}
	lane.mu.Unlock()
}

// close tears down the lane's connection.
func (lane *connLane) close() error {
	lane.mu.Lock()
	defer lane.mu.Unlock()
	lane.closed = true
	if lane.client != nil {
		err := lane.client.Close()
		lane.client = nil
		return err
	}
	return nil
}

// withRetry runs op against the given lane's connection, retrying until it succeeds,
// ctx ends, or the retry budget is exhausted. The whole op runs on the one lane passed
// in, so a multiplexed transaction (BeginTable...Commit) stays coherent on one
// connection. It classifies failures:
//   - optimistic write-write conflict (*db.ConflictError): a healthy connection, so
//     retry the whole operation immediately (bounded by maxConflictRetries);
//   - server/logical error (*dbrpc.ServerError): deterministic, surfaced at once;
//   - context error: surfaced (the caller's deadline/cancellation wins);
//   - anything else (transport failure, dbrpc.ErrConnClosed, a dial error): the
//     connection is suspect -- drop it, back off, and replay (every op is
//     idempotent-by-key, so a replay after an ambiguous failure is safe).
func (b *RPCBackend) withRetry(ctx context.Context, lane *connLane, op func(cl *dbrpc.Client) error) error {
	// Count this operation as in-flight on its lane for its whole lifetime (including
	// retries): on the single write lane a sustained value >1 is head-of-line blocking.
	Metrics.rpcInflight.WithLabelValues(lane.kind).Inc()
	defer Metrics.rpcInflight.WithLabelValues(lane.kind).Dec()

	start := time.Now()
	backoff := b.policy.Initial
	conflicts := 0
	var lastErr error
	var cause string
	for {
		cl, err := lane.conn(ctx)
		if err == nil {
			opErr := op(cl)
			if opErr == nil {
				return nil
			}
			switch {
			case isConflict(opErr):
				conflicts++
				if conflicts > maxConflictRetries {
					return opErr
				}
				continue // immediate retry on the same healthy connection
			case isPermanent(opErr):
				return opErr
			case ctx.Err() != nil:
				return ctx.Err()
			default:
				lane.drop(cl)
				lastErr = opErr
				cause = causeOf(opErr)
			}
		} else {
			if ctx.Err() != nil {
				return err
			}
			lastErr = err
			cause = "dial"
		}
		if b.policy.MaxElapsed > 0 && time.Since(start) >= b.policy.MaxElapsed {
			return fmt.Errorf("collector: ad database unavailable, gave up after %s: %w", b.policy.MaxElapsed, lastErr)
		}
		// A retry: count it BY CAUSE (which transient failure this was), log it
		// rate-limited (so a silent retry storm becomes visible without spamming), and
		// measure the time parked in backoff -- the direct measure of how much a
		// slow/down/flaky database stalls the update path.
		Metrics.retriesTotal.WithLabelValues(cause).Inc()
		logTransient(cause, lastErr)
		waitStart := time.Now()
		select {
		case <-ctx.Done():
			Metrics.backoffSeconds.Observe(time.Since(waitStart).Seconds())
			return ctx.Err()
		case <-time.After(lane.backoff(backoff)):
		}
		Metrics.backoffSeconds.Observe(time.Since(waitStart).Seconds())
		backoff = lane.grow(backoff)
	}
}

// backoff applies jitter to d (a random +/- policy.Jitter fraction).
func (lane *connLane) backoff(d time.Duration) time.Duration {
	if lane.policy.Jitter <= 0 {
		return d
	}
	// Deterministic-free jitter: derive from the current nanosecond, avoiding a
	// dependency on math/rand's global state. +/- Jitter fraction.
	frac := lane.policy.Jitter
	n := time.Now().UnixNano()
	r := float64(n%1000)/1000.0*2 - 1 // in [-1, 1)
	j := 1 + r*frac
	if j < 0 {
		j = 0
	}
	return time.Duration(float64(d) * j)
}

// grow multiplies the backoff by the policy multiplier, capped at Max.
func (lane *connLane) grow(d time.Duration) time.Duration {
	next := time.Duration(float64(d) * lane.policy.Multiplier)
	if next > lane.policy.Max {
		return lane.policy.Max
	}
	if next <= 0 {
		return lane.policy.Initial
	}
	return next
}

// isConflict reports an optimistic write-write conflict (retry on the same
// connection) rather than a transport or logical failure.
func isConflict(err error) bool {
	var ce *db.ConflictError
	return errors.As(err, &ce)
}

// isPermanent reports a deterministic server/logical error (bad constraint, unknown
// table, malformed op): replaying it fails identically, so it is surfaced at once.
// A "no such transaction" is explicitly NOT permanent: the transaction was aborted
// underneath us (a connection reset triggers the server's disconnect cleanup to abort
// its open transactions, and an op can arrive just after), which a replay on a fresh
// connection + transaction resolves. So it falls through to the transport path
// (drop + backoff + replay).
func isPermanent(err error) bool {
	var nr *errNoReplay
	if errors.As(err, &nr) {
		return true
	}
	var se *dbrpc.ServerError
	return errors.As(err, &se) && !isNoSuchTxn(err)
}

// errNoReplay wraps a streaming failure that must NOT be retried because rows were
// already delivered to the caller -- a replay would re-deliver (duplicate) the ads a
// relay has already forwarded to its own client. isPermanent treats it as permanent so
// withRetry surfaces it immediately instead of replaying. (A failure before the first
// row is delivered is left un-wrapped, so it retries normally: nothing was relayed yet.)
type errNoReplay struct{ err error }

func (e *errNoReplay) Error() string { return e.err.Error() }
func (e *errNoReplay) Unwrap() error { return e.err }

// isNoSuchTxn reports the server's "no such transaction" error -- the transaction
// this op names is gone (committed, aborted, or cleaned up when its connection
// dropped). It is transient: a fresh transaction succeeds. The string matches the
// message dbrpc's server sends (dbrpc.ServerError carries only a message, no code).
func isNoSuchTxn(err error) bool {
	var se *dbrpc.ServerError
	return errors.As(err, &se) && se.Msg == "no such transaction"
}

// causeOf buckets a transient (retryable) failure for the retries_total{cause} metric
// and the transient-error log, so a retry storm can be attributed: a server that keeps
// losing transactions ("no_txn"), a connection that keeps breaking ("conn_closed"),
// operations timing out on a wedged connection ("deadline"), a failing (re)dial
// ("dial"), or any other transport error ("transport").
func causeOf(err error) string {
	switch {
	case isNoSuchTxn(err):
		return "no_txn"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, dbrpc.ErrConnClosed):
		return "conn_closed"
	default:
		return "transport"
	}
}

// transientLog rate-limits the transient-error WARN to at most one per cause per
// window, so a burst of retries surfaces the cause in the log without flooding it.
var transientLog struct {
	mu   sync.Mutex
	last map[string]time.Time
}

const transientLogEvery = 2 * time.Second

// logTransient emits a rate-limited WARN naming why a database op is being retried.
// withRetry otherwise swallows transient failures silently (it replays them), which is
// why a flaky database shows up as latency with "no errors in the log" -- this makes
// the cause visible. The full, unthrottled breakdown is in retries_total{cause}.
func logTransient(cause string, err error) {
	now := time.Now()
	transientLog.mu.Lock()
	if transientLog.last == nil {
		transientLog.last = make(map[string]time.Time)
	}
	if now.Sub(transientLog.last[cause]) < transientLogEvery {
		transientLog.mu.Unlock()
		return
	}
	transientLog.last[cause] = now
	transientLog.mu.Unlock()
	slog.Warn("collector: transient database error, retrying with backoff (rate-limited; see retries_total{cause})",
		"cause", cause, "err", err)
}

type keyedText struct{ key, text string }

// UpdateBatch applies a buffer of upserts as one transaction per table, wrapped in a
// single retry envelope. Because every write is an idempotent keyed upsert, replaying
// the whole batch after a transient failure (including tables that already committed)
// converges to the same state.
func (b *RPCBackend) UpdateBatch(ctx context.Context, batch []PendingUpdate) error {
	byTable := make(map[string][]keyedText)
	for _, p := range batch {
		if p.Pvt {
			key, ok := hashKeyFromText(StartdAd, p.Text)
			if !ok {
				continue
			}
			pvt := p.PvtText
			if !strings.HasSuffix(pvt, "\n") {
				pvt += "\n"
			}
			header := copyAttrLines(p.Text, attrName, attrMyAddress, attrMyType)
			table := StartdPvtAd.String()
			byTable[table] = append(byTable[table], keyedText{string(key), stampText(header+pvt, b.now())})
			continue
		}
		key, ok := hashKeyFromText(p.Type, p.Text)
		if !ok {
			continue
		}
		byTable[p.Type.String()] = append(byTable[p.Type.String()], keyedText{string(key), stampText(p.Text, b.now())})
	}
	// Build the commit units. Normally one per table; a table whose batch is large is
	// split into up to write-pool-many chunks so a heavy flush commits across several
	// lanes concurrently instead of one table after another on one lane. Parallelism thus
	// GROWS WITH THE BATCH SIZE: a light flush yields one small unit per table and does not
	// fan out (so it pays no extra commits/fsyncs), while a heavy flush fans out for
	// throughput. Because all units come from one drained snapshot over disjoint keys,
	// splitting never reorders a key's writes.
	units := b.buildCommitUnits(byTable)

	// Light load: a single unit commits inline on one lane under one retry envelope --
	// byte-for-byte the pre-fan-out path, so the common small flush pays no goroutine or
	// parallelism overhead.
	if len(units) <= 1 {
		if len(units) == 0 {
			return nil
		}
		u := units[0]
		unitStart := time.Now()
		err := b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
			return putBatchTx(ctx, cl, u.table, u.items)
		})
		Metrics.unitSeconds.Observe(time.Since(unitStart).Seconds())
		return err
	}
	return b.commitUnitsConcurrent(ctx, units)
}

// commitUnit is one table's (sub-)batch committed as a single transaction on one lane.
type commitUnit struct {
	table string
	items []keyedText
}

// adaptiveFanoutChunk is the per-table batch size below which a table commits as a single
// transaction. Above it, the table is split into up to write-pool-many chunks that commit
// concurrently. It is deliberately large so ordinary flushes stay one commit per table
// (no extra fsyncs); only a genuinely heavy flush fans out and accepts the extra commits
// for the throughput it needs.
const adaptiveFanoutChunk = 256

// buildCommitUnits turns the grouped-by-table batch into commit units, splitting a large
// table into balanced chunks -- bounded so the WHOLE batch fits one wave of the write
// pool. The per-table cap alone was not enough: several split tables plus a few singles
// could total more units than lanes, and the excess queued behind the pool semaphore for
// a second serial wave (an unmeasured 1-3s stall on a heavy flush). Shrinking the
// most-split table first keeps chunks balanced; every table keeps at least one unit (a
// unit never spans tables), so a flush touching more tables than lanes still waves --
// that much is inherent in per-table transactions.
func (b *RPCBackend) buildCommitUnits(byTable map[string][]keyedText) []commitUnit {
	lanes := len(b.writes)
	type tableSplit struct {
		table  string
		items  []keyedText
		chunks int
	}
	splits := make([]tableSplit, 0, len(byTable))
	total := 0
	for table, items := range byTable {
		if len(items) == 0 {
			continue
		}
		chunks := 1
		if lanes > 1 && len(items) > adaptiveFanoutChunk {
			chunks = (len(items) + adaptiveFanoutChunk - 1) / adaptiveFanoutChunk
			if chunks > lanes {
				chunks = lanes
			}
		}
		splits = append(splits, tableSplit{table, items, chunks})
		total += chunks
	}
	for total > lanes {
		maxI := -1
		for i := range splits {
			if splits[i].chunks > 1 && (maxI < 0 || splits[i].chunks > splits[maxI].chunks) {
				maxI = i
			}
		}
		if maxI < 0 {
			break // every table already at one unit
		}
		splits[maxI].chunks--
		total--
	}
	var units []commitUnit
	for _, s := range splits {
		if s.chunks == 1 {
			units = append(units, commitUnit{s.table, s.items})
			continue
		}
		chunk := (len(s.items) + s.chunks - 1) / s.chunks
		for start := 0; start < len(s.items); start += chunk {
			end := start + chunk
			if end > len(s.items) {
				end = len(s.items)
			}
			units = append(units, commitUnit{s.table, s.items[start:end]})
		}
	}
	return units
}

// commitUnitsConcurrent commits units in parallel across the write pool, bounded to the
// number of write lanes (extra units queue for a lane). Each unit is its own retry
// envelope on its own lane; every write is an idempotent keyed upsert, so an independent
// replay after a transient failure is safe. On the first hard error it cancels the rest
// (they will be replayed if the caller retries the batch) and returns that error.
func (b *RPCBackend) commitUnitsConcurrent(ctx context.Context, units []commitUnit) error {
	Metrics.commitFanout.Observe(float64(len(units)))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, len(b.writes))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for _, u := range units {
		u := u
		wg.Add(1)
		go func() {
			defer wg.Done()
			semStart := time.Now()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			// Semaphore wait is the "second wave" serialization on an over-fanned flush;
			// buildCommitUnits bounds units to one wave, so nonzero time here means either
			// more touched tables than lanes or a regression in that bound.
			Metrics.semWaitSeconds.Observe(time.Since(semStart).Seconds())
			unitStart := time.Now()
			err := b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
				return putBatchTx(ctx, cl, u.table, u.items)
			})
			Metrics.unitSeconds.Observe(time.Since(unitStart).Seconds())
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel() // stop the rest; the batch will be replayed on retry
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// adBatchChunkSize bounds how many ads ride in one opNewAdBatch frame. The chunks are
// pipelined (sent back-to-back without waiting for acks), so a whole table's batch costs
// ~one round-trip regardless of ad count -- collapsing the per-ad request/ack round-trips
// that used to dominate the write path -- while each frame stays well under the stream's
// max message size.
const adBatchChunkSize = 64

// abortDetached best-effort aborts tx even when the operation's context is already
// cancelled -- exactly the case that orphans a server-side transaction: the plain
// tx.Abort(ctx) would be dropped client-side (callCtx refuses a done context), leaving
// the txn holding its buffered ad writes on the server heap until the idle reaper
// finds it. Bounded so a dead server cannot stall the caller.
func abortDetached(ctx context.Context, tx *dbrpc.Tx) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Abort(actx)
}

// putBatchTx upserts all items into one table in one transaction (no internal retry;
// withRetry owns retry/backoff/reconnect). The ads are sent as pipelined batches instead
// of one request/ack per ad.
func putBatchTx(ctx context.Context, cl *dbrpc.Client, table string, items []keyedText) error {
	if len(items) == 0 {
		return nil
	}
	beginStart := time.Now()
	tx, err := cl.BeginTable(ctx, table)
	Metrics.rpcBeginSeconds.Observe(time.Since(beginStart).Seconds())
	if err != nil {
		return err
	}
	// Whatever path exits without a commit must abort, or the server-side transaction
	// (and every buffered ad in it) outlives this call. Detached: a context cancellation
	// mid-flight -- a read-triggered flush's deadline firing -- is the common abandoner.
	committed := false
	defer func() {
		if !committed {
			abortDetached(ctx, tx)
		}
	}()
	kvs := make([]dbrpc.AdKV, len(items))
	for i, it := range items {
		kvs[i] = dbrpc.AdKV{Key: it.key, Ad: it.text}
	}
	// A returned error is a whole-chunk failure: the transaction is gone ("no such
	// transaction" -- e.g. a connection reset cleaned it up server-side) or the transport
	// failed. The batch was not applied, so abort (the deferred detached abort) and
	// return; withRetry replays the whole batch on a fresh transaction. Every write is an
	// idempotent keyed upsert, so replay is safe.
	writeStart := time.Now()
	rejects, err := tx.NewClassAdBatchPipelined(ctx, kvs, adBatchChunkSize)
	Metrics.batchWriteSeconds.Observe(time.Since(writeStart).Seconds())
	if err != nil {
		return err
	}
	// Rejects are per-ad: the server could not parse those ads and left them out, but the
	// transaction stays open and every other ad was applied. Log each offending ad (the
	// wire bytes are otherwise encrypted) and commit the good ones -- one bad ad does not
	// lose the batch. (This mirrors the old per-ad skip-on-ServerError behavior.)
	for _, r := range rejects {
		if r.Index < 0 || r.Index >= len(items) {
			continue
		}
		it := items[r.Index]
		slog.Warn("collector: db rejected ad update; skipping (batch continues)",
			"table", table, "name", AdName(it.text), "err", r.Err, "ad", AdExcerpt(it.text))
	}
	commitStart := time.Now()
	err = tx.Commit(ctx)
	Metrics.commitSeconds.Observe(time.Since(commitStart).Seconds())
	if err == nil {
		committed = true
	} else if isConflict(err) {
		// A write-write MVCC conflict: a key in this batch was modified by a concurrent
		// committer since this txn's snapshot (typically a hot ad re-advertised across two
		// overlapping flushes). Count it, by table (which pool is churning) -- this is the
		// stall's first-class signal, invisible to retries_total (transient failures only).
		Metrics.conflictsTotal.WithLabelValues(table).Inc()
		// Then ACCEPT the partial commit instead of replaying the whole batch. The DB
		// already committed every non-conflicted key (opCommit's contract) and returned
		// only the conflicted ones; the old behavior bubbled the error to withRetry, which
		// re-ran the ENTIRE begin+batch+commit -- repeatedly for a still-churning key --
		// stalling a flush for seconds with no backoff. For idempotent last-writer ad
		// upserts a conflicted key is benign: a concurrent (typically fresher) writer
		// already set it, and the losing daemon re-advertises within an update cycle. So
		// treat it as a successful flush and move on. (The versioned/LWW upsert by
		// UpdateSequenceNumber makes conflicts impossible and supersedes this entirely.)
		committed = true // the batch committed (minus dropped conflicts); do not abort/replay
		return nil
	}
	// On a commit error the txn was consumed server-side (win or lose), so the deferred
	// abort is a harmless "no such transaction" -- except when the commit never reached
	// the server (context cancelled first), the exact case the abort must cover.
	return err
}

// put upserts key=text in table under one retry envelope. text must already be a
// complete old-ClassAd body.
func (b *RPCBackend) put(ctx context.Context, table, key, text string) error {
	return b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
		tx, err := cl.BeginTable(ctx, table)
		if err != nil {
			return err
		}
		if err := tx.NewClassAd(ctx, key, text); err != nil {
			abortDetached(ctx, tx)
			return err
		}
		return tx.Commit(ctx)
	})
}

// stampText appends a LastHeardFrom line to an old-ClassAd body (daemons do not
// send it; the collector stamps it, as the in-memory backend does on ingest).
func stampText(text string, now int64) string {
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	return text + attrLastHeardFrom + " = " + fmt.Sprintf("%d", now) + "\n"
}

func (b *RPCBackend) Update(ctx context.Context, t AdType, ad *classad.ClassAd) error {
	key, ok := HashKey(t, ad)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	ad.InsertAttr(attrLastHeardFrom, b.now())
	return b.put(ctx, t.String(), string(key), ad.MarshalOldWithPrivate())
}

func (b *RPCBackend) UpdateOldText(ctx context.Context, t AdType, text string) error {
	key, ok := hashKeyFromText(t, text)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	return b.put(ctx, t.String(), string(key), stampText(text, b.now()))
}

func (b *RPCBackend) UpdatePvt(ctx context.Context, publicText, pvtText string) error {
	key, ok := hashKeyFromText(StartdAd, publicText)
	if !ok {
		return fmt.Errorf("collector: startd private ad's public ad has no Name to key on")
	}
	// Copy identifying attributes from the public ad so the private ad is
	// self-describing (matches the in-memory and embedded-db backends).
	header := copyAttrLines(publicText, attrName, attrMyAddress, attrMyType)
	if !strings.HasSuffix(pvtText, "\n") {
		pvtText += "\n"
	}
	return b.put(ctx, StartdPvtAd.String(), string(key), stampText(header+pvtText, b.now()))
}

// sliceSeq yields the elements of s.
func sliceSeq[T any](s []T) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range s {
			if !yield(v) {
				return
			}
		}
	}
}

// fetchTable fetches matching ads from one table as parsed ClassAds, under one retry
// envelope. Fetching eagerly (rather than lazily inside the returned iterator) lets
// Query surface a persistent outage as an error to the caller instead of a silently
// empty result.
func (b *RPCBackend) fetchTable(ctx context.Context, t AdType, constraint string, limit int) ([]*classad.ClassAd, error) {
	var out []*classad.ClassAd
	err := b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		qStart := time.Now()
		rows, e := cl.QueryTable(ctx, t.String(), rpcConstraint(constraint), limit)
		observeQuery(qStart, len(rows), e)
		if e != nil {
			return e
		}
		out = out[:0]
		for _, text := range rows {
			if ad, e := classad.Parse(text); e == nil {
				out = append(out, ad)
			}
		}
		return nil
	})
	return out, err
}

func (b *RPCBackend) Query(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[*classad.ClassAd], error) {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return nil, err
		}
		var all []*classad.ClassAd
		for at := AnyAd + 1; at < numAdTypes; at++ {
			if at == StartdPvtAd {
				continue
			}
			ads, err := b.fetchTable(ctx, at, constraint, limit)
			if err != nil {
				return nil, err
			}
			all = append(all, ads...)
		}
		return sliceSeq(all), nil
	}
	ads, err := b.fetchTable(ctx, t, constraint, limit)
	if err != nil {
		return nil, err
	}
	return sliceSeq(ads), nil
}

// QueryRaw makes RPCBackend a store.RawQueryer: it fetches matching ads as
// old-ClassAd wire text (the dbrpc QueryRaw op) and rebuilds a collections.RawAd
// from each by splitting lines -- no AST parse -- so the collector's unprojected
// query fast path relays them. The collector connects privileged, so private
// attributes arrive here and are redacted per-client upstream.
func (b *RPCBackend) QueryRaw(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return nil, err
		}
		var all []collections.RawAd
		for at := AnyAd + 1; at < numAdTypes; at++ {
			if at == StartdPvtAd {
				continue
			}
			ras, err := b.fetchRawTable(ctx, at, constraint, limit)
			if err != nil {
				return nil, err
			}
			all = append(all, ras...)
		}
		return sliceSeq(all), nil
	}
	ras, err := b.fetchRawTable(ctx, t, constraint, limit)
	if err != nil {
		return nil, err
	}
	return sliceSeq(ras), nil
}

func (b *RPCBackend) fetchRawTable(ctx context.Context, t AdType, constraint string, limit int) ([]collections.RawAd, error) {
	var out []collections.RawAd
	err := b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		qStart := time.Now()
		rows, e := cl.QueryRawTable(ctx, t.String(), rpcConstraint(constraint), limit)
		observeQuery(qStart, len(rows), e)
		if e != nil {
			return e
		}
		out = out[:0]
		for _, text := range rows {
			out = append(out, rawAdFromOldText(text))
		}
		return nil
	})
	return out, err
}

// QueryRawProject pushes the projection to the database (dbrpc QueryRawProject),
// so only the requested attributes (plus MyType/TargetType) come back over the
// wire, then rebuilds each already-projected ad as a RawAd for the server's raw
// send path.
func (b *RPCBackend) QueryRawProject(ctx context.Context, t AdType, constraint string, projection []string, limit int) (iter.Seq[collections.RawAd], error) {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return nil, err
		}
		var all []collections.RawAd
		for at := AnyAd + 1; at < numAdTypes; at++ {
			if at == StartdPvtAd {
				continue
			}
			ras, err := b.fetchRawTableProject(ctx, at, constraint, projection, limit)
			if err != nil {
				return nil, err
			}
			all = append(all, ras...)
		}
		return sliceSeq(all), nil
	}
	ras, err := b.fetchRawTableProject(ctx, t, constraint, projection, limit)
	if err != nil {
		return nil, err
	}
	return sliceSeq(ras), nil
}

func (b *RPCBackend) fetchRawTableProject(ctx context.Context, t AdType, constraint string, projection []string, limit int) ([]collections.RawAd, error) {
	var out []collections.RawAd
	err := b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		qStart := time.Now()
		rows, e := cl.QueryRawProject(ctx, t.String(), rpcConstraint(constraint), projection, limit)
		observeQuery(qStart, len(rows), e)
		if e != nil {
			return e
		}
		out = out[:0]
		for _, text := range rows {
			out = append(out, rawAdFromOldText(text))
		}
		return nil
	})
	return out, err
}

// QueryRawStream makes RPCBackend a store.RawStreamer: it hands each matching ad to
// yield as it arrives over the wire (dbrpc QueryRawTableStream) instead of buffering the
// whole result -- so the collector's query relay forwards each ad to its own client
// without holding the entire result set in memory. yield returns false to stop early.
//
// Unlike the buffered QueryRaw, a transient failure is retried only until the FIRST ad is
// delivered; after that a failure is terminal (errNoReplay), because a replay would
// re-deliver ads the relay has already forwarded. The error surfaces a mid-stream backend
// failure to the caller (an iter.Seq could not), so the relay tears its response down
// rather than silently truncating it.
func (b *RPCBackend) QueryRawStream(ctx context.Context, t AdType, constraint string, limit int, yield func(collections.RawAd) bool) error {
	return b.streamRaw(ctx, t, constraint, nil, limit, yield)
}

// QueryRawProjectStream is QueryRawStream with a server-side projection pushed down
// (dbrpc QueryRawProjectStream), so only the requested attributes cross the wire.
func (b *RPCBackend) QueryRawProjectStream(ctx context.Context, t AdType, constraint string, projection []string, limit int, yield func(collections.RawAd) bool) error {
	return b.streamRaw(ctx, t, constraint, projection, limit, yield)
}

// streamRaw drives QueryRaw{,Project}Stream. For AnyAd it streams every public table in
// turn as one flat sequence, stopping the table walk as soon as the consumer asks to stop
// (e.g. a global limit is reached) so it does not open a query against tables it will not
// read.
func (b *RPCBackend) streamRaw(ctx context.Context, t AdType, constraint string, projection []string, limit int, yield func(collections.RawAd) bool) error {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return err
		}
		for at := AnyAd + 1; at < numAdTypes; at++ {
			if at == StartdPvtAd {
				continue
			}
			stopped, err := b.streamRawTable(ctx, at, constraint, projection, limit, yield)
			if err != nil {
				return err
			}
			if stopped {
				return nil
			}
		}
		return nil
	}
	_, err := b.streamRawTable(ctx, t, constraint, projection, limit, yield)
	return err
}

// streamRawTable streams one table, converting each old-ClassAd text to a RawAd for yield.
// It reports whether the consumer stopped early (yield returned false) so the AnyAd walk
// can end without querying the remaining tables.
func (b *RPCBackend) streamRawTable(ctx context.Context, t AdType, constraint string, projection []string, limit int, yield func(collections.RawAd) bool) (stopped bool, err error) {
	delivered := false
	wrap := func(row string) bool {
		delivered = true
		if !yield(rawAdFromOldText(row)) {
			stopped = true
			return false
		}
		return true
	}
	err = b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		var e error
		if len(projection) == 0 {
			e = cl.QueryRawTableStream(ctx, t.String(), rpcConstraint(constraint), limit, wrap)
		} else {
			e = cl.QueryRawProjectStream(ctx, t.String(), rpcConstraint(constraint), projection, limit, wrap)
		}
		if e != nil && delivered {
			return &errNoReplay{e} // rows already relayed; a replay would duplicate them
		}
		return e
	})
	return stopped, err
}

// rawAdFromOldText rebuilds a collections.RawAd from old-ClassAd wire text: the
// MyType/TargetType tag lines become the RawAd's type fields and every other
// non-blank line is an expression, all without building an AST.
func rawAdFromOldText(text string) collections.RawAd {
	var ra collections.RawAd
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		switch strings.ToLower(rawLineName(line)) {
		case "mytype":
			ra.MyType = rawLineValue(line)
		case "targettype":
			ra.TargetType = rawLineValue(line)
		default:
			ra.Exprs = append(ra.Exprs, []byte(line))
		}
	}
	return ra
}

// rawLineName returns the attribute name of a "Name = value" line.
func rawLineName(line string) string {
	if i := strings.IndexByte(line, '='); i >= 0 {
		return strings.TrimSpace(line[:i])
	}
	return strings.TrimSpace(line)
}

// rawLineValue returns the value of a "Name = value" line, unquoted.
func rawLineValue(line string) string {
	i := strings.IndexByte(line, '=')
	if i < 0 {
		return ""
	}
	v := strings.TrimSpace(line[i+1:])
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		v = v[1 : len(v)-1]
	}
	return v
}

func (b *RPCBackend) Get(ctx context.Context, t AdType, keyAd *classad.ClassAd) (*classad.ClassAd, bool) {
	key, ok := HashKey(t, keyAd)
	if !ok {
		return nil, false
	}
	var found *classad.ClassAd
	err := b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		tx, err := cl.BeginTable(ctx, t.String())
		if err != nil {
			return err
		}
		defer abortDetached(ctx, tx)
		text, present, err := tx.LookupClassAd(ctx, string(key))
		if err != nil {
			return err
		}
		if !present {
			found = nil
			return nil
		}
		ad, perr := classad.Parse(text)
		if perr != nil {
			found = nil
			return nil
		}
		found = ad
		return nil
	})
	if err != nil || found == nil {
		return nil, false
	}
	return found, true
}

func (b *RPCBackend) Invalidate(ctx context.Context, t AdType, constraint string, keyAd *classad.ClassAd) (int, error) {
	if constraint == "" {
		if keyAd == nil {
			return 0, nil
		}
		key, ok := HashKey(t, keyAd)
		if !ok {
			return 0, nil
		}
		return b.deleteKeys(ctx, t, []string{string(key)})
	}
	if t != StartdAd {
		var n int
		err := b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
			var e error
			n, e = cl.DeleteWhereTable(ctx, t.String(), rpcConstraint(constraint))
			return e
		})
		return n, err
	}
	// startd: match public ads, then delete their keys from both tables.
	ads, err := b.fetchTable(ctx, StartdAd, constraint, 0)
	if err != nil {
		return 0, err
	}
	var keys []string
	for _, ad := range ads {
		if key, ok := HashKey(StartdAd, ad); ok {
			keys = append(keys, string(key))
		}
	}
	return b.deleteKeys(ctx, StartdAd, keys)
}

// deleteKeys removes the given keys from table t (and, for startd, the private
// shadow), each in its own retried transaction, returning how many public ads were
// present.
func (b *RPCBackend) deleteKeys(ctx context.Context, t AdType, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	n := 0
	for _, k := range keys {
		var present bool
		err := b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
			tx, err := cl.BeginTable(ctx, t.String())
			if err != nil {
				return err
			}
			_, present, err = tx.LookupClassAd(ctx, k)
			if err != nil {
				abortDetached(ctx, tx)
				return err
			}
			if present {
				_ = tx.DestroyClassAd(ctx, k)
			}
			return tx.Commit(ctx)
		})
		if err != nil {
			return n, err
		}
		if present {
			n++
		}
		if t == StartdAd {
			_ = b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
				tx, err := cl.BeginTable(ctx, StartdPvtAd.String())
				if err != nil {
					return err
				}
				_ = tx.DestroyClassAd(ctx, k)
				return tx.Commit(ctx)
			})
		}
	}
	return n, nil
}

func (b *RPCBackend) Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error) {
	var q = (*queryMatcher)(nil)
	if constraint != "" {
		cq, err := parseConstraint(constraint)
		if err != nil {
			return nil, fmt.Errorf("collector: watch constraint %q: %w", constraint, err)
		}
		if cq != nil {
			q = &queryMatcher{cq.Matches}
		}
	}
	var ch <-chan dbrpc.WatchEvent
	var cancel func()
	err := b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		c, cf, e := cl.WatchTable(ctx, t.String(), cursor)
		if e != nil {
			return e
		}
		ch, cancel = c, cf
		return nil
	})
	if err != nil {
		return nil, err
	}
	seq := func(yield func(collections.WatchEvent) bool) {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				ce := dbrpcToCollectionsWatch(ev)
				if !yield(ce) {
					return
				}
			}
		}
	}
	if q != nil {
		return collections.WatchFilter(seq, q.match), nil
	}
	return seq, nil
}

type queryMatcher struct{ match func(*classad.ClassAd) bool }

// dbrpcToCollectionsWatch converts a wire watch event, parsing the upsert's ad
// text. The Kind values match collections' (Upsert=0, Delete=1, Reset=2).
func dbrpcToCollectionsWatch(ev dbrpc.WatchEvent) collections.WatchEvent {
	ce := collections.WatchEvent{
		Kind:   collections.WatchKind(ev.Kind),
		Key:    []byte(ev.Key),
		Cursor: ev.Cursor,
	}
	if ev.Kind == 0 && ev.AdText != "" { // upsert carries the ad
		if ad, err := classad.Parse(ev.AdText); err == nil {
			ce.Ad = ad
		}
	}
	return ce
}

func (b *RPCBackend) Expire(ctx context.Context) (int, error) {
	now := b.now()
	constraint := fmt.Sprintf("%d > %s + ifThenElse(%s =!= undefined, %s, %d)",
		now, attrLastHeardFrom, attrClassAdLifetime, attrClassAdLifetime, b.defaultLifetime)
	total := 0
	for t := AnyAd + 1; t < numAdTypes; t++ {
		var n int
		err := b.withRetry(ctx, b.writeLane(), func(cl *dbrpc.Client) error {
			var e error
			n, e = cl.DeleteWhereTable(ctx, t.String(), constraint)
			return e
		})
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (b *RPCBackend) Len(ctx context.Context, t AdType) (int, error) {
	var n int
	err := b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		rows, e := cl.QueryTable(ctx, t.String(), "true", 0)
		if e != nil {
			return e
		}
		n = len(rows)
		return nil
	})
	return n, err
}

// DBDiagnostics fetches per-table diagnostics (storage stats + operational timing
// counters) from the remote database over dbrpc -- one entry per ad-type table that
// responds. It is best-effort: a table that errors (e.g. not yet created) is skipped
// and the first such error is returned alongside whatever succeeded, so a partial
// snapshot still feeds metrics. Reads ride the read-lane pool. The fetch is not free
// (the server samples ads for index suggestions), so a scraper should cache it.
func (b *RPCBackend) DBDiagnostics(ctx context.Context) (map[string]*dbrpc.Diagnostics, error) {
	out := make(map[string]*dbrpc.Diagnostics)
	var firstErr error
	for t := AnyAd + 1; t < numAdTypes; t++ {
		table := t.String()
		var d *dbrpc.Diagnostics
		err := b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
			var e error
			d, e = cl.DiagnosticsTable(ctx, table)
			return e
		})
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out[table] = d
	}
	return out, firstErr
}

// Close tears down every lane (the whole write pool and the whole read pool),
// returning the first close error encountered.
func (b *RPCBackend) Close() error {
	var firstErr error
	for _, w := range b.writes {
		if err := w.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, r := range b.reads {
		if err := r.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// rpcConstraint maps the Backend's "" (match everything) to the "true" the dbrpc
// query expects.
func rpcConstraint(constraint string) string {
	if constraint == "" {
		return "true"
	}
	return constraint
}

// QueryRawWireStream makes RPCBackend a store.WireRowStreamer: matching ads
// stream from the database as batched wire-form rows (dbrpc opQueryRawWire) --
// subset-assembled and, when redact is set, private-stripped at the source --
// for rendering at the collector's client edge. An older database without the
// op fails before any row is delivered, and the caller falls back to the text
// row streams.
func (b *RPCBackend) QueryRawWireStream(ctx context.Context, t AdType, constraint string, projection []string, limit int, redact bool, yield func(row []byte) bool) error {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return err
		}
		for at := AnyAd + 1; at < numAdTypes; at++ {
			if at == StartdPvtAd {
				continue
			}
			stopped, err := b.streamWireTable(ctx, at, constraint, projection, limit, redact, yield)
			if err != nil {
				return err
			}
			if stopped {
				return nil
			}
		}
		return nil
	}
	if _, err := parseConstraint(constraint); err != nil {
		return err
	}
	_, err := b.streamWireTable(ctx, t, constraint, projection, limit, redact, yield)
	return err
}

func (b *RPCBackend) streamWireTable(ctx context.Context, t AdType, constraint string, projection []string, limit int, redact bool, yield func(row []byte) bool) (stopped bool, err error) {
	delivered := false
	wrap := func(row []byte) bool {
		delivered = true
		if !yield(row) {
			stopped = true
			return false
		}
		return true
	}
	err = b.withRetry(ctx, b.readLane(), func(cl *dbrpc.Client) error {
		e := cl.QueryRawWireStream(ctx, t.String(), rpcConstraint(constraint), projection, limit, redact, wrap)
		if e != nil && delivered {
			return &errNoReplay{e} // rows already relayed; a replay would duplicate them
		}
		return e
	})
	return stopped, err
}
