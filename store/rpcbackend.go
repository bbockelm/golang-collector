package store

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"sync"
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
// It is production-hardened against transient outages: a single connection is
// established by a SINGLE-FLIGHT dial (concurrent operations share one dial attempt
// rather than each hammering a down server -- the reconnect-storm guard), every
// operation is retried through withRetry with exponential backoff + jitter until it
// succeeds, its context ends, or the retry budget is exhausted, and because every
// operation is idempotent-by-key (keyed upserts, keyed/constraint deletes, reads) a
// replay after an ambiguous failure converges to the same state -- so no operation
// needs an idempotency token.
type RPCBackend struct {
	dial   func(context.Context) (dbrpc.MsgConn, error)
	ctx    context.Context
	policy RetryPolicy

	mu          sync.Mutex
	client      *dbrpc.Client
	dialing     chan struct{} // non-nil while a shared dial is in flight
	dialErr     error         // result of the last failed shared dial
	nextDialAt  time.Time     // shared backoff gate: no dial before this instant
	dialBackoff time.Duration // current dial backoff, grown per consecutive failure
	closed      bool

	now             func() int64
	defaultLifetime int64
}

var (
	_ Backend     = (*RPCBackend)(nil)
	_ RawQueryer  = (*RPCBackend)(nil)
	_ BatchWriter = (*RPCBackend)(nil)
)

var errBackendClosed = errors.New("collector: rpc backend is closed")

// NewRPCBackend builds a remote-database backend whose connection is produced by
// dial (called to (re)establish the single shared connection; each call returns a
// fresh MsgConn -- e.g. a freshly authenticated CEDAR DBSession stream wrapped with
// dbrpc.NewCedarConn). ctx bounds the backend's lifetime (and the shared dial).
// policy governs retry/backoff; a zero policy is replaced with DefaultRetryPolicy.
func NewRPCBackend(ctx context.Context, dial func(context.Context) (dbrpc.MsgConn, error), policy RetryPolicy) *RPCBackend {
	if policy == (RetryPolicy{}) {
		policy = DefaultRetryPolicy
	}
	return &RPCBackend{
		dial:            dial,
		ctx:             ctx,
		policy:          policy,
		now:             func() int64 { return time.Now().Unix() },
		defaultLifetime: DefaultLifetime,
	}
}

// conn returns the current dbrpc client, establishing one with a SINGLE-FLIGHT dial
// if needed: at most one dial runs at a time, and every concurrent caller waits on
// that one attempt (bounded by its own ctx) instead of dialing independently -- so a
// burst of operations against a down database produces one reconnect attempt, not a
// storm. The shared dial itself is bounded by the backend lifetime ctx (it serves
// all waiters, not any single caller).
func (b *RPCBackend) conn(ctx context.Context) (*dbrpc.Client, error) {
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, errBackendClosed
		}
		if b.client != nil {
			cl := b.client
			b.mu.Unlock()
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
		if wait := time.Until(b.nextDialAt); wait > 0 {
			b.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		// Gate open: single-flight the dial (concurrent callers share this one).
		if b.dialing == nil {
			b.dialing = make(chan struct{})
			go b.dialShared()
		}
		done := b.dialing
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, errBackendClosed
		}
		if b.client != nil {
			cl := b.client
			b.mu.Unlock()
			return cl, nil
		}
		err := b.dialErr
		b.mu.Unlock()
		return nil, err
	}
}

// dialShared performs the one shared dial and wakes every waiter.
func (b *RPCBackend) dialShared() {
	mc, err := b.dial(b.ctx)
	var cl *dbrpc.Client
	if err == nil {
		cl = dbrpc.NewClient(mc)
		// The server does not auto-create tables on first write; ensure each AdType's
		// table exists. CreateTable is idempotent; ignore errors (a genuinely dead
		// connection surfaces on the first real operation).
		for t := AnyAd + 1; t < numAdTypes; t++ {
			_ = cl.CreateTable(b.ctx, t.String())
		}
	}
	b.mu.Lock()
	if err == nil {
		b.client = cl
		b.dialErr = nil
		b.dialBackoff = 0
		b.nextDialAt = time.Time{}
	} else {
		b.dialErr = fmt.Errorf("collector: connect to ad database: %w", err)
		b.dialBackoff = b.grow(b.dialBackoff) // 0 -> Initial on the first failure
		b.nextDialAt = time.Now().Add(b.backoff(b.dialBackoff))
	}
	close(b.dialing)
	b.dialing = nil
	b.mu.Unlock()
}

// drop discards cl so the next operation redials, but only if cl is still the
// current client (a concurrent reconnect may already have replaced it).
func (b *RPCBackend) drop(cl *dbrpc.Client) {
	b.mu.Lock()
	if b.client != nil && b.client == cl {
		_ = b.client.Close()
		b.client = nil
	}
	b.mu.Unlock()
}

// withRetry runs op against the shared connection, retrying until it succeeds, ctx
// ends, or the retry budget is exhausted. It classifies failures:
//   - optimistic write-write conflict (*db.ConflictError): a healthy connection, so
//     retry the whole operation immediately (bounded by maxConflictRetries);
//   - server/logical error (*dbrpc.ServerError): deterministic, surfaced at once;
//   - context error: surfaced (the caller's deadline/cancellation wins);
//   - anything else (transport failure, dbrpc.ErrConnClosed, a dial error): the
//     connection is suspect -- drop it, back off, and replay (every op is
//     idempotent-by-key, so a replay after an ambiguous failure is safe).
func (b *RPCBackend) withRetry(ctx context.Context, op func(cl *dbrpc.Client) error) error {
	start := time.Now()
	backoff := b.policy.Initial
	conflicts := 0
	var lastErr error
	for {
		cl, err := b.conn(ctx)
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
				b.drop(cl)
				lastErr = opErr
			}
		} else {
			if ctx.Err() != nil {
				return err
			}
			lastErr = err
		}
		if b.policy.MaxElapsed > 0 && time.Since(start) >= b.policy.MaxElapsed {
			return fmt.Errorf("collector: ad database unavailable, gave up after %s: %w", b.policy.MaxElapsed, lastErr)
		}
		// A retry: count it, and measure the time parked in backoff (blocked, not
		// working) -- the direct measure of how much a slow/down database stalls us.
		Metrics.retriesTotal.Inc()
		waitStart := time.Now()
		select {
		case <-ctx.Done():
			Metrics.backoffSeconds.Observe(time.Since(waitStart).Seconds())
			return ctx.Err()
		case <-time.After(b.backoff(backoff)):
		}
		Metrics.backoffSeconds.Observe(time.Since(waitStart).Seconds())
		backoff = b.grow(backoff)
	}
}

// backoff applies jitter to d (a random +/- policy.Jitter fraction).
func (b *RPCBackend) backoff(d time.Duration) time.Duration {
	if b.policy.Jitter <= 0 {
		return d
	}
	// Deterministic-free jitter: derive from the current nanosecond, avoiding a
	// dependency on math/rand's global state. +/- Jitter fraction.
	frac := b.policy.Jitter
	n := time.Now().UnixNano()
	r := float64(n%1000)/1000.0*2 - 1 // in [-1, 1)
	j := 1 + r*frac
	if j < 0 {
		j = 0
	}
	return time.Duration(float64(d) * j)
}

// grow multiplies the backoff by the policy multiplier, capped at Max.
func (b *RPCBackend) grow(d time.Duration) time.Duration {
	next := time.Duration(float64(d) * b.policy.Multiplier)
	if next > b.policy.Max {
		return b.policy.Max
	}
	if next <= 0 {
		return b.policy.Initial
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
	var se *dbrpc.ServerError
	return errors.As(err, &se) && !isNoSuchTxn(err)
}

// isNoSuchTxn reports the server's "no such transaction" error -- the transaction
// this op names is gone (committed, aborted, or cleaned up when its connection
// dropped). It is transient: a fresh transaction succeeds. The string matches the
// message dbrpc's server sends (dbrpc.ServerError carries only a message, no code).
func isNoSuchTxn(err error) bool {
	var se *dbrpc.ServerError
	return errors.As(err, &se) && se.Msg == "no such transaction"
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
	return b.withRetry(ctx, func(cl *dbrpc.Client) error {
		for table, items := range byTable {
			if err := putBatchTx(ctx, cl, table, items); err != nil {
				return err
			}
		}
		return nil
	})
}

// putBatchTx upserts all items into one table in one transaction (no internal retry;
// withRetry owns retry/backoff/reconnect).
func putBatchTx(ctx context.Context, cl *dbrpc.Client, table string, items []keyedText) error {
	tx, err := cl.BeginTable(ctx, table)
	if err != nil {
		return err
	}
	for _, it := range items {
		if err := tx.NewClassAd(ctx, it.key, it.text); err != nil {
			// "no such transaction" means the transaction is gone (aborted underneath
			// us -- e.g. a connection reset cleaned it up server-side), NOT that this
			// ad was rejected. The remaining ads were never given a chance, so do not
			// keep skipping them (that silently drops them and mislabels them as
			// "rejected"): abort and return so withRetry replays the whole batch on a
			// fresh transaction. Every write is an idempotent keyed upsert, so replay
			// is safe.
			if isNoSuchTxn(err) {
				_ = tx.Abort(ctx)
				return err
			}
			// Any other *dbrpc.ServerError means the server rejected THIS ad (e.g. an
			// unparseable value): opNewAd returned an error without adding it, and the
			// transaction stays open. Log the offending ad (the wire bytes are otherwise
			// encrypted) and skip just it -- a partial commit using the transaction the
			// good ads already sit in -- so one bad ad does not lose the batch.
			var se *dbrpc.ServerError
			if errors.As(err, &se) {
				slog.Warn("collector: db rejected ad update; skipping (batch continues)",
					"table", table, "name", AdName(it.text), "err", se, "ad", AdExcerpt(it.text))
				continue
			}
			// A transport/transaction failure: abort and let withRetry retry the batch.
			_ = tx.Abort(ctx)
			return err
		}
	}
	return tx.Commit(ctx)
}

// put upserts key=text in table under one retry envelope. text must already be a
// complete old-ClassAd body.
func (b *RPCBackend) put(ctx context.Context, table, key, text string) error {
	return b.withRetry(ctx, func(cl *dbrpc.Client) error {
		tx, err := cl.BeginTable(ctx, table)
		if err != nil {
			return err
		}
		if err := tx.NewClassAd(ctx, key, text); err != nil {
			_ = tx.Abort(ctx)
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
	err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
		rows, e := cl.QueryTable(ctx, t.String(), rpcConstraint(constraint), limit)
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
	err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
		rows, e := cl.QueryRawTable(ctx, t.String(), rpcConstraint(constraint), limit)
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
	err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
		rows, e := cl.QueryRawProject(ctx, t.String(), rpcConstraint(constraint), projection, limit)
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
	err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
		tx, err := cl.BeginTable(ctx, t.String())
		if err != nil {
			return err
		}
		defer func() { _ = tx.Abort(ctx) }()
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
		err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
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
		err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
			tx, err := cl.BeginTable(ctx, t.String())
			if err != nil {
				return err
			}
			_, present, err = tx.LookupClassAd(ctx, k)
			if err != nil {
				_ = tx.Abort(ctx)
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
			_ = b.withRetry(ctx, func(cl *dbrpc.Client) error {
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
	err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
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
		err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
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
	err := b.withRetry(ctx, func(cl *dbrpc.Client) error {
		rows, e := cl.QueryTable(ctx, t.String(), "true", 0)
		if e != nil {
			return e
		}
		n = len(rows)
		return nil
	})
	return n, err
}

func (b *RPCBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.client != nil {
		err := b.client.Close()
		b.client = nil
		return err
	}
	return nil
}

// rpcConstraint maps the Backend's "" (match everything) to the "true" the dbrpc
// query expects.
func rpcConstraint(constraint string) string {
	if constraint == "" {
		return "true"
	}
	return constraint
}
