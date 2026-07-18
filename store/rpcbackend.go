package store

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// RPCBackend is a store.Backend that keeps ads in an external database reached
// over the classad/dbrpc protocol -- typically an htcondordb daemon spoken to
// over CEDAR -- so storage is decoupled from the collector process and can be
// shared or made highly available independently of it. Ad text flows over the
// wire (dbrpc is text/constraint based), so this backend materializes and
// re-encodes ads that the in-memory backend would relay untouched; it trades
// throughput for external, restart-independent storage.
//
// The dbrpc transport is injected (a dial that returns a fresh MsgConn) so the
// CEDAR client and the backend logic stay separable and testable. A single
// multiplexed connection is used and lazily (re)established; an operation that
// fails drops the connection so the next one redials.
type RPCBackend struct {
	dial func(context.Context) (dbrpc.MsgConn, error)
	ctx  context.Context

	mu     sync.Mutex
	client *dbrpc.Client
	closed bool

	now             func() int64
	defaultLifetime int64
}

var (
	_ Backend     = (*RPCBackend)(nil)
	_ RawQueryer  = (*RPCBackend)(nil)
	_ BatchWriter = (*RPCBackend)(nil)
)

type keyedText struct{ key, text string }

// UpdateBatch applies a buffer of upserts as one transaction per table (one
// round trip per table instead of per ad -- the batching win over a remote
// database).
func (b *RPCBackend) UpdateBatch(batch []PendingUpdate) error {
	cl, err := b.conn()
	if err != nil {
		return err
	}
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
	for table, items := range byTable {
		if err := b.putBatch(cl, table, items); err != nil {
			return err
		}
	}
	return nil
}

// putBatch upserts all items into one table in a single optimistic transaction,
// retrying the commit on conflict.
func (b *RPCBackend) putBatch(cl *dbrpc.Client, table string, items []keyedText) error {
	for attempt := 0; attempt < 8; attempt++ {
		tx, err := cl.BeginTable(table)
		if err != nil {
			b.drop()
			return err
		}
		for _, it := range items {
			if err := tx.NewClassAd(it.key, it.text); err != nil {
				_ = tx.Abort()
				b.drop()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			if isConflict(err) {
				continue
			}
			b.drop()
			return err
		}
		return nil
	}
	return fmt.Errorf("collector: batch write to %s did not commit after repeated conflicts", table)
}

// NewRPCBackend builds a remote-database backend whose connection is produced by
// dial (call it repeatedly; each returns a fresh MsgConn -- e.g. a freshly
// authenticated CEDAR DBSession stream wrapped with dbrpc.NewCedarConn). ctx
// bounds the backend's lifetime.
func NewRPCBackend(ctx context.Context, dial func(context.Context) (dbrpc.MsgConn, error)) *RPCBackend {
	return &RPCBackend{
		dial:            dial,
		ctx:             ctx,
		now:             func() int64 { return time.Now().Unix() },
		defaultLifetime: DefaultLifetime,
	}
}

// conn returns the current dbrpc client, dialing one if needed.
func (b *RPCBackend) conn() (*dbrpc.Client, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("collector: rpc backend is closed")
	}
	if b.client != nil {
		return b.client, nil
	}
	mc, err := b.dial(b.ctx)
	if err != nil {
		return nil, fmt.Errorf("collector: connect to ad database: %w", err)
	}
	b.client = dbrpc.NewClient(mc)
	// The server does not auto-create a table on first write, so ensure each
	// AdType's table exists. CreateTable is idempotent; ignore its error
	// (best-effort -- a genuinely unusable connection surfaces on the first real
	// operation).
	for t := AnyAd + 1; t < numAdTypes; t++ {
		_ = b.client.CreateTable(t.String())
	}
	return b.client, nil
}

// drop discards the current connection so the next operation redials; called when
// an operation fails (the connection may be dead).
func (b *RPCBackend) drop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != nil {
		_ = b.client.Close()
		b.client = nil
	}
}

// put upserts key=text in table, retrying the optimistic-concurrency commit on
// conflict. text must already be a complete old-ClassAd body.
func (b *RPCBackend) put(table, key, text string) error {
	cl, err := b.conn()
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 8; attempt++ {
		tx, err := cl.BeginTable(table)
		if err != nil {
			b.drop()
			return err
		}
		if err := tx.NewClassAd(key, text); err != nil {
			_ = tx.Abort()
			b.drop()
			return err
		}
		if err := tx.Commit(); err != nil {
			// A commit conflict is retryable; anything else likely means the
			// connection is unusable, so drop it and surface the error.
			if isConflict(err) {
				continue
			}
			b.drop()
			return err
		}
		return nil
	}
	return fmt.Errorf("collector: write to %s did not commit after repeated conflicts", table)
}

// isConflict reports whether a dbrpc commit error is an optimistic write-write
// conflict (retryable) rather than a transport/logical failure.
func isConflict(err error) bool {
	return err != nil && strings.Contains(err.Error(), "conflict")
}

// stampText appends a LastHeardFrom line to an old-ClassAd body (daemons do not
// send it; the collector stamps it, as the in-memory backend does on ingest).
func stampText(text string, now int64) string {
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	return text + attrLastHeardFrom + " = " + fmt.Sprintf("%d", now) + "\n"
}

func (b *RPCBackend) Update(t AdType, ad *classad.ClassAd) error {
	key, ok := HashKey(t, ad)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	ad.InsertAttr(attrLastHeardFrom, b.now())
	return b.put(t.String(), string(key), ad.MarshalOldWithPrivate())
}

func (b *RPCBackend) UpdateOldText(t AdType, text string) error {
	key, ok := hashKeyFromText(t, text)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	return b.put(t.String(), string(key), stampText(text, b.now()))
}

func (b *RPCBackend) UpdatePvt(publicText, pvtText string) error {
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
	return b.put(StartdPvtAd.String(), string(key), stampText(header+pvtText, b.now()))
}

// query returns matching ads from one table as parsed ClassAds.
func (b *RPCBackend) queryTable(t AdType, constraint string, limit int) iter.Seq[*classad.ClassAd] {
	return func(yield func(*classad.ClassAd) bool) {
		cl, err := b.conn()
		if err != nil {
			return
		}
		rows, err := cl.QueryTable(t.String(), rpcConstraint(constraint), limit)
		if err != nil {
			b.drop()
			return
		}
		for _, text := range rows {
			ad, err := classad.Parse(text)
			if err != nil {
				continue
			}
			if !yield(ad) {
				return
			}
		}
	}
}

func (b *RPCBackend) Query(t AdType, constraint string, limit int) (iter.Seq[*classad.ClassAd], error) {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return nil, err
		}
		return func(yield func(*classad.ClassAd) bool) {
			for at := AnyAd + 1; at < numAdTypes; at++ {
				if at == StartdPvtAd {
					continue
				}
				for ad := range b.queryTable(at, constraint, limit) {
					if !yield(ad) {
						return
					}
				}
			}
		}, nil
	}
	return b.queryTable(t, constraint, limit), nil
}

// QueryRaw makes RPCBackend a store.RawQueryer: it fetches matching ads as
// old-ClassAd wire text (the dbrpc QueryRaw op) and rebuilds a collections.RawAd
// from each by splitting lines -- no AST parse -- so the collector's unprojected
// query fast path relays them. The collector connects privileged, so private
// attributes arrive here and are redacted per-client upstream.
func (b *RPCBackend) QueryRaw(t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return nil, err
		}
		return func(yield func(collections.RawAd) bool) {
			for at := AnyAd + 1; at < numAdTypes; at++ {
				if at == StartdPvtAd {
					continue
				}
				for ra := range b.queryRawTable(at, constraint, limit) {
					if !yield(ra) {
						return
					}
				}
			}
		}, nil
	}
	return b.queryRawTable(t, constraint, limit), nil
}

func (b *RPCBackend) queryRawTable(t AdType, constraint string, limit int) iter.Seq[collections.RawAd] {
	return func(yield func(collections.RawAd) bool) {
		cl, err := b.conn()
		if err != nil {
			return
		}
		rows, err := cl.QueryRawTable(t.String(), rpcConstraint(constraint), limit)
		if err != nil {
			b.drop()
			return
		}
		for _, text := range rows {
			if !yield(rawAdFromOldText(text)) {
				return
			}
		}
	}
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

func (b *RPCBackend) Get(t AdType, keyAd *classad.ClassAd) (*classad.ClassAd, bool) {
	key, ok := HashKey(t, keyAd)
	if !ok {
		return nil, false
	}
	cl, err := b.conn()
	if err != nil {
		return nil, false
	}
	tx, err := cl.BeginTable(t.String())
	if err != nil {
		b.drop()
		return nil, false
	}
	defer func() { _ = tx.Abort() }()
	text, ok, err := tx.LookupClassAd(string(key))
	if err != nil {
		b.drop()
		return nil, false
	}
	if !ok {
		return nil, false
	}
	ad, err := classad.Parse(text)
	if err != nil {
		return nil, false
	}
	return ad, true
}

func (b *RPCBackend) Invalidate(t AdType, constraint string, keyAd *classad.ClassAd) (int, error) {
	cl, err := b.conn()
	if err != nil {
		return 0, err
	}
	if constraint == "" {
		if keyAd == nil {
			return 0, nil
		}
		key, ok := HashKey(t, keyAd)
		if !ok {
			return 0, nil
		}
		return b.deleteKeys(cl, t, []string{string(key)})
	}
	if t != StartdAd {
		n, err := cl.DeleteWhereTable(t.String(), rpcConstraint(constraint))
		if err != nil {
			b.drop()
		}
		return n, err
	}
	// startd: match public ads, then delete their keys from both tables.
	var keys []string
	for ad := range b.queryTable(StartdAd, constraint, 0) {
		if key, ok := HashKey(StartdAd, ad); ok {
			keys = append(keys, string(key))
		}
	}
	return b.deleteKeys(cl, StartdAd, keys)
}

// deleteKeys removes the given keys from table t (and, for startd, the private
// shadow) in one transaction, returning how many public ads were present.
func (b *RPCBackend) deleteKeys(cl *dbrpc.Client, t AdType, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	n := 0
	for _, k := range keys {
		tx, err := cl.BeginTable(t.String())
		if err != nil {
			b.drop()
			return n, err
		}
		if _, present, err := tx.LookupClassAd(k); err == nil && present {
			_ = tx.DestroyClassAd(k)
			n++
		}
		if err := tx.Commit(); err != nil {
			b.drop()
			return n, err
		}
		if t == StartdAd {
			if tx, err := cl.BeginTable(StartdPvtAd.String()); err == nil {
				_ = tx.DestroyClassAd(k)
				_ = tx.Commit()
			}
		}
	}
	return n, nil
}

func (b *RPCBackend) Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error) {
	cl, err := b.conn()
	if err != nil {
		return nil, err
	}
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
	ch, cancel, err := cl.WatchTable(t.String(), cursor)
	if err != nil {
		b.drop()
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

func (b *RPCBackend) Expire() (int, error) {
	cl, err := b.conn()
	if err != nil {
		return 0, err
	}
	now := b.now()
	constraint := fmt.Sprintf("%d > %s + ifThenElse(%s =!= undefined, %s, %d)",
		now, attrLastHeardFrom, attrClassAdLifetime, attrClassAdLifetime, b.defaultLifetime)
	total := 0
	for t := AnyAd + 1; t < numAdTypes; t++ {
		n, err := cl.DeleteWhereTable(t.String(), constraint)
		total += n
		if err != nil {
			b.drop()
			return total, err
		}
	}
	return total, nil
}

func (b *RPCBackend) Len(t AdType) (int, error) {
	cl, err := b.conn()
	if err != nil {
		return 0, err
	}
	rows, err := cl.QueryTable(t.String(), "true", 0)
	if err != nil {
		b.drop()
		return 0, err
	}
	return len(rows), nil
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
