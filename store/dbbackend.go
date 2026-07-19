package store

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
)

// DBBackend is a store.Backend that persists ads in an embedded classad/db
// Catalog -- one table per AdType -- instead of in-memory collections. It gives
// the collector restart-survivable state (and, with pool keys, encryption at
// rest) in exchange for the in-memory backend's compression/footprint tuning.
//
// Ads are keyed by the same HashKey the in-memory backend uses, so a startd's
// public and private ads share a key across the StartdAd and StartdPvtAd tables.
// Writes go through db.Put/Delete/DeleteWhere, which retry the store's optimistic
// concurrency internally, so the collector never sees a commit conflict.
type DBBackend struct {
	cat             *db.Catalog
	tables          map[AdType]*db.DB
	now             func() int64
	defaultLifetime int64
}

var (
	_ Backend     = (*DBBackend)(nil)
	_ RawQueryer  = (*DBBackend)(nil)
	_ BatchWriter = (*DBBackend)(nil)
)

// UpdateBatch applies a buffer of upserts, grouping ordinary ads by table into
// one wire-native shard-commit each (db.UpdateOldBatch); private ads go through
// UpdatePvt individually (they share the public ad's key).
func (b *DBBackend) UpdateBatch(ctx context.Context, batch []PendingUpdate) error {
	byTable := make(map[AdType][]db.OldAdText)
	for _, p := range batch {
		if p.Pvt {
			if err := b.UpdatePvt(ctx, p.Text, p.PvtText); err != nil {
				return err
			}
			continue
		}
		key, ok := hashKeyFromText(p.Type, p.Text)
		if !ok {
			continue
		}
		byTable[p.Type] = append(byTable[p.Type], db.OldAdText{Key: string(key), Text: stampText(p.Text, b.now())})
	}
	for t, items := range byTable {
		tbl := b.tables[t]
		if tbl == nil {
			continue
		}
		if err := tbl.UpdateOldBatch(items); err != nil {
			return err
		}
	}
	return nil
}

// KEK is a key-encryption key (a pool signing key) that wraps the ad database's
// at-rest master key. Re-exported from the db layer so callers configure
// encryption without importing db directly.
type KEK = db.KEK

// NewDBBackend opens (or creates) a persistent, unencrypted ad database under dir
// with one table per storage AdType, reloading any ads a prior run persisted -- so
// a collector restart resumes with its pool intact (stale ads are pruned by the
// startup expiry sweep, not lost on restart).
func NewDBBackend(dir string) (*DBBackend, error) {
	return newDBBackend(db.CatalogConfig{Dir: dir})
}

// NewDBBackendEncrypted is NewDBBackend with encryption at rest: every table's
// master key is wrapped under keys (the pool signing keys), so the on-disk ad data
// -- including private ads -- is unreadable without one. Passing no keys is
// equivalent to NewDBBackend (plaintext).
func NewDBBackendEncrypted(dir string, keys []KEK) (*DBBackend, error) {
	return newDBBackend(db.CatalogConfig{Dir: dir, PoolKeys: keys})
}

func newDBBackend(cfg db.CatalogConfig) (*DBBackend, error) {
	cat, err := db.OpenCatalogConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("collector: open ad database at %q: %w", cfg.Dir, err)
	}
	b := &DBBackend{
		cat:             cat,
		tables:          make(map[AdType]*db.DB),
		now:             func() int64 { return time.Now().Unix() },
		defaultLifetime: DefaultLifetime,
	}
	for t := AnyAd + 1; t < numAdTypes; t++ {
		tbl, err := cat.EnsureTable(t.String())
		if err != nil {
			_ = cat.Close()
			return nil, fmt.Errorf("collector: create %s table: %w", t, err)
		}
		b.tables[t] = tbl
	}
	return b, nil
}

// Update inserts or replaces ad in table t, stamping ATTR_LAST_HEARD_FROM.
func (b *DBBackend) Update(ctx context.Context, t AdType, ad *classad.ClassAd) error {
	tbl := b.tables[t]
	if tbl == nil {
		return fmt.Errorf("collector: %s is not a storage table", t)
	}
	key, ok := HashKey(t, ad)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	ad.InsertAttr(attrLastHeardFrom, b.now())
	return tbl.Put(string(key), ad)
}

// UpdateOldText ingests an ad from old-ClassAd wire text via db.UpdateOld -- the
// wire-native path, no AST -- stamping ATTR_LAST_HEARD_FROM into the text. (An
// encrypted store takes the parse+seal path internally; see db.UpdateOld.)
func (b *DBBackend) UpdateOldText(ctx context.Context, t AdType, text string) error {
	tbl := b.tables[t]
	if tbl == nil {
		return fmt.Errorf("collector: %s is not a storage table", t)
	}
	key, ok := hashKeyFromText(t, text)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	return tbl.UpdateOld(string(key), stampText(text, b.now()))
}

// UpdatePvt stores a startd's PRIVATE ad (keyed by the public ad's HashKey) in
// the StartdPvtAd table. Like the in-memory backend it stores only the private
// ad; the caller stores the public ad separately via UpdateOldText. Identifying
// attributes (Name/MyAddress/MyType) are copied from the public ad so the private
// ad is self-describing.
func (b *DBBackend) UpdatePvt(ctx context.Context, publicText, pvtText string) error {
	key, ok := hashKeyFromText(StartdAd, publicText)
	if !ok {
		return fmt.Errorf("collector: startd private ad's public ad has no Name to key on")
	}
	// Copy identifying attributes from the public ad's text so the private ad is
	// self-describing; wire-native, no AST (matches the in-memory backend).
	header := copyAttrLines(publicText, attrName, attrMyAddress, attrMyType)
	if !strings.HasSuffix(pvtText, "\n") {
		pvtText += "\n"
	}
	return b.tables[StartdPvtAd].UpdateOld(string(key), stampText(header+pvtText, b.now()))
}

// dbConstraint maps the Backend's "" (match everything) to the ClassAd expression
// db.Query/DeleteWhere expect for that ("true"); a non-empty constraint is passed
// through unchanged.
func dbConstraint(constraint string) string {
	if constraint == "" {
		return "true"
	}
	return constraint
}

// Query yields ads in table t matching constraint (all if ""). For AnyAd it spans
// every public table (never the private one).
func (b *DBBackend) Query(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[*classad.ClassAd], error) {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil { // validate once, before iterating
			return nil, err
		}
		return func(yield func(*classad.ClassAd) bool) {
			for at := AnyAd + 1; at < numAdTypes; at++ {
				if at == StartdPvtAd {
					continue // private ads are never returned by an ANY query
				}
				seq, err := b.tables[at].Query(dbConstraint(constraint))
				if err != nil {
					return
				}
				for ad := range seq {
					if !yield(ad) {
						return
					}
				}
			}
		}, nil
	}
	tbl := b.tables[t]
	if tbl == nil {
		return nil, fmt.Errorf("collector: %s is not a storage table", t)
	}
	return tbl.Query(dbConstraint(constraint))
}

// QueryRaw makes DBBackend a store.RawQueryer: it yields matching ads in
// wire-form (collections.RawAd, no AST) via db.QueryRaw, which decodes straight
// from the (inline) stored records, so the collector's unprojected query fast
// path relays them without materializing each ad.
func (b *DBBackend) QueryRaw(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	if t == AnyAd {
		if _, err := parseConstraint(constraint); err != nil {
			return nil, err
		}
		return func(yield func(collections.RawAd) bool) {
			for at := AnyAd + 1; at < numAdTypes; at++ {
				if at == StartdPvtAd {
					continue
				}
				seq, err := b.tables[at].QueryRaw(dbConstraint(constraint))
				if err != nil {
					return
				}
				for ra := range seq {
					if !yield(ra) {
						return
					}
				}
			}
		}, nil
	}
	tbl := b.tables[t]
	if tbl == nil {
		return nil, fmt.Errorf("collector: %s is not a storage table", t)
	}
	return tbl.QueryRaw(dbConstraint(constraint))
}

// Get returns the ad stored under keyAd's key.
func (b *DBBackend) Get(ctx context.Context, t AdType, keyAd *classad.ClassAd) (*classad.ClassAd, bool) {
	tbl := b.tables[t]
	if tbl == nil {
		return nil, false
	}
	key, ok := HashKey(t, keyAd)
	if !ok {
		return nil, false
	}
	return tbl.LookupClassAd(string(key))
}

// Invalidate removes ads from table t: the single ad keyAd identifies (constraint
// ""), else every ad matching constraint. A startd's private ad is removed with
// its public ad.
func (b *DBBackend) Invalidate(ctx context.Context, t AdType, constraint string, keyAd *classad.ClassAd) (int, error) {
	tbl := b.tables[t]
	if tbl == nil {
		return 0, nil
	}
	if constraint == "" {
		if keyAd == nil {
			return 0, nil
		}
		key, ok := HashKey(t, keyAd)
		if !ok {
			return 0, nil
		}
		return b.deleteKey(ctx, t, string(key))
	}
	// Non-startd tables have no private shadow, so the bulk delete-by-constraint
	// pushdown removes the whole match set in one call.
	if t != StartdAd {
		return tbl.DeleteWhere(dbConstraint(constraint))
	}
	// The startd public and private ads share a key, so we must drop both: match
	// the public ads, then delete their keys from both tables.
	seq, err := tbl.Query(dbConstraint(constraint))
	if err != nil {
		return 0, err
	}
	var keys []string
	for ad := range seq {
		if key, ok := HashKey(StartdAd, ad); ok {
			keys = append(keys, string(key))
		}
	}
	n := 0
	for _, k := range keys {
		removed, err := tbl.Delete(k)
		if err != nil {
			return n, err
		}
		if removed {
			n++
		}
		_, _ = b.tables[StartdPvtAd].Delete(k)
	}
	return n, nil
}

// deleteKey removes key from table t and, for the startd table, its private shadow.
func (b *DBBackend) deleteKey(ctx context.Context, t AdType, key string) (int, error) {
	removed, err := b.tables[t].Delete(key)
	if err != nil {
		return 0, err
	}
	if t == StartdAd {
		_, _ = b.tables[StartdPvtAd].Delete(key)
	}
	if removed {
		return 1, nil
	}
	return 0, nil
}

// Watch streams table t's changes, adapting db watch events to the collector's
// collections.WatchEvent and applying an optional constraint filter.
func (b *DBBackend) Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error) {
	tbl := b.tables[t]
	if tbl == nil {
		return nil, fmt.Errorf("collector: %s is not a storage table", t)
	}
	seq, err := tbl.Watch(ctx, cursor)
	if err != nil {
		return nil, err
	}
	out := dbToCollectionsWatch(seq)
	if constraint != "" {
		q, err := parseConstraint(constraint)
		if err != nil {
			return nil, fmt.Errorf("collector: watch constraint %q: %w", constraint, err)
		}
		if q != nil {
			out = collections.WatchFilter(out, q.Matches)
		}
	}
	return out, nil
}

// dbToCollectionsWatch adapts db.WatchEvent to collections.WatchEvent. The two
// WatchKind enums share the same values (Upsert=0, Delete=1, Reset=2); only the
// key type differs (db string vs collections []byte).
func dbToCollectionsWatch(seq iter.Seq[db.WatchEvent]) iter.Seq[collections.WatchEvent] {
	return func(yield func(collections.WatchEvent) bool) {
		for e := range seq {
			if !yield(collections.WatchEvent{
				Kind:   collections.WatchKind(e.Kind),
				Key:    []byte(e.Key),
				Ad:     e.Ad,
				Cursor: e.Cursor,
			}) {
				return
			}
		}
	}
}

// Expire removes every ad past its lifetime across all tables, expressed as the
// db bulk delete-by-constraint pushdown (now > LastHeardFrom + lifetime) so the
// sweep runs inside the store rather than scanning ads out to the collector. Ads
// with no LastHeardFrom never match, so they are never expired (as in-memory).
func (b *DBBackend) Expire(ctx context.Context) (int, error) {
	now := b.now()
	constraint := fmt.Sprintf("%d > %s + ifThenElse(%s =!= undefined, %s, %d)",
		now, attrLastHeardFrom, attrClassAdLifetime, attrClassAdLifetime, b.defaultLifetime)
	total := 0
	for t := AnyAd + 1; t < numAdTypes; t++ {
		n, err := b.tables[t].DeleteWhere(constraint)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// Len returns the number of ads in table t.
func (b *DBBackend) Len(ctx context.Context, t AdType) (int, error) {
	tbl := b.tables[t]
	if tbl == nil {
		return 0, nil
	}
	return tbl.Len(), nil
}

// Close flushes and closes the underlying database.
func (b *DBBackend) Close() error { return b.cat.Close() }
