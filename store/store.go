package store

import (
	"context"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/collections/vm"
)

// DefaultLifetime is the ad lifetime (seconds) used for expiration when an ad
// does not carry its own ATTR_CLASSAD_LIFETIME. It matches the C++ collector's
// CLASSAD_LIFETIME default.
const DefaultLifetime = 900

// DefaultWatchHistory is the per-shard delete-journal capacity enabling the Watch
// subscription on every table. It bounds how far back a disconnected watcher can
// resume incrementally before falling to a full replay; with no active watchers
// its only cost is recording deletes.
const DefaultWatchHistory = 8192

// startdHotAttrs are front-loaded in each startd ad's hot header so the common
// matchmaking/status queries that filter on them resolve in O(1) rather than
// decoding the whole ad. The startd table is by far the largest, so this is
// where it matters.
var startdHotAttrs = []string{
	"State", "Activity", "Cpus", "Memory", "Disk", "Arch", "OpSys", "SlotType",
}

// startdCategoricalAttrs / startdValueAttrs are indexed on the startd table so a
// query filtering on them visits only candidate ads instead of scanning the
// whole (largest) table. Categorical = string equality / set membership; value =
// numeric equality and range. Name is indexed because targeted (-name / -direct)
// lookups are common.
var (
	startdCategoricalAttrs = []string{"State", "Activity", "Arch", "OpSys", "SlotType", "Name", "Machine"}
	startdValueAttrs       = []string{"Cpus", "Memory", "Disk"}
)

// Store holds the collector's ad tables: one classad Collection per AdType,
// each keyed by the ad's HashKey. Collections are safe for concurrent use, so
// Store is too; the table set is fixed at construction, so no locking is needed
// around table lookup.
type Store struct {
	cols            map[AdType]*collections.Collection
	now             func() int64
	defaultLifetime int64
}

// New creates an empty Store with every storage table pre-created.
func New() *Store {
	s := &Store{
		cols:            make(map[AdType]*collections.Collection),
		now:             func() int64 { return time.Now().Unix() },
		defaultLifetime: DefaultLifetime,
	}
	for t := AnyAd + 1; t < numAdTypes; t++ {
		opts := collections.Options{WatchHistory: DefaultWatchHistory}
		if t == StartdAd {
			opts.HotAttrs = startdHotAttrs
			opts.CategoricalAttrs = startdCategoricalAttrs
			opts.ValueAttrs = startdValueAttrs
		}
		s.cols[t] = collections.New(opts)
	}
	return s
}

// Watch streams changes to table t as a resumable subscription: pass a nil cursor
// for a full replay, or a cursor returned in a prior WatchSynced event to resume.
// A non-empty constraint (a ClassAd expression) delivers only events for ads that
// match it -- an update that leaves the match set arrives as a Delete, so a
// filtered view stays consistent (see collections.WatchFilter). Errors if t is
// not a storage table or the constraint does not parse.
func (s *Store) Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error) {
	col := s.cols[t]
	if col == nil {
		return nil, fmt.Errorf("collector: %s is not a storage table", t)
	}
	seq, err := col.Watch(ctx, cursor)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(constraint) != "" {
		q, err := vm.Parse(constraint)
		if err != nil {
			return nil, fmt.Errorf("collector: watch constraint %q: %w", constraint, err)
		}
		seq = collections.WatchFilter(seq, q.Matches)
	}
	return seq, nil
}

// Update inserts or replaces ad in table t, stamping it with the current time
// as ATTR_LAST_HEARD_FROM (which drives expiration). It returns an error only if
// the ad carries no name to key on.
func (s *Store) Update(ctx context.Context, t AdType, ad *classad.ClassAd) error {
	col := s.cols[t]
	if col == nil {
		return fmt.Errorf("collector: %s is not a storage table", t)
	}
	key, ok := HashKey(t, ad)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	ad.InsertAttr(attrLastHeardFrom, s.now())
	return col.Put(key, ad)
}

// UpdateOldText inserts or replaces an ad supplied as old-ClassAd wire text (the
// form message.GetClassAdRaw yields), stamping it with the current time as
// ATTR_LAST_HEARD_FROM. It streams the text straight into the collection's wire
// form via UpdateOld, without building an intermediate *classad.ClassAd -- the
// efficient ingest path for ads read off a socket. It errors only if the text
// carries no name to key on.
func (s *Store) UpdateOldText(ctx context.Context, t AdType, text string) error {
	col := s.cols[t]
	if col == nil {
		return fmt.Errorf("collector: %s is not a storage table", t)
	}
	key, ok := hashKeyFromText(t, text)
	if !ok {
		return fmt.Errorf("collector: %s ad has no Name/Machine to key on", t)
	}
	// Stamp the receive time (drives expiration). If the ad already carried a
	// LastHeardFrom, the encoder's duplicate handling keeps the last one -- ours.
	if !strings.HasSuffix(text, "\n") {
		text += "\n"
	}
	text += attrLastHeardFrom + " = " + strconv.FormatInt(s.now(), 10) + "\n"
	return col.UpdateOld([]collections.OldAdUpdate{{Key: key, Text: text}})
}

// UpdatePvt stores a startd *private* ad (supplementary attributes like claim
// ids that only the negotiator may see) in the StartdPvtAd table, keyed the same
// as its public ad -- whose wire text is publicText.
//
// The startd's raw private ad carries only its secret(s) (the claim capability),
// not identifying attributes. The negotiator, however, correlates a private ad
// back to its public slot ad by (Name, MyAddress) and drops any private ad that
// lacks them (reporting "no claim id"). So, exactly like the C++ collector
// (collector_engine.cpp receive_update), we copy Name, MyAddress and MyType from
// the public ad into the private ad. These are prepended so they win under the
// wire encoder's first-occurrence-wins semantics.
func (s *Store) UpdatePvt(ctx context.Context, publicText, pvtText string) error {
	col := s.cols[StartdPvtAd]
	if col == nil {
		return fmt.Errorf("collector: StartdPvt is not a storage table")
	}
	key, ok := hashKeyFromText(StartdAd, publicText)
	if !ok {
		return fmt.Errorf("collector: startd private ad's public ad has no Name to key on")
	}
	header := copyAttrLines(publicText, attrName, attrMyAddress, attrMyType)
	if !strings.HasSuffix(pvtText, "\n") {
		pvtText += "\n"
	}
	pvtText = header + pvtText +
		attrLastHeardFrom + " = " + strconv.FormatInt(s.now(), 10) + "\n"
	return col.UpdateOld([]collections.OldAdUpdate{{Key: key, Text: pvtText}})
}

// copyAttrLines returns the verbatim "Attr = Value" lines for the named
// attributes (first occurrence of each) from old-ClassAd wire text, newline
// terminated -- used to copy identifying attributes from a public ad into its
// private ad.
func copyAttrLines(text string, attrs ...string) string {
	want := make(map[string]bool, len(attrs))
	for _, a := range attrs {
		want[strings.ToLower(a)] = true
	}
	seen := make(map[string]bool, len(attrs))
	var b strings.Builder
	for _, line := range strings.Split(text, "\n") {
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(line[:eq]))
		if want[name] && !seen[name] {
			b.WriteString(strings.TrimRight(line, "\r"))
			b.WriteByte('\n')
			seen[name] = true
		}
	}
	return b.String()
}

// RetrainDict retrains each table's ZSTD dictionary from a sample of up to
// sampleMax of its live ads and recompacts under the new codec. A fresh
// collection starts with the identity (no-compression) codec; training a
// dictionary over the real ad population is what unlocks the collection's
// compression, so the collector should call this once enough ads have arrived
// and periodically thereafter (see StartAutoRetrain).
func (s *Store) RetrainDict(sampleMax int) {
	for _, col := range s.cols {
		if col != nil {
			_, _ = col.RetrainDict(sampleMax)
		}
	}
}

// StartAutoRetrain starts periodic ZSTD dictionary retraining on every table in
// a background goroutine, and returns a stop function (which blocks until the
// goroutines exit). Without this the collection stays on the identity codec and
// stores ads uncompressed; a dictionary trained over the real ad population
// compresses a pool of similar ads several-fold. hotTopN is 0 here because the
// collector front-loads a fixed hot-attribute set rather than auto-tuning it.
func (s *Store) StartAutoRetrain(interval time.Duration, sampleMax int) func() {
	stops := make([]func(), 0, len(s.cols))
	for _, col := range s.cols {
		if col != nil {
			stops = append(stops, col.StartAutoRetrain(interval, sampleMax, 0))
		}
	}
	return func() {
		for _, stop := range stops {
			stop()
		}
	}
}

// Stats returns per-table storage statistics (ad counts and compressed byte
// footprint) for every storage table, for observability / metrics. Only tables
// that hold ads are included.
func (s *Store) Stats() map[AdType]collections.Stats {
	out := make(map[AdType]collections.Stats, len(s.cols))
	for t, col := range s.cols {
		if col != nil {
			out[t] = col.Stats()
		}
	}
	return out
}

// Get returns the stored ad identified the same way ad would be (by HashKey).
func (s *Store) Get(ctx context.Context, t AdType, ad *classad.ClassAd) (*classad.ClassAd, bool) {
	col := s.cols[t]
	if col == nil {
		return nil, false
	}
	key, ok := HashKey(t, ad)
	if !ok {
		return nil, false
	}
	return col.Get(key)
}

// parseConstraint compiles a constraint string into a *vm.Query, treating "" and
// a literal "true" as "match everything" (a nil query). It is the single place
// the Backend's string constraints become the collection's compiled form.
func parseConstraint(constraint string) (*vm.Query, error) {
	s := strings.TrimSpace(constraint)
	if s == "" || strings.EqualFold(s, "true") {
		return nil, nil
	}
	q, err := vm.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("collector: constraint %q: %w", constraint, err)
	}
	return q, nil
}

// Query yields every ad in table t matching constraint (a ClassAd expression, or
// "" for all ads). For AnyAd it yields matches across all public tables. limit is
// accepted for the Backend contract but not enforced here -- the caller stops
// iteration at the limit (the query handlers already do). It errors only if the
// constraint does not parse.
func (s *Store) Query(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[*classad.ClassAd], error) {
	q, err := parseConstraint(constraint)
	if err != nil {
		return nil, err
	}
	return s.query(t, q), nil
}

// query is the compiled-query core shared by Query (string) and in-process
// callers that already hold a *vm.Query.
func (s *Store) query(t AdType, q *vm.Query) iter.Seq[*classad.ClassAd] {
	if t == AnyAd {
		return func(yield func(*classad.ClassAd) bool) {
			for at := AnyAd + 1; at < numAdTypes; at++ {
				if at == StartdPvtAd {
					continue // private ads are never returned by an ANY query
				}
				for ad := range s.queryOne(at, q) {
					if !yield(ad) {
						return
					}
				}
			}
		}
	}
	return s.queryOne(t, q)
}

func (s *Store) queryOne(t AdType, q *vm.Query) iter.Seq[*classad.ClassAd] {
	col := s.cols[t]
	if col == nil {
		return func(func(*classad.ClassAd) bool) {}
	}
	if q == nil {
		return col.Scan()
	}
	return col.Query(q)
}

// QueryRaw is Query, but yields each matching ad as collections.RawAd -- the
// old-ClassAd expression strings decoded straight from the stored form with no
// AST -- for streaming a result set to the wire via message.PutClassAdRaw. It is
// used only when the query has no projection (raw ads are whole ads). QueryRaw
// makes Store a store.RawQueryer. limit is not enforced here (the caller stops).
func (s *Store) QueryRaw(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error) {
	q, err := parseConstraint(constraint)
	if err != nil {
		return nil, err
	}
	return s.queryRaw(t, q), nil
}

func (s *Store) queryRaw(t AdType, q *vm.Query) iter.Seq[collections.RawAd] {
	if t == AnyAd {
		return func(yield func(collections.RawAd) bool) {
			for at := AnyAd + 1; at < numAdTypes; at++ {
				if at == StartdPvtAd {
					continue // private ads are never returned by an ANY query
				}
				for ra := range s.queryOneRaw(at, q) {
					if !yield(ra) {
						return
					}
				}
			}
		}
	}
	return s.queryOneRaw(t, q)
}

func (s *Store) queryOneRaw(t AdType, q *vm.Query) iter.Seq[collections.RawAd] {
	col := s.cols[t]
	if col == nil {
		return func(func(collections.RawAd) bool) {}
	}
	if q == nil {
		return col.ScanRaw()
	}
	return col.QueryRaw(q)
}

// Invalidate removes ads from table t. If a constraint q is given, every ad it
// matches is removed; otherwise the single ad identified by keyAd (by HashKey)
// is removed. It returns the number of ads removed.
func (s *Store) Invalidate(ctx context.Context, t AdType, constraint string, keyAd *classad.ClassAd) (int, error) {
	q, err := parseConstraint(constraint)
	if err != nil {
		return 0, err
	}
	col := s.cols[t]
	if col == nil {
		return 0, nil
	}

	// Determine the keys to remove: the single identified ad, or every ad the
	// constraint matches. (Collecting keys before deleting keeps the intent
	// obvious; deleting mid-scan would be safe under the collection's MVCC too.)
	var keys [][]byte
	if q == nil {
		if keyAd == nil {
			return 0, nil
		}
		if key, ok := HashKey(t, keyAd); ok {
			keys = append(keys, key)
		}
	} else {
		for ad := range col.Query(q) {
			if key, ok := HashKey(t, ad); ok {
				keys = append(keys, key)
			}
		}
	}

	// A startd's private ad is keyed like its public ad, so invalidate it too.
	var pvt *collections.Collection
	if t == StartdAd {
		pvt = s.cols[StartdPvtAd]
	}
	n := 0
	for _, key := range keys {
		if col.Delete(key) {
			n++
		}
		if pvt != nil {
			pvt.Delete(key)
		}
	}
	return n, nil
}

// Expire removes ads whose ATTR_LAST_HEARD_FROM is older than their lifetime
// (ATTR_CLASSAD_LIFETIME, or DefaultLifetime). It is meant to be called on a
// timer and at startup/shutdown. It returns the number of ads reaped. The error
// is always nil for the in-memory backend (the Backend contract allows a
// persistent backend's sweep to fail).
func (s *Store) Expire(ctx context.Context) (int, error) {
	now := s.now()
	n := 0
	for at := AnyAd + 1; at < numAdTypes; at++ {
		col := s.cols[at]
		if col == nil {
			continue
		}
		var stale [][]byte
		for ad := range col.Scan() {
			lastHeard, ok := ad.EvaluateAttrInt(attrLastHeardFrom)
			if !ok {
				continue
			}
			lifetime, ok := ad.EvaluateAttrInt(attrClassAdLifetime)
			if !ok {
				lifetime = s.defaultLifetime
			}
			if now-lastHeard > lifetime {
				if key, ok := HashKey(at, ad); ok {
					stale = append(stale, key)
				}
			}
		}
		for _, key := range stale {
			if col.Delete(key) {
				n++
			}
		}
	}
	return n, nil
}

// Len returns the number of ads in table t.
func (s *Store) Len(ctx context.Context, t AdType) (int, error) {
	if col := s.cols[t]; col != nil {
		return col.Len(), nil
	}
	return 0, nil
}

// Close releases the in-memory backend. It is a no-op (nothing to flush or
// close), present to satisfy store.Backend.
func (s *Store) Close() error { return nil }
