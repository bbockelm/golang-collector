package store

import (
	"context"
	"iter"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
)

// Backend is the storage contract the collector's command handlers depend on: an
// indexed, watchable ClassAd store partitioned into the collector's ad tables
// (one per AdType). The default implementation (Store) keeps ads in in-memory
// collections; alternative backends persist them in an embedded classad/db or a
// remote classad/dbrpc database (see the db and dbrpc backends).
//
// Queries and invalidations take a *constraint string* (a ClassAd expression, ""
// meaning "match everything") rather than a compiled *vm.Query: it is the form
// that arrives on the wire, the only form a remote database can be handed, and
// the compiled query has no source text to recover. An in-memory backend compiles
// it once per call. Watch events are collections.WatchEvent -- the collector's
// native change-event type -- which every backend adapts its store's events to.
//
// Optional capabilities a backend MAY also implement, discovered by type
// assertion: RawQueryer (a wire-form query fast path), Retrainer (dictionary
// compression maintenance), Statser (introspection metrics). A backend that does
// not implement one simply loses that optimization/feature, never correctness.
// Every I/O method takes a context.Context as its first argument: for a remote
// backend (RPCBackend) it bounds and cancels the operation -- there are no implicit
// timeouts, the deadline is exactly what the caller passes -- and drives retry over
// a transient database outage. Local backends (the in-memory Store, the embedded
// DBBackend) accept it for interface uniformity and honor cancellation where cheap.
type Backend interface {
	// Update inserts or replaces ad in table t, stamping ATTR_LAST_HEARD_FROM.
	// It errors if the ad has no name/machine to key on.
	Update(ctx context.Context, t AdType, ad *classad.ClassAd) error
	// UpdateOldText ingests an ad from old-ClassAd wire text (the socket form),
	// stamping ATTR_LAST_HEARD_FROM, without building an intermediate ClassAd.
	UpdateOldText(ctx context.Context, t AdType, text string) error
	// UpdatePvt ingests a startd's public + private ads (both keyed alike) from
	// their old-ClassAd wire texts.
	UpdatePvt(ctx context.Context, publicText, pvtText string) error

	// Query yields every ad in table t matching constraint (all ads if
	// constraint is ""). For AnyAd it yields matches across all public tables.
	// limit > 0 caps the result count (a hint backends may push down; callers
	// must still honor it). It errors only if constraint does not parse.
	Query(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[*classad.ClassAd], error)

	// Get returns the ad stored under keyAd's key.
	Get(ctx context.Context, t AdType, keyAd *classad.ClassAd) (*classad.ClassAd, bool)

	// Invalidate removes ads from table t: the single ad keyAd identifies when
	// constraint is "", otherwise every ad matching constraint. Returns the count
	// removed. A startd's private ad is removed alongside its public ad.
	Invalidate(ctx context.Context, t AdType, constraint string, keyAd *classad.ClassAd) (int, error)

	// Watch streams changes to table t as a resumable subscription: a nil cursor
	// replays from the start, a cursor from a prior WatchSynced event resumes. A
	// non-empty constraint delivers only events for matching ads (an ad leaving
	// the match set arrives as a Delete). Errors if t is not a table or the
	// constraint does not parse.
	Watch(ctx context.Context, t AdType, cursor []byte, constraint string) (iter.Seq[collections.WatchEvent], error)

	// Expire removes every ad whose ATTR_LAST_HEARD_FROM + lifetime has passed
	// (ATTR_CLASSAD_LIFETIME, or DefaultLifetime), across all tables. Returns the
	// count reaped. Meant to be called on a timer and at startup/shutdown.
	Expire(ctx context.Context) (int, error)

	// Len returns the number of ads in table t.
	Len(ctx context.Context, t AdType) (int, error)

	// Close flushes and releases the backend (a persistent backend syncs and
	// closes its database; the in-memory backend is a no-op). After Close the
	// backend must not be used.
	Close() error
}

// RawQueryer is an optional Backend fast path: it streams query results in
// wire-ready old-ClassAd form (collections.RawAd -- expression strings decoded
// straight from the stored representation, no AST) so a whole-ad (unprojected)
// response can be relayed to the client without materializing and re-encoding
// each ad. The in-memory backend implements it; backends whose results arrive as
// materialized ads or remote text do not, and the server falls back to Query.
type RawQueryer interface {
	QueryRaw(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error)
}

// Retrainer is an optional Backend capability: periodic maintenance of the
// ClassAd compression dictionary (the in-memory backend's memory-footprint
// lever). Backends that manage their own storage do not implement it.
type Retrainer interface {
	// StartAutoRetrain begins periodic dictionary retraining and returns a stop
	// function. RetrainDict runs one retraining pass immediately.
	StartAutoRetrain(interval time.Duration, sampleMax int) func()
	RetrainDict(sampleMax int)
}

// Statser is an optional Backend capability: per-table introspection metrics
// (ad counts and stored-byte footprints) for the collector's metrics endpoint.
type Statser interface {
	Stats() map[AdType]collections.Stats
}

// The in-memory Store is the default backend and implements every capability.
var (
	_ Backend    = (*Store)(nil)
	_ RawQueryer = (*Store)(nil)
	_ Retrainer  = (*Store)(nil)
	_ Statser    = (*Store)(nil)
)
