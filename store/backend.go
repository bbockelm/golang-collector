package store

import (
	"context"
	"errors"
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

// ProjectedRawQueryer is RawQueryer with a server-side projection: each returned
// RawAd carries only the requested attributes (plus MyType/TargetType). A remote
// database backend implements this by pushing the projection to the server, so a
// projected query (e.g. condor_status -totals) does not pull every attribute of
// every ad across the wire and then discard most of it. Backends without it fall
// back to the materialized Query path, which projects locally.
type ProjectedRawQueryer interface {
	QueryRawProject(ctx context.Context, t AdType, constraint string, projection []string, limit int) (iter.Seq[collections.RawAd], error)
}

// RawStreamer is the streaming form of RawQueryer: instead of returning an iterator
// over an already-fetched result, it delivers each matching RawAd to yield as it arrives
// from the backend, so a relay (the collector) can forward each ad to its own client
// without buffering the whole result set. yield returns false to stop early.
//
// A remote-database backend implements it (the in-memory backend's RawQueryer already
// iterates its store lazily, so it needs no separate streamer). Unlike an iter.Seq, the
// returned error surfaces a mid-stream backend failure -- the relay then fails its
// response instead of silently truncating it. The server prefers this over RawQueryer
// when the backend offers it.
type RawStreamer interface {
	QueryRawStream(ctx context.Context, t AdType, constraint string, limit int, yield func(collections.RawAd) bool) error
}

// ProjectedRawStreamer is RawStreamer with a server-side projection pushed down (see
// ProjectedRawQueryer).
type ProjectedRawStreamer interface {
	QueryRawProjectStream(ctx context.Context, t AdType, constraint string, projection []string, limit int, yield func(collections.RawAd) bool) error
}

// ErrRedactionNotSupported reports that a backend cannot guarantee source-side
// redaction of private attributes; the caller should fall back to redacting the
// results itself. Returned by wrappers (BufferedBackend) whose method set cannot
// mirror the wrapped backend's capabilities statically.
var ErrRedactionNotSupported = errors.New("collector: backend does not support source-side redaction")

// RedactedRawQueryer is an optional Backend capability: QueryRawRedacted is
// QueryRaw with the backend GUARANTEEING that no private (secret) attribute
// appears in any yielded ad. The in-memory store strips them at the source --
// its intern table flags each attribute name as private once, when the name is
// first interned, and the redacting de-intern skips flagged ids with a single
// bool check, never rendering the private value at all. When a backend offers
// this, the server serves a redacted (public) query without scanning each ad's
// attribute names itself; backends without it fall back to the server's per-ad
// redaction pass (redactRawExprs).
type RedactedRawQueryer interface {
	QueryRawRedacted(ctx context.Context, t AdType, constraint string, limit int) (iter.Seq[collections.RawAd], error)
}

// RedactedProjectedRawQueryer is RedactedRawQueryer with a projection applied to
// the already-redacted ads (see ProjectedRawQueryer).
type RedactedProjectedRawQueryer interface {
	QueryRawProjectRedacted(ctx context.Context, t AdType, constraint string, projection []string, limit int) (iter.Seq[collections.RawAd], error)
}

// ErrWireRowsNotSupported reports that a backend cannot stream wire-form rows;
// the caller falls back to the text row streams. Returned by wrappers whose
// method set cannot mirror the wrapped backend's capabilities statically.
var ErrWireRowsNotSupported = errors.New("collector: backend does not stream wire-form rows")

// WireRowStreamer is an optional Backend capability: stream matching ads as
// self-contained WIRE-FORM ROWS -- collections inline subset ads assembled by
// slice copies at the source (projection and, when redact is set, private-
// attribute stripping applied there), with the old-ClassAd render deferred to
// the caller's client edge (collections.RenderRawAdInline). This is the fast
// relay for a remote-database backend: many rows batch per transport frame and
// nothing is rendered or re-parsed in the middle. The row slice passed to
// yield is valid only until yield returns.
type WireRowStreamer interface {
	QueryRawWireStream(ctx context.Context, t AdType, constraint string, projection []string, limit int, redact bool, yield func(row []byte) bool) error
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
	_ Backend                     = (*Store)(nil)
	_ RawQueryer                  = (*Store)(nil)
	_ ProjectedRawQueryer         = (*Store)(nil)
	_ RedactedRawQueryer          = (*Store)(nil)
	_ RedactedProjectedRawQueryer = (*Store)(nil)
	_ Retrainer                   = (*Store)(nil)
	_ Statser                     = (*Store)(nil)
)
