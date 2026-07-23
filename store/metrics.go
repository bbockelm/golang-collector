package store

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// SlowOpThreshold: a backend operation (an ad update, or a buffered batch flush)
// taking at least this long is logged as a slow-op WARN, so a stall surfaces in the
// log even when the op eventually succeeds -- withRetry otherwise hides it as latency.
// 0 disables it. Default 2s (only real stalls fire); overridable via COLLECTOR_SLOW_OP_MS.
var SlowOpThreshold = 2 * time.Second

// MaybeLogSlow emits a WARN when d reaches SlowOpThreshold. op is a short label
// ("update", "update-pvt", "batch-flush"); attrs are extra slog key/values for context.
func MaybeLogSlow(op string, d time.Duration, attrs ...any) {
	if SlowOpThreshold <= 0 || d < SlowOpThreshold {
		return
	}
	slog.Warn("collector: slow "+op+" (backend stall)", append([]any{"elapsed", d}, attrs...)...)
}

// MetricsRegistry holds the collector's operational (as opposed to storage-
// footprint) metrics: how long updates take, how much time is lost to
// retry/backoff against a slow or down database, and how many updates/batches
// flow through. It is a dedicated registry (not the global default) so the
// metrics endpoint can gather it alongside the storage gauges without any
// global-registry coupling, and so it is exposed regardless of backend type --
// the operational metrics matter most for the remote-database (RPCBackend)
// backend, which has no storage stats of its own.
var MetricsRegistry = prometheus.NewRegistry()

const metricsNamespace = "condor_collector"

// Metrics is the set of operational instruments, updated at the point of each
// operation (push style), in contrast to the scrape-time storage gauges.
var Metrics = newMetrics(MetricsRegistry)

type metrics struct {
	// updateSeconds is the wall-clock time of one ad update through the backend
	// (UpdateOldText / UpdatePvt), including any batching flush + retry/backoff it
	// triggers. Labeled by op ("public"/"private") -- the private path is the one
	// that goes through the separate per-ad transaction.
	updateSeconds *prometheus.HistogramVec
	// batchSeconds is the wall-clock time to flush one buffered batch (the whole
	// begin->commit across all touched tables, including retry/backoff).
	batchSeconds prometheus.Histogram
	// backoffSeconds is time spent parked in retry backoff (blocked, not working)
	// before a database operation was retried -- the direct measure of how much a
	// slow/down database stalls the update path.
	backoffSeconds prometheus.Histogram
	// updatesTotal / batchesTotal count updates and flushed batches; retriesTotal
	// counts backoff-and-replay attempts (a rising rate means the database is
	// flaky/slow); adsPerBatch records how many ads each flushed batch carried.
	updatesTotal *prometheus.CounterVec
	batchesTotal prometheus.Counter
	retriesTotal *prometheus.CounterVec
	adsPerBatch  prometheus.Histogram
	// backpressureTotal counts times a producer had to flush inline because the buffer
	// hit its hard cap (the background writer could not keep up). A rising rate means the
	// update stream is being throttled by commit throughput -- the signal that writes are
	// no longer pipelining freely.
	backpressureTotal prometheus.Counter
	// commitFanout records how many concurrent commit units a fanned-out UpdateBatch split
	// into (1 = no fan-out). It shows the adaptive write parallelism engaging: values >1
	// appear only when a flush is large enough to spread across the write pool.
	commitFanout prometheus.Histogram

	// RPC-layer instruments for the remote-database backend -- where the update/query
	// path actually spends its time once the DB itself is fast. These isolate the wire
	// cost (per-request round-trips, queueing on a shared connection) from DB work.
	//
	// rpcInflight is the number of backend operations currently awaiting the database,
	// by lane ("write"/"read"). The write lane is a single connection, so a sustained
	// value >1 there is head-of-line blocking -- ops queued behind a slow one.
	rpcInflight *prometheus.GaugeVec
	// batchWriteSeconds is the round-trip to write one table's batch of ads: a single
	// pipelined NewClassAdBatch send (all chunks in flight at once, acks collected),
	// NOT one round-trip per ad. So batch_flush_seconds ~= batchWrite + commit,
	// regardless of ad count -- the pipelined batch is what replaced the old chatty
	// per-ad write path (a 2ms round-trip x 1000 ads = 2s) with ~one round-trip.
	batchWriteSeconds prometheus.Histogram
	// commitSeconds is the transaction Commit round-trip (includes the server-side
	// durability sync), separated from the per-ad writes so a slow commit is
	// distinguishable from chatty per-ad round-trips.
	commitSeconds prometheus.Histogram
	// querySeconds is one query's round-trip: request out, all matching rows streamed
	// back and collected. queryRows is how many rows that query returned -- together
	// they show whether a slow read is latency, row volume, or the DB.
	querySeconds prometheus.Histogram
	queryRows    prometheus.Histogram
	// rpcBeginSeconds is the BeginTable round-trip -- the previously-unmeasured gap in a
	// flush (batch_flush = begin + batch_write + commit, and only the latter two were
	// timed). If a stall lives here, rpcCallPhaseSeconds{op="begin"} says which phase.
	rpcBeginSeconds prometheus.Histogram
	// rpcCallPhaseSeconds decomposes every write-path RPC round-trip into where the time
	// goes, so a stall is localized to client / comms / server without a server profile:
	//   phase="write_wait" -- blocked on the single-writer lock behind another send (client),
	//   phase="send"       -- in conn.WriteMsg; spikes here are TCP backpressure because the
	//                         server stopped reading the socket (comms / server-read stall),
	//   phase="wait"       -- request left promptly, server+return path was slow (server).
	// Labeled by op (begin/commit/batch); begin's three phases sum to rpcBeginSeconds.
	rpcCallPhaseSeconds *prometheus.HistogramVec
}

// retryCauses is the closed set of transient-failure causes labeled onto
// retries_total (see causeOf in rpcbackend.go). They are pre-initialized to zero so
// the metric family is always present on /metrics (a CounterVec emits nothing until a
// label is observed) and dashboards/alerts have a stable series from the first scrape.
var retryCauses = []string{"no_txn", "deadline", "conn_closed", "transport", "dial"}

func newMetrics(reg prometheus.Registerer) *metrics {
	fa := promauto.With(reg)
	// Latency buckets from ~1ms to ~30s: updates should be sub-ms to ms; anything
	// into the hundreds of ms / seconds is the "blocking" we are hunting.
	latency := []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	// Finer buckets for a single wire round-trip: a healthy localhost round-trip is
	// tens-to-hundreds of microseconds, so start at 25µs to distinguish "fast RTT" from
	// the millisecond-scale RTTs that turn a large batch into a multi-second flush.
	rtt := []float64{.000025, .00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5}
	rows := []float64{1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000}
	m := &metrics{
		updateSeconds: fa.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "update_seconds",
			Help:    "Wall-clock time to apply one ad update through the backend (incl. batching/retry), by op.",
			Buckets: latency,
		}, []string{"op"}),
		batchSeconds: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "batch_flush_seconds",
			Help:    "Wall-clock time to flush one buffered batch (begin->commit across touched tables, incl. retry).",
			Buckets: latency,
		}),
		backoffSeconds: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "backoff_seconds",
			Help:    "Time parked in retry backoff before a database op was retried (blocked, not working).",
			Buckets: latency,
		}),
		updatesTotal: fa.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "updates_total",
			Help: "Ad updates applied, by op.",
		}, []string{"op"}),
		batchesTotal: fa.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "batches_total",
			Help: "Buffered batches flushed.",
		}),
		backpressureTotal: fa.NewCounter(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "backpressure_total",
			Help: "Times a producer flushed inline because the buffer hit its hard cap (the background writer fell behind).",
		}),
		commitFanout: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "commit_fanout",
			Help:    "Concurrent commit units a fanned-out batch split into (1 = no fan-out); shows adaptive write parallelism engaging under load.",
			Buckets: []float64{1, 2, 4, 8, 16, 32},
		}),
		retriesTotal: fa.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "retries_total",
			Help: "Database operations retried after a transient failure (backoff-and-replay attempts), by cause.",
		}, []string{"cause"}),
		adsPerBatch: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "ads_per_batch",
			Help:    "Ads carried by each flushed batch (a batch touches ~all shards, so this drives lock contention).",
			Buckets: []float64{1, 2, 4, 6, 8, 12, 16, 24, 32, 64, 128, 256, 512, 1024, 2048},
		}),
		rpcInflight: fa.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: metricsNamespace, Name: "rpc_inflight",
			Help: "Backend operations currently awaiting the database, by lane. Sustained >1 on the write lane (a single connection) is head-of-line blocking.",
		}, []string{"lane"}),
		batchWriteSeconds: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "rpc_batch_write_seconds",
			Help:    "Round-trip to write one table's batch of ads as a single pipelined NewClassAdBatch (all chunks in flight, acks collected) -- ~one round-trip regardless of ad count, not one per ad.",
			Buckets: rtt,
		}),
		commitSeconds: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "rpc_commit_seconds",
			Help:    "Round-trip to commit a batch transaction (includes the server-side durability sync).",
			Buckets: rtt,
		}),
		rpcBeginSeconds: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "rpc_begin_seconds",
			Help:    "BeginTable round-trip -- the previously-untimed gap in a flush; if this is where the stall lives, rpc_call_phase_seconds{op=\"begin\"} says which phase (client write-lock / socket send / server wait).",
			Buckets: latency, // spans µs-healthy to the multi-second stalls we are hunting
		}),
		rpcCallPhaseSeconds: fa.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "rpc_call_phase_seconds",
			Help:    "Per-RPC round-trip time split by phase to localize a stall: write_wait (client single-writer lock), send (conn.WriteMsg; TCP backpressure = server not reading), wait (server + return). Labeled by op.",
			Buckets: latency,
		}, []string{"op", "phase"}),
		querySeconds: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "rpc_query_seconds",
			Help:    "Round-trip for one query: request out, all matching rows streamed back and collected.",
			Buckets: rtt,
		}),
		queryRows: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "rpc_query_rows",
			Help:    "Rows returned by one query (paired with rpc_query_seconds to tell latency from row volume).",
			Buckets: rows,
		}),
	}
	for _, c := range retryCauses {
		m.retriesTotal.WithLabelValues(c) // materialize the zero series
	}
	for _, lane := range []string{"write", "read"} {
		m.rpcInflight.WithLabelValues(lane) // materialize the zero series
	}
	return m
}

// observeUpdate records one ad update's duration under op ("public"/"private").
func (m *metrics) observeUpdate(op string, d time.Duration) {
	m.updateSeconds.WithLabelValues(op).Observe(d.Seconds())
	m.updatesTotal.WithLabelValues(op).Inc()
}

// observeQuery records one query round-trip: its latency always (even a failed query
// took time), and -- only on success -- the row count, so a slow read is attributable
// to latency versus row volume.
func observeQuery(start time.Time, rows int, err error) {
	Metrics.querySeconds.Observe(time.Since(start).Seconds())
	if err == nil {
		Metrics.queryRows.Observe(float64(rows))
	}
}

// ObserveUpdate records one handler-observed ad update's latency (the collector's
// responsiveness signal: how long the command handler was blocked applying it).
// op is "public" or "private". Exported for the server package.
func ObserveUpdate(op string, d time.Duration) { Metrics.observeUpdate(op, d) }
