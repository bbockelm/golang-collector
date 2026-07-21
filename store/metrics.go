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
		retriesTotal: fa.NewCounterVec(prometheus.CounterOpts{
			Namespace: metricsNamespace, Name: "retries_total",
			Help: "Database operations retried after a transient failure (backoff-and-replay attempts), by cause.",
		}, []string{"cause"}),
		adsPerBatch: fa.NewHistogram(prometheus.HistogramOpts{
			Namespace: metricsNamespace, Name: "ads_per_batch",
			Help:    "Ads carried by each flushed batch (a batch touches ~all shards, so this drives lock contention).",
			Buckets: []float64{1, 2, 4, 6, 8, 12, 16, 24, 32, 64, 128, 256, 512, 1024, 2048},
		}),
	}
	for _, c := range retryCauses {
		m.retriesTotal.WithLabelValues(c) // materialize the zero series
	}
	return m
}

// observeUpdate records one ad update's duration under op ("public"/"private").
func (m *metrics) observeUpdate(op string, d time.Duration) {
	m.updateSeconds.WithLabelValues(op).Observe(d.Seconds())
	m.updatesTotal.WithLabelValues(op).Inc()
}

// ObserveUpdate records one handler-observed ad update's latency (the collector's
// responsiveness signal: how long the command handler was blocked applying it).
// op is "public" or "private". Exported for the server package.
func ObserveUpdate(op string, d time.Duration) { Metrics.observeUpdate(op, d) }
