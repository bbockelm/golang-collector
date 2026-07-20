// Package metrics exposes the collector's storage footprint as Prometheus
// metrics, so an operator can size a pool (and watch its compressed memory
// footprint grow) without attaching a profiler. The gauges are computed live on
// each scrape from the store, so they never go stale.
package metrics

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bbockelm/golang-collector/store"
)

const namespace = "condor_collector"

// storeCollector implements prometheus.Collector over a store.Statser, emitting
// per-ad-type storage gauges. Reading on Collect (rather than caching gauges)
// keeps the numbers exact and lock-scoped to the scrape.
type storeCollector struct {
	st store.Statser

	ads      *prometheus.Desc
	arena    *prometheus.Desc
	live     *prometheus.Desc
	dead     *prometheus.Desc
	segments *prometheus.Desc
}

func newStoreCollector(st store.Statser) *storeCollector {
	label := []string{"ad_type"}
	return &storeCollector{
		st: st,
		ads: prometheus.NewDesc(namespace+"_ads",
			"Number of live ads held, by ad type.", label, nil),
		arena: prometheus.NewDesc(namespace+"_arena_bytes",
			"Compressed arena bytes reserved for record storage -- the dominant resident memory footprint -- by ad type.", label, nil),
		live: prometheus.NewDesc(namespace+"_live_bytes",
			"Compressed bytes of live records, by ad type.", label, nil),
		dead: prometheus.NewDesc(namespace+"_dead_bytes",
			"Compressed bytes of superseded records reclaimable by compaction, by ad type.", label, nil),
		segments: prometheus.NewDesc(namespace+"_segments",
			"Number of arena segments, by ad type.", label, nil),
	}
}

func (c *storeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ads
	ch <- c.arena
	ch <- c.live
	ch <- c.dead
	ch <- c.segments
}

func (c *storeCollector) Collect(ch chan<- prometheus.Metric) {
	for t, s := range c.st.Stats() {
		adType := t.String()
		g := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, adType)
		}
		g(c.ads, float64(s.Ads))
		g(c.arena, float64(s.ArenaBytes))
		g(c.live, float64(s.LiveBytes()))
		g(c.dead, float64(s.DeadBytes))
		g(c.segments, float64(s.Segments))
	}
}

// DBDiagnoser fetches per-table diagnostics (storage stats + operational timing
// counters) from a remote database backend. The RPCBackend implements it; local
// backends do not (their storage is covered by storeCollector).
type DBDiagnoser interface {
	DBDiagnostics(ctx context.Context) (map[string]*dbrpc.Diagnostics, error)
}

// dbMetricsTTL bounds how often a scrape triggers a remote diagnostics fetch. The
// fetch is not free (the server samples ads for index suggestions), so repeated
// scrapes within the TTL reuse the last snapshot.
const dbMetricsTTL = 15 * time.Second

// dbCollector exposes a remote database's per-table storage footprint and operational
// timing counters (fetched over dbrpc, under the condor_collector_db_ prefix), so an
// operator scraping only the collector still sees whether the database itself is
// "blocking the world" -- long shard-write holds, slow syncs, expensive maintenance.
// It caches the fetched snapshot for dbMetricsTTL and serves the last good snapshot if
// a fetch fails, so a transient database hiccup does not blank the metrics.
type dbCollector struct {
	dbd DBDiagnoser

	mu    sync.Mutex
	at    time.Time
	cache map[string]*dbrpc.Diagnostics

	ads       *prometheus.Desc
	arena     *prometheus.Desc
	used      *prometheus.Desc
	live      *prometheus.Desc
	dead      *prometheus.Desc
	segments  *prometheus.Desc
	opSeconds *prometheus.Desc
	opOps     *prometheus.Desc
}

func newDBCollector(dbd DBDiagnoser) *dbCollector {
	p := namespace + "_db"
	tbl := []string{"table"}
	tblOp := []string{"table", "op"}
	return &dbCollector{
		dbd: dbd,
		ads: prometheus.NewDesc(p+"_ads",
			"Live ads held by the remote database, by table.", tbl, nil),
		arena: prometheus.NewDesc(p+"_arena_bytes",
			"Compressed arena bytes reserved by the remote database, by table.", tbl, nil),
		used: prometheus.NewDesc(p+"_used_bytes",
			"Compressed bytes written into segments (live plus reclaimable dead), by table.", tbl, nil),
		live: prometheus.NewDesc(p+"_live_bytes",
			"Compressed bytes of live records in the remote database, by table.", tbl, nil),
		dead: prometheus.NewDesc(p+"_dead_bytes",
			"Compressed bytes reclaimable by compaction in the remote database, by table.", tbl, nil),
		segments: prometheus.NewDesc(p+"_segments",
			"Arena segments in the remote database, by table.", tbl, nil),
		opSeconds: prometheus.NewDesc(p+"_op_seconds_total",
			"Cumulative wall time spent in each remote-store stall point (shard write lock wait/hold, segment allocation, durability sync, compaction/retrain/reindex, snapshot lock), by table and op.", tblOp, nil),
		opOps: prometheus.NewDesc(p+"_op_ops_total",
			"Cumulative number of times each remote-store stall point ran, by table and op. Divide op_seconds_total by this for mean latency.", tblOp, nil),
	}
}

// snapshot returns the cached diagnostics, refetching when older than dbMetricsTTL.
// A failed/empty fetch keeps the previous snapshot rather than blanking metrics.
func (c *dbCollector) snapshot() map[string]*dbrpc.Diagnostics {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cache != nil && time.Since(c.at) < dbMetricsTTL {
		return c.cache
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d, _ := c.dbd.DBDiagnostics(ctx) // best-effort; partial results are usable
	if len(d) > 0 {
		c.cache = d
		c.at = time.Now()
	}
	return c.cache
}

func (c *dbCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.ads
	ch <- c.arena
	ch <- c.used
	ch <- c.live
	ch <- c.dead
	ch <- c.segments
	ch <- c.opSeconds
	ch <- c.opOps
}

func (c *dbCollector) Collect(ch chan<- prometheus.Metric) {
	for table, d := range c.snapshot() {
		if d == nil {
			continue
		}
		st := d.Stats
		gauge := func(desc *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, table)
		}
		gauge(c.ads, float64(st.Ads))
		gauge(c.arena, float64(st.ArenaBytes))
		gauge(c.used, float64(st.UsedBytes))
		gauge(c.live, float64(st.LiveBytes()))
		gauge(c.dead, float64(st.DeadBytes))
		gauge(c.segments, float64(st.Segments))
		for _, e := range dbOpStatList(d.OpStats) {
			ch <- prometheus.MustNewConstMetric(c.opOps, prometheus.CounterValue, float64(e.stat.Count), table, e.op)
			ch <- prometheus.MustNewConstMetric(c.opSeconds, prometheus.CounterValue, float64(e.stat.Nanos)/1e9, table, e.op)
		}
	}
}

// dbOpStatList flattens a db.OpStats into (op-name, counter) pairs for the op= label.
func dbOpStatList(o db.OpStats) []struct {
	op   string
	stat db.OpStat
} {
	return []struct {
		op   string
		stat db.OpStat
	}{
		{"shard_write_wait", o.ShardWriteWait},
		{"shard_write_hold", o.ShardWriteHold},
		{"segment_alloc", o.SegmentAlloc},
		{"sync", o.Sync},
		{"compact", o.Compact},
		{"retrain", o.Retrain},
		{"reindex", o.Reindex},
		{"snapshot_lock", o.SnapshotLock},
	}
}

// Handler returns an http.Handler that serves the collector's Prometheus metrics:
// the operational instruments (update/batch/backoff timings and counts, from
// store.MetricsRegistry), standard Go runtime and process (RSS, open FDs, ...)
// collectors, the per-ad-type storage gauges when st is non-nil, and -- when dbd is
// non-nil (the remote-database backend) -- the remote database's per-table storage and
// operational timing counters. st is nil for a backend with no local storage stats
// (the RPCBackend), which is exactly the one that supplies dbd. A private registry is
// combined with the store's operational registry via a Gatherers set, so this can be
// mounted anywhere without global-registry collisions.
func Handler(st store.Statser, dbd DBDiagnoser) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	if st != nil {
		reg.MustRegister(newStoreCollector(st))
	}
	if dbd != nil {
		reg.MustRegister(newDBCollector(dbd))
	}
	return promhttp.HandlerFor(prometheus.Gatherers{reg, store.MetricsRegistry}, promhttp.HandlerOpts{})
}
