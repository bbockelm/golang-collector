// Package metrics exposes the collector's storage footprint as Prometheus
// metrics, so an operator can size a pool (and watch its compressed memory
// footprint grow) without attaching a profiler. The gauges are computed live on
// each scrape from the store, so they never go stale.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/bbockelm/golang-collector/store"
)

const namespace = "condor_collector"

// storeCollector implements prometheus.Collector over a *store.Store, emitting
// per-ad-type storage gauges. Reading on Collect (rather than caching gauges)
// keeps the numbers exact and lock-scoped to the scrape.
type storeCollector struct {
	st *store.Store

	ads      *prometheus.Desc
	arena    *prometheus.Desc
	live     *prometheus.Desc
	dead     *prometheus.Desc
	segments *prometheus.Desc
}

func newStoreCollector(st *store.Store) *storeCollector {
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

// Handler returns an http.Handler that serves Prometheus metrics for the store:
// the per-ad-type storage gauges above, plus standard Go runtime and process
// (RSS, open FDs, ...) collectors. It uses a private registry so it can be
// mounted alongside any other metrics without global-registry collisions.
func Handler(st *store.Store) http.Handler {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		newStoreCollector(st),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
