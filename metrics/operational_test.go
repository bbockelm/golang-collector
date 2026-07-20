package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-collector/store"
)

// TestOperationalMetricsServedWithoutStatser is the guard for the endpoint fix:
// the operational metrics (update/batch/backoff timings + counts) must be served
// even when the backend has no store.Statser -- i.e. the remote-database
// (RPCBackend) collector, which previously had no /metrics endpoint at all.
func TestOperationalMetricsServedWithoutStatser(t *testing.T) {
	// Record one of each so the families are populated.
	store.ObserveUpdate("private", 5*time.Millisecond)
	store.ObserveUpdate("public", 1*time.Millisecond)

	h := Handler(nil, nil) // nil Statser == the RPCBackend backend
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()

	for _, want := range []string{
		"condor_collector_update_seconds",
		"condor_collector_updates_total",
		"condor_collector_batch_flush_seconds",
		"condor_collector_backoff_seconds",
		"condor_collector_batches_total",
		"condor_collector_retries_total",
		"condor_collector_ads_per_batch",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics output missing operational metric %q", want)
		}
	}
	if !strings.Contains(body, `op="private"`) || !strings.Contains(body, `op="public"`) {
		t.Errorf("update op labels missing from /metrics output")
	}
	// Go/process collectors should still be present.
	if !strings.Contains(body, "go_goroutines") {
		t.Errorf("runtime collectors missing")
	}
}
