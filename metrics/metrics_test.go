package metrics

import (
	"context"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/store"
)

// TestMetricsHandler advertises ads into a store and scrapes /metrics, checking
// the per-ad-type storage gauges are present and reflect the live state.
func TestMetricsHandler(t *testing.T) {
	st := store.New()
	const n = 100
	for i := 0; i < n; i++ {
		ad, err := classad.Parse(`[MyType="Machine"; Name="slot` + strconv.Itoa(i) +
			`@h"; MyAddress="<1.2.3.4:` + strconv.Itoa(i) + `>"; Cpus=8; Memory=2048]`)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Update(context.Background(), store.StartdAd, ad); err != nil {
			t.Fatal(err)
		}
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	Handler(st, nil).ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("scrape status = %d", w.Code)
	}
	body := w.Body.String()

	// The Startd table's live count is exact; the byte/segment gauges must be
	// present (values depend on compression, so just assert they were emitted).
	for _, want := range []string{
		`condor_collector_ads{ad_type="Startd"} 100`,
		`condor_collector_arena_bytes{ad_type="Startd"}`,
		`condor_collector_live_bytes{ad_type="Startd"}`,
		`condor_collector_segments{ad_type="Startd"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
	// Arena bytes for the populated table must be non-zero.
	if !storageNonZero(body, "condor_collector_arena_bytes", "Startd") {
		t.Errorf("condor_collector_arena_bytes{ad_type=\"Startd\"} was zero; want > 0\n%s", body)
	}
}

// storageNonZero reports whether the gauge line for (metric, adType) has a value > 0.
func storageNonZero(body, metric, adType string) bool {
	prefix := metric + `{ad_type="` + adType + `"} `
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, prefix) {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
			return err == nil && v > 0
		}
	}
	return false
}
