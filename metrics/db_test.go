package metrics

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/collections"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

type fakeDiag struct{ m map[string]*dbrpc.Diagnostics }

func (f fakeDiag) DBDiagnostics(context.Context) (map[string]*dbrpc.Diagnostics, error) {
	return f.m, nil
}

// TestDBCollectorExposesRemoteOpStats: with a DBDiagnoser wired in, the collector's
// /metrics emits the remote database's per-table storage gauges and operational timing
// counters under condor_collector_db_.
func TestDBCollectorExposesRemoteOpStats(t *testing.T) {
	f := fakeDiag{m: map[string]*dbrpc.Diagnostics{
		"Startd": {
			Stats: db.Stats{Ads: 42, DeadBytes: 100, Segments: 3},
			OpStats: db.OpStats{
				OpStats:      collections.OpStats{ShardWriteHold: collections.OpStat{Count: 5, Nanos: 1_000_000_000}},
				SnapshotLock: collections.OpStat{Count: 1, Nanos: 2_000_000_000},
			},
		},
	}}

	rec := httptest.NewRecorder()
	Handler(nil, f).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	want := []string{
		`condor_collector_db_ads{table="Startd"} 42`,
		`condor_collector_db_dead_bytes{table="Startd"} 100`,
		`condor_collector_db_op_ops_total{op="shard_write_hold",table="Startd"} 5`,
		`condor_collector_db_op_seconds_total{op="shard_write_hold",table="Startd"} 1`,
		`condor_collector_db_op_ops_total{op="snapshot_lock",table="Startd"} 1`,
		`condor_collector_db_op_seconds_total{op="snapshot_lock",table="Startd"} 2`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("metrics output missing %q", w)
		}
	}
}
