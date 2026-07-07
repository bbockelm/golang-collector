package server

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// BenchmarkGrid sweeps a grid of workload x concurrency against both the Go and
// the C++ collector, over the same plaintext CEDAR transport and the same client,
// so their end-to-end ClassAd processing rates are directly comparable. Each op is
// its own connection (the real collector model), so run it with a bounded,
// explicit iteration count to keep the single-host ephemeral-port range from being
// the bottleneck at high concurrency, e.g.:
//
//	go test ./server/ -run=^$ -bench=BenchmarkGrid -benchtime=20000x -timeout=30m
//
// Scenarios exercise the interesting query profiles: a point lookup and a
// partially-selective predicate (both on indexed attributes in the Go collector,
// but scans in the C++ collector), and an un-indexed full scan. The cpp arm skips
// if condor_collector is unavailable.
func BenchmarkGrid(b *testing.B) {
	backends := []struct {
		name  string
		start func(testing.TB) (string, func())
	}{
		{"go", startPlaintextGoCollector},
		{"cpp", startCppCollector},
	}
	concurrencies := []int{1, 10, 100}
	scenarios := []struct {
		name     string
		writePct int
		query    string // "" for a write-only scenario
	}{
		{"write", 100, ""},
		{"read-point", 0, `Name == "slot5@host5"`},      // unique -> 1 match (indexed in Go)
		{"read-selective", 0, `State == "Claimed"`},     // ~1/4 match (indexed categorical in Go)
		{"read-scan", 0, `Activity == "Idle"`},          // all match, un-indexed -> full scan
		{"mixed", 50, `Name == "slot5@host5"`},          // 50/50 write + point read
	}
	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			addr, stop := be.start(b)
			defer stop()
			prepopulate(b, addr, plaintextSec())
			for _, conc := range concurrencies {
				for _, sc := range scenarios {
					b.Run(fmt.Sprintf("conc=%03d/%s", conc, sc.name), func(b *testing.B) {
						runScenario(b, addr, conc, sc.writePct, sc.query, plaintextSec(), comparePrepop)
					})
				}
			}
		})
	}
}

// runScenario drives b.N operations across conc goroutines, each op its own
// connection: an update with probability writePct%, otherwise a query with the
// given constraint. It reports ops/sec.
func runScenario(b *testing.B, addr string, conc, writePct int, query string, sec *security.SecurityConfig, keyspace int) {
	baseCtx := htcondor.WithSecurityConfig(context.Background(), sec)
	var claimed int64
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			col := htcondor.NewCollector(addr)
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			for {
				if atomic.AddInt64(&claimed, 1) > int64(b.N) {
					return
				}
				var err error
				if writePct > 0 && rng.Intn(100) < writePct {
					err = col.Advertise(baseCtx, benchAd(rng.Intn(keyspace)),
						&htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD})
				} else {
					_, err = col.QueryAds(baseCtx, "Machine", query)
				}
				if err != nil {
					b.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "ops/sec")
	}
}
