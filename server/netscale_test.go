package server

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	loadgenOnce sync.Once
	loadgenBin  string
	loadgenErr  error
)

func buildLoadgen(tb testing.TB) string {
	tb.Helper()
	loadgenOnce.Do(func() {
		out := filepath.Join(os.TempDir(), "loadgen-bench")
		cmd := exec.Command("go", "build", "-o", out, "./cmd/loadgen")
		cmd.Dir = ".."
		if b, err := cmd.CombinedOutput(); err != nil {
			loadgenErr = fmt.Errorf("build loadgen: %v\n%s", err, b)
			return
		}
		loadgenBin = out
	})
	if loadgenErr != nil {
		tb.Fatal(loadgenErr)
	}
	return loadgenBin
}

// TestNetworkedScaling measures how a collector's networked read throughput scales
// as client load is spread across separate processes. A single in-process client
// caps throughput by how fast one process can consume ad bytes; spreading the load
// across N loadgen processes (each with its own CPU/GC, OS-scheduled independently
// of the server) reveals the server's true networked ceiling. It reports aggregate
// ads/sec for the Go collector (as its own process) and the C++ collector.
//
//	go test ./server/ -run TestNetworkedScaling -v -timeout 20m
func TestNetworkedScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("networked scaling test is long; skipped under -short")
	}
	lg := buildLoadgen(t)
	ads := loadLargeMachineAds(t, 2000)
	const concPerProc = 4
	const dur = 3 * time.Second

	backends := []struct {
		name  string
		start func(testing.TB) (string, func())
	}{
		{"go-sub", func(tb testing.TB) (string, func()) {
			addr, stop := startSubprocessGoCollector(tb)
			prepopulateLarge(tb, addr, plaintextSec(), ads)
			return addr, stop
		}},
		{"cpp", func(tb testing.TB) (string, func()) {
			addr, stop := startCppCollector(tb)
			prepopulateLarge(tb, addr, plaintextSec(), ads)
			return addr, stop
		}},
	}

	for _, be := range backends {
		addr, stop := be.start(t)
		t.Logf("=== %s ===", be.name)
		var base float64
		for _, procs := range []int{1, 2, 4, 8} {
			totalAds, wall := runLoadgens(t, lg, addr, procs, concPerProc, dur)
			aps := float64(totalAds) / wall
			if procs == 1 {
				base = aps
			}
			t.Logf("%s procs=%d (conc/proc=%d, total conc=%d): %.0f ads/sec  (%.2fx)",
				be.name, procs, concPerProc, procs*concPerProc, aps, aps/base)
		}
		stop()
	}
}

// runLoadgens launches procs loadgen processes against addr, each with concPerProc
// connections for dur, waits for all, and returns the total ads drained and the
// wall-clock time of the batch.
func runLoadgens(t *testing.T, bin, addr string, procs, concPerProc int, dur time.Duration) (int64, float64) {
	t.Helper()
	var wg sync.WaitGroup
	var total int64
	var mu sync.Mutex
	start := time.Now()
	for p := 0; p < procs; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cmd := exec.Command(bin,
				"-addr", addr,
				"-conc", strconv.Itoa(concPerProc),
				"-dur", dur.String())
			out, err := cmd.Output()
			if err != nil {
				t.Errorf("loadgen: %v", err)
				return
			}
			n := parseResultAds(string(out))
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wg.Wait()
	return total, time.Since(start).Seconds()
}

// parseResultAds extracts the ads= count from loadgen's "RESULT ads=.. ops=.. secs=.." line.
func parseResultAds(out string) int64 {
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "RESULT ") {
			continue
		}
		for _, f := range strings.Fields(line) {
			if v, ok := strings.CutPrefix(f, "ads="); ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				return n
			}
		}
	}
	return 0
}
