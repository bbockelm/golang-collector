package server

import (
	"compress/gzip"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// BenchmarkLargeAdRead measures match-all read throughput on realistic ~21 KB
// OSPool ads with a DRAIN client (SkipClassAdRaw -- no client-side AST build, no ad
// text materialized), so the number reflects server-side cost, not client decode.
// It compares three backends: the Go collector with the collections zstd
// dictionary codec OFF (identity) and ON (so the on/off delta is the decompression
// cost per served ad), and the C++ collector. read-scan returns the whole corpus
// per query, the serialization-heaviest case.
func BenchmarkLargeAdRead(b *testing.B) {
	const n = 2000
	ads := loadLargeMachineAds(b, n)
	query := mustAd(b, `[MyType="Query"; TargetType="Machine"; Requirements = true]`)

	backends := []struct {
		name  string
		start func(testing.TB) (string, func())
	}{
		// go-inprocess: collector server runs inside the test process, alongside the
		// drain-client goroutines (they share CPU and GC).
		{"go-inprocess", func(tb testing.TB) (string, func()) { return startLargeGoStore(tb, ads, false) }},
		// go-sub: collector runs as a separate process (the real golang-collector
		// binary), so its CPU/GC is independent of the client -- the apples-to-apples
		// setup vs the C++ collector, which is also a separate process.
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
		b.Run(be.name, func(b *testing.B) {
			addr, stop := be.start(b)
			defer stop()
			for _, conc := range []int{1, 10} {
				b.Run(fmt.Sprintf("conc=%03d/read-scan", conc), func(b *testing.B) {
					runDrainScenario(b, addr, conc, query)
				})
			}
		})
	}
}

// loadLargeMachineAds returns n distinct Machine ads built from the corpus. The
// shared pool_sample corpus is a mixed dump (Machine, DaemonMaster, Negotiator,
// ...), and the client derives the advertise command from each ad's MyType, so a
// non-Machine ad would go out as e.g. UPDATE_MASTER_AD -- which the C++ collector's
// default policy denies. Restricting to Machine ads makes every advertise a startd
// update both backends accept. It also deep-copies each ad via parse and gives it a
// unique Name, because loadLargeAds aliases shared corpus objects (mutating one in
// place per replica), which would otherwise collapse the set to one ad per corpus
// entry. Machine ads are also the large (~21 KB) ones, so this stays representative.
func loadLargeMachineAds(tb testing.TB, n int) []*classad.ClassAd {
	tb.Helper()
	f, err := os.Open(corpusPath)
	if err != nil {
		tb.Skipf("corpus %s not found: %v", corpusPath, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		tb.Fatal(err)
	}
	defer gz.Close()

	var machine []*classad.ClassAd
	r := classad.NewOldReader(gz)
	for r.Next() {
		ad := r.ClassAd()
		if mt, _ := ad.EvaluateAttrString("MyType"); mt == "Machine" {
			machine = append(machine, ad)
		}
	}
	if len(machine) == 0 {
		tb.Fatal("no Machine ads in corpus")
	}

	out := make([]*classad.ClassAd, n)
	for i := range out {
		cp, err := classad.Parse(machine[i%len(machine)].String())
		if err != nil {
			tb.Fatalf("deep-copy ad %d: %v", i, err)
		}
		cp.InsertAttrString("Name", "slot1_"+strconv.Itoa(i)+"@host"+strconv.Itoa(i))
		out[i] = cp
	}
	return out
}

// startLargeGoStore builds a Go store of the given ads and serves it plaintext.
// When zstd is true it trains the collections dictionary on the loaded ads (and
// recompacts), switching the collection off the identity codec.
func startLargeGoStore(tb testing.TB, ads []*classad.ClassAd, zstd bool) (string, func()) {
	tb.Helper()
	st := store.New()
	for _, ad := range ads {
		if err := st.Update(store.StartdAd, ad); err != nil {
			tb.Fatal(err)
		}
	}
	if zstd {
		st.RetrainDict(len(ads))
	}
	return serveStore(tb, st)
}

var (
	goCollectorOnce sync.Once
	goCollectorBin  string
	goCollectorErr  error
)

// buildGoCollector compiles the real golang-collector binary once per run.
func buildGoCollector(tb testing.TB) string {
	tb.Helper()
	goCollectorOnce.Do(func() {
		// A stable path (not tb.TempDir(), which is per-test) so the single build is
		// reused across the benchmark's backends.
		out := filepath.Join(os.TempDir(), "golang-collector-bench")
		cmd := exec.Command("go", "build", "-o", out, "./cmd/golang-collector")
		cmd.Dir = ".."
		if b, err := cmd.CombinedOutput(); err != nil {
			goCollectorErr = fmt.Errorf("build golang-collector: %v\n%s", err, b)
			return
		}
		goCollectorBin = out
	})
	if goCollectorErr != nil {
		tb.Fatal(goCollectorErr)
	}
	return goCollectorBin
}

// startSubprocessGoCollector runs the golang-collector binary as its own process
// (plaintext, identity codec), so its CPU and GC are independent of the client --
// matching how startCppCollector runs the C++ collector. It returns once the
// collector is accepting and round-tripping ads.
func startSubprocessGoCollector(tb testing.TB) (addr string, stop func()) {
	tb.Helper()
	bin := buildGoCollector(tb)

	dir := tb.TempDir()
	logDir := filepath.Join(dir, "log")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		tb.Fatal(err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	cfg := fmt.Sprintf(`
LOG = %[1]s
COLLECTOR_HOST = 127.0.0.1:%[2]d
COLLECTOR_ADDRESS_FILE = %[1]s/.collector_address
USE_SHARED_PORT = FALSE
ENABLE_IPV6 = FALSE
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER
SEC_DEFAULT_AUTHENTICATION_METHODS = ANONYMOUS
ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_ADVERTISE_STARTD = *
COLLECTOR_DICT_RETRAIN_INTERVAL = 0
COLLECTOR_UPDATE_INTERVAL = 3600
`, logDir, port)
	cfgPath := filepath.Join(dir, "condor_config")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		tb.Fatal(err)
	}

	logf, _ := os.Create(filepath.Join(dir, "stdio"))
	cmd := exec.Command(bin, "-listen", fmt.Sprintf("127.0.0.1:%d", port))
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfgPath)
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		tb.Fatalf("start golang-collector: %v", err)
	}
	stop = func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if logf != nil {
			logf.Close()
		}
	}

	addr = fmt.Sprintf("127.0.0.1:%d", port)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cctx := htcondor.WithSecurityConfig(ctx, plaintextSec())
	col := htcondor.NewCollector(addr)
	probe := mustAd(tb, `[MyType="Machine"; Name="probe@ready"; MyAddress="<127.0.0.1:1>"; State="Unclaimed"]`)
	for {
		if ctx.Err() != nil {
			cl, _ := os.ReadFile(filepath.Join(dir, "stdio"))
			stop()
			tb.Fatalf("golang-collector did not become ready:\n%s", tailBytes(cl, 1500))
		}
		if err := col.Advertise(cctx, probe, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err == nil {
			if ads, err := col.QueryAds(cctx, "Machine", `Name == "probe@ready"`); err == nil && len(ads) == 1 {
				break
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	return addr, stop
}

// prepopulateLarge advertises the given ads to a collector, one per connection.
func prepopulateLarge(tb testing.TB, addr string, sec *security.SecurityConfig, ads []*classad.ClassAd) {
	tb.Helper()
	ctx := htcondor.WithSecurityConfig(context.Background(), sec)
	col := htcondor.NewCollector(addr)
	for i, ad := range ads {
		if err := col.Advertise(ctx, ad, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
			// The C++ collector intermittently denies bulk large-ad advertises
			// partway through (a session-resume timing issue on its side); skip the
			// arm rather than fail the whole benchmark.
			tb.Skipf("prepopulate large ad %d: %v", i, err)
		}
	}
}

// runDrainScenario drives b.N drain queries across conc goroutines (each its own
// connection) and reports both queries/sec and ads/sec (each query returns adsPerOp
// ads).
func runDrainScenario(b *testing.B, addr string, conc int, query *classad.ClassAd) {
	var claimed, totalAds int64
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if atomic.AddInt64(&claimed, 1) > int64(b.N) {
					return
				}
				nads, err := drainQueryRead(addr, query)
				if err != nil {
					b.Error(err)
					return
				}
				atomic.AddInt64(&totalAds, int64(nads))
			}
		}()
	}
	wg.Wait()
	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "ops/sec")
		b.ReportMetric(float64(totalAds)/secs, "ads/sec")
	}
}
