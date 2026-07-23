package server

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// startPlaintextGoCollector stands up the Go collector serving plaintext CEDAR
// (empty table), so it can be compared head-to-head with the C++ collector using
// the same client, workload and (plaintext) security.
func startPlaintextGoCollector(tb testing.TB) (addr string, stop func()) {
	tb.Helper()
	srv := New(store.New(), plaintextSec(), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = srv.ServeConn(ctx, conn) }()
		}
	}()
	return ln.Addr().String(), func() { cancel(); ln.Close() }
}

// comparePrepop is the table size for the Go-vs-C++ comparison. It is smaller
// than benchNumAds because prepopulation uses one ad per connection (the C++
// collector rejects multi-ad connections), so a huge table would be slow to load
// and churn ephemeral ports.
const comparePrepop = 2000

// prepopulate loads a collector to comparePrepop startd ads, one ad per
// connection (single Advertise) -- the protocol both collectors accept.
func prepopulate(tb testing.TB, addr string, sec *security.SecurityConfig) {
	tb.Helper()
	ctx := htcondor.WithSecurityConfig(context.Background(), sec)
	col := htcondor.NewCollector(addr)
	for i := 0; i < comparePrepop; i++ {
		if err := col.Advertise(ctx, benchAd(i),
			&htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
			tb.Fatalf("prepopulate ad %d: %v", i, err)
		}
	}
}

// BenchmarkGoVsCppCollector runs the identical workload (bulk updates + queries,
// over plaintext CEDAR, from the same client) against the Go collector and the
// C++ condor_collector, so their throughput is directly comparable. The C++ arm
// skips if condor_collector is not available.
func BenchmarkGoVsCppCollector(b *testing.B) {
	backends := []struct {
		name  string
		start func(testing.TB) (string, func())
	}{
		{"go", startPlaintextGoCollector},
		{"cpp", startCppCollector},
	}
	concurrencies := []int{1, 8}
	mixes := []struct {
		name     string
		queryPct int
	}{
		{"update-only", 0},
		{"query-50pct", 50},
	}
	for _, be := range backends {
		b.Run(be.name, func(b *testing.B) {
			addr, stop := be.start(b)
			defer stop()
			prepopulate(b, addr, plaintextSec())
			for _, conc := range concurrencies {
				for _, mix := range mixes {
					b.Run(fmt.Sprintf("conc=%d/%s", conc, mix.name), func(b *testing.B) {
						runMixSingle(b, addr, conc, mix.queryPct, plaintextSec(), comparePrepop)
					})
				}
			}
		})
	}
}

// cppCollectorBinary locates the C++ condor_collector: $CONDOR_COLLECTOR, then
// the local build, then $PATH. Returns "" if unavailable (benchmarks skip).
func cppCollectorBinary() string {
	if p := os.Getenv("CONDOR_COLLECTOR"); p != "" {
		return p
	}
	def := "/Users/bbockelm/projects/htcondor/build/release_dir/sbin/condor_collector"
	if _, err := os.Stat(def); err == nil {
		return def
	}
	if p, err := exec.LookPath("condor_collector"); err == nil {
		return p
	}
	return ""
}

// startCppCollector spawns a standalone C++ condor_collector on a free port with
// a minimal plaintext, unauthenticated config -- matching a plaintext Go client
// so the two are directly comparable. It returns the collector's address once it
// is accepting and processing ads.
func startCppCollector(tb testing.TB) (addr string, stop func()) {
	tb.Helper()
	bin := cppCollectorBinary()
	if bin == "" {
		tb.Skip("condor_collector not found (set CONDOR_COLLECTOR to compare)")
	}

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
SPOOL = %[2]s
LOCK = %[2]s
RUN = %[2]s
EXECUTE = %[2]s
COLLECTOR_LOG = $(LOG)/CollectorLog
COLLECTOR_HOST = 127.0.0.1:%[3]d
COLLECTOR_ADDRESS_FILE = $(LOG)/.collector_address
USE_SHARED_PORT = FALSE
ENABLE_IPV6 = FALSE
NETWORK_INTERFACE = 127.0.0.1
DAEMON_LIST = COLLECTOR
# No UDP command socket: sandboxed benchmark environments may deny the UDP bind
# (it is not exercised by these TCP-only query benchmarks anyway).
WANT_UDP_COMMAND_SOCKET = FALSE
# Plaintext + unauthenticated, to match the Go client's plaintext handshake.
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_DEFAULT_ENCRYPTION = NEVER
SEC_DEFAULT_INTEGRITY = NEVER
SEC_DEFAULT_AUTHENTICATION_METHODS = ANONYMOUS
ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_ADVERTISE_STARTD = *
`, logDir, dir, port)
	cfgPath := filepath.Join(dir, "condor_config")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		tb.Fatal(err)
	}

	logf, _ := os.Create(filepath.Join(dir, "stdio"))
	cmd := exec.Command(bin, "-f", "-p", fmt.Sprint(port))
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfgPath)
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		tb.Fatalf("start condor_collector: %v", err)
	}
	stop = func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if logf != nil {
			logf.Close()
		}
	}

	addr = fmt.Sprintf("127.0.0.1:%d", port)
	// Readiness: advertise one ad and query it back until it round-trips.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cctx := htcondor.WithSecurityConfig(ctx, plaintextSec())
	col := htcondor.NewCollector(addr)
	probe := mustAd(tb, `[MyType="Machine"; Name="probe@ready"; MyAddress="<127.0.0.1:1>"; State="Unclaimed"]`)
	for {
		if ctx.Err() != nil {
			cl, _ := os.ReadFile(filepath.Join(logDir, "CollectorLog"))
			stop()
			tb.Fatalf("condor_collector did not become ready:\n%s", tailBytes(cl, 1500))
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

func tailBytes(b []byte, n int) string {
	if len(b) > n {
		b = b[len(b)-n:]
	}
	return string(b)
}

// TestCppCollectorInterop verifies the Go client can advertise to and query a
// real C++ condor_collector -- the prerequisite for the comparison benchmark.
func TestCppCollectorInterop(t *testing.T) {
	addr, stop := startCppCollector(t)
	defer stop()

	ctx := htcondor.WithSecurityConfig(context.Background(), plaintextSec())
	col := htcondor.NewCollector(addr)

	// Advertise several distinct ads via AdvertiseMultiple. This is the
	// regression case: the old ad-frame protocol delivered only the first ad to
	// the C++ collector, so every ad must now come back.
	ads := []*classad.ClassAd{
		mustAd(t, `[MyType="Machine"; Name="multi@a"; MyAddress="<127.0.0.1:2>"; Cpus=8]`),
		mustAd(t, `[MyType="Machine"; Name="multi@b"; MyAddress="<127.0.0.1:3>"; Cpus=4]`),
		mustAd(t, `[MyType="Machine"; Name="multi@c"; MyAddress="<127.0.0.1:4>"; Cpus=16]`),
	}
	for _, e := range col.AdvertiseMultiple(ctx, ads, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}) {
		if e != nil {
			t.Fatalf("advertise: %v", e)
		}
	}

	got, err := col.QueryAds(ctx, "Machine", `regexp("^multi@", Name)`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 3 {
		names := make([]string, len(got))
		for i, a := range got {
			names[i], _ = a.EvaluateAttrString("Name")
		}
		t.Fatalf("AdvertiseMultiple delivered %d/3 ads to the C++ collector: %v", len(got), names)
	}
}
