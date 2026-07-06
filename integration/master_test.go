// Package integration holds end-to-end tests that run golang-collector as the
// pool's condor_collector under a real condor_master, proving it is a drop-in
// replacement: real C++ daemons (master, schedd, negotiator, startd) advertise
// to it and are queryable through it. These tests skip unless the HTCondor
// binaries are on PATH (set PATH to the build's sbin+bin to run them).
package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestGoCollectorUnderCondorMaster runs golang-collector as the COLLECTOR daemon
// under condor_master, with a non-trivial security policy (authentication
// REQUIRED via FS -- so every C++ daemon must authenticate to the Go collector
// to advertise). It then waits for the C++ startd to advertise and confirms every
// daemon type is locatable through the Go collector, and that condor_status (the
// C++ query tool) sees the pool. Compatibility end to end.
func TestGoCollectorUnderCondorMaster(t *testing.T) {
	if _, err := exec.LookPath("condor_master"); err != nil {
		t.Skip("condor_master not found in PATH, skipping integration test")
	}

	tmp := t.TempDir()

	// Build the collector binary the master will launch as the COLLECTOR daemon.
	collBin := filepath.Join(tmp, "golang-collector")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", collBin, "../cmd/golang-collector")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building golang-collector: %v\n%s", err, out)
	}

	extra := fmt.Sprintf(`
# --- Run golang-collector as the pool's COLLECTOR under shared_port ---
# The collector is a DaemonCore daemon in DC_DAEMON_LIST, so condor_master
# pre-creates its command socket. Under USE_SHARED_PORT (the harness default) we
# inherit the shared-port endpoint (sock=collector) rather than trying to re-bind
# a fixed port, which the master already holds.
COLLECTOR = %s
COLLECTOR_LOG = $(LOG)/CollectorLog
COLLECTOR_ADDRESS_FILE = $(LOG)/.collector_address
COLLECTOR_DEBUG = D_FULLDEBUG
# We do not implement CONDOR_VIEW_HOST forwarding yet; the harness points it at
# the collector itself, so disable it to avoid meaningless forward attempts.
CONDOR_VIEW_HOST =

# --- Authentication REQUIRED: every C++ daemon must authenticate (FS) to the ---
# --- Go collector to advertise; reads too. This is the real compatibility proof. ---
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_CLIENT_AUTHENTICATION_METHODS = FS
# cedar (and thus the Go collector) implements only AES-GCM; force the family /
# non-negotiated master<->child session to AES-GCM rather than a legacy cipher.
SEC_DEFAULT_CRYPTO_METHODS = AES
`, collBin)

	h := htcondor.SetupCondorHarnessWithConfig(t, extra)
	defer h.Shutdown()
	logDir := h.GetLogDir()
	defer saveLogs(t, logDir)

	// The C++ startd must authenticate to and advertise to the Go collector.
	if err := h.WaitForStartd(90 * time.Second); err != nil {
		dumpLog(t, filepath.Join(logDir, "CollectorLog"))
		dumpLog(t, filepath.Join(logDir, "StartLog"))
		t.Fatalf("startd did not become visible via the Go collector: %v", err)
	}

	// Every daemon type the harness runs should be locatable through the Go
	// collector (proves updates of each ad type landed and queries return them).
	// Each daemon advertises on its own schedule (the master and negotiator less
	// often than the startd), so poll with a deadline rather than single-shot.
	ctx := context.Background()
	col := htcondor.NewCollector(h.GetCollectorAddr())
	for _, dt := range []string{"Master", "Schedd", "Negotiator", "Startd"} {
		addr := locateWithRetry(t, ctx, col, dt, 75*time.Second)
		if addr == "" {
			dumpLog(t, filepath.Join(logDir, "CollectorLog"))
			t.Fatalf("%s never became locatable via the Go collector", dt)
		}
		t.Logf("%s located via the Go collector at %s", dt, addr)
	}

	// condor_status (the C++ query tool) must also see the pool through the Go
	// collector -- proving query wire-compat with the real client.
	out := runCondor(t, h.GetConfigFile(), 30*time.Second, "condor_status", "-any")
	if out == "" {
		dumpLog(t, filepath.Join(logDir, "CollectorLog"))
		t.Fatal("condor_status -any returned no output")
	}
	t.Logf("condor_status -any via the Go collector:\n%s", out)
}

// locateWithRetry polls the collector for a daemon of the given type until one
// with a non-empty address appears or the timeout elapses, returning its address
// ("" on timeout). Different daemons advertise on different intervals, so a
// single query can race a daemon that has not yet sent its first update.
func locateWithRetry(t *testing.T, ctx context.Context, col *htcondor.Collector, adType string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if loc, err := col.LocateDaemon(ctx, adType, ""); err == nil && loc != nil && loc.Address != "" {
			return loc.Address
		}
		time.Sleep(500 * time.Millisecond)
	}
	return ""
}

// runCondor runs an HTCondor tool against the harness config and returns its
// combined output, failing the test on error.
func runCondor(t *testing.T, configFile string, timeout time.Duration, name string, args ...string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s not found: %v", name, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+configFile)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
	return string(out)
}

// saveLogs copies the harness log directory somewhere stable (t.TempDir is
// deleted on exit) and prints the path, so daemon logs can be inspected after.
func saveLogs(t *testing.T, logDir string) {
	dest, err := os.MkdirTemp("", "collector-itest-logs-")
	if err != nil {
		t.Logf("could not preserve logs: %v", err)
		return
	}
	if out, err := exec.Command("cp", "-a", logDir+"/.", dest).CombinedOutput(); err != nil {
		t.Logf("could not copy logs to %s: %v\n%s", dest, err, out)
		return
	}
	t.Logf("preserved HTCondor logs at: %s", dest)
}

func dumpLog(t *testing.T, path string) {
	t.Helper()
	if data, err := os.ReadFile(path); err == nil {
		t.Logf("=== %s ===\n%s", filepath.Base(path), data)
	} else {
		t.Logf("(could not read %s: %v)", path, err)
	}
}
