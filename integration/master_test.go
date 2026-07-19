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
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestGoCollectorUnderCondorMaster runs golang-collector as the COLLECTOR daemon
// under condor_master, with a non-trivial security policy (authentication
// REQUIRED via FS -- so every C++ daemon must authenticate to the Go collector to
// advertise). It waits for the C++ startd to advertise, confirms every daemon type
// is locatable through the Go collector and that condor_status sees the pool, then
// drives a FULL NEGOTIATION CYCLE: submit a vanilla job and wait for it to run to
// completion. That last step is the real proof of correct private-ad handling --
// the negotiator queries the Go collector's QUERY_STARTD_PVT_ADS for each matched
// slot's claim capability and hands it to the schedd, which claims the startd and
// runs the job. No private ads, no match, no job.
func TestGoCollectorUnderCondorMaster(t *testing.T) {
	for _, tool := range []string{"condor_master", "condor_submit", "condor_wait"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH, skipping integration test", tool)
		}
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

# --- Make the single slot runnable and matchmaking prompt, so a submitted job ---
# --- negotiates and runs quickly. ---
START = TRUE
SUSPEND = FALSE
CONTINUE = TRUE
PREEMPT = FALSE
KILL = FALSE
RUNBENCHMARKS = FALSE
NEGOTIATOR_INTERVAL = 5
NEGOTIATOR_MIN_INTERVAL = 1
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

	// The Startd and Schedd must be locatable through the Go collector -- they are
	// the two daemons a job needs matched and run. (The Master and Negotiator
	// advertise on much slower intervals and aren't required for a job to run, so
	// they're checked best-effort below.)
	ctx := context.Background()
	col := htcondor.NewCollector(h.GetCollectorAddr())
	for _, dt := range []string{"Schedd", "Startd"} {
		addr := locateWithRetry(t, ctx, col, dt, 75*time.Second)
		if addr == "" {
			dumpLog(t, filepath.Join(logDir, "CollectorLog"))
			t.Fatalf("%s never became locatable via the Go collector", dt)
		}
		t.Logf("%s located via the Go collector at %s", dt, addr)
	}
	for _, dt := range []string{"Master", "Negotiator"} {
		if addr := locateWithRetry(t, ctx, col, dt, 10*time.Second); addr != "" {
			t.Logf("%s located via the Go collector at %s", dt, addr)
		} else {
			t.Logf("%s not yet advertised (slow update interval); continuing", dt)
		}
	}

	// condor_status (the C++ query tool) must also see the pool through the Go
	// collector -- proving query wire-compat with the real client.
	out := runCondor(t, h.GetConfigFile(), 30*time.Second, "condor_status", "-any")
	if out == "" {
		dumpLog(t, filepath.Join(logDir, "CollectorLog"))
		t.Fatal("condor_status -any returned no output")
	}
	t.Logf("condor_status -any via the Go collector:\n%s", out)

	// Full negotiation cycle. Submit a trivial vanilla job and wait for it to run
	// to completion. Force a real file transfer (transfer_executable) so a slot is
	// actually claimed and a starter runs. We transfer a small shell script that
	// execs /bin/sleep rather than /bin/sleep itself: on macOS the OS refuses to
	// exec a *copied* system Mach-O binary (SIP/code-signing) and the starter
	// stalls, but a transferred shell script runs fine and execs system sleep.
	jobScript := filepath.Join(tmp, "job.sh")
	if err := os.WriteFile(jobScript, []byte("#!/bin/sh\nexec /bin/sleep \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(tmp, "job.log")
	submitFile := filepath.Join(tmp, "job.sub")
	sub := fmt.Sprintf(`universe = vanilla
executable = %s
arguments = 1
log = %s
output = %s
error = %s
should_transfer_files = YES
transfer_executable = true
when_to_transfer_output = ON_EXIT
queue 1
`, jobScript, logFile, filepath.Join(tmp, "job.out"), filepath.Join(tmp, "job.err"))
	if err := os.WriteFile(submitFile, []byte(sub), 0o600); err != nil {
		t.Fatal(err)
	}

	runCondor(t, h.GetConfigFile(), 60*time.Second, "condor_submit", submitFile)
	// condor_wait blocks until the job logs a terminate event (or times out).
	runCondor(t, h.GetConfigFile(), 180*time.Second, "condor_wait", "-wait", "150", logFile)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("reading job log: %v", err)
	}
	if !strings.Contains(string(data), "Job terminated") {
		for _, name := range []string{"CollectorLog", "NegotiatorLog", "MatchLog", "SchedLog", "StartLog", "ShadowLog"} {
			dumpLog(t, filepath.Join(logDir, name))
		}
		t.Fatalf("job did not terminate normally; job log:\n%s", data)
	}
	t.Log("job completed: full negotiation cycle (incl. private-ad claim capabilities) succeeded through the Go collector")
}

// locateWithRetry polls the collector for a daemon of the given type until one
// with a non-empty address appears or the timeout elapses, returning its address
// ("" on timeout). Different daemons advertise on different intervals, so a
// single query can race a daemon that has not yet sent its first update.
func locateWithRetry(t *testing.T, ctx context.Context, col *htcondor.Collector, daemonType string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// v0.8.0 LocateDaemon takes a typed htcondor.DaemonType; callers pass the daemon
		// type name ("Schedd", "Master", ...), which matches the DaemonType constants.
		if loc, err := col.LocateDaemon(ctx, htcondor.DaemonType(daemonType), ""); err == nil && loc != nil && loc.Address != "" {
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
