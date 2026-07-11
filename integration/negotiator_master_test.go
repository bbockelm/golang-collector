package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestGoNegotiatorUnderCondorMaster runs golang-negotiator as the pool's
// NEGOTIATOR daemon under a real condor_master, with the stock C++ collector,
// schedd, and startd, and a non-trivial security policy (authentication
// REQUIRED via FS). It submits a vanilla job and waits for it to complete —
// the full proof: the Go negotiator queried the C++ collector (public machine
// + submitter ads AND the NEGOTIATOR-authorized startd-private claim ids), ran
// the pie spin, opened a NEGOTIATE session to the C++ schedd, and delivered a
// match whose claim id let the schedd claim the C++ startd and run the job.
//
// Skips (like the other integration tests) unless the HTCondor binaries are on
// PATH — point PATH at ~/projects/htcondor-build/release_dir/{sbin,bin} to run.
func TestGoNegotiatorUnderCondorMaster(t *testing.T) {
	for _, tool := range []string{"condor_master", "condor_submit", "condor_wait"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH, skipping integration test", tool)
		}
	}

	tmp := t.TempDir()

	// Build the Go negotiator binary the master will launch as the NEGOTIATOR.
	negBin := filepath.Join(tmp, "golang-negotiator")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", negBin, "../cmd/golang-negotiator")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building golang-negotiator: %v\n%s", err, out)
	}

	extra := fmt.Sprintf(`
# --- Run golang-negotiator as the pool's NEGOTIATOR under shared_port ---
# NEGOTIATOR is a DaemonCore daemon in DC_DAEMON_LIST, so condor_master
# pre-creates its shared-port command endpoint (sock=negotiator), which the Go
# daemon inherits via CONDOR_INHERIT — the same launch path the Go collector
# uses in TestGoCollectorUnderCondorMaster.
NEGOTIATOR = %s
NEGOTIATOR_LOG = $(LOG)/NegotiatorLog
NEGOTIATOR_ADDRESS_FILE = $(LOG)/.negotiator_address
NEGOTIATOR_DEBUG = D_FULLDEBUG

# Negotiate promptly so the submitted job matches within the test budget.
NEGOTIATOR_INTERVAL = 5
NEGOTIATOR_MIN_INTERVAL = 1
NEGOTIATOR_CYCLE_DELAY = 1
NEGOTIATOR_UPDATE_INTERVAL = 5

# --- Authentication REQUIRED (FS): the Go negotiator must authenticate to the
# --- C++ collector for its queries/updates (incl. the NEGOTIATOR-authorized
# --- startd-private claim-id query) and to the C++ schedd for NEGOTIATE.
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_CLIENT_AUTHENTICATION_METHODS = FS
# cedar implements only AES-GCM; keep every session on it.
SEC_DEFAULT_CRYPTO_METHODS = AES

# --- A runnable slot and prompt matchmaking ---
START = TRUE
SUSPEND = FALSE
CONTINUE = TRUE
PREEMPT = FALSE
KILL = FALSE
RUNBENCHMARKS = FALSE
`, negBin)

	h := htcondor.SetupCondorHarnessWithConfig(t, extra)
	defer h.Shutdown()
	logDir := h.GetLogDir()
	defer saveLogs(t, logDir)

	// The C++ startd must be up and advertised before a match can happen.
	if err := h.WaitForStartd(90 * time.Second); err != nil {
		dumpLog(t, filepath.Join(logDir, "NegotiatorLog"))
		dumpLog(t, filepath.Join(logDir, "StartLog"))
		t.Fatalf("startd did not become available: %v", err)
	}

	// The Go negotiator must have started and begun cycling (its log is the
	// cheapest liveness probe before spending the submit budget).
	waitForLog(t, filepath.Join(logDir, "NegotiatorLog"), "golang-negotiator starting", 60*time.Second)

	// Submit a trivial vanilla job and wait for it to complete. Force a real
	// file transfer so a slot is actually claimed and a starter runs; a shell
	// script (not a copied system binary) keeps macOS SIP happy.
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
	runCondor(t, h.GetConfigFile(), 240*time.Second, "condor_wait", "-wait", "200", logFile)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("reading job log: %v", err)
	}
	if !strings.Contains(string(data), "Job terminated") {
		for _, name := range []string{"NegotiatorLog", "CollectorLog", "SchedLog", "StartLog", "ShadowLog", "MasterLog"} {
			dumpLog(t, filepath.Join(logDir, name))
		}
		t.Fatalf("job did not terminate normally; job log:\n%s", data)
	}
	t.Log("job completed: the Go negotiator matched a C++ schedd's job to a C++ startd end-to-end")

	// Bonus wire-compat probe (best-effort): the real condor_userprio tool
	// locates the negotiator through the collector's NegotiatorAd and speaks
	// GET_PRIORITY at it. Failures are logged, not fatal — daemon location
	// through shared-port sinfuls is timing-sensitive and not this test's
	// gate (the job completing is).
	if path, err := exec.LookPath("condor_userprio"); err == nil {
		cmd := exec.Command(path, "-all")
		cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+h.GetConfigFile())
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("condor_userprio -all failed (non-fatal): %v\n%s", err, out)
		} else {
			t.Logf("condor_userprio -all via the Go negotiator:\n%s", out)
		}
	}
}

// waitForLog polls path until it contains needle or the timeout elapses.
func waitForLog(t *testing.T, path, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil && strings.Contains(string(data), needle) {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	dumpLog(t, path)
	t.Fatalf("%s never contained %q", path, needle)
}
