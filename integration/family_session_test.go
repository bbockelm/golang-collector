package integration

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestFamilySessionAfterUDPCommand runs golang-collector under condor_master
// in a pool where the C++ schedd and negotiator have their own UDP command
// ports, under HTCondor's default crypto methods (AES,BLOWFISH,3DES). A
// submit makes the schedd send RESCHEDULE to the negotiator over UDP, on the
// master's family session; UDP has no AES, so the negotiator receives it with
// the session's BLOWFISH key and from then on prefers BLOWFISH on that
// session for its TCP commands too, including its queries of the Go
// collector. The job completing proves those queries succeed: the Go
// collector refuses the BLOWFISH resume (SID_NOT_FOUND) and the negotiator
// negotiates a new AES session, where a collector that decrypts with the AES
// key regardless fails every query with a payload.
func TestFamilySessionAfterUDPCommand(t *testing.T) {
	for _, tool := range []string{"condor_master", "condor_submit", "condor_wait"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH, skipping integration test", tool)
		}
	}

	tmp := t.TempDir()
	collBin := filepath.Join(tmp, "golang-collector")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", collBin, "../cmd/golang-collector")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building golang-collector: %v\n%s", err, out)
	}
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	extra := fmt.Sprintf(`
COLLECTOR = %s
COLLECTOR_LOG = $(LOG)/CollectorLog
COLLECTOR_ADDRESS_FILE = $(LOG)/.collector_address
COLLECTOR_DEBUG = D_FULLDEBUG
CONDOR_VIEW_HOST =

# The schedd and negotiator bind their own TCP and UDP command ports, so the
# schedd sends RESCHEDULE over UDP, and both use the family session with
# every local daemon (with shared port, only with peers on their own port).
# The Go collector stays behind shared port.
SCHEDD.USE_SHARED_PORT = False
NEGOTIATOR.USE_SHARED_PORT = False

SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_ENCRYPTION = REQUIRED
SEC_DEFAULT_INTEGRITY = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_CLIENT_AUTHENTICATION_METHODS = FS
# HTCondor's default: the family session gets a key per method, and a UDP
# command uses BLOWFISH.
SEC_DEFAULT_CRYPTO_METHODS = AES,BLOWFISH,3DES

START = TRUE
SUSPEND = FALSE
CONTINUE = TRUE
PREEMPT = FALSE
KILL = FALSE
RUNBENCHMARKS = FALSE
NEGOTIATOR_DEBUG = D_FULLDEBUG D_SECURITY

ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_DAEMON = %[2]s@* condor@* condor_pool@*
ALLOW_NEGOTIATOR = %[2]s@* condor@* condor_pool@*
ALLOW_ADMINISTRATOR = %[2]s@* condor@* condor_pool@*
`, collBin, me.Username)

	h := htcondor.SetupCondorHarnessWithConfig(t, extra)
	defer h.Shutdown()
	logDir := h.GetLogDir()
	defer saveLogs(t, logDir)
	dumpAll := func() {
		for _, name := range []string{"CollectorLog", "NegotiatorLog", "SchedLog", "StartLog", "ShadowLog"} {
			dumpLog(t, filepath.Join(logDir, name))
		}
	}

	if err := h.WaitForStartd(90 * time.Second); err != nil {
		dumpAll()
		t.Fatalf("startd did not become visible via the Go collector: %v", err)
	}

	jobScript := filepath.Join(tmp, "job.sh")
	if err := os.WriteFile(jobScript, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(tmp, "job.log")
	submitFile := filepath.Join(tmp, "job.sub")
	sub := fmt.Sprintf(`universe = vanilla
executable = %s
log = %s
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
queue 1
`, jobScript, logFile)
	if err := os.WriteFile(submitFile, []byte(sub), 0o600); err != nil {
		t.Fatal(err)
	}
	runCondor(t, h.GetConfigFile(), 60*time.Second, "condor_submit", submitFile)
	// condor_wait exits nonzero on timeout; read the log rather than fail in
	// runCondor, so the daemon logs are dumped.
	waitCmd := exec.Command("condor_wait", "-wait", "90", logFile)
	waitCmd.Env = append(os.Environ(), "CONDOR_CONFIG="+h.GetConfigFile())
	_ = waitCmd.Run()

	data, _ := os.ReadFile(logFile)
	if !strings.Contains(string(data), "Job terminated") {
		dumpAll()
		t.Fatalf("job did not complete: the negotiator's queries of the Go collector failed after the UDP RESCHEDULE; job log:\n%s", data)
	}
	// The case under test happened: the negotiator took a UDP command on the
	// family session (daemon_command.cpp logs its sender under D_SECURITY).
	neg, _ := os.ReadFile(filepath.Join(logDir, "NegotiatorLog"))
	if !strings.Contains(string(neg), "UDP message is from condor@family") {
		dumpLog(t, filepath.Join(logDir, "NegotiatorLog"))
		t.Fatal("the negotiator received no UDP command on the family session, so its later TCP commands never used BLOWFISH")
	}
}
