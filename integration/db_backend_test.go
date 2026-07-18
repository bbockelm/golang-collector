// Integration test for the embedded-database ad store (COLLECTOR_STORE=db): the
// real proof of the persistent backend is that ads advertised with the actual
// HTCondor client tools survive a collector restart, instead of vanishing as they
// would with the in-memory store. Skips unless the HTCondor binaries are on PATH.
package integration

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGoCollectorDBBackendRestartResume(t *testing.T) {
	for _, tool := range []string{"condor_advertise", "condor_status"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH; skipping db-backend integration test", tool)
		}
	}

	tmp := t.TempDir()
	collBin := buildGoCollector(t, tmp)
	dbPath := filepath.Join(tmp, "collector-db")
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// Permissive security so condor_advertise/condor_status work without tokens;
	// the point under test is persistence, not auth. COLLECTOR_STORE=db persists
	// ads to COLLECTOR_DB_PATH so a restart reloads them.
	collDir := filepath.Join(tmp, "coll")
	if err := os.MkdirAll(filepath.Join(collDir, "log"), 0o755); err != nil {
		t.Fatal(err)
	}
	collCfg := filepath.Join(tmp, "coll_config")
	writeFile(t, collCfg, fmt.Sprintf(`
RELEASE_DIR = %s
LOCAL_DIR = %s
LOG = %s/log
COLLECTOR_LOG = %s/log/CollectorLog
COLLECTOR_ADDRESS_FILE = %s/log/.collector_address
USE_SHARED_PORT = False
CONDOR_VIEW_HOST =
COLLECTOR_STORE = db
COLLECTOR_DB_PATH = %s
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_DEFAULT_ENCRYPTION = OPTIONAL
SEC_DEFAULT_CRYPTO_METHODS = AES
ALLOW_READ = *
ALLOW_ADVERTISE = *
ALLOW_DAEMON = *
`, releaseDir(t), collDir, collDir, collDir, collDir, dbPath))

	clientCfg := filepath.Join(tmp, "client_config")
	writeFile(t, clientCfg, fmt.Sprintf(`
RELEASE_DIR = %s
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_DEFAULT_ENCRYPTION = OPTIONAL
SEC_DEFAULT_CRYPTO_METHODS = AES
`, releaseDir(t)))

	machineAd := filepath.Join(tmp, "machine.ad")
	writeFile(t, machineAd, `MyType = "Machine"
TargetType = "Job"
Name = "slot1@persisted"
Machine = "persisted"
StartdIpAddr = "<127.0.0.1:9999>"
State = "Unclaimed"
Activity = "Idle"
`)

	// --- First run: advertise an ad, confirm it is visible. ---
	stop1 := startDBCollector(t, collBin, collCfg, addr)
	if out, err := runCondorErr(clientCfg, 30*time.Second, "condor_advertise", "-pool", addr, "UPDATE_STARTD_AD", machineAd); err != nil {
		t.Fatalf("advertise failed: %v\n%s", err, out)
	}
	if !statusHasPersisted(t, clientCfg, addr, 15*time.Second) {
		t.Fatal("advertised ad not visible before restart")
	}
	t.Log("ad advertised and visible in first collector run")

	// --- Restart: stop the collector, start a new process on the same db path. ---
	stop1()
	stop2 := startDBCollector(t, collBin, collCfg, addr)
	defer stop2()

	// The ad must still be there -- reloaded from the database, not re-advertised
	// (no daemon is running to re-advertise it).
	if !statusHasPersisted(t, clientCfg, addr, 20*time.Second) {
		dumpLog(t, filepath.Join(collDir, "log", "CollectorLog"))
		t.Fatal("ad did NOT survive restart: the db backend failed to persist/reload it")
	}
	t.Log("ad survived collector restart -- reloaded from the embedded database")
}

// statusHasPersisted polls condor_status until it reports the persisted slot or
// the timeout elapses.
func statusHasPersisted(t *testing.T, clientCfg, addr string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := runCondorErr(clientCfg, 15*time.Second, "condor_status", "-pool", addr)
		if err == nil && strings.Contains(out, "persisted") {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

// startDBCollector starts the htc-collector binary on addr with the given config
// and waits until it is accepting connections. It returns a stop function that
// terminates the process (and waits for it), so a test can restart it.
func startDBCollector(t *testing.T, bin, configFile, addr string) (stop func()) {
	t.Helper()
	logf, err := os.OpenFile(configFile+".stdio.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-listen", addr)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+configFile)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		t.Fatalf("starting collector: %v", err)
	}
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
		_ = logf.Close()
	}
	t.Cleanup(stop)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			_ = c.Close()
			return stop
		}
		time.Sleep(200 * time.Millisecond)
	}
	stop()
	t.Fatalf("collector never listened on %s", addr)
	return stop
}
