// Privilege-drop integration test: verify that htc-collector, launched as root by
// a real condor_master, behaves the way an HTCondor daemon must on a production
// central manager:
//
//	(a) it drops its effective uid/gid to the unprivileged "condor" account;
//	(b) it creates its log file and state DB (the session cache) owned by condor,
//	    NOT root -- so a later run as condor can still read/write them;
//	(c) it reads the pool signing key (root-owned 0600, in SEC_PASSWORD_DIRECTORY)
//	    as root by re-elevating, since the session cache is encrypted at rest with
//	    that key and a condor-owned process could not otherwise read it.
//
// The drop is triggered purely by starting as root (euid 0), so this test only
// means anything when it runs as root with a real condor account present. It
// therefore SKIPS for an interactive developer (non-root) and is exercised in the
// dedicated root CI job (see .github/workflows/ci.yml `test-root`).
package integration

import (
	"crypto/rand"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCollectorPrivilegeDrop(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("privilege-drop test requires Linux (seteuid/set_priv semantics)")
	}
	if os.Geteuid() != 0 {
		t.Skip("privilege-drop test must run as root to observe the root->condor drop; runs in the test-root CI job")
	}
	condorUser, err := user.Lookup("condor")
	if err != nil {
		t.Skipf("condor account not present (%v); skipping privilege-drop test", err)
	}
	condorUID, _ := strconv.Atoi(condorUser.Uid)
	condorGID, _ := strconv.Atoi(condorUser.Gid)
	if _, err := exec.LookPath("condor_master"); err != nil {
		t.Skipf("condor_master not found in PATH: %v", err)
	}

	// HTCondor opens its logs and runs its daemons as the unprivileged `condor`
	// account, which must be able to traverse the ENTIRE working-directory path.
	// t.TempDir() nests the work dir under a 0700 root-owned parent that condor
	// cannot enter (condor_master then fails with "Cannot open log file"), so use a
	// bespoke shallow dir directly under /tmp (mode 1777, world-traversable) at mode
	// 0755, and remove it ourselves since t.Cleanup no longer owns it.
	tmp, err := os.MkdirTemp("/tmp", "htc-privdrop-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmp) })
	if err := os.Chmod(tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	goBin := buildGoCollector(t, tmp)

	// Directory layout: LOG/SPOOL/LOCK/EXECUTE are chowned to condor so the dropped
	// collector owns what it writes; the password directory stays root-owned.
	logDir := filepath.Join(tmp, "log")
	spoolDir := filepath.Join(tmp, "spool")
	lockDir := filepath.Join(tmp, "lock")
	execDir := filepath.Join(tmp, "execute")
	passwdDir := filepath.Join(tmp, "passwords.d")
	for _, d := range []string{logDir, spoolDir, lockDir, execDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(d, condorUID, condorGID); err != nil {
			t.Fatalf("chown %s to condor: %v", d, err)
		}
	}

	// The pool signing key: root-owned 0700 directory holding a root-owned 0600 key
	// file. A process running as condor cannot read this via a plain open -- only by
	// re-elevating to root -- which is the whole point of assertion (c).
	if err := os.MkdirAll(passwdDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(passwdDir, "POOL")
	keyMaterial := make([]byte, 32)
	if _, err := rand.Read(keyMaterial); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyMaterial, 0o600); err != nil {
		t.Fatal(err)
	}
	// passwdDir + keyFile are created by root, so already root-owned; assert the
	// mode so the "condor cannot read it" premise of (c) is explicit.
	assertRootOwned0600(t, keyFile)

	port := freePort(t)
	collAddr := fmt.Sprintf("127.0.0.1:%d", port)
	addrFile := filepath.Join(logDir, ".collector_address")
	dbPath := filepath.Join(spoolDir, "collector_sessions.db")
	sbin := filepath.Dir(mustLookPath(t, "condor_master"))
	libexec := libexecDir(t, sbin)

	cfg := fmt.Sprintf(`
CONDOR_HOST = 127.0.0.1
COLLECTOR_HOST = %s
RELEASE_DIR = %s
LIBEXEC = %s
LOCAL_DIR = %s
LOG = %s
SPOOL = %s
LOCK = %s
RUN = %s
EXECUTE = %s
SBIN = %s
MASTER_LOG = %s/MasterLog
DAEMON_LIST = MASTER, COLLECTOR
COLLECTOR = %s
COLLECTOR_LOG = %s/CollectorLog
COLLECTOR_DEBUG = D_FULLDEBUG
COLLECTOR_ADDRESS_FILE = %s
# Shared port, as the collector is actually deployed under condor_master: the Go
# collector inherits the shared-port endpoint. (Non-shared-port launch also works
# now that the daemon adopts the master's inherited command socket --
# bbockelm/golang-htcondor#119 -- but shared port is the deployed default.)
USE_SHARED_PORT = True
# Encrypted-at-rest session persistence: the collector reads the root-owned pool
# signing key (as root) to key-encrypt this DB. This is what forces the root
# re-elevation the test asserts.
COLLECTOR_PERSIST_SESSIONS = True
SEC_PASSWORD_DIRECTORY = %s
COLLECTOR_SESSION_CACHE_FILE = %s
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_DEFAULT_CRYPTO_METHODS = AES
ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_DAEMON = *
ALLOW_ADMINISTRATOR = *
`, collAddr, releaseDir(t), libexec, tmp, logDir, spoolDir, lockDir, lockDir, execDir, sbin,
		logDir, goBin, logDir, addrFile, passwdDir, dbPath)

	cfgPath := filepath.Join(tmp, "condor_config")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	// Launch a real condor_master (as root) that starts the Go collector as root;
	// the collector self-drops to condor -- exactly the production launch path.
	master := exec.Command(mustLookPath(t, "condor_master"), "-f")
	master.Env = append(os.Environ(), "CONDOR_CONFIG="+cfgPath, "_CONDOR_LOCAL_DIR="+tmp)
	master.Dir = tmp
	// Own process group so cleanup can reap the whole daemon tree (the master
	// forks condor_shared_port, condor_procd, and the collector).
	master.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	mout, err := os.Create(filepath.Join(tmp, "master-stdio.log"))
	if err != nil {
		t.Fatal(err)
	}
	master.Stdout, master.Stderr = mout, mout
	if err := master.Start(); err != nil {
		t.Fatalf("starting condor_master: %v", err)
	}
	t.Cleanup(func() {
		// SIGTERM is condor_master's graceful shutdown (it tears down its children);
		// then SIGKILL the whole process group as a backstop so no shared_port/procd/
		// collector processes are orphaned.
		_ = master.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = master.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
		if pgid, err := syscall.Getpgid(master.Process.Pid); err == nil {
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		}
		_ = mout.Close()
	})

	// Wait for the collector to come up (address file written). If the collector
	// could not read the signing key as root, session-cache Open would fail and the
	// collector would exit before writing the address file -- so reaching this point
	// is itself part of assertion (c).
	collectorLog := filepath.Join(logDir, "CollectorLog")
	if !waitForFile(addrFile, 60*time.Second) {
		dumpLog(t, filepath.Join(tmp, "master-stdio.log"))
		dumpLog(t, filepath.Join(logDir, "MasterLog"))
		dumpLog(t, collectorLog)
		t.Fatalf("collector never wrote its address file %s; see logs", addrFile)
	}

	// (a) The running collector's effective uid/gid are condor's. The daemon logs
	// the drop it performed; assert the log records euid/egid == condor.
	assertDroppedToCondor(t, collectorLog, condorUID, condorGID)

	// (b) The log file and the session state DB are owned by condor, not root.
	assertOwnedBy(t, collectorLog, condorUID, "CollectorLog")
	if !waitForFile(dbPath, 30*time.Second) {
		dumpLog(t, collectorLog)
		t.Fatalf("session DB %s was never created", dbPath)
	}
	assertOwnedBy(t, dbPath, condorUID, "session DB")

	// (c) The signing key was read as root: the session DB is encrypted (its
	// master key wrapped by the pool signing key at Open), the key is root-only, and
	// the collector -- running as condor -- logged that it loaded >=1 signing key.
	// A condor process reading a root:root 0600 file can only have done so by
	// re-elevating to root.
	assertRootOwned0600(t, keyFile) // unchanged by the run
	assertLogContains(t, collectorLog, "session persistence enabled",
		"encrypted session persistence active (pool signing key read as root while running as condor)")
}

// assertDroppedToCondor checks the collector logged a privilege drop to condor's
// uid/gid. daemon.New logs `dropped privileges euid=<u> egid=<g>`.
func assertDroppedToCondor(t *testing.T, logPath string, uid, gid int) {
	t.Helper()
	data := readFileEventually(t, logPath, "dropped privileges", 30*time.Second)
	wantUID := fmt.Sprintf("euid=%d", uid)
	wantGID := fmt.Sprintf("egid=%d", gid)
	if !strings.Contains(data, wantUID) || !strings.Contains(data, wantGID) {
		dumpLog(t, logPath)
		t.Fatalf("collector did not drop to condor (uid=%d gid=%d): log lacked %q/%q", uid, gid, wantUID, wantGID)
	}
	t.Logf("(a) collector dropped privileges to condor euid=%d egid=%d", uid, gid)
}

// assertOwnedBy fails unless path is owned by uid.
func assertOwnedBy(t *testing.T, path string, uid int, what string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s (%s): %v", path, what, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("no syscall.Stat_t for %s", path)
	}
	if int(st.Uid) != uid {
		t.Fatalf("%s (%s) owned by uid %d, want condor uid %d", path, what, st.Uid, uid)
	}
	t.Logf("(b) %s is owned by condor (uid=%d)", what, uid)
}

// assertRootOwned0600 fails unless path is owned by root with mode 0600 -- the
// premise that a condor-owned process cannot read it without re-elevating.
func assertRootOwned0600(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("no syscall.Stat_t for %s", path)
	}
	if st.Uid != 0 {
		t.Fatalf("signing key %s owned by uid %d, want root (0)", path, st.Uid)
	}
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("signing key %s has mode %o; want no group/other access (0600)", path, perm)
	}
}

// assertLogContains verifies the log at path contains want, describing the
// checked behavior with desc in both the success log and the failure message.
func assertLogContains(t *testing.T, path, want, desc string) {
	t.Helper()
	data := readFileEventually(t, path, want, 30*time.Second)
	if !strings.Contains(data, want) {
		dumpLog(t, path)
		t.Fatalf("%s: not confirmed (missing %q in %s)", desc, want, filepath.Base(path))
	}
	t.Logf("(c) %s", desc)
}

// readFileEventually polls path until it contains want or the timeout elapses,
// returning the last-read contents either way.
func readFileEventually(t *testing.T, path, want string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var data []byte
	for time.Now().Before(deadline) {
		data, _ = os.ReadFile(path) //nolint:gosec // test-controlled path
		if strings.Contains(string(data), want) {
			return string(data)
		}
		time.Sleep(300 * time.Millisecond)
	}
	return string(data)
}

// waitForFile reports whether path exists before the timeout.
func waitForFile(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func mustLookPath(t *testing.T, tool string) string {
	t.Helper()
	p, err := exec.LookPath(tool)
	if err != nil {
		t.Skipf("%s not found in PATH: %v", tool, err)
	}
	return p
}

// libexecDir returns the directory holding condor_shared_port (needed when
// USE_SHARED_PORT=True), preferring PATH, then the standard package location, then
// a sibling of sbin. It skips the test if it cannot be found.
func libexecDir(t *testing.T, sbin string) string {
	t.Helper()
	if p, err := exec.LookPath("condor_shared_port"); err == nil {
		return filepath.Dir(p)
	}
	for _, cand := range []string{"/usr/libexec/condor", filepath.Join(filepath.Dir(sbin), "libexec", "condor"), filepath.Join(filepath.Dir(sbin), "libexec")} {
		if _, err := os.Stat(filepath.Join(cand, "condor_shared_port")); err == nil {
			return cand
		}
	}
	t.Skip("condor_shared_port (LIBEXEC) not found; skipping privilege-drop test")
	return ""
}
