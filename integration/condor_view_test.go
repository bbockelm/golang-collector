// Condor-view integration tests: exercise golang-collector on both ends of an
// HTCondor "condor view" (CONDOR_VIEW_HOST) relationship, the way it will first be
// deployed -- a Go collector standing up as a view host for the existing OSPool
// top-level (C++) collector, and (the reverse) a Go pool collector forwarding to a
// C++ view host. In both cases we assert the typical daemon ads (Master, Startd,
// Schedd, Negotiator) propagate across the view link.
//
// A "view host" is just a second condor_collector that receives a copy of every
// ad the primary collector receives. We run that second collector as a standalone
// process (not under condor_master) on a fixed loopback port: a view sink only
// receives updates and answers queries, so it needs no master, shared port, or
// privilege handling -- which keeps these tests focused on the forwarding path.
package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// viewDaemonTypes are the daemon ad types every healthy pool advertises and that
// must therefore show up on a view host. Startd/Schedd advertise promptly; Master
// and Negotiator use slower intervals, so they are polled with a longer timeout.
var viewDaemonTypes = []string{"Startd", "Schedd", "Master", "Negotiator"}

// TestGoCollectorAsViewTarget runs a Go collector as the condor-view HOST for a
// standard C++ pool: the C++ pool collector forwards every ad it receives to the
// Go view collector via CONDOR_VIEW_HOST. This is the OSPool rollout scenario --
// the Go collector observing a live pool it does not own. We assert the typical
// daemon ads arrive at, and are queryable through, the Go view collector.
func TestGoCollectorAsViewTarget(t *testing.T) {
	requireViewTools(t)

	tmp := t.TempDir()
	goBin := buildGoCollector(t, tmp)

	// Start the Go collector as a standalone view sink on a fixed loopback port.
	viewAddr, viewLog := startViewCollector(t, "go", goBin, tmp)

	// Bring up a normal pool whose (default, C++) collector forwards to the Go view
	// host. The C++ collector authenticates to the view host to forward; same-host
	// FS auth makes that work without provisioning tokens.
	extra := fmt.Sprintf(`
CONDOR_VIEW_HOST = %s
COLLECTOR_DEBUG = D_FULLDEBUG
# By default a collector forwards only Machine (startd) ads to a view host (the
# classic CondorView utilization use). A view host that mirrors the pool's daemons
# must opt every daemon ad type in. MyType values: Startd=Machine, Schedd=Scheduler,
# Master=DaemonMaster, Negotiator=Negotiator.
CONDOR_VIEW_CLASSAD_TYPES = Machine, Scheduler, Negotiator, DaemonMaster
# The Go collector (cedar) is TCP-only, but forwarding to a view host defaults to
# UDP (UPDATE_VIEW_COLLECTOR_WITH_TCP=false) -- so the forwarded ads would be
# silently dropped. Force TCP forwarding. (Direct advertises already use TCP:
# UPDATE_COLLECTOR_WITH_TCP defaults true.)
UPDATE_VIEW_COLLECTOR_WITH_TCP = True
# Force FS-only + AES on the forward path: the Go view sink implements only FS
# auth and AES-GCM, so the C++ collector must forward that way (as the C++ daemons
# already advertise that way in master_test).
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_CLIENT_AUTHENTICATION_METHODS = FS
SEC_DEFAULT_CRYPTO_METHODS = AES
START = TRUE
# Advertise every ad type frequently so all four show up in the view within the
# poll window (Schedd/Negotiator otherwise re-advertise only every few minutes).
NEGOTIATOR_INTERVAL = 5
MASTER_UPDATE_INTERVAL = 5
UPDATE_INTERVAL = 5
SCHEDD_INTERVAL = 5
NEGOTIATOR_UPDATE_INTERVAL = 5
`, viewAddr)

	h := htcondor.SetupCondorHarnessWithConfig(t, extra)
	defer h.Shutdown()
	defer saveLogs(t, h.GetLogDir())

	if err := h.WaitForStartd(90 * time.Second); err != nil {
		dumpLog(t, filepath.Join(h.GetLogDir(), "CollectorLog"))
		dumpLog(t, viewLog)
		t.Fatalf("pool did not come up: %v", err)
	}

	// The proof: every daemon ad reaches the Go VIEW collector (not the pool's own
	// collector). Query the view collector directly.
	assertDaemonsVisible(t, viewAddr, viewLog, "Go view host")
}

// TestGoCollectorAsViewSource runs a Go collector as the pool's primary collector,
// forwarding to a standalone C++ view host via CONDOR_VIEW_HOST. This exercises the
// Go collector's own view-forwarding path (server.Forwarder) and its wire-compat
// with a C++ view sink. We assert the daemon ads propagate to the C++ view host.
func TestGoCollectorAsViewSource(t *testing.T) {
	requireViewTools(t)

	tmp := t.TempDir()
	goBin := buildGoCollector(t, tmp)

	// Standalone C++ collector as the view sink.
	viewAddr, viewLog := startViewCollector(t, "cpp", "", tmp)

	// Pool with the GO collector as its primary collector, forwarding to the C++
	// view host. Unlike the C++-source case, the Go collector forwards every ad
	// type it receives (no CONDOR_VIEW_CLASSAD_TYPES filter) over TCP (cedar is
	// TCP-only), so those C++ view-forwarding knobs are unnecessary here -- only the
	// fast advertise intervals are needed to see all four daemons within the poll.
	extra := fmt.Sprintf(`
COLLECTOR = %s
COLLECTOR_LOG = $(LOG)/CollectorLog
COLLECTOR_ADDRESS_FILE = $(LOG)/.collector_address
COLLECTOR_DEBUG = D_FULLDEBUG
CONDOR_VIEW_HOST = %s
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_CLIENT_AUTHENTICATION_METHODS = FS
SEC_DEFAULT_CRYPTO_METHODS = AES
START = TRUE
NEGOTIATOR_INTERVAL = 5
MASTER_UPDATE_INTERVAL = 5
UPDATE_INTERVAL = 5
SCHEDD_INTERVAL = 5
NEGOTIATOR_UPDATE_INTERVAL = 5
`, goBin, viewAddr)

	h := htcondor.SetupCondorHarnessWithConfig(t, extra)
	defer h.Shutdown()
	defer saveLogs(t, h.GetLogDir())

	if err := h.WaitForStartd(90 * time.Second); err != nil {
		dumpLog(t, filepath.Join(h.GetLogDir(), "CollectorLog"))
		dumpLog(t, viewLog)
		t.Fatalf("pool with Go collector did not come up: %v", err)
	}

	assertDaemonsVisible(t, viewAddr, viewLog, "C++ view host")
}

// assertDaemonsVisible polls the collector at addr until every viewDaemonTypes ad
// is locatable, failing (with the view log) if any never appears.
func assertDaemonsVisible(t *testing.T, addr, viewLog, who string) {
	t.Helper()
	ctx := context.Background()
	col := htcondor.NewCollector(addr)
	for _, dt := range viewDaemonTypes {
		if got := locateWithRetry(t, ctx, col, dt, 75*time.Second); got == "" {
			dumpLog(t, viewLog)
			t.Fatalf("%s never received a %s ad via the view link", who, dt)
		}
		t.Logf("%s: %s ad present via the view link", who, dt)
	}
}

// requireViewTools skips unless condor_master (for the pool) and condor_collector
// (for a C++ view sink) are present.
func requireViewTools(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"condor_master", "condor_collector"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH; skipping condor-view test", tool)
		}
	}
}

// buildGoCollector builds the htc-collector binary into dir and returns its path.
func buildGoCollector(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "htc-collector")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "../cmd/golang-collector")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building htc-collector: %v\n%s", err, out)
	}
	return bin
}

// startViewCollector starts a standalone collector -- a Go htc-collector (kind
// "go") or a C++ condor_collector (kind "cpp") -- as a condor-view sink on a fixed
// loopback port. It is deliberately NOT under condor_master: a view sink only
// receives forwarded updates and answers queries. Security is permissive (same-host
// FS or anonymous) so the forwarding collector is accepted without tokens. Returns
// the sink's address and its log-file path; the sink is torn down at test end.
func startViewCollector(t *testing.T, kind, goBin, tmp string) (addr, logPath string) {
	t.Helper()

	port := freePort(t)
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	viewDir := filepath.Join(tmp, "view-"+kind)
	if err := os.MkdirAll(viewDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath = filepath.Join(viewDir, "ViewCollectorLog")

	cfgPath := filepath.Join(viewDir, "condor_config")
	cfg := fmt.Sprintf(`
CONDOR_HOST = 127.0.0.1
COLLECTOR_HOST = %s
RELEASE_DIR = %s
LOCAL_DIR = %s
LOG = %s
LOCK = %s
RUN = %s
SPOOL = %s
EXECUTE = %s
COLLECTOR_LOG = %s
COLLECTOR_ADDRESS_FILE = %s
COLLECTOR_DEBUG = D_FULLDEBUG
USE_SHARED_PORT = False
CONDOR_VIEW_HOST =
DAEMON_LIST = COLLECTOR
# FS auth + AES-GCM, matching how C++ daemons advertise to a Go collector in
# master_test (the forwarding collector authenticates same-host via FS).
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS
SEC_CLIENT_AUTHENTICATION_METHODS = FS
SEC_DEFAULT_CRYPTO_METHODS = AES
ALLOW_READ = *
ALLOW_WRITE = *
ALLOW_ADVERTISE = *
ALLOW_DAEMON = *
# UPDATE_NEGOTIATOR_AD / UPDATE_ACCOUNTING_AD require NEGOTIATOR-level authz,
# which defaults to deny; without this a C++ view sink drops forwarded negotiator
# ads ("NEGOTIATOR authorization policy denies all access").
ALLOW_NEGOTIATOR = *
`, addr, releaseDir(t), viewDir, viewDir, viewDir, viewDir, viewDir, viewDir, logPath,
		filepath.Join(viewDir, ".view_collector_address"))
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	var cmd *exec.Cmd
	switch kind {
	case "go":
		cmd = exec.Command(goBin, "-listen", addr)
	case "cpp":
		cpp, err := exec.LookPath("condor_collector")
		if err != nil {
			t.Skipf("condor_collector not found: %v", err)
		}
		// Bind the fixed port explicitly: run standalone (not under condor_master),
		// DaemonCore would otherwise pick an ephemeral command port and ignore the
		// port in COLLECTOR_HOST (normally condor_master passes -p to the collector).
		cmd = exec.Command(cpp, "-f", "-p", fmt.Sprintf("%d", port))
	default:
		t.Fatalf("unknown view collector kind %q", kind)
	}
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfgPath)
	logf, err := os.Create(filepath.Join(viewDir, kind+"-stdio.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s view collector: %v", kind, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
		}
		_ = logf.Close()
	})

	// Wait until the sink is accepting connections on its port.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			return addr, logPath
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("%s view collector never listened on %s", kind, addr)
	return addr, logPath
}

// freePort returns a currently-free loopback TCP port. There is an inherent race
// (the port could be taken before the collector binds it), but it is small and
// standard for test harnesses.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// releaseDir returns the HTCondor RELEASE_DIR (the parent of the sbin holding
// condor_collector), so a standalone C++ collector finds its libexec helpers.
func releaseDir(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("condor_collector")
	if err != nil {
		return ""
	}
	// .../release_dir/sbin/condor_collector -> .../release_dir
	return filepath.Dir(filepath.Dir(p))
}
