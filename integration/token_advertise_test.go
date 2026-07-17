// Per-authorization-level security against a standalone htc-collector, exercised
// with the real C++ client tools. This is the deployment story that motivated
// per-level negotiation: monitoring (READ) is public so condor_status works with
// no credentials, while publishing (ADVERTISE) requires an authenticated,
// encrypted session -- here an HTCondor IDTOKEN. We assert all three arms:
//
//   - condor_status (READ) succeeds unauthenticated and sees the pool;
//   - condor_advertise (ADVERTISE) with a valid IDTOKEN + encryption succeeds;
//   - condor_advertise WITHOUT a token is refused.
//
// The collector runs standalone (not under condor_master) on a fixed loopback
// port so the per-level SEC_* policy under test is exactly what we set, with no
// pool defaults mixed in.
package integration

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCollectorPerLevelTokenAdvertise(t *testing.T) {
	for _, tool := range []string{"condor_advertise", "condor_status", "condor_token_create"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH; skipping per-level token test", tool)
		}
	}

	tmp := t.TempDir()
	collBin := buildGoCollector(t, tmp)

	// --- Token infrastructure: a POOL signing key both condor_token_create and
	// the collector (cedar) read, and an IDTOKEN for advertise@testpool. The key
	// file is raw bytes; both sides apply HTCondor's simple_scramble, so any
	// consistent content works. condor_store_cred can't mint it here (no master),
	// so write it directly with the perms condor_token_create's secure read wants.
	passwdDir := filepath.Join(tmp, "passwords.d")
	tokenDir := filepath.Join(tmp, "tokens.d")
	for _, d := range []string{passwdDir, tokenDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	poolKey := filepath.Join(passwdDir, "POOL")
	keyBytes := make([]byte, 50)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(poolKey, keyBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	trustDomain := "testpool"
	tokenCfg := filepath.Join(tmp, "token_config")
	writeFile(t, tokenCfg, fmt.Sprintf(`
RELEASE_DIR = %s
SEC_PASSWORD_DIRECTORY = %s
SEC_TOKEN_POOL_SIGNING_KEY_FILE = %s
SEC_TOKEN_DIRECTORY = %s
TRUST_DOMAIN = %s
`, releaseDir(t), passwdDir, poolKey, tokenDir, trustDomain))

	tokenFile := filepath.Join(tokenDir, "adv.token")
	out, err := runCondorErr(tokenCfg, 20*time.Second, "condor_token_create",
		"-identity", "advertise@"+trustDomain, "-key", "POOL")
	if err != nil {
		t.Fatalf("condor_token_create failed: %v\n%s", err, out)
	}
	if err := os.WriteFile(tokenFile, []byte(out), 0o600); err != nil {
		t.Fatal(err)
	}

	// --- Collector: READ permissive (unauthenticated condor_status), ADVERTISE
	// requires IDTOKENS auth + encryption + integrity.
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
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
SEC_PASSWORD_DIRECTORY = %s
SEC_TOKEN_POOL_SIGNING_KEY_FILE = %s
TRUST_DOMAIN = %s
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_READ_AUTHENTICATION = OPTIONAL
SEC_ADVERTISE_AUTHENTICATION = REQUIRED
SEC_ADVERTISE_ENCRYPTION = REQUIRED
SEC_ADVERTISE_INTEGRITY = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS, IDTOKENS
SEC_ADVERTISE_AUTHENTICATION_METHODS = IDTOKENS
SEC_DEFAULT_CRYPTO_METHODS = AES
ALLOW_READ = *
ALLOW_ADVERTISE = *
ALLOW_DAEMON = *
`, releaseDir(t), collDir, collDir, collDir, collDir, passwdDir, poolKey, trustDomain))

	startStandaloneCollector(t, collBin, collCfg, addr)

	machineAd := filepath.Join(tmp, "machine.ad")
	writeFile(t, machineAd, `MyType = "Machine"
TargetType = "Job"
Name = "slot1@tokenhost"
Machine = "tokenhost"
StartdIpAddr = "<127.0.0.1:9999>"
State = "Unclaimed"
Activity = "Idle"
`)

	// Arm 1: advertise WITH the token (+encryption) must succeed.
	authCfg := filepath.Join(tmp, "auth_client_config")
	writeFile(t, authCfg, fmt.Sprintf(`
RELEASE_DIR = %s
SEC_TOKEN_DIRECTORY = %s
TRUST_DOMAIN = %s
SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_CLIENT_AUTHENTICATION_METHODS = IDTOKENS
SEC_DEFAULT_CRYPTO_METHODS = AES
SEC_CLIENT_ENCRYPTION = REQUIRED
`, releaseDir(t), tokenDir, trustDomain))

	out, err = runCondorErr(authCfg, 30*time.Second, "condor_advertise", "-pool", addr, "UPDATE_STARTD_AD", machineAd)
	if err != nil {
		t.Fatalf("token-authenticated advertise failed: %v\n%s", err, out)
	}
	t.Logf("authenticated advertise: %s", out)

	// Arm 2: condor_status (READ, unauthenticated) must see the advertised ad.
	readCfg := filepath.Join(tmp, "read_client_config")
	writeFile(t, readCfg, fmt.Sprintf(`
RELEASE_DIR = %s
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_CLIENT_AUTHENTICATION_METHODS = FS, IDTOKENS
SEC_DEFAULT_CRYPTO_METHODS = AES
`, releaseDir(t)))

	var seen bool
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		out, err = runCondorErr(readCfg, 15*time.Second, "condor_status", "-pool", addr)
		if err == nil && strings.Contains(out, "tokenhost") {
			seen = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !seen {
		t.Fatalf("condor_status (unauthenticated READ) did not see the token-advertised ad; last output:\n%s (err=%v)", out, err)
	}
	t.Log("condor_status (unauthenticated READ) sees the token-advertised ad")

	// Arm 3: advertise WITHOUT a token must be refused (ADVERTISE requires auth).
	emptyTokens := filepath.Join(tmp, "empty.d")
	if err := os.MkdirAll(emptyTokens, 0o700); err != nil {
		t.Fatal(err)
	}
	noTokCfg := filepath.Join(tmp, "notoken_config")
	writeFile(t, noTokCfg, fmt.Sprintf(`
RELEASE_DIR = %s
SEC_TOKEN_DIRECTORY = %s
TRUST_DOMAIN = %s
SEC_DEFAULT_AUTHENTICATION = OPTIONAL
SEC_CLIENT_AUTHENTICATION_METHODS = IDTOKENS
SEC_DEFAULT_CRYPTO_METHODS = AES
`, releaseDir(t), emptyTokens, trustDomain))

	out, err = runCondorErr(noTokCfg, 30*time.Second, "condor_advertise", "-pool", addr, "UPDATE_STARTD_AD", machineAd)
	if err == nil && !strings.Contains(out, "Sent 0 of") {
		t.Fatalf("unauthenticated advertise should have been refused, but succeeded:\n%s", out)
	}
	t.Logf("unauthenticated advertise refused as expected: %s (err=%v)", out, err)
}

// startStandaloneCollector launches the htc-collector binary as a standalone
// process on addr with the given config, and waits until it is accepting
// connections. It is torn down at test end.
func startStandaloneCollector(t *testing.T, bin, configFile, addr string) {
	t.Helper()
	logf, err := os.Create(configFile + ".stdio.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-listen", addr, "-debug")
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+configFile)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting standalone collector: %v", err)
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

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("standalone collector never listened on %s", addr)
}

// runCondorErr runs an HTCondor tool against configFile and returns its combined
// output and error (unlike runCondor, it does not fail the test -- callers that
// need to assert a failure use the returned error).
func runCondorErr(configFile string, timeout time.Duration, name string, args ...string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+configFile)
	done := make(chan struct{})
	var out []byte
	go func() { out, err = cmd.CombinedOutput(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		return string(out), fmt.Errorf("%s timed out", name)
	}
	return string(out), err
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

