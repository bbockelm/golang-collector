package integration

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

// pingLevelCount is the number of results `condor_ping ALL` reports: ALLOW,
// READ, WRITE, NEGOTIATOR, ADMINISTRATOR, OWNER, CONFIG, DAEMON and the three
// ADVERTISE_* levels.
const pingLevelCount = 11

// checkPingLevels checks that `condor_ping ALL` against a Go daemon (goAddr)
// answers every level as it does against a C++ daemon of the same pool
// (cppAddr), which enforces the same ALLOW_/DENY_ policy: ALLOWED where the
// tool's identity is authorized, DENIED where it is not (CONFIG, as
// ALLOW_CONFIG is unset), and never CMD_NOT_FOUND.
func checkPingLevels(t *testing.T, configFile, goAddr, cppAddr string) {
	t.Helper()
	goRes := pingLevels(t, configFile, goAddr)
	cppRes := pingLevels(t, configFile, cppAddr)
	if len(goRes) != pingLevelCount {
		t.Errorf("condor_ping ALL against the Go daemon reported %d levels, want %d: %v", len(goRes), pingLevelCount, goRes)
	}
	for cmd, res := range goRes {
		if res != "ALLOWED" && res != "DENIED" {
			t.Errorf("condor_ping %s against the Go daemon: %s, want ALLOWED or DENIED", cmd, res)
		}
	}
	if goRes["DC_NOP_READ"] != "ALLOWED" {
		t.Errorf("condor_ping READ against the Go daemon: %s, want ALLOWED (ALLOW_READ = *)", goRes["DC_NOP_READ"])
	}
	if goRes["DC_NOP_CONFIG"] != "DENIED" {
		t.Errorf("condor_ping CONFIG against the Go daemon: %s, want DENIED (ALLOW_CONFIG unset)", goRes["DC_NOP_CONFIG"])
	}
	if !reflect.DeepEqual(goRes, cppRes) {
		t.Errorf("condor_ping ALL: Go daemon %v, C++ daemon %v", goRes, cppRes)
	}
}

// pingLevels runs `condor_ping ALL` against the daemon at addr: a
// DC_SEC_QUERY for each authorization level's DC_NOP_* command. It returns
// each level's result keyed by command name (DC_NOP_READ, ...): ALLOWED,
// DENIED (refused by the ALLOW_/DENY_ policy), or the daemon's other answer
// (CMD_NOT_FOUND: no command registered at the level). It also logs
// `condor_ping -table ALL`, the table an administrator reads.
func pingLevels(t *testing.T, configFile, addr string) map[string]string {
	t.Helper()
	// condor_ping exits non-zero when any level fails, so its exit status is
	// not checked; the output is.
	t.Logf("condor_ping -table ALL against %s:\n%s", addr, runPing(t, configFile, "-address", addr, "-table", "ALL"))
	out := runPing(t, configFile, "-address", addr, "ALL", "-af", "Name", "Result")
	results := make(map[string]string)
	for _, line := range strings.Split(out, "\n") {
		if f := strings.Fields(line); len(f) == 2 && strings.HasPrefix(f[0], "DC_") {
			results[f[0]] = f[1]
		}
	}
	if len(results) == 0 {
		t.Fatalf("condor_ping ALL -af Name Result against %s printed no results:\n%s", addr, out)
	}
	return results
}

func runPing(t *testing.T, configFile string, args ...string) string {
	t.Helper()
	path, err := exec.LookPath("condor_ping")
	if err != nil {
		t.Skipf("condor_ping not found: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+configFile)
	out, _ := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("condor_ping %v timed out:\n%s", args, out)
	}
	return string(out)
}
