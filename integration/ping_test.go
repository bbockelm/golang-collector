package integration

import (
	"context"
	"os"
	"os/exec"
	"os/user"
	"reflect"
	"strings"
	"testing"
	"time"
)

// pingLevelNames are the levels `condor_ping ALL` queries, in its order.
var pingLevelNames = []string{
	"ALLOW", "READ", "WRITE", "NEGOTIATOR", "ADMINISTRATOR", "OWNER", "CONFIG",
	"DAEMON", "ADVERTISE_STARTD", "ADVERTISE_SCHEDD", "ADVERTISE_MASTER",
}

// pingLevel is condor_ping's answer for one authorization level.
type pingLevel struct {
	// Result is ALLOWED, DENIED (refused by the ALLOW_/DENY_ policy), or the
	// daemon's other answer (CMD_NOT_FOUND: no command registered at the
	// level).
	Result string
	// Row is the level's `condor_ping -table` row after the level name:
	// authentication method, encryption, integrity, decision and identity.
	Row []string
}

// checkPingLevels checks that `condor_ping ALL` against a Go daemon (goAddr)
// answers every level as it does against a C++ daemon of the same pool
// (cppAddr), which enforces the same SEC_* and ALLOW_/DENY_ policy: ALLOWED
// where the tool's identity is authorized, DENIED where it is not (CONFIG, as
// ALLOW_CONFIG is unset), never CMD_NOT_FOUND, and each allowed session with
// the same authentication method, encryption, integrity and identity
// (<user>@<UID_DOMAIN>, as the C++ configuration resolves UID_DOMAIN).
func checkPingLevels(t *testing.T, configFile, goAddr, cppAddr string) {
	t.Helper()
	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	uidDomain := strings.TrimSpace(runCondor(t, configFile, 30*time.Second, "condor_config_val", "UID_DOMAIN"))
	identity := me.Username + "@" + uidDomain

	goRes := pingLevels(t, configFile, goAddr)
	cppRes := pingLevels(t, configFile, cppAddr)
	for _, lvl := range pingLevelNames {
		res, ok := goRes[lvl]
		if !ok {
			t.Errorf("condor_ping %s against the Go daemon: no result", lvl)
			continue
		}
		if res.Result != "ALLOWED" && res.Result != "DENIED" {
			t.Errorf("condor_ping %s against the Go daemon: %s, want ALLOWED or DENIED", lvl, res.Result)
		}
		if res.Result == "ALLOWED" {
			if len(res.Row) != 5 || res.Row[3] != "ALLOW" || res.Row[4] != identity {
				t.Errorf("condor_ping -table %s against the Go daemon: %q, want an ALLOW row for %s", lvl, res.Row, identity)
			}
		}
	}
	if goRes["READ"].Result != "ALLOWED" {
		t.Errorf("condor_ping READ against the Go daemon: %s, want ALLOWED (ALLOW_READ = *)", goRes["READ"].Result)
	}
	if goRes["CONFIG"].Result != "DENIED" {
		t.Errorf("condor_ping CONFIG against the Go daemon: %s, want DENIED (ALLOW_CONFIG unset)", goRes["CONFIG"].Result)
	}
	for _, lvl := range pingLevelNames {
		if g, c := goRes[lvl], cppRes[lvl]; !reflect.DeepEqual(g, c) {
			t.Errorf("condor_ping %s: Go daemon %s %q, C++ daemon %s %q", lvl, g.Result, g.Row, c.Result, c.Row)
		}
	}
}

// pingLevels runs `condor_ping ALL` against the daemon at addr: a
// DC_SEC_QUERY for each authorization level's DC_NOP_* command. It returns
// each level's answer keyed by level name, and logs the `-table` output, the
// table an administrator reads.
func pingLevels(t *testing.T, configFile, addr string) map[string]pingLevel {
	t.Helper()
	// condor_ping exits non-zero when any level fails, so its exit status is
	// not checked; the output is.
	table := runPing(t, configFile, "-address", addr, "-table", "ALL")
	t.Logf("condor_ping -table ALL against %s:\n%s", addr, table)
	rows := make(map[string][]string)
	for _, line := range strings.Split(table, "\n") {
		if f := strings.Fields(line); len(f) > 1 {
			rows[f[0]] = f[1:]
		}
	}
	// -table prints FAIL for every failure; -af Result tells a refusal
	// (DENIED) from CMD_NOT_FOUND.
	out := runPing(t, configFile, "-address", addr, "ALL", "-af", "Name", "Result")
	results := make(map[string]pingLevel)
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 || !strings.HasPrefix(f[0], "DC_NOP") {
			continue
		}
		lvl := strings.TrimPrefix(strings.TrimPrefix(f[0], "DC_NOP"), "_")
		if lvl == "" {
			lvl = "ALLOW"
		}
		results[lvl] = pingLevel{Result: f[1], Row: rows[lvl]}
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
