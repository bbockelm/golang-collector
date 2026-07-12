package integration

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// This file is the C++ vs. Go negotiator differential harness (roadmap item
// #1). It drives the IDENTICAL sequence of condor_userprio mutations into (1)
// a real C++ condor_negotiator and (2) the Go golang-negotiator, both running
// under a real condor_master, and compares the resulting accountant state (via
// condor_userprio) and — best-effort — the fair-share slot allocation.
//
// Both negotiators are configured into the Go MVP feature set
// (NEGOTIATOR_CONSIDER_PREEMPTION = FALSE, no concurrency limits) per
// docs/NEGOTIATOR_CPP_DIFFERENCES.md §2, so the comparison is apples-to-apples.
//
// Everything skips (never fails) unless the HTCondor binaries are on PATH.

// mvpFeatureSet pins both negotiators to the deferred-feature-off MVP so the
// differential is fair (differences doc §2). DEFAULT_PRIO_FACTOR/HALFLIFE are
// pinned so neither side's default can drift the comparison.
const mvpFeatureSet = `
NEGOTIATOR_CONSIDER_PREEMPTION = FALSE
DEFAULT_PRIO_FACTOR = 1000
# Advertise the NegotiatorAd promptly so condor_userprio can locate us.
NEGOTIATOR_UPDATE_INTERVAL = 5
# A runnable single slot; matchmaking prompt where it matters.
START = TRUE
SUSPEND = FALSE
CONTINUE = TRUE
PREEMPT = FALSE
KILL = FALSE
RUNBENCHMARKS = FALSE
SEC_DEFAULT_CRYPTO_METHODS = AES
`

// diffPool is one running pool (C++ or Go negotiator) plus its config path.
type diffPool struct {
	h    *htcondor.CondorTestHarness
	cfg  string
	logs string
	kind string // "cpp" or "go"
}

// requireCondor skips the test unless the binaries the harness needs are on
// PATH.
func requireCondor(t *testing.T) {
	t.Helper()
	for _, tool := range []string{"condor_master", "condor_userprio"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH, skipping differential test", tool)
		}
	}
}

// buildGoNegotiatorBin builds the golang-negotiator binary the master will run
// as the pool's NEGOTIATOR.
func buildGoNegotiatorBin(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "golang-negotiator")
	build := exec.Command("go", "build", "-buildvcs=false", "-o", bin, "../cmd/golang-negotiator")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building golang-negotiator: %v\n%s", err, out)
	}
	return bin
}

// startDiffPool brings up a condor_master pool. When goBin != "" the pool runs
// the Go negotiator as its NEGOTIATOR; otherwise the stock C++ condor_negotiator.
func startDiffPool(t *testing.T, goBin, knobs string) *diffPool {
	t.Helper()
	extra := mvpFeatureSet + "\n" + knobs
	kind := "cpp"
	if goBin != "" {
		kind = "go"
		extra += fmt.Sprintf("\nNEGOTIATOR = %s\nNEGOTIATOR_LOG = $(LOG)/NegotiatorLog\nNEGOTIATOR_ADDRESS_FILE = $(LOG)/.negotiator_address\nNEGOTIATOR_DEBUG = D_FULLDEBUG\n", goBin)
	}
	h := htcondor.SetupCondorHarnessWithConfig(t, extra)
	p := &diffPool{h: h, cfg: h.GetConfigFile(), logs: h.GetLogDir(), kind: kind}
	return p
}

// userprio runs condor_userprio against the pool and returns combined output.
func (p *diffPool) userprio(t *testing.T, args ...string) (string, error) {
	t.Helper()
	path, err := exec.LookPath("condor_userprio")
	if err != nil {
		t.Skip("condor_userprio not found")
	}
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+p.cfg)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// waitNegotiatorReady polls until condor_userprio can query the negotiator
// (its ReportState ad parses). This absorbs the NegotiatorAd-locate propagation
// delay for both negotiators, and specifically the known "Can't locate
// negotiator in local pool" warning against the Go negotiator: we parse the ad
// it prints regardless of that warning.
func (p *diffPool) waitNegotiatorReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastOut string
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := p.userprio(t, "-l", "-modular", "-allusers")
		lastOut, lastErr = out, err
		if _, perr := negtest.ParseUserprioModular(out); perr == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	dumpLog(t, filepath.Join(p.logs, "NegotiatorLog"))
	t.Fatalf("[%s] negotiator never became queryable via condor_userprio (last err=%v):\n%s", p.kind, lastErr, lastOut)
}

// seed applies the identical userprio mutation sequence to the pool.
func (p *diffPool) seed(t *testing.T, cmds [][]string) {
	t.Helper()
	for _, c := range cmds {
		// Retry a couple times: a set can race NegotiatorAd propagation.
		var out string
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			out, err = p.userprio(t, c...)
			if err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if err != nil {
			dumpLog(t, filepath.Join(p.logs, "NegotiatorLog"))
			t.Fatalf("[%s] seed %v failed: %v\n%s", p.kind, c, err, out)
		}
	}
}

// readState reads the current accountant state via condor_userprio -l -modular.
func (p *diffPool) readState(t *testing.T) map[string]negtest.SubmitterPrio {
	t.Helper()
	out, _ := p.userprio(t, "-l", "-modular", "-allusers")
	st, err := negtest.ParseUserprioModular(out)
	if err != nil {
		dumpLog(t, filepath.Join(p.logs, "NegotiatorLog"))
		t.Fatalf("[%s] parsing userprio state: %v\n%s", p.kind, err, out)
	}
	return st
}

// Tolerance for the accountant comparison. These attributes round-trip through
// ClassAd text exactly (e.g. 40.0, 123.5), so the tolerance only needs to
// cover representation; we use a small absolute floor plus a tiny relative
// term for large values (e.g. 1e7-scale remote factors).
const (
	diffAbsTol = 1e-3
	diffRelTol = 1e-6
)

// TestAccountantDifferential is the must-deliver: the accountant math + the
// userprio wire protocol, compared between the C++ and Go negotiators.
func TestAccountantDifferential(t *testing.T) {
	requireCondor(t)
	goBin := buildGoNegotiatorBin(t)

	t.Run("exact_state", func(t *testing.T) {
		// Freeze decay so the comparison is deterministic: with the automatic
		// cycle interval far in the future, no UpdatePriorities tick runs during
		// the test, so seeded state is read back untouched (and no idle-record
		// GC fires). PRIORITY_HALFLIFE is pinned huge as belt-and-suspenders.
		frozen := `
NEGOTIATOR_INTERVAL = 3600
NEGOTIATOR_MIN_INTERVAL = 3600
NEGOTIATOR_CYCLE_DELAY = 3600
PRIORITY_HALFLIFE = 1000000000000
`
		// The identical mutation sequence driven into both negotiators. It
		// exercises: default-factor init (carol: real 0.5 x default 1000 = 500),
		// explicit SET_PRIORITYFACTOR (bob: 0.5 x 2000 = 1000), SET_PRIORITY +
		// SET_PRIORITYFACTOR + SET_ACCUMUSAGE (alice: 4 x 10 = 40, WAU 123.5),
		// and SET_BEGINTIME/SET_LASTTIME (dave: 2 x 5 = 10, times seeded).
		seedCmds := [][]string{
			{"-setprio", "alice@differ.test", "4.0"},
			{"-setfactor", "alice@differ.test", "10.0"},
			{"-setaccum", "alice@differ.test", "123.5"},
			{"-setfactor", "bob@differ.test", "2000.0"},
			{"-setprio", "carol@differ.test", "0.5"},
			{"-setprio", "dave@differ.test", "2.0"},
			{"-setfactor", "dave@differ.test", "5.0"},
			{"-setbegin", "dave@differ.test", "1700000000"},
			{"-setlast", "dave@differ.test", "1700000900"},
		}

		cppState, goState := runPairParallel(t, goBin, func(t *testing.T, goBin string) map[string]negtest.SubmitterPrio {
			return runSeedRead(t, goBin, frozen, seedCmds)
		})

		// 1) Differential: C++ vs Go agree on every seeded submitter.
		diffs := negtest.DiffPrioStates(cppState, goState, diffAbsTol, diffRelTol)
		if len(diffs) != 0 {
			var b strings.Builder
			for _, d := range diffs {
				fmt.Fprintf(&b, "  %s\n", d)
			}
			t.Fatalf("C++ vs Go accountant state diverged:\n%sC++=%v\nGo=%v", b.String(), dump(cppState), dump(goState))
		}
		t.Logf("C++ and Go accountant state agree on all %d submitters (abs=%g rel=%g)", len(negtest.Submitters(cppState)), diffAbsTol, diffRelTol)

		// 2) Known-value assertions (catch a case where both are identically
		// wrong): effective priority = real x factor, default factor init.
		want := map[string]struct{ eff, factor, wau float64 }{
			"alice@differ.test": {40, 10, 123.5},
			"bob@differ.test":   {1000, 2000, 0},
			"carol@differ.test": {500, 1000, 0}, // default-factor init: 0.5 x 1000
			"dave@differ.test":  {10, 5, 0},
		}
		for _, st := range []struct {
			name  string
			state map[string]negtest.SubmitterPrio
		}{{"cpp", cppState}, {"go", goState}} {
			for name, w := range want {
				sp, ok := st.state[name]
				if !ok {
					t.Errorf("[%s] submitter %s missing", st.name, name)
					continue
				}
				if !negtest.FloatClose(sp.EffectivePriority, w.eff, diffAbsTol, diffRelTol) {
					t.Errorf("[%s] %s effective priority = %g, want %g", st.name, name, sp.EffectivePriority, w.eff)
				}
				if !negtest.FloatClose(sp.PriorityFactor, w.factor, diffAbsTol, diffRelTol) {
					t.Errorf("[%s] %s priority factor = %g, want %g", st.name, name, sp.PriorityFactor, w.factor)
				}
				if !negtest.FloatClose(sp.WeightedAccumulatedUsage, w.wau, diffAbsTol, diffRelTol) {
					t.Errorf("[%s] %s WeightedAccumulatedUsage = %g, want %g", st.name, name, sp.WeightedAccumulatedUsage, w.wau)
				}
			}
		}
	})

	t.Run("decay", func(t *testing.T) {
		// Live cross-daemon decay timing is genuinely non-deterministic: each
		// negotiator decays real priority on its own cycle boundaries against
		// its own internal LastUpdateTime baseline, so the two never share an
		// elapsed-time reference. We therefore compare at the DECAY LIMIT, which
		// IS deterministic: with no usage, real priority decays geometrically
		// toward the MinPriority floor (0.5) every cycle. After enough cycles
		// both negotiators converge to exactly the floor, so effective priority
		// converges to 0.5 x factor on both regardless of timing skew. An
		// explicit factor is set so the decayed-to-floor record is not GC'd.
		fast := `
NEGOTIATOR_INTERVAL = 2
NEGOTIATOR_MIN_INTERVAL = 1
NEGOTIATOR_CYCLE_DELAY = 1
PRIORITY_HALFLIFE = 2
`
		// erin starts far above the floor (100 x 5 = 500 effective); after many
		// 2s half-lives it must decay to the floor (0.5 x 5 = 2.5).
		seedCmds := [][]string{
			{"-setfactor", "erin@differ.test", "5.0"},
			{"-setprio", "erin@differ.test", "100.0"},
		}
		const settle = 24 * time.Second // ~12 half-lives: 100/2^12 << 0.5 floor

		cppState, goState := runPairParallel(t, goBin, func(t *testing.T, goBin string) map[string]negtest.SubmitterPrio {
			return runSeedReadDelay(t, goBin, fast, seedCmds, settle)
		})

		const floorEff = 0.5 * 5.0
		// Loose tolerance: both must have converged to the floor.
		const decayTol = 0.05
		for _, st := range []struct {
			name  string
			state map[string]negtest.SubmitterPrio
		}{{"cpp", cppState}, {"go", goState}} {
			sp, ok := st.state["erin@differ.test"]
			if !ok {
				t.Fatalf("[%s] erin missing after decay", st.name)
			}
			if sp.EffectivePriority >= 500 {
				t.Errorf("[%s] erin did not decay: effective priority still %g", st.name, sp.EffectivePriority)
			}
			if !negtest.FloatClose(sp.EffectivePriority, floorEff, decayTol, 0) {
				t.Errorf("[%s] erin effective priority = %g, want floor %g (decay limit)", st.name, sp.EffectivePriority, floorEff)
			}
		}
		// Differential at the limit.
		c := cppState["erin@differ.test"].EffectivePriority
		g := goState["erin@differ.test"].EffectivePriority
		if !negtest.FloatClose(c, g, 2*decayTol, 0) {
			t.Fatalf("C++ vs Go decayed effective priority diverged: cpp=%g go=%g", c, g)
		}
		t.Logf("decay: C++ and Go both converged erin to the floor (cpp=%g go=%g, floor=%g)", c, g, floorEff)
	})
}

// runSeedRead brings up a pool, seeds it, reads its state, and tears it down.
func runSeedRead(t *testing.T, goBin, knobs string, cmds [][]string) map[string]negtest.SubmitterPrio {
	return runSeedReadDelay(t, goBin, knobs, cmds, 0)
}

// runSeedReadDelay is runSeedRead with a settle delay between seeding and
// reading (used by the decay comparison to let the automatic cycles decay).
func runSeedReadDelay(t *testing.T, goBin, knobs string, cmds [][]string, settle time.Duration) map[string]negtest.SubmitterPrio {
	t.Helper()
	p := startDiffPool(t, goBin, knobs)
	defer p.h.Shutdown()
	defer saveLogs(t, p.logs)
	p.waitNegotiatorReady(t, 90*time.Second)
	p.seed(t, cmds)
	if settle > 0 {
		time.Sleep(settle)
	}
	return p.readState(t)
}

// runPairParallel runs the C++ pool (goBin "") and the Go pool concurrently —
// each in its own parallel subtest, so t.Fatalf stays on the correct goroutine —
// and returns both results once both have finished. The per-pool startup, settle
// and negotiation waits overlap, roughly halving the wall time versus running
// the two pools back to back. The enclosing "pools" subtest blocks until both
// parallel children complete, so the results are ready when it returns. Only two
// condor pools are ever live at once (the differential units run serially).
func runPairParallel[T any](t *testing.T, goBin string, run func(t *testing.T, goBin string) T) (cppRes, goRes T) {
	t.Helper()
	t.Run("pools", func(t *testing.T) {
		t.Run("cpp", func(t *testing.T) { t.Parallel(); cppRes = run(t, "") })
		t.Run("go", func(t *testing.T) { t.Parallel(); goRes = run(t, goBin) })
	})
	return cppRes, goRes
}

// ---- Flavor B: fair-share allocation differential (best-effort) ----

// fairShareFactors: submitter fair share is proportional to 1/(realPrio x
// factor). With both at the 0.5 floor, share is proportional to 1/factor, so a
// factor ratio of 1:3 yields a slot ratio of 3:1. faira has the smaller factor
// (better priority) and should win the larger share.
const (
	fairSlots   = 4
	faira       = "faira"
	fairb       = "fairb"
	fairFactorA = 1000.0
	fairFactorB = 3000.0
)

// TestFairShareAllocationDifferential is the best-effort allocation flavor: one
// pool of fairSlots static slots, two submitters at a 1:3 priority-factor ratio,
// each with more idle jobs than slots. After negotiation the slots divide by
// fair share (~3:1). We run the pool once with the C++ negotiator and once with
// the Go negotiator and compare the per-submitter slot split (a robust
// aggregate, not slot identity), asserting the same fair-share proportion on
// both within a +/-1 rounding tolerance.
//
// Scheduling is nondeterministic (claim timing, which jobs are idle when a
// cycle fires); if the aggregate split proves unstable in practice this test is
// written to fail loudly here rather than flake silently, and the blocker is
// documented for the next agent (see the tolerance/robustness notes inline).
func TestFairShareAllocationDifferential(t *testing.T) {
	requireCondor(t)
	for _, tool := range []string{"condor_submit", "condor_q", "condor_config_val"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found in PATH, skipping fair-share differential", tool)
		}
	}
	goBin := buildGoNegotiatorBin(t)

	cppSplit, goSplit := runPairParallel(t, goBin, runFairShare)

	t.Logf("fair-share split: C++ %v  Go %v (slots=%d, factor %g:%g)", cppSplit, goSplit, fairSlots, fairFactorA, fairFactorB)

	// Both negotiators must give faira (better priority) strictly more slots
	// than fairb — the fair-share effect itself.
	for _, s := range []struct {
		kind  string
		split map[string]int
	}{{"cpp", cppSplit}, {"go", goSplit}} {
		if s.split[faira]+s.split[fairb] == 0 {
			t.Fatalf("[%s] no slots were claimed by either submitter; cannot compare allocation", s.kind)
		}
		if s.split[faira] <= s.split[fairb] {
			t.Errorf("[%s] fair share not observed: faira=%d fairb=%d (faira has the better priority factor and should win more slots)",
				s.kind, s.split[faira], s.split[fairb])
		}
	}
	// C++ and Go must agree on the split within +/-1 slot per submitter.
	for _, name := range []string{faira, fairb} {
		if d := cppSplit[name] - goSplit[name]; d < -1 || d > 1 {
			t.Errorf("submitter %s slot split diverged: C++=%d Go=%d (tolerance +/-1)", name, cppSplit[name], goSplit[name])
		}
	}
}

// runFairShare brings up a pool, sets the two submitters' priority factors,
// submits a saturating batch of long jobs for each, waits for the slots to be
// claimed, and returns the per-submitter running-slot count.
func runFairShare(t *testing.T, goBin string) map[string]int {
	t.Helper()
	knobs := fmt.Sprintf(`
# Static slots so slot->submitter accounting is unambiguous.
NUM_CPUS = %d
MEMORY = 4096
SLOT_TYPE_1 = cpus=1 mem=256
NUM_SLOTS_TYPE_1 = %d
SLOT_TYPE_1_PARTITIONABLE = false
# Prompt, repeated negotiation so the pie is divided quickly.
NEGOTIATOR_INTERVAL = 5
NEGOTIATOR_MIN_INTERVAL = 2
NEGOTIATOR_CYCLE_DELAY = 1
# Keep decay from perturbing the equal-real-priority fair share during the run.
PRIORITY_HALFLIFE = 1000000000000
`, fairSlots, fairSlots)

	p := startDiffPool(t, goBin, knobs)
	defer p.h.Shutdown()
	defer saveLogs(t, p.logs)
	p.waitNegotiatorReady(t, 90*time.Second)

	// The submitter identity for a job with accounting_group_user=U is
	// U@<UID_DOMAIN>. Discover the domain so we can set factors up front.
	domain := strings.TrimSpace(condorTool(t, p.cfg, "condor_config_val", "UID_DOMAIN"))
	if domain == "" {
		domain = "differ.test"
	}
	nameA := faira + "@" + domain
	nameB := fairb + "@" + domain

	// Set the priority factors BEFORE any jobs negotiate, so the very first
	// cycle already divides by the intended ratio.
	p.seed(t, [][]string{
		{"-setfactor", nameA, fmt.Sprintf("%g", fairFactorA)},
		{"-setfactor", nameB, fmt.Sprintf("%g", fairFactorB)},
	})

	// A long-running job script (exec system sleep; a transferred shell script
	// keeps macOS SIP happy, matching the e2e test).
	tmp := t.TempDir()
	jobScript := filepath.Join(tmp, "job.sh")
	if err := os.WriteFile(jobScript, []byte("#!/bin/sh\nexec /bin/sleep \"$@\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Saturate: each submitter offers more jobs than there are slots.
	perSubmitter := fairSlots + 2
	for _, who := range []string{faira, fairb} {
		submitFairJobs(t, p.cfg, tmp, jobScript, who, perSubmitter)
	}

	// Poll until the slots are claimed (or a steady split is observed), then
	// return the running-job count per submitter.
	return p.waitFairSplit(t, 150*time.Second)
}

// submitFairJobs submits n long sleep jobs under the given accounting_group_user.
func submitFairJobs(t *testing.T, cfg, tmp, jobScript, who string, n int) {
	t.Helper()
	sub := filepath.Join(tmp, "job_"+who+".sub")
	body := fmt.Sprintf(`universe = vanilla
executable = %s
arguments = 600
accounting_group_user = %s
should_transfer_files = YES
transfer_executable = true
when_to_transfer_output = ON_EXIT
log = %s
output = %s
error = %s
queue %d
`, jobScript, who,
		filepath.Join(tmp, who+".log"),
		filepath.Join(tmp, who+".out"),
		filepath.Join(tmp, who+".err"), n)
	if err := os.WriteFile(sub, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runToolEnv(cfg, 60*time.Second, "condor_submit", sub)
	if err != nil {
		t.Fatalf("condor_submit for %s failed: %v\n%s", who, err, out)
	}
}

// waitFairSplit polls condor_q for the running-job count per submitter until all
// slots are claimed (or the counts hold steady), returning the split.
func (p *diffPool) waitFairSplit(t *testing.T, timeout time.Duration) map[string]int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]int
	stable := 0
	for time.Now().Before(deadline) {
		split := p.runningBySubmitter(t)
		total := split[faira] + split[fairb]
		if total >= fairSlots {
			return split
		}
		if last != nil && split[faira] == last[faira] && split[fairb] == last[fairb] && total > 0 {
			stable++
			// Slots may be fewer than requested if some jobs can't run; accept a
			// split that has held steady across several polls.
			if stable >= 4 {
				return split
			}
		} else {
			stable = 0
		}
		last = split
		time.Sleep(3 * time.Second)
	}
	if last == nil {
		last = map[string]int{}
	}
	t.Logf("[%s] fair-share poll timed out; last split %v", p.kind, last)
	return last
}

// runningBySubmitter returns the count of RUNNING jobs (JobStatus==2) per
// accounting_group_user via condor_q.
func (p *diffPool) runningBySubmitter(t *testing.T) map[string]int {
	t.Helper()
	out := condorTool(t, p.cfg, "condor_q", "-allusers", "-af", "AcctGroupUser", "JobStatus")
	split := map[string]int{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(strings.TrimSpace(line))
		if len(f) != 2 {
			continue
		}
		if f[1] == "2" { // Running
			split[f[0]]++
		}
	}
	return split
}

// condorTool runs a condor tool against the config and returns stdout (fatal on
// error). runToolEnv is the non-fatal variant.
func condorTool(t *testing.T, cfg, name string, args ...string) string {
	t.Helper()
	out, err := runToolEnv(cfg, 30*time.Second, name, args...)
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
	return out
}

func runToolEnv(cfg string, timeout time.Duration, name string, args ...string) (string, error) {
	path, err := exec.LookPath(name)
	if err != nil {
		return "", err
	}
	cmd := exec.Command(path, args...)
	cmd.Env = append(os.Environ(), "CONDOR_CONFIG="+cfg)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// dump renders a state map compactly for failure messages.
func dump(state map[string]negtest.SubmitterPrio) string {
	subs := negtest.Submitters(state)
	names := make([]string, 0, len(subs))
	for n := range subs {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		sp := subs[n]
		fmt.Fprintf(&b, "{%s eff=%g factor=%g wau=%g} ", n, sp.EffectivePriority, sp.PriorityFactor, sp.WeightedAccumulatedUsage)
	}
	return b.String()
}
