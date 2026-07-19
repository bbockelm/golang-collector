// End-to-end tests for the Negotiator daemon object: the userprio command
// handlers over an in-process cedar server, RESCHEDULE firing an early cycle,
// and the collector-embedding smoke test (negotiator + collector protocol on
// ONE shared command server, matches delivered to a loopback schedd).
package negotiator_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"

	collector "github.com/bbockelm/golang-collector"
	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/negotiator/accountant"
	"github.com/bbockelm/golang-collector/negotiator/cycle"
	"github.com/bbockelm/golang-collector/negotiator/negtest"
	"github.com/bbockelm/golang-collector/negotiator/protocol"
	"github.com/bbockelm/golang-collector/negotiator/source"
	"github.com/bbockelm/golang-collector/store"
)

// testVersion is advertised by test clients so the SET_PRIORITYFACTOR handler
// takes the >= 8.9.9 reply-ad path.
const testVersion = "$CondorVersion: 25.0.0 2026-01-01 BuildID: negotiator-test $"

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func allowAll(string, string, string) bool { return true }

// readOnly authorizes only READ — the authz-denial fixture.
func readOnly(perm, _, _ string) bool { return perm == "READ" }

// stubCycle is a negotiator.Cycle recording each Run.
type stubCycle struct{ runs chan time.Time }

func newStubCycle() *stubCycle { return &stubCycle{runs: make(chan time.Time, 64)} }

func (s *stubCycle) Run(ctx context.Context) (*negotiator.CycleStats, error) {
	s.runs <- time.Now()
	now := time.Now()
	return &negotiator.CycleStats{Start: now, End: now, Matches: 1}, nil
}

func (s *stubCycle) wait(t *testing.T, ctx context.Context, what string) time.Time {
	t.Helper()
	select {
	case ts := <-s.runs:
		return ts
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s", what)
		return time.Time{}
	}
}

// newDaemon builds a Negotiator over an empty embedded store, a fresh
// accountant, and a stub cycle, and serves it on its own cedar server.
func newDaemon(t *testing.T, ctx context.Context, authorizer func(string, string, string) bool, cfgMut func(*negotiator.Config)) (addr string, acct *accountant.Accountant, neg *negotiator.Negotiator) {
	t.Helper()
	st := store.New()
	src, err := source.NewEmbedded(st, source.Config{})
	if err != nil {
		t.Fatal(err)
	}
	acct, err = accountant.New(accountant.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = acct.Close() })

	cfg := negotiator.Config{
		Source:         src,
		Accountant:     acct,
		Cycle:          newStubCycle(),
		NegotiatorName: "neg@test",
		Authorizer:     authorizer,
	}
	if cfgMut != nil {
		cfgMut(&cfg)
	}
	neg, err = negotiator.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	srv := cedarserver.New(negtest.ServerSecurity())
	neg.RegisterOn(srv)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ctx, ln) }()
	return ln.Addr().String(), acct, neg
}

// dialCmd opens an authenticated, encrypted session for one command.
func dialCmd(t *testing.T, ctx context.Context, addr string, cmd int) *client.HTCondorClient {
	t.Helper()
	sec := negtest.ClientSecurity()
	sec.Command = cmd
	sec.RemoteVersion = testVersion
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("dialing command %d: %v", cmd, err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// waitClosed reads until the server closes the connection, proving the
// handler for the (reply-less) command has finished.
func waitClosed(ctx context.Context, cl *client.HTCondorClient) {
	rm := message.NewMessageFromStream(cl.GetStream())
	_, _ = rm.GetInt(ctx)
}

// sendSetter sends "string submitter [+ value]" + EOM for a userprio setter
// and waits for the server to finish (setters send no reply; the connection
// close is the completion signal).
func sendSetter(t *testing.T, ctx context.Context, addr string, cmd int, submitter string, put func(*message.Message) error) {
	t.Helper()
	cl := dialCmd(t, ctx, addr, cmd)
	m := message.NewMessageForStream(cl.GetStream())
	if err := m.PutString(ctx, submitter); err != nil {
		t.Fatalf("cmd %d: sending submitter: %v", cmd, err)
	}
	if put != nil {
		if err := put(m); err != nil {
			t.Fatalf("cmd %d: sending value: %v", cmd, err)
		}
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatalf("cmd %d: finishing: %v", cmd, err)
	}
	waitClosed(ctx, cl)
}

// getPriorityAd runs GET_PRIORITY (or GET_PRIORITY_ROLLUP) and parses the
// no-types reply ad the way condor_userprio's getClassAdNoTypes does: an
// expression count followed by that many "Attr = Value" strings, with NO
// MyType/TargetType trailer.
func getPriorityAd(t *testing.T, ctx context.Context, addr string, cmd int) *classad.ClassAd {
	t.Helper()
	cl := dialCmd(t, ctx, addr, cmd)
	m := message.NewMessageForStream(cl.GetStream())
	if err := m.FinishMessage(ctx); err != nil { // the bare-request EOM
		t.Fatalf("GET_PRIORITY: sending EOM: %v", err)
	}
	rm := message.NewMessageFromStream(cl.GetStream())
	n, err := rm.GetInt(ctx)
	if err != nil {
		t.Fatalf("GET_PRIORITY: reading expression count: %v", err)
	}
	var b strings.Builder
	for i := 0; i < n; i++ {
		s, err := rm.GetString(ctx)
		if err != nil {
			t.Fatalf("GET_PRIORITY: reading expression %d/%d: %v", i, n, err)
		}
		b.WriteString(s)
		b.WriteByte('\n')
	}
	ad, err := classad.ParseOld(b.String())
	if err != nil {
		t.Fatalf("GET_PRIORITY: parsing reply ad: %v", err)
	}
	return ad
}

// approx compares within CEDAR's frexp/ldexp double-encoding precision (the
// wire fraction is an int32, so values round-trip to ~1e-9 relative).
func approx(got, want float64) bool {
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	scale := want
	if scale < 0 {
		scale = -scale
	}
	if scale < 1 {
		scale = 1
	}
	return diff <= 1e-6*scale
}

// findEntry locates the numbered ReportState entry for a submitter name.
func findEntry(ad *classad.ClassAd, name string) (int, bool) {
	n, _ := classad.GetAs[int64](ad, "NumSubmittors")
	for i := 1; i <= int(n); i++ {
		if v, ok := ad.EvaluateAttrString(fmt.Sprintf("Name%d", i)); ok && v == name {
			return i, true
		}
	}
	return 0, false
}

// TestUserprioHandlers round-trips the userprio protocol over a live cedar
// server: SET_PRIORITYFACTOR (with the versioned {ErrorCode} reply),
// SET_PRIORITY, SET_CEILING mutate, and GET_PRIORITY/GET_PRIORITY_ROLLUP
// return the ReportState ad reflecting them.
func TestUserprioHandlers(t *testing.T) {
	ctx := testCtx(t)
	addr, acct, _ := newDaemon(t, ctx, allowAll, nil)
	const alice = "alice@pool.test"

	// SET_PRIORITYFACTOR (WRITE): submitter + double, reply {ErrorCode}.
	{
		cl := dialCmd(t, ctx, addr, commands.SET_PRIORITYFACTOR)
		m := message.NewMessageForStream(cl.GetStream())
		if err := m.PutString(ctx, alice); err != nil {
			t.Fatal(err)
		}
		if err := m.PutDouble(ctx, 2000); err != nil {
			t.Fatal(err)
		}
		if err := m.FinishMessage(ctx); err != nil {
			t.Fatal(err)
		}
		reply, err := message.NewMessageFromStream(cl.GetStream()).GetClassAd(ctx)
		if err != nil {
			t.Fatalf("SET_PRIORITYFACTOR: reading reply ad: %v", err)
		}
		if code, ok := classad.GetAs[int64](reply, "ErrorCode"); !ok || code != 0 {
			t.Fatalf("SET_PRIORITYFACTOR reply ErrorCode = %d (ok=%v), want 0; ad: %s", code, ok, reply)
		}
	}

	// SET_PRIORITY (ADMINISTRATOR): submitter + double.
	sendSetter(t, ctx, addr, commands.SET_PRIORITY, alice, func(m *message.Message) error {
		return m.PutDouble(ctx, 4.0)
	})
	// SET_CEILING (ADMINISTRATOR): submitter + int.
	sendSetter(t, ctx, addr, commands.SET_CEILING, alice, func(m *message.Message) error {
		return m.PutInt(ctx, 42)
	})

	if got := acct.GetPriorityFactor(alice); !approx(got, 2000) {
		t.Errorf("accountant factor = %v, want ~2000", got)
	}

	// GET_PRIORITY (READ): the ReportState ad reflects all three mutations.
	ad := getPriorityAd(t, ctx, addr, commands.GET_PRIORITY)
	i, ok := findEntry(ad, alice)
	if !ok {
		t.Fatalf("GET_PRIORITY reply has no entry for %s: %s", alice, ad)
	}
	if v, _ := classad.GetAs[float64](ad, fmt.Sprintf("PriorityFactor%d", i)); !approx(v, 2000) {
		t.Errorf("PriorityFactor%d = %v, want ~2000", i, v)
	}
	// Priority<i> is the EFFECTIVE priority: real (4.0) x factor (2000).
	if v, _ := classad.GetAs[float64](ad, fmt.Sprintf("Priority%d", i)); !approx(v, 8000) {
		t.Errorf("Priority%d = %v, want ~8000", i, v)
	}
	if v, _ := classad.GetAs[int64](ad, fmt.Sprintf("Ceiling%d", i)); v != 42 {
		t.Errorf("Ceiling%d = %v, want 42", i, v)
	}

	// GET_PRIORITY_ROLLUP replies the same wire shape.
	rollup := getPriorityAd(t, ctx, addr, commands.GET_PRIORITY_ROLLUP)
	if _, ok := findEntry(rollup, alice); !ok {
		t.Fatalf("GET_PRIORITY_ROLLUP reply has no entry for %s", alice)
	}

	// RESET_USAGE + SET_ACCUMUSAGE round-trip through their distinct wire
	// shapes (name-only and name+double).
	sendSetter(t, ctx, addr, commands.SET_ACCUMUSAGE, alice, func(m *message.Message) error {
		return m.PutDouble(ctx, 123.5)
	})
	ad = getPriorityAd(t, ctx, addr, commands.GET_PRIORITY)
	i, _ = findEntry(ad, alice)
	if v, _ := classad.GetAs[float64](ad, fmt.Sprintf("WeightedAccumulatedUsage%d", i)); !approx(v, 123.5) {
		t.Errorf("WeightedAccumulatedUsage%d = %v, want ~123.5", i, v)
	}
	sendSetter(t, ctx, addr, commands.RESET_USAGE, alice, nil)
	ad = getPriorityAd(t, ctx, addr, commands.GET_PRIORITY)
	i, _ = findEntry(ad, alice)
	if v, _ := classad.GetAs[float64](ad, fmt.Sprintf("WeightedAccumulatedUsage%d", i)); v != 0 {
		t.Errorf("after RESET_USAGE, WeightedAccumulatedUsage%d = %v, want 0", i, v)
	}
}

// TestUserprioAuthzDenied proves a setter is refused when the peer only holds
// READ: the ADMINISTRATOR-gated SET_CEILING must not mutate, while READ-level
// GET_PRIORITY on the same server keeps working.
func TestUserprioAuthzDenied(t *testing.T) {
	ctx := testCtx(t)
	addr, acct, _ := newDaemon(t, ctx, readOnly, nil)
	const bob = "bob@pool.test"

	cl := dialCmd(t, ctx, addr, commands.SET_CEILING)
	m := message.NewMessageForStream(cl.GetStream())
	if err := m.PutString(ctx, bob); err != nil {
		t.Fatal(err)
	}
	if err := m.PutInt(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatal(err)
	}
	waitClosed(ctx, cl) // server dropped the connection after the denial

	if got := acct.GetCeiling(bob); got != -1 {
		t.Errorf("ceiling after denied SET_CEILING = %v, want -1 (unset)", got)
	}

	// SET_PRIORITYFACTOR at WRITE-but-not-ADMINISTRATOR: refused with the
	// versioned reply ad carrying a non-zero ErrorCode (the C++
	// returnPrioFactor path). READ-only misses WRITE too, but exercise the
	// reply shape with a dedicated WRITE-only authorizer.
	writeOnly := func(perm, _, _ string) bool { return perm == "WRITE" || perm == "READ" }
	addr2, acct2, _ := newDaemon(t, ctx, writeOnly, nil)
	cl2 := dialCmd(t, ctx, addr2, commands.SET_PRIORITYFACTOR)
	m2 := message.NewMessageForStream(cl2.GetStream())
	if err := m2.PutString(ctx, bob); err != nil {
		t.Fatal(err)
	}
	if err := m2.PutDouble(ctx, 50); err != nil {
		t.Fatal(err)
	}
	if err := m2.FinishMessage(ctx); err != nil {
		t.Fatal(err)
	}
	reply, err := message.NewMessageFromStream(cl2.GetStream()).GetClassAd(ctx)
	if err != nil {
		t.Fatalf("reading SET_PRIORITYFACTOR denial reply: %v", err)
	}
	if code, _ := classad.GetAs[int64](reply, "ErrorCode"); code == 0 {
		t.Errorf("denied SET_PRIORITYFACTOR replied ErrorCode 0; ad: %s", reply)
	}
	if got := acct2.GetPriorityFactor(bob); got == 50 {
		t.Error("denied SET_PRIORITYFACTOR mutated the factor")
	}

	// The read path still works against the read-only server.
	if ad := getPriorityAd(t, ctx, addr, commands.GET_PRIORITY); ad == nil {
		t.Fatal("GET_PRIORITY under read-only authorizer failed")
	}
}

// TestRescheduleTriggersEarlyCycle proves RESCHEDULE fires the cycle timer
// early: with a one-hour interval, the second cycle runs only because of the
// wire command (and the guards still space it from the first).
func TestRescheduleTriggersEarlyCycle(t *testing.T) {
	ctx := testCtx(t)
	stub := newStubCycle()
	addr, _, neg := newDaemon(t, ctx, allowAll, func(cfg *negotiator.Config) {
		cfg.Cycle = stub
		cfg.Interval = time.Hour // never fires within the test
		cfg.CycleDelay = 10 * time.Millisecond
		cfg.MinInterval = 10 * time.Millisecond
		cfg.UpdateInterval = time.Hour
	})

	// StartBackground runs the first cycle immediately (the C++
	// Register_Timer(0, NegotiatorInterval)).
	stop := neg.StartBackground(ctx)
	defer stop()

	first := stub.wait(t, ctx, "the immediate first cycle")

	// RESCHEDULE over the wire.
	cl := dialCmd(t, ctx, addr, commands.RESCHEDULE)
	m := message.NewMessageForStream(cl.GetStream())
	if err := m.FinishMessage(ctx); err != nil { // bare command + EOM
		t.Fatal(err)
	}

	second := stub.wait(t, ctx, "the RESCHEDULE-triggered cycle")
	if gap := second.Sub(first); gap > 30*time.Second {
		t.Errorf("second cycle came %v after the first; RESCHEDULE did not fire it early", gap)
	}

	// No third cycle without another RESCHEDULE (interval is an hour).
	select {
	case ts := <-stub.runs:
		t.Errorf("unexpected third cycle at %v", ts)
	case <-time.After(300 * time.Millisecond):
	}
}

// ---------------------------------------------------------------------------
// Embedded smoke test: collector + negotiator on one command server.
// ---------------------------------------------------------------------------

func machineAd(t *testing.T, name string, cpus int64) *classad.ClassAd {
	t.Helper()
	addr := "<192.168.7.1:9618?slot=" + name + ">"
	ad := classad.New()
	ad.InsertAttrString("MyType", "Machine")
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("MyAddress", addr)
	ad.InsertAttrString("StartdIpAddr", addr)
	ad.InsertAttrString("State", "Unclaimed")
	ad.InsertAttrString("Activity", "Idle")
	ad.InsertAttr("Cpus", cpus)
	ad.InsertAttr("Memory", 4096)
	ad.InsertAttr("Disk", 1<<22)
	req, err := classad.ParseExpr("TARGET.RequestCpus <= MY.Cpus && TARGET.RequestMemory <= MY.Memory")
	if err != nil {
		t.Fatal(err)
	}
	ad.InsertExpr("Requirements", req)
	return ad
}

func pvtAd(name, claim string) *classad.ClassAd {
	addr := "<192.168.7.1:9618?slot=" + name + ">"
	ad := classad.New()
	ad.InsertAttrString("MyType", "Machine")
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("MyAddress", addr)
	ad.InsertAttrString("ClaimId", claim)
	return ad
}

func submitterAd(name, scheddName, scheddAddr string, idle int64) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("MyType", "Submitter")
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("ScheddName", scheddName)
	ad.InsertAttrString("ScheddIpAddr", scheddAddr)
	ad.InsertAttr("IdleJobs", idle)
	ad.InsertAttr("RunningJobs", 0)
	ad.InsertAttrString("SubmitterTag", "")
	return ad
}

func jobAd(t *testing.T, requestCpus int64) *classad.ClassAd {
	t.Helper()
	ad := classad.New()
	ad.InsertAttr("RequestCpus", requestCpus)
	ad.InsertAttr("RequestMemory", 512)
	ad.InsertAttr("RequestDisk", 1024)
	req, err := classad.ParseExpr("TARGET.Cpus >= MY.RequestCpus && TARGET.Memory >= MY.RequestMemory")
	if err != nil {
		t.Fatal(err)
	}
	ad.InsertExpr("Requirements", req)
	return ad
}

// TestEmbeddedNegotiatorSmoke embeds the negotiator on the COLLECTOR's own
// command server (the NEGOTIATOR_EMBEDDED wiring, minus the config plumbing):
// the store is seeded with machine + private + submitter ads, StartBackground
// runs cycles on a tiny interval, and a loopback schedd receives the match
// with the claim id from the private ad. The collector's own query handlers
// keep working on the very same server, and the negotiator's ad shows up in
// the collector's store.
func TestEmbeddedNegotiatorSmoke(t *testing.T) {
	ctx := testCtx(t)

	// Loopback schedd offering one single-job request group in round 0.
	sched, err := negtest.Start(ctx, [][]negtest.Group{{
		{RepCluster: 1, RepProc: 0, AutoClusterID: 1,
			Members: []negtest.Job{negtest.J(1, 0)}, RepAd: jobAd(t, 1)},
	}})
	if err != nil {
		t.Fatal(err)
	}

	// The host collector; its server will carry BOTH protocols.
	c, err := collector.New(collector.Config{Security: negtest.ServerSecurity()})
	if err != nil {
		t.Fatal(err)
	}
	st := c.Store()
	negtest.SeedStore(t, st, []*classad.ClassAd{
		machineAd(t, "slot1@ep.test", 4),
		submitterAd("alice@pool.test", "schedd.test", sched.Addr(), 1),
	})
	if err := st.Update(context.Background(), store.StartdPvtAd, pvtAd("slot1@ep.test", "claim-secret-xyz")); err != nil {
		t.Fatal(err)
	}

	src, err := source.NewEmbedded(st, source.Config{})
	if err != nil {
		t.Fatal(err)
	}
	acct, err := accountant.New(accountant.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = acct.Close() })
	acct.UpdatePriorities(time.Now())

	sf := protocol.NewFactory(negtest.ClientSecurity(), protocol.WithNegotiatorName("go-neg@test"))
	t.Cleanup(sf.CloseAll)
	cycCfg := cycle.DefaultConfig()
	cycCfg.NegotiatorName = "go-neg@test"
	cyc, err := cycle.New(src, acct, sf, cycCfg)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	neg, err := negotiator.New(negotiator.Config{
		Source:         src,
		Accountant:     acct,
		Cycle:          cyc,
		NegotiatorName: "go-neg@test",
		AdvertisedAddr: ln.Addr().String(),
		Interval:       300 * time.Millisecond,
		CycleDelay:     30 * time.Millisecond,
		MinInterval:    20 * time.Millisecond,
		UpdateInterval: 50 * time.Millisecond,
		Authorizer:     allowAll,
	})
	if err != nil {
		t.Fatal(err)
	}

	// ONE command server: collector protocol + negotiator protocol.
	srv := c.Server()
	neg.RegisterOn(srv)
	go func() { _ = srv.Serve(ctx, ln) }()
	addr := ln.Addr().String()

	stop := neg.StartBackground(ctx)
	defer stop()

	// The loopback schedd must receive the match, carrying the claim id from
	// the store's private ad.
	if err := sched.WaitRounds(ctx, 1); err != nil {
		t.Fatalf("waiting for the first NEGOTIATE round: %v", err)
	}
	logs := sched.Logs()
	if len(logs) == 0 || len(logs[0].Matches) != 1 {
		t.Fatalf("expected 1 match in round 0, logs: %+v", logs)
	}
	match := logs[0].Matches[0]
	if match.ClaimID != "claim-secret-xyz" {
		t.Errorf("match claim id = %q, want claim-secret-xyz", match.ClaimID)
	}
	if match.SlotName != "slot1@ep.test" {
		t.Errorf("match slot = %q, want slot1@ep.test", match.SlotName)
	}

	// The collector's own query handlers still answer on the SAME server.
	qctx := htcondor.WithSecurityConfig(ctx, negtest.ClientSecurity())
	col := htcondor.NewCollector(addr)
	machines, err := col.QueryAds(qctx, "Machine", "")
	if err != nil {
		t.Fatalf("collector query on the shared server: %v", err)
	}
	if len(machines) != 1 {
		t.Fatalf("machine query returned %d ads, want 1", len(machines))
	}

	// The negotiator publishes its own ad into the collector's store.
	deadline := time.Now().Add(10 * time.Second)
	for {
		negAds, err := col.QueryAds(qctx, "Negotiator", "")
		if err == nil && len(negAds) > 0 {
			if name, _ := negAds[0].EvaluateAttrString("Name"); name != "go-neg@test" {
				t.Errorf("negotiator ad Name = %q, want go-neg@test", name)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("negotiator ad never appeared in the collector store")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// And the userprio surface answers on the shared socket too: the cycle's
	// spin stamped alice's SubmitterShare (D-item 1) into ReportState.
	ad := getPriorityAd(t, ctx, addr, commands.GET_PRIORITY)
	i, ok := findEntry(ad, "alice@pool.test")
	if !ok {
		t.Fatalf("GET_PRIORITY on the shared server has no alice entry: %s", ad)
	}
	if v, _ := classad.GetAs[float64](ad, fmt.Sprintf("SubmitterShare%d", i)); v <= 0 {
		t.Errorf("SubmitterShare%d = %v, want > 0 (spin-1 stamp)", i, v)
	}
}
