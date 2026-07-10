package accountant

import (
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

const epsAcct = 1e-9

func almost(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > epsAcct {
		t.Errorf("%s: got %.12g, want %.12g (diff %.3g)", what, got, want, got-want)
	}
}

func newMem(t *testing.T, tweak func(*Config)) *Accountant {
	t.Helper()
	cfg := DefaultConfig()
	if tweak != nil {
		tweak(&cfg)
	}
	a, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

func slotAd(name, ip, state, user string, weight float64) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("StartdIpAddr", ip)
	ad.InsertAttrString("State", state)
	if user != "" {
		ad.InsertAttrString("RemoteUser", user)
	}
	ad.InsertAttrFloat("SlotWeight", weight)
	return ad
}

func realPrio(a *Accountant, name string) float64 {
	v, _ := a.store.getFloat(tableCustomer, name, attrPriority)
	return v
}
func wru(a *Accountant, name string) float64 {
	v, _ := a.store.getFloat(tableCustomer, name, attrWeightedResourcesUsed)
	return v
}
func hier(a *Accountant, name string) float64 {
	v, _ := a.store.getFloat(tableCustomer, name, attrHierWeightedResourcesUsed)
	return v
}

// --- Factor selection matrix --------------------------------------------

func TestFactorSelection(t *testing.T) {
	tests := []struct {
		name   string
		tweak  func(*Config)
		stored float64 // <0 means none
		want   float64
	}{
		{"default-local", func(c *Config) {}, -1, 1000},
		{"nice-user", func(c *Config) {}, -1, 1e10},
		{"group-factor", func(c *Config) {
			c.GroupPrioFactor = func(g string) float64 {
				if g == "physics" {
					return 5000
				}
				return 0
			}
		}, -1, 5000},
		{"remote-domain", func(c *Config) { c.LocalDomain = "pool.test" }, -1, 1e7},
		{"local-domain-match", func(c *Config) { c.LocalDomain = "pool.test" }, -1, 1000},
		{"stored-wins", func(c *Config) {}, 2.0, 2.0},
		{"nice-user-beats-remote", func(c *Config) { c.LocalDomain = "pool.test" }, -1, 1e10},
	}
	// submitter name per case
	names := map[string]string{
		"default-local":          "bob@pool.test",
		"nice-user":              "nice-user.bob@pool.test",
		"group-factor":           "physics.bob@pool.test",
		"remote-domain":          "bob@other.test",
		"local-domain-match":     "bob@pool.test",
		"stored-wins":            "bob@pool.test",
		"nice-user-beats-remote": "nice-user.bob@other.test",
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := newMem(t, tc.tweak)
			name := names[tc.name]
			if tc.stored >= 0 {
				if err := a.SetPriorityFactor(name, tc.stored); err != nil {
					t.Fatal(err)
				}
			}
			got := a.GetPriorityFactor(name)
			almost(t, "factor", got, tc.want)
			// Write-on-read: the chosen factor is persisted.
			stored, ok := a.store.getFloat(tableCustomer, name, attrPriorityFactor)
			if !ok {
				t.Fatalf("factor not persisted")
			}
			almost(t, "persisted factor", stored, tc.want)
		})
	}
}

// --- Write-on-read for GetPriority --------------------------------------

func TestWriteOnRead(t *testing.T) {
	a := newMem(t, nil)
	const name = "carol@pool.test"
	got := a.GetPriority(name)
	almost(t, "effective priority", got, MinPriority*1000) // 0.5 * 1000

	// The factor is persisted on read.
	f, ok := a.store.getFloat(tableCustomer, name, attrPriorityFactor)
	if !ok {
		t.Fatalf("priority factor not persisted on read")
	}
	almost(t, "persisted factor", f, 1000)

	// Faithful to C++: the real priority is only *written* when it is below the
	// floor. A fresh submitter's default (== MinPriority) is not < MinPriority,
	// so no Priority attribute is stored; GetPriority still returns 0.5*factor.
	if _, ok := a.store.getFloat(tableCustomer, name, attrPriority); ok {
		t.Errorf("did not expect a stored Priority attr for a fresh submitter")
	}
	// Stable on a second read.
	almost(t, "effective priority (reread)", a.GetPriority(name), 500)

	// A stored sub-floor priority is clamped AND persisted on read.
	a.SetPriority(name, 0.1)
	almost(t, "effective after sub-floor set", a.GetPriority(name), 500)
	rp, _ := a.store.getFloat(tableCustomer, name, attrPriority)
	almost(t, "clamped stored priority", rp, MinPriority)
}

// --- Pure decay golden vectors ------------------------------------------

func TestDecayPureGolden(t *testing.T) {
	cases := []struct {
		name     string
		halfLife float64
		dt       float64
		start    float64
	}{
		{"halflife-step", 100, 100, 100}, // aging 0.5
		{"sub-halflife", 100, 50, 100},   // aging 0.5^0.5
		{"long-halflife", 3600, 300, 64}, // aging 0.5^(300/3600)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newMem(t, func(c *Config) { c.HalfLife = time.Duration(tc.halfLife) * time.Second })
			base := time.Unix(1_000_000, 0)
			a.UpdatePriorities(base) // establish baseline
			a.SetPriority("alice@pool.test", tc.start)

			aging := math.Pow(0.5, tc.dt/tc.halfLife)
			want := tc.start
			now := base
			for i := 0; i < 6; i++ {
				now = now.Add(time.Duration(tc.dt) * time.Second)
				a.UpdatePriorities(now)
				want = want * aging // no usage -> weightedRecentUsage == 0
				if want < MinPriority {
					want = MinPriority
				}
				got := realPrio(a, "alice@pool.test")
				// Once GC removes a floored, factor-less, usage-less record the
				// stored value reads back as 0; stop asserting there.
				if _, exists := a.store.getRecord(tableCustomer, "alice@pool.test"); !exists {
					break
				}
				almost(t, "priority tick", got, want)
			}
		})
	}
}

// --- Decay with usage across an AddMatch/UpdatePriorities boundary -------

func TestUnchargedTimeDance(t *testing.T) {
	const H = 100.0
	a := newMem(t, func(c *Config) { c.HalfLife = time.Duration(H) * time.Second })
	base := time.Unix(2_000_000, 0)
	a.UpdatePriorities(base) // lastUpdate = base

	const user = "alice@pool.test"
	// Match starts 30s into the window, weight 2.
	t1 := base.Add(30 * time.Second)
	a.AddMatch(user, slotAd("slot1@ep1", "<10.0.0.1:9>", "Claimed", user, 2), t1)

	// Pre-charge: UnchargedTime -= (t1-lastUpdate) = -30 ; weighted x2 = -60.
	if ut, _ := a.store.getInt(tableCustomer, user, attrUnchargedTime); ut != -30 {
		t.Errorf("UnchargedTime pre-charge: got %d, want -30", ut)
	}
	almost(t, "WeightedUnchargedTime pre-charge", func() float64 {
		v, _ := a.store.getFloat(tableCustomer, user, attrWeightedUnchargedTime)
		return v
	}(), -60)
	almost(t, "WeightedResourcesUsed", wru(a, user), 2)

	// Close the 100s window.
	t2 := base.Add(100 * time.Second)
	a.UpdatePriorities(t2)

	dt := 100.0
	aging := math.Pow(0.5, dt/H) // 0.5
	weightedRecent := 2.0 + (-60.0)/dt
	wantPrio := MinPriority*aging + weightedRecent*(1-aging)
	almost(t, "priority after settle", realPrio(a, user), wantPrio)
	// Accumulated usage: ResourcesUsed*dt + UnchargedTime = 1*100 + (-30) = 70.
	acc, _ := a.store.getFloat(tableCustomer, user, attrAccumulatedUsage)
	almost(t, "AccumulatedUsage", acc, 70)
	wacc, _ := a.store.getFloat(tableCustomer, user, attrWeightedAccumulatedUsage)
	almost(t, "WeightedAccumulatedUsage", wacc, 2*100+(-60))
	// Uncharged buckets zeroed.
	if ut, _ := a.store.getInt(tableCustomer, user, attrUnchargedTime); ut != 0 {
		t.Errorf("UnchargedTime not zeroed: %d", ut)
	}

	// Now remove the match 50s past the last update; StartTime clamps up to
	// LastUpdateTime (t2), so charged = 50.
	t3 := t2.Add(50 * time.Second)
	a.RemoveMatch("slot1@ep1@<10.0.0.1:9>", t3)
	ut, _ := a.store.getInt(tableCustomer, user, attrUnchargedTime)
	if ut != 50 {
		t.Errorf("UnchargedTime settle: got %d, want 50", ut)
	}
	almost(t, "WeightedUnchargedTime settle", func() float64 {
		v, _ := a.store.getFloat(tableCustomer, user, attrWeightedUnchargedTime)
		return v
	}(), 100)
	almost(t, "WeightedResourcesUsed after remove", wru(a, user), 0)
	if _, ok := a.store.getRecord(tableResource, "slot1@ep1@<10.0.0.1:9>"); ok {
		t.Errorf("resource record should be deleted after RemoveMatch")
	}
}

// --- CheckMatches rebuild -----------------------------------------------

func TestCheckMatchesRebuild(t *testing.T) {
	a := newMem(t, nil)
	base := time.Unix(3_000_000, 0)
	a.UpdatePriorities(base)

	slots := []*classad.ClassAd{
		slotAd("s1@ep1", "<1:9>", "Claimed", "alice@pool.test", 2),
		slotAd("s2@ep2", "<2:9>", "Claimed", "alice@pool.test", 2),
		slotAd("s3@ep3", "<3:9>", "Claimed", "bob@pool.test", 1),
		slotAd("s4@ep4", "<4:9>", "Unclaimed", "", 1),
	}
	a.CheckMatches(slots, base)
	almost(t, "alice weighted", wru(a, "alice@pool.test"), 4)
	almost(t, "bob weighted", wru(a, "bob@pool.test"), 1)

	// alice's second slot becomes unclaimed; rebuild must reflect it and reap
	// the stale Resource record.
	slots[1] = slotAd("s2@ep2", "<2:9>", "Unclaimed", "", 2)
	a.CheckMatches(slots, base)
	almost(t, "alice weighted after drop", wru(a, "alice@pool.test"), 2)
	almost(t, "bob weighted unchanged", wru(a, "bob@pool.test"), 1)
	if _, ok := a.store.getRecord(tableResource, "s2@ep2@<2:9>"); ok {
		t.Errorf("stale resource record s2 not reaped")
	}
	if _, ok := a.store.getRecord(tableResource, "s1@ep1@<1:9>"); !ok {
		t.Errorf("live resource record s1 should remain")
	}
}

// --- Group rollup + HierWeightedResourcesUsed ancestor chain -------------

func TestGroupRollupHierChain(t *testing.T) {
	a := newMem(t, nil)
	base := time.Unix(4_000_000, 0)
	a.UpdatePriorities(base)

	const sub = "group_a.b.alice@pool.test"
	if g := AssignedGroupName(sub); g != "group_a.b" {
		t.Fatalf("AssignedGroupName = %q, want group_a.b", g)
	}
	a.AddMatch(sub, slotAd("s1@e1", "<1:9>", "Claimed", sub, 3), base)
	a.AddMatch(sub, slotAd("s2@e2", "<2:9>", "Claimed", sub, 2), base)

	almost(t, "submitter WRU", wru(a, sub), 5)
	almost(t, "group WRU", wru(a, "group_a.b"), 5)
	almost(t, "hier group_a.b", hier(a, "group_a.b"), 5)
	almost(t, "hier group_a", hier(a, "group_a"), 5)

	a.RemoveMatch("s1@e1@<1:9>", base) // weight 3
	almost(t, "submitter WRU after remove", wru(a, sub), 2)
	almost(t, "group WRU after remove", wru(a, "group_a.b"), 2)
	almost(t, "hier group_a.b after remove", hier(a, "group_a.b"), 2)
	almost(t, "hier group_a after remove", hier(a, "group_a"), 2)
}

func TestAncestorChain(t *testing.T) {
	got := ancestorChain("group_a.b.c")
	want := []string{"group_a.b.c", "group_a.b", "group_a"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ancestorChain = %v, want %v", got, want)
	}
	if g := AssignedGroupName("alice@pool.test"); g != RootGroupName {
		t.Errorf("flat submitter maps to %q, want %q", g, RootGroupName)
	}
}

// --- Store round-trip ----------------------------------------------------

func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "acct.log")
	cfg := DefaultConfig()
	cfg.LogFile = path
	cfg.HalfLife = 100 * time.Second

	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Unix(5_000_000, 0)
	a.UpdatePriorities(base)
	a.SetPriorityFactor("alice@pool.test", 25)
	a.SetPriority("alice@pool.test", 3.5)
	a.AddMatch("alice@pool.test", slotAd("s1@e1", "<1:9>", "Claimed", "alice@pool.test", 2), base)
	a.UpdatePriorities(base.Add(100 * time.Second))
	before := dumpAll(a)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	after := dumpAll(b)

	if !reflect.DeepEqual(before, after) {
		t.Errorf("state changed across reopen:\nbefore=%v\nafter =%v", before, after)
	}
	// The reloaded resource record is intact.
	if u, ok := b.store.getString(tableResource, "s1@e1@<1:9>", attrRemoteUser); !ok || u != "alice@pool.test" {
		t.Errorf("resource record lost across reopen: %q,%v", u, ok)
	}
}

func dumpAll(a *Accountant) map[string]map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[string]map[string]any{}
	for tbl := table(0); tbl < numTables; tbl++ {
		a.store.forEach(tbl, func(key string, r *record) bool {
			m := map[string]any{}
			for k, v := range r.attrs {
				m[k] = v
			}
			out[tbl.String()+"."+key] = m
			return true
		})
	}
	return out
}

// --- GC of idle, factor-less, floored records ---------------------------

func TestGCIdleRecord(t *testing.T) {
	a := newMem(t, func(c *Config) { c.HalfLife = 100 * time.Second })
	base := time.Unix(6_000_000, 0)
	a.UpdatePriorities(base)

	// A ghost record with a real priority but no explicit factor and no usage.
	a.SetPriority("ghost@pool.test", 1.0)
	now := base
	purged := false
	for i := 0; i < 5; i++ {
		now = now.Add(100 * time.Second) // aging 0.5 each tick
		a.UpdatePriorities(now)
		if _, ok := a.store.getRecord(tableCustomer, "ghost@pool.test"); !ok {
			purged = true
			break
		}
	}
	if !purged {
		t.Errorf("idle floored factor-less record was not garbage collected")
	}

	// A record with an explicit factor must NOT be GC'd even when floored.
	a.SetPriority("keep@pool.test", 1.0)
	a.SetPriorityFactor("keep@pool.test", 1000)
	for i := 0; i < 5; i++ {
		now = now.Add(100 * time.Second)
		a.UpdatePriorities(now)
	}
	if _, ok := a.store.getRecord(tableCustomer, "keep@pool.test"); !ok {
		t.Errorf("record with explicit factor was wrongly GC'd")
	}
}

// --- ReportState attribute exactness + mutators --------------------------

func TestReportState(t *testing.T) {
	a := newMem(t, nil)
	base := time.Unix(7_000_000, 0)
	a.UpdatePriorities(base)

	const alice = "alice@pool.test"
	a.SetPriority(alice, 4.0)
	a.SetPriorityFactor(alice, 10.0)
	// Ceiling/Floor have no public setter in the frozen interface; write them
	// through the store directly for this rendering check.
	a.store.setInt(tableCustomer, alice, attrCeiling, 8)
	a.store.setInt(tableCustomer, alice, attrFloor, 2)

	ad := a.ReportState(false)

	// Root group is entry 1 (groups first, breadth-first).
	if name, _ := classad.GetAs[string](ad, "Name1"); name != RootGroupName {
		t.Errorf("Name1 = %q, want %q", name, RootGroupName)
	}
	if g, _ := classad.GetAs[bool](ad, "IsAccountingGroup1"); !g {
		t.Errorf("IsAccountingGroup1 should be true")
	}
	// alice is entry 2.
	if name, _ := classad.GetAs[string](ad, "Name2"); name != alice {
		t.Errorf("Name2 = %q, want %q", name, alice)
	}
	if g, _ := classad.GetAs[bool](ad, "IsAccountingGroup2"); g {
		t.Errorf("IsAccountingGroup2 should be false")
	}
	if ag, _ := classad.GetAs[string](ad, "AccountingGroup2"); ag != RootGroupName {
		t.Errorf("AccountingGroup2 = %q, want %q", ag, RootGroupName)
	}
	// Priority2 is the EFFECTIVE priority: real(4.0) x factor(10) = 40.
	if p, _ := classad.GetAs[float64](ad, "Priority2"); math.Abs(p-40) > epsAcct {
		t.Errorf("Priority2 = %v, want 40", p)
	}
	if pf, _ := classad.GetAs[float64](ad, "PriorityFactor2"); math.Abs(pf-10) > epsAcct {
		t.Errorf("PriorityFactor2 = %v, want 10", pf)
	}
	if c, _ := classad.GetAs[int64](ad, "Ceiling2"); c != 8 {
		t.Errorf("Ceiling2 = %v, want 8", c)
	}
	if f, _ := classad.GetAs[int64](ad, "Floor2"); f != 2 {
		t.Errorf("Floor2 = %v, want 2", f)
	}
	if n, _ := classad.GetAs[int64](ad, "NumSubmittors"); n != 2 {
		t.Errorf("NumSubmittors = %v, want 2", n)
	}
	if _, ok := classad.GetAs[int64](ad, "LastUpdate"); !ok {
		t.Errorf("LastUpdate missing")
	}
}

func TestMutators(t *testing.T) {
	a := newMem(t, nil)
	const u = "dave@pool.test"

	// SetPriorityFactor clamps below the minimum.
	if err := a.SetPriorityFactor(u, 0.2); err != nil {
		t.Fatal(err)
	}
	almost(t, "clamped factor", a.GetPriorityFactor(u), minPriorityFactor)

	a.SetPriority(u, 7.0)
	almost(t, "real priority", realPrio(a, u), 7.0)

	a.SetAccumUsage(u, 123.0) // sets WeightedAccumulatedUsage (C++ semantics)
	v, _ := a.store.getFloat(tableCustomer, u, attrWeightedAccumulatedUsage)
	almost(t, "accum usage", v, 123)

	bt := time.Unix(111, 0)
	a.SetBeginTime(u, bt)
	if got, _ := a.store.getInt(tableCustomer, u, attrBeginUsageTime); got != 111 {
		t.Errorf("BeginUsageTime = %d, want 111", got)
	}
	lt := time.Unix(222, 0)
	a.SetLastTime(u, lt)
	if got, _ := a.store.getInt(tableCustomer, u, attrLastUsageTime); got != 222 {
		t.Errorf("LastUsageTime = %d, want 222", got)
	}

	a.store.setFloat(tableCustomer, u, attrAccumulatedUsage, 99)
	a.ResetUsage(u)
	if got, _ := a.store.getFloat(tableCustomer, u, attrAccumulatedUsage); got != 0 {
		t.Errorf("AccumulatedUsage after reset = %v, want 0", got)
	}

	if err := a.DeleteRecord(u); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.store.getRecord(tableCustomer, u); ok {
		t.Errorf("record not deleted")
	}

	// Empty name is rejected.
	if err := a.SetPriority("", 1); err == nil {
		t.Errorf("expected error for empty submitter")
	}
}

func TestResetAllUsage(t *testing.T) {
	a := newMem(t, nil)
	a.store.setFloat(tableCustomer, "x@d", attrAccumulatedUsage, 10)
	a.store.setFloat(tableCustomer, "y@d", attrWeightedAccumulatedUsage, 20)
	if err := a.ResetAllUsage(); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"x@d", "y@d"} {
		if v, _ := a.store.getFloat(tableCustomer, n, attrAccumulatedUsage); v != 0 {
			t.Errorf("%s accum not reset", n)
		}
		if v, _ := a.store.getFloat(tableCustomer, n, attrWeightedAccumulatedUsage); v != 0 {
			t.Errorf("%s weighted accum not reset", n)
		}
	}
}

func TestSlotWeightDisabled(t *testing.T) {
	a := newMem(t, func(c *Config) { c.UseSlotWeights = false })
	base := time.Unix(8_000_000, 0)
	a.UpdatePriorities(base)
	// SlotWeight of 4 must be ignored (weight 1.0) when weights are disabled.
	a.AddMatch("eve@pool.test", slotAd("s@e", "<9:9>", "Claimed", "eve@pool.test", 4), base)
	almost(t, "weighted usage disabled", wru(a, "eve@pool.test"), 1)
}
