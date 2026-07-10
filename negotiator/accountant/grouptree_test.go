package accountant

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// ---- helpers ----

func subAd(name string, idle, running int) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("Name", name)
	ad.InsertAttr("IdleJobs", int64(idle))
	ad.InsertAttr("RunningJobs", int64(running))
	return ad
}

func subAdWeighted(name string, idle, running int, widle, wrunning float64) *classad.ClassAd {
	ad := subAd(name, idle, running)
	ad.InsertAttrFloat("WeightedIdleJobs", widle)
	ad.InsertAttrFloat("WeightedRunningJobs", wrunning)
	return ad
}

func zeroUsage(string) float64 { return 0 }

func mapUsage(m map[string]float64) func(string) float64 {
	return func(name string) float64 { return m[name] }
}

func nodeByName(root *negotiator.GroupNode, name string) *negotiator.GroupNode {
	for _, n := range BreadthFirst(root) {
		if n.Name == name {
			return n
		}
	}
	return nil
}

const gEps = 1e-6

func gApprox(t *testing.T, what string, got, want float64) {
	t.Helper()
	if got < want-gEps || got > want+gEps {
		t.Errorf("%s = %g, want %g", what, got, want)
	}
}

// cfgWith builds a DefaultGroupConfig with the given group names and applies fn.
func cfgWith(names []string, fn func(*GroupConfig)) GroupConfig {
	c := DefaultGroupConfig()
	c.GroupNames = names
	if fn != nil {
		fn(&c)
	}
	return c
}

// ---- Tree construction ----

func TestTreeRootAlwaysPresent(t *testing.T) {
	root, _, err := BuildGroupTree(DefaultGroupConfig())
	if err != nil {
		t.Fatal(err)
	}
	if root == nil || root.Name != RootGroupName {
		t.Fatalf("root missing or misnamed: %+v", root)
	}
	if !root.AcceptSurplus {
		t.Errorf("root must accept surplus")
	}
	if len(root.Children) != 0 {
		t.Errorf("empty config should yield childless root, got %d", len(root.Children))
	}
}

func TestTreeNestedPaths(t *testing.T) {
	cfg := cfgWith([]string{"a.b.c", "a", "a.b"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 10, "a.b": 5, "a.b.c": 2}
	})
	root, warns, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(warns) != 0 {
		t.Errorf("unexpected warnings: %v", warns)
	}
	a := nodeByName(root, "a")
	if a == nil || a.Parent != root {
		t.Fatalf("node a not under root")
	}
	ab := nodeByName(root, "a.b")
	if ab == nil || ab.Parent != a {
		t.Fatalf("node a.b not under a")
	}
	abc := nodeByName(root, "a.b.c")
	if abc == nil || abc.Parent != ab {
		t.Fatalf("node a.b.c not under a.b")
	}
	if !abc.StaticQuota || abc.ConfigQuota != 2 {
		t.Errorf("a.b.c quota wrong: static=%v cfg=%g", abc.StaticQuota, abc.ConfigQuota)
	}
}

func TestTreeMissingParentSkipped(t *testing.T) {
	// "a.b" has no parent "a" defined -> skip.
	cfg := cfgWith([]string{"a.b"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a.b": 5}
	})
	root, warns, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Children) != 0 {
		t.Errorf("expected a.b skipped (missing parent), got children %d", len(root.Children))
	}
	if len(warns) == 0 {
		t.Errorf("expected a missing-parent warning")
	}
}

func TestTreeDuplicateSkipped(t *testing.T) {
	cfg := cfgWith([]string{"a", "a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5}
	})
	root, warns, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Children) != 1 {
		t.Errorf("duplicate not skipped: %d children", len(root.Children))
	}
	found := false
	for _, w := range warns {
		if gContains(w, "duplicate") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected duplicate warning, got %v", warns)
	}
}

func TestTreeReservedRootNameIgnored(t *testing.T) {
	cfg := cfgWith([]string{"<none>", "a"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5}
	})
	root, warns, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(root.Children) != 1 {
		t.Errorf("expected only 'a' child, got %d", len(root.Children))
	}
	found := false
	for _, w := range warns {
		if gContains(w, "reserved") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected reserved-name warning")
	}
}

func TestTreeFlagInheritance(t *testing.T) {
	cfg := cfgWith([]string{"a", "b"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"a": 5, "b": 5}
		c.DefaultAcceptSurplus = true
		c.DefaultAutoregroup = true
		// b overrides accept-surplus off.
		c.GroupAcceptSurplus = map[string]bool{"b": false}
	})
	root, _, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	a := nodeByName(root, "a")
	b := nodeByName(root, "b")
	if !a.AcceptSurplus {
		t.Errorf("a should inherit default accept-surplus=true")
	}
	if b.AcceptSurplus {
		t.Errorf("b override to accept-surplus=false failed")
	}
	if !a.Autoregroup || !b.Autoregroup {
		t.Errorf("autoregroup should inherit default true")
	}
	if !root.Autoregroup {
		t.Errorf("root autoregroup should be set from global autoregroup")
	}
}

func TestTreeDynamicVsStatic(t *testing.T) {
	cfg := cfgWith([]string{"s", "d"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"s": 7}
		c.GroupQuotaDynamic = map[string]float64{"d": 0.5}
	})
	root, _, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := nodeByName(root, "s")
	d := nodeByName(root, "d")
	if !s.StaticQuota || s.ConfigQuota != 7 {
		t.Errorf("s should be static 7, got static=%v q=%g", s.StaticQuota, s.ConfigQuota)
	}
	if d.StaticQuota || d.ConfigQuota != 0.5 {
		t.Errorf("d should be dynamic 0.5, got static=%v q=%g", d.StaticQuota, d.ConfigQuota)
	}
}

func TestTreeBadSortExprErrors(t *testing.T) {
	cfg := DefaultGroupConfig()
	cfg.GroupSortExpr = "this is (not valid"
	if _, _, err := BuildGroupTree(cfg); err == nil {
		t.Errorf("expected error on unparseable GROUP_SORT_EXPR")
	}
}

// ---- Submitter -> group mapping ----

func TestGetAssignedGroup(t *testing.T) {
	cfg := cfgWith([]string{"group", "group.sub"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"group": 10, "group.sub": 5}
	})
	root, _, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nm := BuildNameMap(root)

	cases := []struct {
		name string
		want string
	}{
		{"group.sub.user@domain", "group.sub"},
		{"group.user@domain", "group"},
		{"user@domain", RootGroupName},               // no group separator -> root
		{"group@domain", RootGroupName},              // no dot before @ -> root
		{"group", "group"},                           // bare defined group -> itself
		{"group.sub", "group.sub"},                   // bare defined subgroup -> itself
		{"group.unknown.user@domain", "group"},       // unknown subgroup -> deepest matched
		{"totally.bogus.user@domain", RootGroupName}, // unknown top -> root
		{"undefinedbare", RootGroupName},             // unknown bare name -> root
	}
	for _, tc := range cases {
		got := GetAssignedGroup(root, nm, tc.name)
		if got == nil || got.Name != tc.want {
			gotName := "<nil>"
			if got != nil {
				gotName = got.Name
			}
			t.Errorf("GetAssignedGroup(%q) = %q, want %q", tc.name, gotName, tc.want)
		}
	}
}

func TestGetAssignedGroupCaseInsensitive(t *testing.T) {
	cfg := cfgWith([]string{"Group", "Group.Sub"}, func(c *GroupConfig) {
		c.GroupQuota = map[string]float64{"Group": 10, "Group.Sub": 5}
	})
	root, _, err := BuildGroupTree(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nm := BuildNameMap(root)
	got := GetAssignedGroup(root, nm, "group.sub.user@domain")
	if got == nil || got.Name != "Group.Sub" {
		t.Errorf("case-insensitive mapping failed: %v", got)
	}
}

func gContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
