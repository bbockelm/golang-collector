package store

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
)

func mustAd(t *testing.T, s string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return ad
}

func mustQuery(t *testing.T, expr string) *vm.Query {
	t.Helper()
	q, err := vm.Parse(expr)
	if err != nil {
		t.Fatalf("compile %q: %v", expr, err)
	}
	return q
}

func count(seq func(func(*classad.ClassAd) bool)) int {
	n := 0
	for range seq {
		n++
	}
	return n
}

func TestUpdateGetQuery(t *testing.T) {
	s := New()
	s.Update(StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@a"; State="Unclaimed"; Cpus=8]`))
	s.Update(StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@b"; State="Claimed"; Cpus=4]`))
	if got := s.Len(StartdAd); got != 2 {
		t.Fatalf("Len=%d, want 2", got)
	}

	// An update to an existing key replaces, not duplicates.
	s.Update(StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@a"; State="Claimed"; Cpus=8]`))
	if got := s.Len(StartdAd); got != 2 {
		t.Fatalf("Len after re-update=%d, want 2", got)
	}

	ad, ok := s.Get(StartdAd, mustAd(t, `[Name="slot1@a"]`))
	if !ok {
		t.Fatal("Get(slot1@a) miss")
	}
	if st, _ := ad.EvaluateAttrString("State"); st != "Claimed" {
		t.Errorf("slot1@a State=%q, want Claimed (the re-update)", st)
	}
	if _, ok := ad.EvaluateAttrInt(attrLastHeardFrom); !ok {
		t.Error("stored ad missing LastHeardFrom stamp")
	}

	// Constraint queries hit the collection's native matcher.
	if got := count(s.Query(StartdAd, mustQuery(t, `State == "Claimed"`))); got != 2 {
		t.Errorf("State==Claimed matched %d, want 2", got)
	}
	if got := count(s.Query(StartdAd, mustQuery(t, `Cpus > 5`))); got != 1 {
		t.Errorf("Cpus>5 matched %d, want 1", got)
	}
	// nil query = match all.
	if got := count(s.Query(StartdAd, nil)); got != 2 {
		t.Errorf("Query(nil) yielded %d, want 2", got)
	}
}

func TestQueryAny(t *testing.T) {
	s := New()
	s.Update(StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@a"; Cpus=8]`))
	s.Update(ScheddAd, mustAd(t, `[MyType="Scheduler"; Name="sched@a"; MyAddress="<1.2.3.4:5>"]`))
	// ANY spans every public table.
	if got := count(s.Query(AnyAd, nil)); got != 2 {
		t.Errorf("Query(Any) yielded %d, want 2", got)
	}
	if got := count(s.Query(AnyAd, mustQuery(t, `MyType == "Scheduler"`))); got != 1 {
		t.Errorf("Query(Any, MyType==Scheduler) yielded %d, want 1", got)
	}
}

func TestInvalidate(t *testing.T) {
	s := New()
	s.Update(StartdAd, mustAd(t, `[Name="slot1@a"; State="Unclaimed"]`))
	s.Update(StartdAd, mustAd(t, `[Name="slot1@b"; State="Claimed"]`))

	// By constraint.
	if got := s.Invalidate(StartdAd, mustQuery(t, `State == "Claimed"`), nil); got != 1 {
		t.Fatalf("Invalidate(constraint) removed %d, want 1", got)
	}
	if got := s.Len(StartdAd); got != 1 {
		t.Fatalf("Len after constraint invalidate=%d, want 1", got)
	}

	// By specific ad (fast path).
	if got := s.Invalidate(StartdAd, nil, mustAd(t, `[Name="slot1@a"]`)); got != 1 {
		t.Fatalf("Invalidate(ad) removed %d, want 1", got)
	}
	if got := s.Len(StartdAd); got != 0 {
		t.Fatalf("Len after ad invalidate=%d, want 0", got)
	}
}

func TestExpire(t *testing.T) {
	s := New()
	now := int64(1000)
	s.now = func() int64 { return now }

	s.Update(ScheddAd, mustAd(t, `[Name="sched@a"; MyAddress="<1.2.3.4:5>"]`))
	// An ad with a short explicit lifetime.
	s.Update(ScheddAd, mustAd(t, `[Name="sched@b"; MyAddress="<1.2.3.4:6>"; ClassAdLifetime=10]`))
	if s.Len(ScheddAd) != 2 {
		t.Fatalf("Len=%d, want 2", s.Len(ScheddAd))
	}

	// After 11s, only the short-lived ad is stale.
	now = 1011
	if got := s.Expire(); got != 1 {
		t.Fatalf("Expire@1011 reaped %d, want 1 (short-lived)", got)
	}
	if s.Len(ScheddAd) != 1 {
		t.Fatalf("Len after first expire=%d, want 1", s.Len(ScheddAd))
	}

	// Just at the default lifetime boundary: not yet stale.
	now = 1000 + DefaultLifetime
	if got := s.Expire(); got != 0 {
		t.Fatalf("Expire at exact lifetime reaped %d, want 0", got)
	}
	// One second past: reaped.
	now = 1000 + DefaultLifetime + 1
	if got := s.Expire(); got != 1 {
		t.Fatalf("Expire past lifetime reaped %d, want 1", got)
	}
	if s.Len(ScheddAd) != 0 {
		t.Fatalf("Len after final expire=%d, want 0", s.Len(ScheddAd))
	}
}
