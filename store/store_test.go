package store

import (
	"context"
	"iter"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

func mustAd(t *testing.T, s string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return ad
}

// mustQueryAds runs a store query, failing the test on error, and returns the
// result iterator.
func mustQueryAds(t *testing.T, s *Store, at AdType, constraint string) iter.Seq[*classad.ClassAd] {
	t.Helper()
	seq, err := s.Query(context.Background(), at, constraint, 0)
	if err != nil {
		t.Fatalf("Query(%v, %q): %v", at, constraint, err)
	}
	return seq
}

// mustLen returns Len, failing the test on error.
func mustLen(t *testing.T, s *Store, at AdType) int {
	t.Helper()
	n, err := s.Len(context.Background(), at)
	if err != nil {
		t.Fatalf("Len(%v): %v", at, err)
	}
	return n
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
	s.Update(context.Background(), StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@a"; State="Unclaimed"; Cpus=8]`))
	s.Update(context.Background(), StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@b"; State="Claimed"; Cpus=4]`))
	if got := mustLen(t, s, StartdAd); got != 2 {
		t.Fatalf("Len=%d, want 2", got)
	}

	// An update to an existing key replaces, not duplicates.
	s.Update(context.Background(), StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@a"; State="Claimed"; Cpus=8]`))
	if got := mustLen(t, s, StartdAd); got != 2 {
		t.Fatalf("Len after re-update=%d, want 2", got)
	}

	ad, ok := s.Get(context.Background(), StartdAd, mustAd(t, `[Name="slot1@a"]`))
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
	if got := count(mustQueryAds(t, s, StartdAd, `State == "Claimed"`)); got != 2 {
		t.Errorf("State==Claimed matched %d, want 2", got)
	}
	if got := count(mustQueryAds(t, s, StartdAd, `Cpus > 5`)); got != 1 {
		t.Errorf("Cpus>5 matched %d, want 1", got)
	}
	// empty constraint = match all.
	if got := count(mustQueryAds(t, s, StartdAd, "")); got != 2 {
		t.Errorf("Query(\"\") yielded %d, want 2", got)
	}
}

func TestQueryAny(t *testing.T) {
	s := New()
	s.Update(context.Background(), StartdAd, mustAd(t, `[MyType="Machine"; Name="slot1@a"; Cpus=8]`))
	s.Update(context.Background(), ScheddAd, mustAd(t, `[MyType="Scheduler"; Name="sched@a"; MyAddress="<1.2.3.4:5>"]`))
	// ANY spans every public table.
	if got := count(mustQueryAds(t, s, AnyAd, "")); got != 2 {
		t.Errorf("Query(Any) yielded %d, want 2", got)
	}
	if got := count(mustQueryAds(t, s, AnyAd, `MyType == "Scheduler"`)); got != 1 {
		t.Errorf("Query(Any, MyType==Scheduler) yielded %d, want 1", got)
	}
}

func TestInvalidate(t *testing.T) {
	s := New()
	s.Update(context.Background(), StartdAd, mustAd(t, `[Name="slot1@a"; State="Unclaimed"]`))
	s.Update(context.Background(), StartdAd, mustAd(t, `[Name="slot1@b"; State="Claimed"]`))

	// By constraint.
	if got, err := s.Invalidate(context.Background(), StartdAd, `State == "Claimed"`, nil); err != nil {
		t.Fatalf("Invalidate(constraint): %v", err)
	} else if got != 1 {
		t.Fatalf("Invalidate(constraint) removed %d, want 1", got)
	}
	if got := mustLen(t, s, StartdAd); got != 1 {
		t.Fatalf("Len after constraint invalidate=%d, want 1", got)
	}

	// By specific ad (fast path).
	if got, err := s.Invalidate(context.Background(), StartdAd, "", mustAd(t, `[Name="slot1@a"]`)); err != nil {
		t.Fatalf("Invalidate(ad): %v", err)
	} else if got != 1 {
		t.Fatalf("Invalidate(ad) removed %d, want 1", got)
	}
	if got := mustLen(t, s, StartdAd); got != 0 {
		t.Fatalf("Len after ad invalidate=%d, want 0", got)
	}
}

func TestExpire(t *testing.T) {
	s := New()
	now := int64(1000)
	s.now = func() int64 { return now }

	s.Update(context.Background(), ScheddAd, mustAd(t, `[Name="sched@a"; MyAddress="<1.2.3.4:5>"]`))
	// An ad with a short explicit lifetime.
	s.Update(context.Background(), ScheddAd, mustAd(t, `[Name="sched@b"; MyAddress="<1.2.3.4:6>"; ClassAdLifetime=10]`))
	if got := mustLen(t, s, ScheddAd); got != 2 {
		t.Fatalf("Len=%d, want 2", got)
	}

	// After 11s, only the short-lived ad is stale.
	now = 1011
	if got, err := s.Expire(context.Background()); err != nil {
		t.Fatalf("Expire@1011: %v", err)
	} else if got != 1 {
		t.Fatalf("Expire@1011 reaped %d, want 1 (short-lived)", got)
	}
	if got := mustLen(t, s, ScheddAd); got != 1 {
		t.Fatalf("Len after first expire=%d, want 1", got)
	}

	// Just at the default lifetime boundary: not yet stale.
	now = 1000 + DefaultLifetime
	if got, err := s.Expire(context.Background()); err != nil {
		t.Fatalf("Expire at exact lifetime: %v", err)
	} else if got != 0 {
		t.Fatalf("Expire at exact lifetime reaped %d, want 0", got)
	}
	// One second past: reaped.
	now = 1000 + DefaultLifetime + 1
	if got, err := s.Expire(context.Background()); err != nil {
		t.Fatalf("Expire past lifetime: %v", err)
	} else if got != 1 {
		t.Fatalf("Expire past lifetime reaped %d, want 1", got)
	}
	if got := mustLen(t, s, ScheddAd); got != 0 {
		t.Fatalf("Len after final expire=%d, want 0", got)
	}
}
