package store

import (
	"context"
	"net"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// backendFactory builds a fresh Backend and a hook to set its clock (so the
// expiry sweep is testable deterministically).
type backendFactory func(t *testing.T) (b Backend, setNow func(int64))

// backends enumerates every store.Backend implementation; each must pass the same
// conformance suite, so a database-backed collector behaves like an in-memory one.
var backends = map[string]backendFactory{
	"memory": func(t *testing.T) (Backend, func(int64)) {
		s := New()
		return s, func(n int64) { s.now = func() int64 { return n } }
	},
	"db": func(t *testing.T) (Backend, func(int64)) {
		b, err := NewDBBackend(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Close() })
		return b, func(n int64) { b.now = func() int64 { return n } }
	},
	// The remote backend, wired to an in-process dbrpc server over an in-memory
	// pipe (the CEDAR transport is exercised separately). Proves the Backend <->
	// dbrpc mapping behaves like the others.
	"rpc": func(t *testing.T) (Backend, func(int64)) {
		cat, err := db.OpenCatalog(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		srv := dbrpc.NewServerCatalog(cat)
		// The collector connects to its database at a privileged level: it needs the
		// full ad (including private attributes like claim capabilities) and applies
		// per-client redaction itself. Serve with IncludePrivate to match.
		dial := func(context.Context) (dbrpc.MsgConn, error) {
			sc, cc := net.Pipe()
			go func() { _ = srv.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{IncludePrivate: true}) }()
			return dbrpc.NewStreamConn(cc), nil
		}
		b := NewRPCBackend(context.Background(), dial)
		t.Cleanup(func() { _ = b.Close(); srv.Close(); _ = cat.Close() })
		return b, func(n int64) { b.now = func() int64 { return n } }
	},
}

func mustParse(t *testing.T, text string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.ParseOld(text)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return ad
}

func countAds(t *testing.T, b Backend, at AdType, constraint string) int {
	t.Helper()
	seq, err := b.Query(at, constraint, 0)
	if err != nil {
		t.Fatalf("query %s %q: %v", at, constraint, err)
	}
	n := 0
	for range seq {
		n++
	}
	return n
}

func TestBackendConformance(t *testing.T) {
	for name, factory := range backends {
		t.Run(name, func(t *testing.T) {
			t.Run("UpdateQueryGetLen", func(t *testing.T) { testUpdateQueryGetLen(t, factory) })
			t.Run("Invalidate", func(t *testing.T) { testInvalidate(t, factory) })
			t.Run("UpdatePvt", func(t *testing.T) { testUpdatePvt(t, factory) })
			t.Run("Expire", func(t *testing.T) { testExpire(t, factory) })
		})
	}
}

func testUpdateQueryGetLen(t *testing.T, factory backendFactory) {
	b, _ := factory(t)

	for _, ad := range []string{
		`Name = "slot1@a"` + "\n" + `State = "Unclaimed"` + "\n" + `Cpus = 4`,
		`Name = "slot2@a"` + "\n" + `State = "Claimed"` + "\n" + `Cpus = 8`,
	} {
		if err := b.UpdateOldText(StartdAd, ad); err != nil {
			t.Fatal(err)
		}
	}

	if n, _ := b.Len(StartdAd); n != 2 {
		t.Fatalf("Len = %d, want 2", n)
	}
	if got := countAds(t, b, StartdAd, ""); got != 2 {
		t.Fatalf("query-all = %d, want 2", got)
	}
	if got := countAds(t, b, StartdAd, `State == "Claimed"`); got != 1 {
		t.Fatalf("query Claimed = %d, want 1", got)
	}
	if got := countAds(t, b, StartdAd, `Cpus > 5`); got != 1 {
		t.Fatalf("query Cpus>5 = %d, want 1", got)
	}
	// AnyAd spans public tables.
	if err := b.Update(ScheddAd, mustParse(t, `Name = "sched@a"`+"\n"+`MyType = "Scheduler"`)); err != nil {
		t.Fatal(err)
	}
	if got := countAds(t, b, AnyAd, ""); got != 3 {
		t.Fatalf("AnyAd query = %d, want 3", got)
	}
	// Get by key.
	if ad, ok := b.Get(StartdAd, mustParse(t, `Name = "slot1@a"`)); !ok {
		t.Fatal("Get slot1@a missing")
	} else if s, _ := ad.EvaluateAttrString("State"); s != "Unclaimed" {
		t.Fatalf("Get slot1@a State = %q, want Unclaimed", s)
	}
}

func testInvalidate(t *testing.T, factory backendFactory) {
	b, _ := factory(t)
	for i, ad := range []string{
		`Name = "slot1@a"` + "\n" + `State = "Unclaimed"`,
		`Name = "slot2@a"` + "\n" + `State = "Claimed"`,
		`Name = "slot3@a"` + "\n" + `State = "Unclaimed"`,
	} {
		if err := b.UpdateOldText(StartdAd, ad); err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
	}

	// By constraint.
	n, err := b.Invalidate(StartdAd, `State == "Unclaimed"`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("invalidate-by-constraint removed %d, want 2", n)
	}
	if got := countAds(t, b, StartdAd, ""); got != 1 {
		t.Fatalf("after constraint invalidate: %d ads, want 1", got)
	}

	// By key.
	n, err = b.Invalidate(StartdAd, "", mustParse(t, `Name = "slot2@a"`))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("invalidate-by-key removed %d, want 1", n)
	}
	if got := countAds(t, b, StartdAd, ""); got != 0 {
		t.Fatalf("after key invalidate: %d ads, want 0", got)
	}
}

func testUpdatePvt(t *testing.T, factory backendFactory) {
	b, _ := factory(t)
	pub := `Name = "slot1@a"` + "\n" + `State = "Claimed"`
	pvt := `Capability = "secret-claim-id"` // Name copied from the public ad
	// The real flow: the public ad is stored on its own, the private ad via
	// UpdatePvt (which stores only the private ad, keyed by the public's key).
	if err := b.UpdateOldText(StartdAd, pub); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdatePvt(pub, pvt); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(StartdAd); n != 1 {
		t.Fatalf("StartdAd len = %d, want 1", n)
	}
	if n, _ := b.Len(StartdPvtAd); n != 1 {
		t.Fatalf("StartdPvtAd len = %d, want 1", n)
	}
	// AnyAd never returns private ads.
	if got := countAds(t, b, AnyAd, ""); got != 1 {
		t.Fatalf("AnyAd returned %d ads, want 1 (private excluded)", got)
	}
	// The private channel is queryable directly and carries the claim id.
	if ad, ok := b.Get(StartdPvtAd, mustParse(t, `Name = "slot1@a"`)); !ok {
		t.Fatal("private ad missing")
	} else if c, _ := ad.EvaluateAttrString("Capability"); c != "secret-claim-id" {
		t.Fatalf("private Capability = %q", c)
	}
	// Invalidating the public ad drops its private shadow too.
	if _, err := b.Invalidate(StartdAd, "", mustParse(t, `Name = "slot1@a"`)); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(StartdPvtAd); n != 0 {
		t.Fatalf("StartdPvtAd len = %d after invalidate, want 0 (shadow removed)", n)
	}
}

func testExpire(t *testing.T, factory backendFactory) {
	b, setNow := factory(t)

	setNow(1000)
	// Two ads stamped at t=1000; one with a short lifetime, one with the default.
	if err := b.UpdateOldText(StartdAd, `Name = "short@a"`+"\n"+`ClassAdLifetime = 60`); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdateOldText(StartdAd, `Name = "default@a"`); err != nil { // default lifetime 900
		t.Fatal(err)
	}

	// Advance past the short lifetime but not the default: only the short one expires.
	setNow(1000 + 61)
	n, err := b.Expire()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("first expire removed %d, want 1 (the short-lived ad)", n)
	}
	if _, ok := b.Get(StartdAd, mustParse(t, `Name = "default@a"`)); !ok {
		t.Fatal("default-lifetime ad should survive the first sweep")
	}

	// Advance past the default lifetime: the second ad expires.
	setNow(1000 + 901)
	n, err = b.Expire()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("second expire removed %d, want 1", n)
	}
	if got := countAds(t, b, StartdAd, ""); got != 0 {
		t.Fatalf("after expiry: %d ads remain, want 0", got)
	}
}
