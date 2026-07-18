package store

import (
	"testing"
)

// TestDBBackendPersistence proves the payoff of the database backend: ads written
// by one collector process survive a restart, so the pool comes back warm instead
// of empty (as the in-memory backend would).
func TestDBBackendPersistence(t *testing.T) {
	dir := t.TempDir()

	b1, err := NewDBBackend(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b1.UpdateOldText(StartdAd, `Name = "slot1@a"`+"\n"+`State = "Claimed"`+"\n"+`Cpus = 8`); err != nil {
		t.Fatal(err)
	}
	if err := b1.UpdatePvt(`Name = "slot1@a"`, `Capability = "claim-xyz"`); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the same directory: the ads (public and private) are still there.
	b2, err := NewDBBackend(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b2.Close() }()

	if n, _ := b2.Len(StartdAd); n != 1 {
		t.Fatalf("after restart StartdAd len = %d, want 1", n)
	}
	if n, _ := b2.Len(StartdPvtAd); n != 1 {
		t.Fatalf("after restart StartdPvtAd len = %d, want 1", n)
	}
	ad, ok := b2.Get(StartdAd, mustParse(t, `Name = "slot1@a"`))
	if !ok {
		t.Fatal("ad missing after restart")
	}
	if c, _ := ad.EvaluateAttrInt("Cpus"); c != 8 {
		t.Fatalf("Cpus = %d after restart, want 8", c)
	}
	pvt, ok := b2.Get(StartdPvtAd, mustParse(t, `Name = "slot1@a"`))
	if !ok {
		t.Fatal("private ad missing after restart")
	}
	if cap, _ := pvt.EvaluateAttrString("Capability"); cap != "claim-xyz" {
		t.Fatalf("private Capability = %q after restart", cap)
	}
}

// TestDBBackendStartupExpiry checks the startup-sweep story: ads that went stale
// while the collector was down are pruned by an Expire() on the reopened backend,
// so a long outage does not resurrect dead ads (a short one keeps them warm).
func TestDBBackendStartupExpiry(t *testing.T) {
	dir := t.TempDir()

	b1, err := NewDBBackend(dir)
	if err != nil {
		t.Fatal(err)
	}
	b1.now = func() int64 { return 1000 } // stamp LastHeardFrom = 1000
	if err := b1.UpdateOldText(StartdAd, `Name = "slot1@a"`+"\n"+`ClassAdLifetime = 300`); err != nil {
		t.Fatal(err)
	}
	if err := b1.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := NewDBBackend(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b2.Close() }()

	// Reopen well after the ad's lifetime elapsed (as if the collector was down a
	// long time): the startup sweep prunes it.
	b2.now = func() int64 { return 1000 + 301 }
	n, err := b2.Expire()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("startup expiry removed %d, want 1 (the stale ad)", n)
	}
	if got := countAds(t, b2, StartdAd, ""); got != 0 {
		t.Fatalf("after startup expiry: %d ads, want 0", got)
	}
}
