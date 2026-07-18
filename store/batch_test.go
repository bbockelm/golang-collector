package store

import (
	"testing"
)

// TestBufferedBackendDedupAndFlush checks the Nagle buffer: repeated updates to
// one ad collapse to the latest, a read flushes first so buffered ads are
// visible, and distinct ads all land.
func TestBufferedBackendDedupAndFlush(t *testing.T) {
	under, err := NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// window 0 -> no background flusher; flush happens on read / size / close.
	b, err := NewBufferedBackend(under, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	// Same ad twice within the buffer -> collapses to the latest value.
	if err := b.UpdateOldText(StartdAd, `Name = "slot1@a"`+"\n"+`State = "Idle"`); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdateOldText(StartdAd, `Name = "slot1@a"`+"\n"+`State = "Claimed"`); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdateOldText(StartdAd, `Name = "slot2@a"`+"\n"+`State = "Idle"`); err != nil {
		t.Fatal(err)
	}

	// Before any flush, the underlying store is empty (updates are buffered).
	if n, _ := under.Len(StartdAd); n != 0 {
		t.Fatalf("underlying Len = %d before flush, want 0 (updates should be buffered)", n)
	}

	// A read through the buffer flushes first, so both distinct ads are visible
	// and slot1 carries the latest (deduped) value.
	if n, _ := b.Len(StartdAd); n != 2 {
		t.Fatalf("Len = %d after flush, want 2 (slot1 deduped, slot2 distinct)", n)
	}
	ad, ok := b.Get(StartdAd, mustParse(t, `Name = "slot1@a"`))
	if !ok {
		t.Fatal("slot1 missing after flush")
	}
	if s, _ := ad.EvaluateAttrString("State"); s != "Claimed" {
		t.Fatalf("slot1 State = %q, want Claimed (latest buffered update wins)", s)
	}
}

// TestBufferedBackendPvtAndInvalidate checks that a buffered private ad lands and
// that an invalidation flushes the buffer first (so it is not overwritten by a
// stale buffered write).
func TestBufferedBackendPvtAndInvalidate(t *testing.T) {
	under, err := NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBufferedBackend(under, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	if err := b.UpdateOldText(StartdAd, `Name = "slot1@a"`+"\n"+`State = "Claimed"`); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdatePvt(`Name = "slot1@a"`, `Capability = "claim-1"`); err != nil {
		t.Fatal(err)
	}
	// Invalidate flushes the buffer (so slot1 lands), then removes it and its
	// private shadow.
	if _, err := b.Invalidate(StartdAd, "", mustParse(t, `Name = "slot1@a"`)); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(StartdAd); n != 0 {
		t.Fatalf("StartdAd Len = %d after invalidate, want 0", n)
	}
	if n, _ := b.Len(StartdPvtAd); n != 0 {
		t.Fatalf("StartdPvtAd Len = %d after invalidate, want 0 (shadow removed)", n)
	}
}
