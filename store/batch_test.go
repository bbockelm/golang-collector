package store

import (
	"context"
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
	if err := b.UpdateOldText(context.Background(), StartdAd, `Name = "slot1@a"`+"\n"+`State = "Idle"`); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdateOldText(context.Background(), StartdAd, `Name = "slot1@a"`+"\n"+`State = "Claimed"`); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdateOldText(context.Background(), StartdAd, `Name = "slot2@a"`+"\n"+`State = "Idle"`); err != nil {
		t.Fatal(err)
	}

	// Before any flush, the underlying store is empty (updates are buffered).
	if n, _ := under.Len(context.Background(), StartdAd); n != 0 {
		t.Fatalf("underlying Len = %d before flush, want 0 (updates should be buffered)", n)
	}

	// A read through the buffer flushes first, so both distinct ads are visible
	// and slot1 carries the latest (deduped) value.
	if n, _ := b.Len(context.Background(), StartdAd); n != 2 {
		t.Fatalf("Len = %d after flush, want 2 (slot1 deduped, slot2 distinct)", n)
	}
	ad, ok := b.Get(context.Background(), StartdAd, mustParse(t, `Name = "slot1@a"`))
	if !ok {
		t.Fatal("slot1 missing after flush")
	}
	if s, _ := ad.EvaluateAttrString("State"); s != "Claimed" {
		t.Fatalf("slot1 State = %q, want Claimed (latest buffered update wins)", s)
	}
}

// TestBufferedBackendDurableUpdate checks the ACK-update path: DurableUpdate must
// commit to the underlying store immediately (bypassing the buffer), so the caller
// can acknowledge only after the write is durable -- and it must flush any earlier
// buffered update of the same ad first, so the durable value wins.
func TestBufferedBackendDurableUpdate(t *testing.T) {
	under, err := NewDBBackend(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBufferedBackend(under, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Close() }()

	// A buffered (non-ack) update of slot1, then a durable (ack) update of the same
	// ad. The durable call must flush the stale buffered value and land the latest.
	if err := b.UpdateOldText(context.Background(), StartdAd, `Name = "slot1@a"`+"\n"+`State = "Idle"`); err != nil {
		t.Fatal(err)
	}
	if err := DurableUpdate(context.Background(), b, StartdAd, `Name = "slot1@a"`+"\n"+`State = "Claimed"`); err != nil {
		t.Fatal(err)
	}

	// Read the UNDERLYING store directly (not through b) to prove durability without
	// a buffer flush: the ad must already be committed.
	if n, _ := under.Len(context.Background(), StartdAd); n != 1 {
		t.Fatalf("underlying Len = %d after DurableUpdate, want 1 (ack write must be committed, not buffered)", n)
	}
	ad, ok := under.Get(context.Background(), StartdAd, mustParse(t, `Name = "slot1@a"`))
	if !ok {
		t.Fatal("slot1 missing from underlying store after DurableUpdate")
	}
	if s, _ := ad.EvaluateAttrString("State"); s != "Claimed" {
		t.Fatalf("slot1 State = %q, want Claimed (durable update must supersede the buffered one)", s)
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

	if err := b.UpdateOldText(context.Background(), StartdAd, `Name = "slot1@a"`+"\n"+`State = "Claimed"`); err != nil {
		t.Fatal(err)
	}
	if err := b.UpdatePvt(context.Background(), `Name = "slot1@a"`, `Capability = "claim-1"`); err != nil {
		t.Fatal(err)
	}
	// Invalidate flushes the buffer (so slot1 lands), then removes it and its
	// private shadow.
	if _, err := b.Invalidate(context.Background(), StartdAd, "", mustParse(t, `Name = "slot1@a"`)); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Len(context.Background(), StartdAd); n != 0 {
		t.Fatalf("StartdAd Len = %d after invalidate, want 0", n)
	}
	if n, _ := b.Len(context.Background(), StartdPvtAd); n != 0 {
		t.Fatalf("StartdPvtAd Len = %d after invalidate, want 0 (shadow removed)", n)
	}
}
