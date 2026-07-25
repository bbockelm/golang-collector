package source

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
)

// queuedSource is a fake AdSource that returns a pre-seeded sequence of snapshots,
// one per Snapshot call.
type queuedSource struct {
	snaps []*negotiator.PoolSnapshot
	i     int
}

func (q *queuedSource) Snapshot(context.Context) (*negotiator.PoolSnapshot, error) {
	s := q.snaps[q.i]
	q.i++
	return s, nil
}
func (q *queuedSource) PublishNegotiatorAd(context.Context, *classad.ClassAd) error    { return nil }
func (q *queuedSource) PublishAccountingAds(context.Context, []*classad.ClassAd) error { return nil }

// slotAd builds a machine ad. wantReval toggles WantAdRevaluate; marker is a
// distinguishing attribute so a test can tell which ad instance survived.
func slotAd(name string, seq int64, wantReval bool, marker string) *classad.ClassAd {
	ad := classad.New()
	ad.InsertAttrString("Name", name)
	ad.InsertAttrString("StartdIpAddr", "<10.0.0.1:9618>")
	ad.InsertAttr("UpdateSequenceNumber", seq)
	ad.InsertAttrString("Marker", marker)
	if wantReval {
		ad.InsertAttrBool("WantAdRevaluate", true)
	}
	return ad
}

func snap(slots ...*classad.ClassAd) *negotiator.PoolSnapshot {
	return &negotiator.PoolSnapshot{Slots: slots}
}

func marker(t *testing.T, snap *negotiator.PoolSnapshot, i int) string {
	t.Helper()
	m, _ := snap.Slots[i].EvaluateAttrString("Marker")
	return m
}

func TestReevalPassthroughWithoutFlag(t *testing.T) {
	inner := &queuedSource{snaps: []*negotiator.PoolSnapshot{snap(slotAd("s@h", 1, false, "a"))}}
	r := NewReevalSource(inner, "")
	got, err := r.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// No WantAdRevaluate anywhere -> the inner snapshot is returned untouched.
	if got != inner.snaps[0] {
		t.Error("snapshot without WantAdRevaluate should pass through unchanged (same pointer)")
	}
}

func TestReevalKeepsStaleAd(t *testing.T) {
	// Cycle 1 stashes seq=5 ("old"); cycle 2 offers seq=3 ("new-stale"). The default
	// expr (target.seq > my.seq) is false, so the stashed seq=5 ad is reused.
	inner := &queuedSource{snaps: []*negotiator.PoolSnapshot{
		snap(slotAd("s@h", 5, true, "old")),
		snap(slotAd("s@h", 3, true, "new-stale")),
	}}
	r := NewReevalSource(inner, "")

	s1, _ := r.Snapshot(context.Background())
	if marker(t, s1, 0) != "old" {
		t.Fatalf("cycle 1 marker = %q, want old", marker(t, s1, 0))
	}
	s2, _ := r.Snapshot(context.Background())
	if marker(t, s2, 0) != "old" {
		t.Errorf("cycle 2 marker = %q, want old (stale seq=3 must not replace stashed seq=5)", marker(t, s2, 0))
	}
}

func TestReevalReplacesOnHigherSeq(t *testing.T) {
	inner := &queuedSource{snaps: []*negotiator.PoolSnapshot{
		snap(slotAd("s@h", 5, true, "old")),
		snap(slotAd("s@h", 9, true, "new-fresh")),
	}}
	r := NewReevalSource(inner, "")

	_, _ = r.Snapshot(context.Background())
	s2, _ := r.Snapshot(context.Background())
	if marker(t, s2, 0) != "new-fresh" {
		t.Errorf("cycle 2 marker = %q, want new-fresh (seq 9 > 5 must replace)", marker(t, s2, 0))
	}
}

func TestReevalCustomExprAlwaysKeep(t *testing.T) {
	// An expression that is always false keeps the first stashed ad forever.
	inner := &queuedSource{snaps: []*negotiator.PoolSnapshot{
		snap(slotAd("s@h", 1, true, "first")),
		snap(slotAd("s@h", 100, true, "second")),
	}}
	r := NewReevalSource(inner, "false")

	_, _ = r.Snapshot(context.Background())
	s2, _ := r.Snapshot(context.Background())
	if marker(t, s2, 0) != "first" {
		t.Errorf("marker = %q, want first (expr=false must never replace)", marker(t, s2, 0))
	}
}

func TestReevalStashPruned(t *testing.T) {
	// Machine vanishes in cycle 2, then returns lower-seq in cycle 3: because its
	// stash was pruned, it is treated as first-seen and accepted (not kept-stale).
	inner := &queuedSource{snaps: []*negotiator.PoolSnapshot{
		snap(slotAd("s@h", 5, true, "old")),
		snap(), // s@h absent
		snap(slotAd("s@h", 2, true, "return")),
	}}
	r := NewReevalSource(inner, "")

	_, _ = r.Snapshot(context.Background())
	_, _ = r.Snapshot(context.Background())
	if len(r.stash) != 0 {
		t.Errorf("stash should be empty after the machine vanished, got %d entries", len(r.stash))
	}
	s3, _ := r.Snapshot(context.Background())
	if marker(t, s3, 0) != "return" {
		t.Errorf("marker = %q, want return (a re-seen machine is first-seen, not kept-stale)", marker(t, s3, 0))
	}
}
