package server

import (
	"context"
	"testing"
	"time"

	"github.com/bbockelm/cedar/watch"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// TestWatchAdsFilter verifies a constrained subscription: only matching ads are
// delivered, a non-matching ad produces nothing, and an ad that is updated so it
// no longer matches arrives as a Delete (so the client's filtered view stays
// consistent).
func TestWatchAdsFilter(t *testing.T) {
	st, addr, stop := startCollector(t)
	defer stop()

	ctx, cancel := context.WithCancel(htcondor.WithSecurityConfig(context.Background(), plaintextSec()))
	defer cancel()
	col := htcondor.NewCollector(addr)

	events, err := col.WatchAds(ctx, "StartdAd", "Cpus >= 8", nil)
	if err != nil {
		t.Fatalf("WatchAds: %v", err)
	}
	next := func() htcondor.WatchEvent {
		t.Helper()
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("watch channel closed unexpectedly")
			}
			return ev
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for a watch event")
			return htcondor.WatchEvent{}
		}
	}

	// Catch-up on an empty table.
	if ev := next(); ev.Kind != watch.KindReset {
		t.Fatalf("first event = %v, want Reset", ev.Kind)
	}
	if ev := next(); ev.Kind != watch.KindSynced {
		t.Fatalf("second event = %v, want Synced", ev.Kind)
	}

	big := mustAd(t, `[MyType="Machine"; Name="w1"; MyAddress="<1.2.3.4:9618>"; Cpus=8; State="Unclaimed"; Activity="Idle"]`)
	small := mustAd(t, `[MyType="Machine"; Name="w2"; MyAddress="<1.2.3.5:9618>"; Cpus=4; State="Unclaimed"; Activity="Idle"]`)

	// A non-matching ad (Cpus=4) must produce no event; a matching ad (Cpus=8)
	// must arrive as an Upsert. Store the non-matching one first: if it leaked, it
	// would be the next event instead of w1.
	if err := st.Update(context.Background(), store.StartdAd, small); err != nil {
		t.Fatal(err)
	}
	if err := st.Update(context.Background(), store.StartdAd, big); err != nil {
		t.Fatal(err)
	}
	up := next()
	if up.Kind != watch.KindUpsert {
		t.Fatalf("event = %v, want Upsert (w2 should have been filtered out)", up.Kind)
	}
	if n, _ := up.Ad.EvaluateAttrString("Name"); n != "w1" {
		t.Fatalf("upsert Name = %q, want w1 (w2 leaked through the filter)", n)
	}

	// Update w1 so it no longer matches (Cpus 8 -> 2): the filtered stream must
	// convert this to a Delete so the client drops it.
	demoted := mustAd(t, `[MyType="Machine"; Name="w1"; MyAddress="<1.2.3.4:9618>"; Cpus=2; State="Unclaimed"; Activity="Idle"]`)
	if err := st.Update(context.Background(), store.StartdAd, demoted); err != nil {
		t.Fatal(err)
	}
	del := next()
	if del.Kind != watch.KindDelete {
		t.Fatalf("event = %v, want Delete (w1 stopped matching)", del.Kind)
	}
}
