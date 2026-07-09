package server

import (
	"context"
	"testing"
	"time"

	"github.com/bbockelm/cedar/watch"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// TestWatchAdsEndToEnd drives the real htcondor WatchAds client against the
// collector's watch handler over CEDAR: catch-up (Reset -> Synced on an empty
// table), a live Upsert when an ad is stored, and an incremental resume from the
// Synced cursor.
func TestWatchAdsEndToEnd(t *testing.T) {
	st, addr, stop := startCollector(t)
	defer stop()

	ctx, cancel := context.WithCancel(htcondor.WithSecurityConfig(context.Background(), plaintextSec()))
	defer cancel()
	col := htcondor.NewCollector(addr)

	events, err := col.WatchAds(ctx, "StartdAd", "", nil)
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

	// Catch-up on an empty table: Reset then Synced (with a durable cursor).
	if ev := next(); ev.Kind != watch.KindReset {
		t.Fatalf("first event kind = %v, want Reset", ev.Kind)
	}
	synced := next()
	if synced.Kind != watch.KindSynced {
		t.Fatalf("kind = %v, want Synced", synced.Kind)
	}
	if synced.Cursor == nil {
		t.Fatal("Synced event carried no cursor")
	}

	// Live: store an ad -> an Upsert carrying that ad and a live cursor.
	ad := benchAd(1)
	wantName, _ := ad.EvaluateAttrString("Name")
	if err := st.Update(store.StartdAd, ad); err != nil {
		t.Fatalf("Update: %v", err)
	}
	up := next()
	if up.Kind != watch.KindUpsert {
		t.Fatalf("kind = %v, want Upsert", up.Kind)
	}
	if up.Ad == nil {
		t.Fatal("Upsert carried no ad")
	}
	if gotName, _ := up.Ad.EvaluateAttrString("Name"); gotName != wantName {
		t.Errorf("upsert ad Name = %q, want %q", gotName, wantName)
	}
	if up.Cursor == nil {
		t.Error("live Upsert carried no cursor")
	}

	// Resume from the Synced cursor on a fresh subscription: the Upsert that
	// happened after the cursor is replayed incrementally (no Reset).
	rctx, rcancel := context.WithCancel(ctx)
	defer rcancel()
	revents, err := col.WatchAds(rctx, "StartdAd", "", synced.Cursor)
	if err != nil {
		t.Fatalf("WatchAds resume: %v", err)
	}
	sawUpsert := false
	for {
		select {
		case ev, ok := <-revents:
			if !ok {
				t.Fatal("resume channel closed before Synced")
			}
			switch ev.Kind {
			case watch.KindReset:
				t.Fatal("resume produced a Reset; expected incremental catch-up")
			case watch.KindUpsert:
				if n, _ := ev.Ad.EvaluateAttrString("Name"); n == wantName {
					sawUpsert = true
				}
			case watch.KindSynced:
				if !sawUpsert {
					t.Fatal("resume Synced without replaying the Upsert")
				}
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out on resume")
		}
	}
}
