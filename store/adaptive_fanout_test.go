package store

import (
	"context"
	"fmt"
	"testing"
)

// TestBuildCommitUnits pins the adaptive split: small tables stay one unit (no fan-out),
// a large table splits into at most write-pool-many balanced chunks, and the split loses
// or duplicates no item.
func TestBuildCommitUnits(t *testing.T) {
	mk := func(table string, n int) (string, []keyedText) {
		items := make([]keyedText, n)
		for i := range items {
			items[i] = keyedText{key: fmt.Sprintf("%s-%d", table, i)}
		}
		return table, items
	}

	// Pool of 8 lanes.
	b := &RPCBackend{writes: make([]*connLane, 8)}

	// Small table: one unit (<= adaptiveFanoutChunk).
	{
		tbl, items := mk("Startd", 10)
		units := b.buildCommitUnits(map[string][]keyedText{tbl: items})
		if len(units) != 1 {
			t.Fatalf("small table -> %d units, want 1", len(units))
		}
	}
	// Large table: splits, capped at the pool size, balanced, complete.
	{
		const n = 5000
		tbl, items := mk("Startd", n)
		units := b.buildCommitUnits(map[string][]keyedText{tbl: items})
		if len(units) != 8 {
			t.Fatalf("large table with 8 lanes -> %d units, want 8 (capped at pool size)", len(units))
		}
		seen := map[string]bool{}
		for _, u := range units {
			if u.table != "Startd" {
				t.Errorf("unit table = %q, want Startd", u.table)
			}
			if len(u.items) == 0 || len(u.items) > (n+7)/8+1 {
				t.Errorf("unit has %d items; unbalanced", len(u.items))
			}
			for _, it := range u.items {
				if seen[it.key] {
					t.Fatalf("item %q appears in two units", it.key)
				}
				seen[it.key] = true
			}
		}
		if len(seen) != n {
			t.Fatalf("units cover %d items, want %d (lost some)", len(seen), n)
		}
	}
	// Single-lane backend never fans out.
	{
		b1 := &RPCBackend{writes: make([]*connLane, 1)}
		_, items := mk("Startd", 5000)
		units := b1.buildCommitUnits(map[string][]keyedText{"Startd": items})
		if len(units) != 1 {
			t.Fatalf("single-lane -> %d units, want 1 (no fan-out)", len(units))
		}
	}
	// Heavy multi-table batch: the TOTAL is bounded by the pool, so the whole flush fits
	// one semaphore wave. Two tables wanting 8 chunks each plus four singles used to yield
	// 20 units over 8 lanes -> a second serial wave; now the split tables shrink to fit.
	{
		byTable := map[string][]keyedText{}
		for _, tbl := range []string{"Startd", "StartdPvt"} {
			_, items := mk(tbl, 5000)
			byTable[tbl] = items
		}
		for _, tbl := range []string{"Master", "Schedd", "Submitter", "Negotiator"} {
			_, items := mk(tbl, 10)
			byTable[tbl] = items
		}
		units := b.buildCommitUnits(byTable)
		if len(units) > 8 {
			t.Fatalf("multi-table batch -> %d units over 8 lanes (second wave), want <= 8", len(units))
		}
		perTable := map[string]int{}
		seen := map[string]bool{}
		for _, u := range units {
			perTable[u.table]++
			for _, it := range u.items {
				if seen[it.key] {
					t.Fatalf("item %q appears in two units", it.key)
				}
				seen[it.key] = true
			}
		}
		for tbl := range byTable {
			if perTable[tbl] == 0 {
				t.Errorf("table %q lost its unit in the shrink", tbl)
			}
		}
		if len(seen) != 5000*2+10*4 {
			t.Fatalf("units cover %d items, want %d", len(seen), 5000*2+10*4)
		}
	}
	// More tables than lanes: every table still gets its unit (a unit never spans
	// tables), so the total may exceed the pool -- waves there are inherent.
	{
		byTable := map[string][]keyedText{}
		for i := 0; i < 10; i++ {
			tbl, items := mk(fmt.Sprintf("T%d", i), 5)
			byTable[tbl] = items
		}
		units := b.buildCommitUnits(byTable)
		if len(units) != 10 {
			t.Fatalf("10 small tables -> %d units, want 10 (one each)", len(units))
		}
	}
}

// TestRPCBackendAdaptiveFanoutCommits drives a large batch through UpdateBatch against a
// real in-process db: it must take the concurrent fan-out path and land every ad.
func TestRPCBackendAdaptiveFanoutCommits(t *testing.T) {
	b := newStreamTestBackend(t) // DefaultWriteConns lanes
	ctx := context.Background()

	const n = 1000 // > adaptiveFanoutChunk, so a single table fans out across lanes
	batch := make([]PendingUpdate, n)
	for i := range batch {
		batch[i] = PendingUpdate{Type: StartdAd, Text: fmt.Sprintf(`Name = "slot%d@a"`+"\n"+`State = "Idle"`, i)}
	}
	if err := b.UpdateBatch(ctx, batch); err != nil {
		t.Fatalf("UpdateBatch (fan-out): %v", err)
	}
	if got := countAds(t, b, StartdAd, "true"); got != n {
		t.Fatalf("stored %d ads after fanned-out commit, want %d", got, n)
	}
	// Spot-check a couple of keys survived intact.
	for _, i := range []int{0, n / 2, n - 1} {
		if _, ok := b.Get(ctx, StartdAd, mustParse(t, fmt.Sprintf(`Name = "slot%d@a"`, i))); !ok {
			t.Errorf("slot%d@a missing after fan-out", i)
		}
	}
}
