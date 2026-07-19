package store

import (
	"context"
	"iter"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/collections"
)

// projectedBody collects a RawAd sequence and returns each ad's expression body
// joined, plus the count, for assertions.
func collectRaw(t *testing.T, seq iter.Seq[collections.RawAd], err error) []collections.RawAd {
	t.Helper()
	if err != nil {
		t.Fatalf("QueryRawProject: %v", err)
	}
	var got []collections.RawAd
	for ra := range seq {
		got = append(got, ra)
	}
	return got
}

func assertProjected(t *testing.T, ra collections.RawAd) {
	t.Helper()
	if ra.MyType != "Machine" {
		t.Errorf("MyType = %q, want Machine (type tag must survive projection)", ra.MyType)
	}
	var body string
	for _, e := range ra.Exprs {
		body += string(e) + "\n"
	}
	for _, want := range []string{"Name", "Cpus"} {
		if !strings.Contains(body, want) {
			t.Errorf("projected ad missing %s: %q", want, body)
		}
	}
	for _, drop := range []string{"Memory", "State"} {
		if strings.Contains(body, drop) {
			t.Errorf("projected ad should not carry %s: %q", drop, body)
		}
	}
}

const projTestAd = `MyType = "Machine"
Name = "slot1@a"
Cpus = 8
Memory = 4096
State = "Idle"`

// TestProjectedRawQueryNonPushdownBackends covers the regression where a projected
// query through BufferedBackend over a backend that cannot push the projection
// down (the in-memory Store, the embedded DBBackend) returned an error -- so the
// query failed and no ads were visible. Both must now serve projected ads.
func TestProjectedRawQueryNonPushdownBackends(t *testing.T) {
	ctx := context.Background()

	t.Run("memory", func(t *testing.T) {
		st := New()
		if err := st.UpdateOldText(ctx, StartdAd, projTestAd); err != nil {
			t.Fatal(err)
		}
		// The in-memory Store is used directly by the server (it is not a
		// BatchWriter, so it is never wrapped in BufferedBackend); its native
		// ProjectedRawQueryer must serve the projection.
		seq, err := st.QueryRawProject(ctx, StartdAd, "true", []string{"Name", "Cpus"}, 0)
		got := collectRaw(t, seq, err)
		if len(got) != 1 {
			t.Fatalf("Store.QueryRawProject: got %d ads, want 1", len(got))
		}
		assertProjected(t, got[0])
	})

	t.Run("embedded-db", func(t *testing.T) {
		db, err := NewDBBackend(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		if err := db.UpdateOldText(ctx, StartdAd, projTestAd); err != nil {
			t.Fatal(err)
		}
		seq, err := db.QueryRawProject(ctx, StartdAd, "true", []string{"Name", "Cpus"}, 0)
		got := collectRaw(t, seq, err)
		if len(got) != 1 {
			t.Fatalf("DBBackend.QueryRawProject: got %d ads, want 1", len(got))
		}
		assertProjected(t, got[0])

		buf, err := NewBufferedBackend(db, 0, 100, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = buf.Close() }()
		bseq, err := buf.QueryRawProject(ctx, StartdAd, "true", []string{"Name", "Cpus"}, 0)
		bgot := collectRaw(t, bseq, err)
		if len(bgot) != 1 {
			t.Fatalf("BufferedBackend(db).QueryRawProject: got %d ads, want 1", len(bgot))
		}
		assertProjected(t, bgot[0])
	})
}
