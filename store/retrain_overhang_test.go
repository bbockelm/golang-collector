package store

import (
	"math/rand"
	"runtime"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/collections"
)

// TestRetrainMemoryOverhang probes the resident-memory behavior of RetrainDict:
// recompaction decodes and rewrites every ad, so it has a high transient peak.
// The question is whether that memory comes back afterward, or whether the
// process RSS stays inflated (which would make periodic retraining a footgun).
//
// It logs, at each stage, the process RSS vs the Go heap breakdown vs the
// collection's own arena/live accounting, so we can tell logical size from
// resident size from Go-runtime overhang. Skipped under -short.
func TestRetrainMemoryOverhang(t *testing.T) {
	if testing.Short() {
		t.Skip("retrain overhang report skipped under -short")
	}
	sample := loadStartdCorpus(t)
	n := envInt("CLASSAD_BENCH_N", 100_000)

	st := New()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		if err := st.Update(StartdAd, mutate(sample[i%len(sample)], i, rng)); err != nil {
			t.Fatal(err)
		}
	}
	logMem(t, "populated (identity codec)", st)

	st.RetrainDict(50_000)
	logMem(t, "after retrain (immediate)", st)

	// Give the runtime's background scavenger a chance, then force again, to see
	// whether the recompaction's transient pages are eventually returned.
	time.Sleep(5 * time.Second)
	logMem(t, "after retrain (+5s + scavenge)", st)

	runtime.KeepAlive(st)
}

// TestRetrainMemoryOverhangBare is TestRetrainMemoryOverhang against a bare
// collection (no HotAttrs, no categorical/value indexes), so comparing the live
// HeapAlloc after retrain isolates how much of the collector store's overhead is
// its indexes (chiefly the Name categorical index over ~100k unique names) vs the
// unavoidable arena + dir + dictionary.
func TestRetrainMemoryOverhangBare(t *testing.T) {
	if testing.Short() {
		t.Skip("retrain overhang report skipped under -short")
	}
	sample := loadStartdCorpus(t)
	n := envInt("CLASSAD_BENCH_N", 100_000)

	col := collections.New(collections.Options{})
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		if err := col.Put([]byte("ad-"+strconv.Itoa(i)), mutate(sample[i%len(sample)], i, rng)); err != nil {
			t.Fatal(err)
		}
	}
	logMemCol(t, "bare: populated (identity)", col)
	if _, err := col.RetrainDict(50_000); err != nil {
		t.Fatal(err)
	}
	logMemCol(t, "bare: after retrain", col)
	runtime.KeepAlive(col)
}

// TestRetrainPeak isolates what drives the RetrainDict transient peak: the
// dictionary-training sample (CollectSamples retains RETRAIN_SAMPLE decoded ads)
// vs the recompaction churn (fixed). Run it at two sample sizes in SEPARATE
// processes (HeapSys is monotonic within a process) and compare the peak:
//
//	RETRAIN_SAMPLE=2000  go test ./store/ -run TestRetrainPeak -v
//	RETRAIN_SAMPLE=50000 go test ./store/ -run TestRetrainPeak -v
func TestRetrainPeak(t *testing.T) {
	if testing.Short() {
		t.Skip("retrain peak report skipped under -short")
	}
	sample := loadStartdCorpus(t)
	n := envInt("CLASSAD_BENCH_N", 100_000)
	sm := envInt("RETRAIN_SAMPLE", 50_000)

	col := collections.New(collections.Options{})
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		if err := col.Put([]byte("ad-"+strconv.Itoa(i)), mutate(sample[i%len(sample)], i, rng)); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	debug.FreeOSMemory()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)

	if _, err := col.RetrainDict(sm); err != nil {
		t.Fatal(err)
	}

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	t.Logf("n=%d sampleMax=%d: HeapSys %s -> %s (peak +%s), TotalAlloc during retrain=%s, NumGC+%d, live_after=%s",
		n, sm, humanBytes(int64(m0.HeapSys)), humanBytes(int64(m1.HeapSys)),
		humanBytes(int64(m1.HeapSys-m0.HeapSys)), humanBytes(int64(m1.TotalAlloc-m0.TotalAlloc)),
		m1.NumGC-m0.NumGC, humanBytes(col.Stats().LiveBytes()))
	runtime.KeepAlive(col)
}

func logMemCol(t *testing.T, label string, col *collections.Collection) {
	t.Helper()
	runtime.GC()
	debug.FreeOSMemory()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	cs := col.Stats()
	t.Logf("%s:", label)
	t.Logf("    RSS=%s | HeapAlloc(live)=%s HeapReleased=%s HeapSys=%s",
		humanBytes(rss(t)), humanBytes(int64(m.HeapAlloc)), humanBytes(int64(m.HeapReleased)), humanBytes(int64(m.HeapSys)))
	t.Logf("    collection: arena=%s live=%s segments=%d", humanBytes(cs.ArenaBytes), humanBytes(cs.LiveBytes()), cs.Segments)
}

func logMem(t *testing.T, label string, st *Store) {
	t.Helper()
	runtime.GC()
	debug.FreeOSMemory()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	cs := st.Stats()[StartdAd]
	t.Logf("%s:", label)
	t.Logf("    RSS=%s | HeapAlloc(live)=%s HeapInuse=%s HeapIdle=%s HeapReleased=%s HeapSys=%s",
		humanBytes(rss(t)), humanBytes(int64(m.HeapAlloc)), humanBytes(int64(m.HeapInuse)),
		humanBytes(int64(m.HeapIdle)), humanBytes(int64(m.HeapReleased)), humanBytes(int64(m.HeapSys)))
	t.Logf("    collection: arena=%s live=%s segments=%d", humanBytes(cs.ArenaBytes), humanBytes(cs.LiveBytes()), cs.Segments)
}
