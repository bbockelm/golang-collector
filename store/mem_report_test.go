package store

import (
	"compress/gzip"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"
)

// defaultCorpus is the committed OSPool `condor_status -l` sample shared with the
// classad collections benchmarks. Override with CLASSAD_BENCH_ADS.
const defaultCorpus = "/Users/bbockelm/projects/golang-classads/collections/vm/testdata/pool_sample.ads.gz"

// TestCollectorMemoryFootprint advertises N (default 100k) randomly-mutated real
// startd ads into the collector's store and reports its resident memory footprint
// -- the interesting number for a collector, whose whole job is to hold a large,
// live ClassAd collection. Each ad is mutated (unique name/address + perturbed
// numerics) so the ads are genuinely distinct, which is the realistic, harder case
// for the collection's dictionary-compressed storage.
//
// It is skipped under -short. Run it directly:
//
//	go test ./store/ -run TestCollectorMemoryFootprint -v
//	CLASSAD_BENCH_N=1000000 go test ./store/ -run TestCollectorMemoryFootprint -v -timeout 20m
func TestCollectorMemoryFootprint(t *testing.T) {
	// Full collector store: StartdAd table with HotAttrs + categorical indexes
	// (State, Activity, Arch, OpSys, SlotType, Name, Machine) + value indexes
	// (Cpus, Memory, Disk). This is the realistic collector footprint.
	sample := loadStartdCorpus(t)
	st := New()
	reportFootprint(t, sample, func(i int, ad *classad.ClassAd) error {
		return st.Update(StartdAd, ad)
	}, func() int { return st.Len(StartdAd) }, st)
}

// TestCollectionMemoryFootprintBare measures the same ads in a bare collection
// (no HotAttrs, no indexes) -- the pure dictionary-compressed ad storage, so the
// difference from TestCollectorMemoryFootprint is the collector's index overhead.
func TestCollectionMemoryFootprintBare(t *testing.T) {
	sample := loadStartdCorpus(t)
	col := collections.New(collections.Options{})
	reportFootprint(t, sample, func(i int, ad *classad.ClassAd) error {
		return col.Put([]byte("ad-"+strconv.Itoa(i)), ad)
	}, col.Len, col)
}

// reportFootprint advertises N (default 100k) randomly-mutated real startd ads
// through ingest and reports the process's resident memory growth -- the
// interesting number for a collector, whose job is to hold a large live ClassAd
// collection. Each ad is mutated (unique name/address + perturbed numerics) so
// tiling the small sample yields genuinely distinct ads -- the realistic, harder
// case for the dictionary-compressed storage.
//
// Skipped under -short. Run directly:
//
//	go test ./store/ -run TestCollectorMemoryFootprint -v
//	CLASSAD_BENCH_N=1000000 go test ./store/ -run TestCollectorMemoryFootprint -v -timeout 20m
func reportFootprint(t *testing.T, sample []*classad.ClassAd, ingest func(i int, ad *classad.ClassAd) error, count func() int, keep interface{}) {
	if testing.Short() {
		t.Skip("memory footprint report skipped under -short")
	}
	n := envInt("CLASSAD_BENCH_N", 100_000)

	runtime.GC()
	debug.FreeOSMemory()
	baseRSS := rss(t)
	var base runtime.MemStats
	runtime.ReadMemStats(&base)

	rng := rand.New(rand.NewSource(1))
	var rawBytes int64
	for i := 0; i < n; i++ {
		ad := mutate(sample[i%len(sample)], i, rng)
		rawBytes += int64(len(ad.MarshalOld()))
		if err := ingest(i, ad); err != nil {
			t.Fatalf("advertise ad %d: %v", i, err)
		}
	}

	stored := count()
	// Settle: drop transient ingest garbage and return heap pages to the OS so the
	// reading reflects retained memory, not high-water allocation.
	runtime.GC()
	debug.FreeOSMemory()
	afterRSS := rss(t)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	rssGrowth := afterRSS - baseRSS

	t.Logf("advertised %d ads, collection holds %d distinct records", n, stored)
	t.Logf("  raw ClassAd text (uncompressed): %s total, %.0f bytes/ad",
		humanBytes(rawBytes), float64(rawBytes)/float64(stored))
	t.Logf("  process RSS: %s -> %s  (growth %s = %.0f bytes/ad)",
		humanBytes(baseRSS), humanBytes(afterRSS), humanBytes(rssGrowth),
		float64(rssGrowth)/float64(stored))
	t.Logf("  Go heap (HeapAlloc): %s -> %s",
		humanBytes(int64(base.HeapAlloc)), humanBytes(int64(after.HeapAlloc)))
	if rssGrowth > 0 {
		t.Logf("  compression ratio (raw text / RSS growth): %.1fx",
			float64(rawBytes)/float64(rssGrowth))
	}
	// The exported Stats() -- what the Prometheus metrics report -- should track
	// the measured footprint closely, so a pool can be sized from the metric alone.
	if st, ok := keep.(*Store); ok {
		for at, cs := range st.Stats() {
			if cs.Ads > 0 {
				t.Logf("  before dict [%s]: live_bytes=%s (%.0f/ad) arena_bytes=%s segments=%d",
					at, humanBytes(cs.LiveBytes()), float64(cs.LiveBytes())/float64(cs.Ads),
					humanBytes(cs.ArenaBytes), cs.Segments)
			}
		}
		// A fresh collection uses the identity (no-compression) codec. Train a
		// ZSTD dictionary over the real ads and recompact, then re-measure -- this
		// is what the collector must do to realize the collection's compression.
		st.RetrainDict(50_000)
		runtime.GC()
		debug.FreeOSMemory()
		afterDictRSS := rss(t)
		t.Logf("  after RetrainDict: RSS %s (growth from empty %s)",
			humanBytes(afterDictRSS), humanBytes(afterDictRSS-baseRSS))
		for at, cs := range st.Stats() {
			if cs.Ads > 0 {
				t.Logf("  after dict [%s]: live_bytes=%s (%.0f/ad) arena_bytes=%s segments=%d",
					at, humanBytes(cs.LiveBytes()), float64(cs.LiveBytes())/float64(cs.Ads),
					humanBytes(cs.ArenaBytes), cs.Segments)
			}
		}
	}
	runtime.KeepAlive(keep)
}

// rss returns the process's resident set size in bytes (macOS/Linux `ps` reports
// it in KiB), which includes the collection's off-heap mmap arenas.
func rss(tb testing.TB) int64 {
	tb.Helper()
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		tb.Fatalf("ps rss: %v", err)
	}
	kib, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		tb.Fatalf("parse rss %q: %v", out, err)
	}
	return kib * 1024
}

// mutate returns ad with a unique identity and perturbed numeric attributes, so
// that tiling a small sample yields genuinely distinct ads. It mutates in place
// (the store serializes each ad on Update and does not retain it).
func mutate(ad *classad.ClassAd, i int, rng *rand.Rand) *classad.ClassAd {
	ad.InsertAttrString("Name", "slot1_"+strconv.Itoa(i)+"@ap"+strconv.Itoa(i%4096)+".pool")
	ad.InsertAttrString("Machine", "ap"+strconv.Itoa(i%4096)+".pool.example.org")
	ad.InsertAttrString("MyAddress", "<10."+strconv.Itoa((i/65536)%256)+"."+
		strconv.Itoa((i/256)%256)+"."+strconv.Itoa(i%256)+":9618>")
	// Perturb a spread of numeric attributes present on real startd ads.
	for _, a := range []string{"Memory", "Disk", "TotalDisk", "KFlops", "Mips", "ClockMin"} {
		if v, ok := ad.EvaluateAttrInt(a); ok {
			ad.InsertAttr(a, v+int64(rng.Intn(4096))-2048)
		}
	}
	for _, a := range []string{"LoadAvg", "CondorLoadAvg", "TotalLoadAvg"} {
		if v, ok := ad.EvaluateAttrReal(a); ok {
			ad.InsertAttrFloat(a, v+rng.Float64())
		}
	}
	return ad
}

func loadStartdCorpus(tb testing.TB) []*classad.ClassAd {
	tb.Helper()
	path := os.Getenv("CLASSAD_BENCH_ADS")
	if path == "" {
		path = defaultCorpus
	}
	f, err := os.Open(path)
	if err != nil {
		tb.Skipf("corpus %s not found; set CLASSAD_BENCH_ADS: %v", path, err)
	}
	defer f.Close()
	var src io.Reader = f
	if len(path) > 3 && path[len(path)-3:] == ".gz" {
		gz, err := gzip.NewReader(f)
		if err != nil {
			tb.Fatalf("gzip %s: %v", path, err)
		}
		defer gz.Close()
		src = gz
	}
	var ads []*classad.ClassAd
	r := classad.NewOldReader(src)
	for r.Next() {
		ads = append(ads, r.ClassAd())
	}
	if err := r.Err(); err != nil {
		tb.Fatalf("read %s: %v", path, err)
	}
	if len(ads) == 0 {
		tb.Fatalf("no ads parsed from %s", path)
	}
	tb.Logf("loaded %d sample ads from %s", len(ads), path)
	return ads
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return strconv.FormatInt(b, 10) + " B"
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return strconv.FormatFloat(float64(b)/float64(div), 'f', 1, 64) + " " + string("KMGT"[exp]) + "iB"
}
