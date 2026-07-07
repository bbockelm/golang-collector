package server

import (
	"compress/gzip"
	"context"
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

const corpusPath = "/Users/bbockelm/projects/golang-classads/collections/vm/testdata/pool_sample.ads.gz"

func loadLargeAds(tb testing.TB, n int) []*classad.ClassAd {
	tb.Helper()
	f, err := os.Open(corpusPath)
	if err != nil {
		tb.Skipf("corpus %s not found: %v", corpusPath, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		tb.Fatal(err)
	}
	defer gz.Close()
	var base []*classad.ClassAd
	r := classad.NewOldReader(gz)
	for r.Next() {
		base = append(base, r.ClassAd())
	}
	if len(base) == 0 {
		tb.Fatal("no ads in corpus")
	}
	out := make([]*classad.ClassAd, n)
	for i := range out {
		a := base[i%len(base)]
		a.InsertAttrString("Name", "slot1_"+strconv.Itoa(i)+"@host"+strconv.Itoa(i))
		out[i] = a
	}
	return out
}

// startLargeGoCollector stands up a Go collector prepopulated with n real ~21 KB
// OSPool ads, so a match-all read returns a realistic (heavy) result set.
func startLargeGoCollector(tb testing.TB, n int) (string, func()) {
	tb.Helper()
	st := store.New()
	for _, ad := range loadLargeAds(tb, n) {
		if err := st.Update(store.StartdAd, ad); err != nil {
			tb.Fatal(err)
		}
	}
	return serveStore(tb, st)
}

// BenchmarkRawClientRead compares a match-all read of realistic large ads with
// the normal client (which parses every returned ad into a *classad.ClassAd) vs a
// raw client (GetClassAdRaw -- reads the wire to text, no AST). The server does
// the same PutClassAd work in both, so the difference is the CLIENT's AST cost --
// isolating whether the client is a bottleneck (and would similarly cap both the
// Go and C++ collectors, making them look alike).
func BenchmarkRawClientRead(b *testing.B) {
	const n = 2000
	addr, stop := startLargeGoCollector(b, n)
	defer stop()
	q := mustAd(b, `[MyType="Query"; TargetType="Machine"; Requirements = true]`)

	b.Run("normal-client-ast", func(b *testing.B) {
		ctx := htcondor.WithSecurityConfig(context.Background(), plaintextSec())
		col := htcondor.NewCollector(addr)
		b.ResetTimer()
		var ads int64
		for i := 0; i < b.N; i++ {
			got, err := col.QueryAds(ctx, "Machine", "")
			if err != nil {
				b.Fatal(err)
			}
			ads += int64(len(got))
		}
		b.StopTimer()
		reportAds(b, ads)
	})

	b.Run("raw-client-no-ast", func(b *testing.B) {
		b.ResetTimer()
		var ads int64
		for i := 0; i < b.N; i++ {
			nads, err := rawQueryRead(addr, q)
			if err != nil {
				b.Fatal(err)
			}
			ads += int64(nads)
		}
		b.StopTimer()
		reportAds(b, ads)
	})
}

// rawQueryRead runs a query and reads the response ads as raw wire text
// (GetClassAdRaw) without building any *classad.ClassAd, returning the count.
func rawQueryRead(addr string, queryAd *classad.ClassAd) (int, error) {
	ctx := context.Background()
	sec := plaintextSec()
	sec.Command = commands.QUERY_STARTD_ADS
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	st := cl.GetStream()

	req := message.NewMessageForStream(st)
	if err := req.PutClassAd(ctx, queryAd); err != nil {
		return 0, err
	}
	if err := req.FinishMessage(ctx); err != nil {
		return 0, err
	}

	resp := message.NewMessageFromStream(st)
	n := 0
	for {
		more, err := resp.GetInt(ctx)
		if err != nil {
			return n, err
		}
		if more == 0 {
			return n, nil
		}
		if _, err := resp.GetClassAdRaw(ctx); err != nil { // wire -> text, NO AST
			return n, err
		}
		n++
	}
}

func reportAds(b *testing.B, ads int64) {
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(ads)/secs, "ads/sec")
	}
}

// serveStore serves an existing store on a random localhost port (plaintext).
func serveStore(tb testing.TB, st *store.Store) (string, func()) {
	tb.Helper()
	srv := New(st, plaintextSec(), nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = srv.ServeConn(ctx, conn) }()
		}
	}()
	return ln.Addr().String(), func() { cancel(); ln.Close() }
}
