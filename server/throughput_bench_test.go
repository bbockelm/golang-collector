package server

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

// encryptedSec negotiates an AES-encrypted, integrity-protected CEDAR session.
// Authentication is left optional -- the ECDH key agreement in the handshake
// still yields the session key that encryption needs, so the transport is
// encrypted end to end even without a strong auth method configured.
func encryptedSec() *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{security.AuthNone},
		Authentication: security.SecurityOptional,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Encryption:     security.SecurityRequired,
		Integrity:      security.SecurityRequired,
	}
}

// benchNumAds is the size of the collector table the benchmark maintains.
const benchNumAds = 10000

// benchAd builds a representative startd ad. Names cycle over benchNumAds so
// updates land as replaces across a realistic keyspace, and the filterable
// attributes vary so query constraints select a meaningful fraction: State is
// "Claimed" for ~1/4 of ads (an indexed categorical with partial selectivity),
// while Activity is "Idle" for all (an un-indexed attribute a scan must evaluate
// on every ad).
func benchAd(i int) *classad.ClassAd {
	i %= benchNumAds
	state := "Unclaimed"
	if i%4 == 0 {
		state = "Claimed"
	}
	ad, err := classad.Parse(fmt.Sprintf(
		`[MyType="Machine"; Name="slot%d@host%d"; MyAddress="<10.%d.%d.%d:9618>"; `+
			`State=%q; Activity="Idle"; Cpus=%d; Memory=%d; Disk=%d; `+
			`Arch="X86_64"; OpSys="LINUX"; SlotType="Partitionable"]`,
		i%64, i, i>>16&0xff, i>>8&0xff, i&0xff, state, i%16+1, (i%16+1)*2048, (i%16+1)*1024*100))
	if err != nil {
		panic(err)
	}
	return ad
}

// startEncryptedCollector stands up the collector server (encrypted transport)
// with a table pre-populated to benchNumAds startd ads.
func startEncryptedCollector(tb testing.TB) (addr string, stop func()) {
	tb.Helper()
	st := store.New()
	for i := 0; i < benchNumAds; i++ {
		if err := st.Update(context.Background(), store.StartdAd, benchAd(i)); err != nil {
			tb.Fatal(err)
		}
	}
	srv := New(st, encryptedSec(), nil)
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

// requireEncrypted fails the benchmark loudly unless the negotiated CEDAR
// session is actually AES-encrypted -- the whole point is to measure throughput
// over encrypted transport, so a silent fall back to plaintext must not pass.
func requireEncrypted(tb testing.TB, addr string) {
	tb.Helper()
	sec := encryptedSec()
	sec.Command = commands.QUERY_STARTD_ADS
	cl, err := client.ConnectAndAuthenticate(context.Background(), addr, sec)
	if err != nil {
		tb.Fatalf("preflight connect: %v", err)
	}
	defer cl.Close()
	neg := cl.GetSecurityNegotiation()
	if neg == nil || !neg.Encryption || !cl.GetStream().IsEncrypted() {
		tb.Fatalf("transport is NOT encrypted (negotiated crypto=%v, encryption=%v)",
			neg.NegotiatedCrypto, neg.Encryption)
	}
}

// BenchmarkCollectorThroughput measures end-to-end ClassAd processing rate over
// encrypted CEDAR at a range of client concurrencies and query/update ratios.
// The reported "ads/sec" metric is total operations (updates + queries) per
// second; each update advertises one ad, each query is a constrained scan of the
// table. Updates use the fast StreamEncoder ingest path.
func BenchmarkCollectorThroughput(b *testing.B) {
	addr, stop := startEncryptedCollector(b)
	defer stop()
	requireEncrypted(b, addr)

	// Each op is its own TCP connection (the collector protocol's model), so run
	// with a bounded, fixed iteration count (-benchtime=NNNx) rather than a
	// time-based one: driven flat-out from a single host, connection-per-op
	// otherwise exhausts the local ephemeral port range (TIME_WAIT) -- a client
	// artifact, not a collector limit.
	concurrencies := []int{1, 8, 32}
	mixes := []struct {
		name     string
		queryPct int
	}{
		{"update-only", 0},
		{"query-10pct", 10},
		{"query-50pct", 50},
		{"query-90pct", 90},
	}
	for _, conc := range concurrencies {
		for _, mix := range mixes {
			b.Run(fmt.Sprintf("conc=%d/%s", conc, mix.name), func(b *testing.B) {
				runMix(b, addr, conc, mix.queryPct)
			})
		}
	}
}

// updateBatch is how many ads one bulk advertise carries on a single connection.
// The real collector protocol streams many ads per connection, so batching here
// keeps the benchmark from opening (and leaking into TIME_WAIT) a socket per ad.
const updateBatch = 100

func runMix(b *testing.B, addr string, conc, queryPct int) {
	runMixSec(b, addr, conc, queryPct, encryptedSec())
}

// runMixSingle drives the workload with one ad per connection (single Advertise),
// the lowest-common-denominator protocol that works against both the Go and the
// C++ collector. keyspace bounds the update key range so updates land as
// replaces across the pre-populated table.
func runMixSingle(b *testing.B, addr string, conc, queryPct int, sec *security.SecurityConfig, keyspace int) {
	baseCtx := htcondor.WithSecurityConfig(context.Background(), sec)
	var claimed int64
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			col := htcondor.NewCollector(addr)
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			for {
				if atomic.AddInt64(&claimed, 1) > int64(b.N) {
					return
				}
				var err error
				if rng.Intn(100) < queryPct {
					_, err = col.QueryAds(baseCtx, "Machine", `Name == "slot0@host0"`)
				} else {
					err = col.Advertise(baseCtx, benchAd(rng.Intn(keyspace)),
						&htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD})
				}
				if err != nil {
					b.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "ops/sec")
	}
}

// runMixSec is runMix with an explicit security policy, so the same workload can
// be driven over plaintext (e.g. against the C++ collector) or encrypted CEDAR.
func runMixSec(b *testing.B, addr string, conc, queryPct int, sec *security.SecurityConfig) {
	baseCtx := htcondor.WithSecurityConfig(context.Background(), sec)
	var claimed int64
	b.ResetTimer()

	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			col := htcondor.NewCollector(addr)
			rng := rand.New(rand.NewSource(int64(seed) + 1))
			for {
				// Claim a chunk of the total op budget for this iteration.
				start := atomic.AddInt64(&claimed, updateBatch) - updateBatch
				if start >= int64(b.N) {
					return
				}
				n := updateBatch
				if rem := int64(b.N) - start; rem < int64(n) {
					n = int(rem)
				}

				if rng.Intn(100) < queryPct {
					// Queries are individual requests (each is its own connection,
					// as condor_status would be).
					for i := 0; i < n; i++ {
						if _, err := col.QueryAds(baseCtx, "Machine", `Name == "slot0@host0"`); err != nil {
							b.Error(err)
							return
						}
					}
					continue
				}
				// Updates: one bulk advertise carries the whole batch on a single
				// connection.
				ads := make([]*classad.ClassAd, n)
				for i := range ads {
					ads[i] = benchAd(rng.Intn(benchNumAds))
				}
				for _, err := range col.AdvertiseMultiple(baseCtx, ads,
					&htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}) {
					if err != nil {
						b.Error(err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()

	b.StopTimer()
	if secs := b.Elapsed().Seconds(); secs > 0 {
		b.ReportMetric(float64(b.N)/secs, "ops/sec")
	}
}
