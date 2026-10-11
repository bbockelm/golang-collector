package collector

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/store"
)

func plaintextSec() *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{},
		Authentication: security.SecurityNever,
		Encryption:     security.SecurityNever,
		Integrity:      security.SecurityNever,
	}
}

// advertiseAndQuery advertises a Machine ad to a collector at addr and queries it
// back, asserting the round-trip. Shared by the standalone and embedded tests.
func advertiseAndQuery(t *testing.T, addr string) {
	t.Helper()
	ctx := htcondor.WithSecurityConfig(context.Background(), plaintextSec())
	col := htcondor.NewCollector(addr)
	ad, err := classad.Parse(`[MyType="Machine"; Name="slot1@embed"; MyAddress="<127.0.0.1:1>"; State="Unclaimed"; Cpus=8]`)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.Advertise(ctx, ad, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
		t.Fatalf("advertise: %v", err)
	}
	// UPDATE_STARTD_AD is fire-and-forget: Advertise returns once the update is
	// sent, with no happens-before against the server storing it, so poll the query
	// until the ad lands rather than racing a single immediate query (which is flaky
	// under load).
	var ads []*classad.ClassAd
	deadline := time.Now().Add(5 * time.Second)
	for {
		var err error
		ads, err = col.QueryAds(ctx, "Machine", `Name == "slot1@embed"`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if len(ads) == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(ads) != 1 {
		t.Fatalf("query returned %d ads, want 1", len(ads))
	}
	if v, _ := ads[0].EvaluateAttrString("Name"); v != "slot1@embed" {
		t.Errorf("Name = %q, want slot1@embed", v)
	}
}

// TestEmbeddableStandalone runs the collector standalone via New + Serve.
func TestEmbeddableStandalone(t *testing.T) {
	c, err := New(Config{Security: plaintextSec()})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Serve(ctx, ln) }()
	stop := c.StartBackground(ctx)
	defer stop()

	advertiseAndQuery(t, ln.Addr().String())

	n, err := c.Store().Len(context.Background(), store.StartdAd)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("Store().Len(StartdAd) = %d, want 1", n)
	}
}

// TestEmbeddableRegisterOn embeds the collector protocol on a host's own cedar
// command server (the socket-sharing embedding mode) and drives it end-to-end.
func TestEmbeddableRegisterOn(t *testing.T) {
	c, err := New(Config{Security: plaintextSec()})
	if err != nil {
		t.Fatal(err)
	}
	// The host daemon owns its command server and serve loop; it embeds the
	// collector by registering the collector protocol onto its server.
	host := cedarserver.New(plaintextSec())
	c.RegisterOn(host)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = host.Serve(ctx, ln) }()

	advertiseAndQuery(t, ln.Addr().String())

	// The ads landed in the embedded collector's store.
	n, err := c.Store().Len(context.Background(), store.StartdAd)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("embedded Store().Len(StartdAd) = %d, want 1", n)
	}
}

// TestEmbeddableSecurityForLevel: with SecurityForLevel, the collector's own
// server negotiates each command under the policy of its level, and a command
// whose level is absent from the map under Security. A plaintext client can
// query (READ) and advertise a schedd (ADVERTISE_SCHEDD, absent), but not a
// startd: ADVERTISE_STARTD requires authentication.
func TestEmbeddableSecurityForLevel(t *testing.T) {
	authRequired := plaintextSec()
	authRequired.Authentication = security.SecurityRequired
	authRequired.AuthMethods = []security.AuthMethod{security.AuthToken}
	c, err := New(Config{
		Security: plaintextSec(),
		SecurityForLevel: map[string]*security.SecurityConfig{
			"READ":             plaintextSec(),
			"ADVERTISE_STARTD": authRequired,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Serve(ctx, ln) }()

	cctx := htcondor.WithSecurityConfig(context.Background(), plaintextSec())
	col := htcondor.NewCollector(ln.Addr().String())
	startd, err := classad.Parse(`[MyType="Machine"; Name="slot1@plain"; MyAddress="<127.0.0.1:1>"]`)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.Advertise(cctx, startd, &htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err == nil {
		t.Error("plaintext UPDATE_STARTD_AD negotiated, though ADVERTISE_STARTD requires authentication")
	}
	schedd, err := classad.Parse(`[MyType="Scheduler"; Name="schedd@plain"; MyAddress="<127.0.0.1:2>"]`)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.Advertise(cctx, schedd, &htcondor.AdvertiseOptions{Command: commands.UPDATE_SCHEDD_AD}); err != nil {
		t.Errorf("plaintext UPDATE_SCHEDD_AD under the base policy: %v", err)
	}
	if _, err := col.QueryAds(cctx, "Machine", ""); err != nil {
		t.Errorf("plaintext QUERY_STARTD_ADS under READ's policy: %v", err)
	}
}
