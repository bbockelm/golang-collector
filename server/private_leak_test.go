package server

import (
	"context"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"

	"github.com/bbockelm/golang-collector/store"
)

// runQuery issues one QUERY_* command with the given query ad and collects the
// returned ads (the PutInt32(1)+ad / PutInt32(0) framing).
func runQuery(t *testing.T, addr string, cmd int, queryAd *classad.ClassAd) []*classad.ClassAd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	sec := plaintextSec()
	sec.Command = cmd
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cl.Close()
	s := cl.GetStream()
	m := message.NewMessageForStream(s)
	if err := m.PutClassAd(ctx, queryAd); err != nil {
		t.Fatalf("put query ad: %v", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatalf("finish query: %v", err)
	}
	rm := message.NewMessageFromStream(s)
	var out []*classad.ClassAd
	for {
		more, err := rm.GetInt(ctx)
		if err != nil {
			t.Fatalf("read marker: %v", err)
		}
		if more == 0 {
			break
		}
		ad, err := rm.GetClassAd(ctx)
		if err != nil {
			t.Fatalf("read ad: %v", err)
		}
		out = append(out, ad)
	}
	return out
}

// TestPublicQueryRedactsPrivate is the collector leak regression guard: a private
// (secret) attribute stored on a PUBLIC-table ad must never come back through a
// public query -- on the raw fast path (no projection) or the materialized slow
// path (with projection), even when the projection explicitly asks for it -- yet
// the authorized StartdPvt channel must still serve it.
func TestPublicQueryRedactsPrivate(t *testing.T) {
	st, addr, stop := startCollector(t)
	defer stop()

	// A public startd ad that (as a misbehaving/compat sender might) carries a
	// secret directly in the public table.
	if err := st.Update(store.StartdAd, mustAd(t,
		`[MyType="Machine"; Name="slot1@h"; MyAddress="<1.2.3.4:5>"; Cpus=8; ClaimId="SECRET-XYZ"; Capability="CAP-XYZ"]`)); err != nil {
		t.Fatal(err)
	}
	// And the legitimate private ad in the private table.
	if err := st.Update(store.StartdPvtAd, mustAd(t,
		`[MyType="Machine"; Name="slot1@h"; MyAddress="<1.2.3.4:5>"; ClaimId="SECRET-XYZ"]`)); err != nil {
		t.Fatal(err)
	}

	assertNoSecret := func(label string, ads []*classad.ClassAd) {
		if len(ads) != 1 {
			t.Fatalf("%s: got %d ads, want 1", label, len(ads))
		}
		if _, leaked := ads[0].EvaluateAttrString("ClaimId"); leaked {
			t.Errorf("%s: leaked ClaimId", label)
		}
		if _, leaked := ads[0].EvaluateAttrString("Capability"); leaked {
			t.Errorf("%s: leaked Capability", label)
		}
		if c, _ := ads[0].EvaluateAttrInt("Cpus"); c != 8 {
			t.Errorf("%s: public attr Cpus not returned (got %d)", label, c)
		}
	}

	// Fast path: no projection.
	assertNoSecret("public fast path",
		runQuery(t, addr, commands.QUERY_STARTD_ADS, mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements=true]`)))

	// Slow path: a projection that explicitly requests the secret must still redact.
	assertNoSecret("public slow path (projection asks for ClaimId)",
		runQuery(t, addr, commands.QUERY_STARTD_ADS,
			mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements=true; Projection="Name Cpus ClaimId Capability"]`)))

	// The authorized private channel must still deliver the secret.
	pvt := runQuery(t, addr, commands.QUERY_STARTD_PVT_ADS, mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements=true]`))
	if len(pvt) != 1 {
		t.Fatalf("pvt query: got %d ads, want 1", len(pvt))
	}
	if cid, _ := pvt[0].EvaluateAttrString("ClaimId"); cid != "SECRET-XYZ" {
		t.Errorf("pvt query redacted the claim id it is meant to serve: got %q", cid)
	}
}
