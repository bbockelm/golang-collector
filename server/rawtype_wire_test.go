package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestWithRawTypeAttrs pins the helper that re-adds MyType/TargetType as attribute lines.
func TestWithRawTypeAttrs(t *testing.T) {
	base := [][]byte{[]byte(`Name = "slot1@a"`), []byte(`Cpus = 8`)}
	got := withRawTypeAttrs(base, "Machine", "Job")
	if len(got) != 4 {
		t.Fatalf("got %d exprs, want 4 (MyType + TargetType + 2)", len(got))
	}
	if string(got[0]) != `MyType = "Machine"` {
		t.Errorf("got[0] = %q, want MyType attr", got[0])
	}
	if string(got[1]) != `TargetType = "Job"` {
		t.Errorf("got[1] = %q, want TargetType attr", got[1])
	}
	// Empty type => omitted.
	if n := len(withRawTypeAttrs(base, "", "")); n != 2 {
		t.Errorf("empty types added attrs (%d exprs, want 2)", n)
	}
	if n := len(withRawTypeAttrs(base, "Machine", "")); n != 3 {
		t.Errorf("MyType only: got %d exprs, want 3", n)
	}
}

// readRawAd reads one wire-form ad off a query response after its PutInt32(1) marker,
// WITHOUT going through GetClassAd (which merges the trailing type strings into the ad,
// masking whether MyType arrived as an attribute). It returns the raw expression lines and
// the trailing MyType/TargetType strings, so a test can assert the on-the-wire encoding.
func readRawAd(t *testing.T, ctx context.Context, m *message.Message) (exprs []string, trailMyType, trailTargetType string) {
	t.Helper()
	count, err := m.GetInt(ctx)
	if err != nil {
		t.Fatalf("read expr count: %v", err)
	}
	for i := 0; i < count; i++ {
		s, err := m.GetString(ctx)
		if err != nil {
			t.Fatalf("read expr %d: %v", i, err)
		}
		exprs = append(exprs, s)
	}
	if trailMyType, err = m.GetString(ctx); err != nil {
		t.Fatalf("read trailing MyType: %v", err)
	}
	if trailTargetType, err = m.GetString(ctx); err != nil {
		t.Fatalf("read trailing TargetType: %v", err)
	}
	return exprs, trailMyType, trailTargetType
}

// TestQueryConveysMyTypeAsAttribute is the regression guard for the multi-query matchmaking
// break: the raw relay must send MyType/TargetType as ad ATTRIBUTES with EMPTY trailing
// type strings (the canonical C++ collector encoding), because the C++ negotiator reads the
// ad's type from the attribute, not the trailing string. A Go-client GetClassAd merges the
// trailing string and would pass even with the broken encoding, so this reads the wire
// directly.
func TestQueryConveysMyTypeAsAttribute(t *testing.T) {
	_, addr, stop := startCollector(t)
	defer stop()

	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), plaintextSec()), 10*time.Second)
	defer cancel()
	col := htcondor.NewCollector(addr)
	if err := col.Advertise(ctx, mustAd(t, `[MyType="Machine"; Name="slot1@a"; Cpus=8; MyAddress="<1.2.3.4:5>"]`),
		&htcondor.AdvertiseOptions{Command: commands.UPDATE_STARTD_AD}); err != nil {
		t.Fatalf("advertise: %v", err)
	}

	// Poll until the ad is queryable (fire-and-forget update).
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got, err := col.QueryAds(ctx, "Machine", "true"); err == nil && len(got) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("advertised ad never became queryable")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Low-level QUERY_STARTD_ADS so we can read the raw response encoding.
	sec := plaintextSec()
	sec.Command = commands.QUERY_STARTD_ADS
	cl, err := client.ConnectAndAuthenticate(ctx, addr, sec)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cl.Close()
	s := cl.GetStream()
	out := message.NewMessageForStream(s)
	if err := out.PutClassAd(ctx, mustAd(t, `[MyType="Query"; TargetType="Machine"; Requirements=true]`)); err != nil {
		t.Fatalf("put query: %v", err)
	}
	if err := out.FinishMessage(ctx); err != nil {
		t.Fatalf("finish: %v", err)
	}

	in := message.NewMessageFromStream(s)
	marker, err := in.GetInt(ctx)
	if err != nil {
		t.Fatalf("read marker: %v", err)
	}
	if marker != 1 {
		t.Fatalf("first marker = %d, want 1 (an ad)", marker)
	}
	exprs, trailMyType, trailTargetType := readRawAd(t, ctx, in)

	// MyType must be present as an ATTRIBUTE line...
	hasMyTypeAttr := false
	for _, e := range exprs {
		if strings.HasPrefix(strings.TrimSpace(e), "MyType") {
			hasMyTypeAttr = true
			if !strings.Contains(e, `"Machine"`) {
				t.Errorf("MyType attribute = %q, want value Machine", e)
			}
		}
	}
	if !hasMyTypeAttr {
		t.Errorf("no MyType attribute in the ad body (exprs=%v) -- the C++ negotiator would see a typeless ad and match nothing", exprs)
	}
	// ...and the trailing type strings must be EMPTY (canonical C++ encoding).
	if trailMyType != "" || trailTargetType != "" {
		t.Errorf("trailing type strings = (%q, %q), want empty (C++ sends MyType as an attribute, empty trailing)", trailMyType, trailTargetType)
	}
}
