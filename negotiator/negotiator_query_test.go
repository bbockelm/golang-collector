package negotiator_test

import (
	"context"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
)

// TestQueryAccountingAds exercises the direct QUERY_ACCOUNTING_ADS command a
// modern condor_userprio -modular / condor_status -direct sends straight to the
// negotiator (instead of GET_PRIORITY). Without a handler the stock tool prints
// "Can't query negotiator for ads". The response must include submitters seeded
// only via SET_PRIORITY / SET_PRIORITYFACTOR (no accrued usage) — the case the
// differential harness relies on — so it uses ReportStateAds (unfiltered), not
// the usage-filtered collector-publish AccountingAds.
func TestQueryAccountingAds(t *testing.T) {
	ctx := testCtx(t)
	addr, acct, _ := newDaemon(t, ctx, allowAll, nil)
	const alice = "alice@pool.test"
	const carol = "carol@pool.test"

	// Seed alice exactly like the differential harness: an explicit factor and
	// real priority, but no usage.
	sendSetter(t, ctx, addr, commands.SET_PRIORITYFACTOR, alice, func(m *message.Message) error {
		return m.PutDouble(ctx, 2000)
	})
	sendSetter(t, ctx, addr, commands.SET_PRIORITY, alice, func(m *message.Message) error {
		return m.PutDouble(ctx, 4.0)
	})
	// Seed carol with ONLY a real priority (no factor). Her reported
	// PriorityFactor must be the write-on-read DEFAULT, not the raw 0 — the exact
	// divergence a stock condor_userprio -modular caught in CI.
	sendSetter(t, ctx, addr, commands.SET_PRIORITY, carol, func(m *message.Message) error {
		return m.PutDouble(ctx, 0.5)
	})

	ads := queryAds(t, ctx, addr, commands.QUERY_ACCOUNTING_ADS, classad.New())

	byName := map[string]*classad.ClassAd{}
	for _, ad := range ads {
		if v, ok := ad.EvaluateAttrString("Name"); ok {
			byName[v] = ad
		}
	}
	found := byName[alice]
	if found == nil {
		t.Fatalf("QUERY_ACCOUNTING_ADS returned no ad for %s (got %d ads)", alice, len(ads))
	}
	if v, ok := found.EvaluateAttrString("MyType"); !ok || v != "Accounting" {
		t.Errorf("accounting ad MyType = %q (ok=%v), want %q", v, ok, "Accounting")
	}
	// Priority is the effective priority: real (4.0) x factor (2000).
	if v, _ := classad.GetAs[float64](found, "Priority"); !approx(v, 8000) {
		t.Errorf("Priority = %v, want ~8000 (real 4 x factor 2000)", v)
	}

	// carol's factor must be the default the accountant would report, not 0.
	if cad := byName[carol]; cad == nil {
		t.Errorf("QUERY_ACCOUNTING_ADS returned no ad for %s (factor-only submitter)", carol)
	} else {
		wantFactor := acct.GetPriorityFactor(carol)
		if v, _ := classad.GetAs[float64](cad, "PriorityFactor"); !approx(v, wantFactor) {
			t.Errorf("carol PriorityFactor = %v, want default %v (write-on-read, not raw 0)", v, wantFactor)
		}
	}

	// A projection restricts the returned attributes.
	projQ := classad.New()
	projQ.InsertAttrString("ProjectionAttributes", "Name,Priority")
	projected := queryAds(t, ctx, addr, commands.QUERY_ACCOUNTING_ADS, projQ)
	for _, ad := range projected {
		if v, _ := ad.EvaluateAttrString("Name"); v != alice {
			continue
		}
		if _, ok := ad.EvaluateAttrString("MyType"); ok {
			t.Errorf("projection Name,Priority should have dropped MyType; ad: %s", ad)
		}
	}
}

// TestGetResListWire exercises the GET_RESLIST command roundtrip (condor_userprio
// -getreslist): a "string submitter + EOM" request, a NO_TYPES resource-list ad
// reply. The fresh daemon accountant has no charged resources, so the reply is a
// well-formed empty ad; ResList content is covered by accountant TestResList.
func TestGetResListWire(t *testing.T) {
	ctx := testCtx(t)
	addr, _, _ := newDaemon(t, ctx, allowAll, nil)

	cl := dialCmd(t, ctx, addr, commands.SCHED_VERS+63) // GET_RESLIST
	req := message.NewMessageForStream(cl.GetStream())
	if err := req.PutString(ctx, "alice@pool.test"); err != nil {
		t.Fatalf("GET_RESLIST: sending submitter: %v", err)
	}
	if err := req.FinishMessage(ctx); err != nil {
		t.Fatalf("GET_RESLIST: finishing request: %v", err)
	}
	// The NO_TYPES reply is an expression count followed by that many strings
	// (the same framing getPriorityAd reads).
	rm := message.NewMessageFromStream(cl.GetStream())
	n, err := rm.GetInt(ctx)
	if err != nil {
		t.Fatalf("GET_RESLIST: reading reply expression count: %v", err)
	}
	for i := 0; i < n; i++ {
		if _, err := rm.GetString(ctx); err != nil {
			t.Fatalf("GET_RESLIST: reading reply expr %d/%d: %v", i, n, err)
		}
	}
}

// queryAds sends a collector-style query ad to the negotiator and reads back the
// PutInt32(1)+ad stream terminated by PutInt32(0).
func queryAds(t *testing.T, ctx context.Context, addr string, cmd int, queryAd *classad.ClassAd) []*classad.ClassAd {
	t.Helper()
	cl := dialCmd(t, ctx, addr, cmd)
	req := message.NewMessageForStream(cl.GetStream())
	if err := req.PutClassAd(ctx, queryAd); err != nil {
		t.Fatalf("sending query ad: %v", err)
	}
	if err := req.FlushFrame(ctx, true); err != nil {
		t.Fatalf("flushing query ad: %v", err)
	}
	rm := message.NewMessageFromStream(cl.GetStream())
	var ads []*classad.ClassAd
	for {
		more, err := rm.GetInt32(ctx)
		if err != nil {
			t.Fatalf("reading more-flag: %v", err)
		}
		if more == 0 {
			break
		}
		ad, err := rm.GetClassAd(ctx)
		if err != nil {
			t.Fatalf("reading query-result ad: %v", err)
		}
		ads = append(ads, ad)
	}
	return ads
}
