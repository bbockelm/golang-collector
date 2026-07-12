package negotiator

import (
	"testing"
	"time"
)

// TestNegotiatorAdLocateContract pins the attributes the C++
// condor_daemon_client requires to locate the negotiator through the collector.
// Daemon::locate(DT_NEGOTIATOR) calls getInfoFromAd (daemon.cpp:2052), which
// returns false — failing the whole locate, so condor_userprio prints "Can't
// locate negotiator in local pool" — unless the ad carries:
//
//   - an address: <Subsys>IpAddr (NegotiatorIpAddr) or MyAddress (daemon.cpp:2070-2082)
//   - ATTR_VERSION: CondorVersion                    (daemon.cpp:2098-2101, fatal)
//   - ATTR_MACHINE: Machine                          (daemon.cpp:2124-2128, fatal)
//
// CondorVersion is the one the real negotiator gets free from
// daemonCore->publish() and the Go ad historically omitted; this test guards
// the regression without needing the full condor_userprio oracle.
func TestNegotiatorAdLocateContract(t *testing.T) {
	n := &Negotiator{cfg: Config{
		NegotiatorName: "neg@example",
		Machine:        "example",
		AdvertisedAddr: "127.0.0.1:9614",
	}}
	ad := n.buildNegotiatorAd()

	requireStr := func(attr string) {
		t.Helper()
		if v, ok := ad.EvaluateAttrString(attr); !ok || v == "" {
			t.Fatalf("NegotiatorAd missing %s (needed for Daemon::locate(DT_NEGOTIATOR)); got %q ok=%v", attr, v, ok)
		}
	}

	requireStr("CondorVersion")
	requireStr("Machine")
	requireStr("MyType")
	// getInfoFromAd accepts either the <subsys>IpAddr or MyAddress form.
	addr, aok := ad.EvaluateAttrString("MyAddress")
	nip, nok := ad.EvaluateAttrString("NegotiatorIpAddr")
	if (!aok || addr == "") && (!nok || nip == "") {
		t.Fatalf("NegotiatorAd has no address attr (MyAddress=%q/%v, NegotiatorIpAddr=%q/%v)", addr, aok, nip, nok)
	}
}

// TestNegotiatorAdCycleStatsRing pins the NegotiatorAd cycle-stats ring: the
// most recent CycleStatsLength cycles are published newest-first (suffix
// 0..N-1) with the C++ attribute set, including the derived Period/MatchRate
// and the counters added in roadmap #7.
func TestNegotiatorAdCycleStatsRing(t *testing.T) {
	n := &Negotiator{cfg: Config{
		NegotiatorName:   "neg@example",
		Machine:          "example",
		CycleStatsLength: 3,
	}}
	base := time.Unix(1_700_000_000, 0)
	// Record 4 cycles; the ring keeps the newest 3 (2,1,0 dropped-oldest).
	for i := 0; i < 4; i++ {
		start := base.Add(time.Duration(i) * time.Minute)
		n.recordCycleLocked(&CycleStats{
			Start: start, End: start.Add(2 * time.Second),
			Matches: i + 1, TotalSlots: 10, NumSchedulers: 2,
			ActiveSubmitters: 3, Pies: 1, SlotShareIter: 5,
		})
	}
	if len(n.cycleRing) != 3 {
		t.Fatalf("ring length = %d, want 3 (capped at CycleStatsLength)", len(n.cycleRing))
	}

	ad := n.buildNegotiatorAd()

	// Suffix 0 is the newest cycle (the 4th recorded, Matches=4).
	if v, ok := classadInt(ad, "LastNegotiationCycleMatches0"); !ok || v != 4 {
		t.Errorf("LastNegotiationCycleMatches0 = %d (ok=%v), want 4 (newest)", v, ok)
	}
	// Suffix 2 is the oldest retained (3rd recorded, Matches=2).
	if v, ok := classadInt(ad, "LastNegotiationCycleMatches2"); !ok || v != 2 {
		t.Errorf("LastNegotiationCycleMatches2 = %d (ok=%v), want 2 (oldest retained)", v, ok)
	}
	// Nothing beyond the ring.
	if _, ok := classadInt(ad, "LastNegotiationCycleMatches3"); ok {
		t.Errorf("LastNegotiationCycleMatches3 present, want absent (ring is 3 deep)")
	}
	// New #7 counters are published.
	for _, attr := range []string{
		"LastNegotiationCycleNumSchedulers0", "LastNegotiationCyclePies0",
		"LastNegotiationCycleSlotShareIter0", "LastNegotiationCycleActiveSubmitterCount0",
	} {
		if _, ok := classadInt(ad, attr); !ok {
			t.Errorf("NegotiatorAd missing counter %s", attr)
		}
	}
	// Period0 is the gap to the next-older cycle's End (60s starts, 2s durations).
	if v, ok := classadInt(ad, "LastNegotiationCyclePeriod0"); !ok || v != 60 {
		t.Errorf("LastNegotiationCyclePeriod0 = %d (ok=%v), want 60", v, ok)
	}
}

func classadInt(ad interface{ EvaluateAttrInt(string) (int64, bool) }, attr string) (int64, bool) {
	return ad.EvaluateAttrInt(attr)
}
