package negotiator

import "testing"

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
