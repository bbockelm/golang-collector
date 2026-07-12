package negtest

import (
	"os"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/store"
)

// ParseAds parses a blank-line-separated sequence of old-syntax ClassAds (the
// testdata/*.ads fixture format: one ad per block, "Attr = Value" lines, blocks
// separated by a blank line). It returns the parsed ads in file order.
func ParseAds(data string) ([]*classad.ClassAd, error) {
	var ads []*classad.ClassAd
	for _, block := range splitBlocks(data) {
		ad, err := classad.ParseOld(block)
		if err != nil {
			return nil, err
		}
		ads = append(ads, ad)
	}
	return ads, nil
}

// splitBlocks splits fixture text into ad blocks on runs of blank lines,
// dropping empty blocks and comment-only whitespace.
func splitBlocks(data string) []string {
	var blocks []string
	var cur []string
	flush := func() {
		if len(cur) > 0 {
			blocks = append(blocks, strings.Join(cur, "\n"))
			cur = nil
		}
	}
	for _, line := range strings.Split(data, "\n") {
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		cur = append(cur, line)
	}
	flush()
	return blocks
}

// LoadAds reads and parses a fixture file (see ParseAds).
func LoadAds(tb testing.TB, path string) []*classad.ClassAd {
	tb.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("read fixture %s: %v", path, err)
	}
	ads, err := ParseAds(string(data))
	if err != nil {
		tb.Fatalf("parse fixture %s: %v", path, err)
	}
	return ads
}

// SeedStore inserts ads into st, routing each to a table by its MyType
// ("Machine" -> StartdAd, "Submitter" -> SubmitterAd, "Accounting" ->
// AccountingAd, "Negotiator" -> NegotiatorAd). Ads with an explicit
// _forcePvt="true" attribute (test convenience) are routed to the StartdPvt
// table instead. Unknown MyTypes are skipped. It fails the test on a store
// error so fixtures with un-keyable ads are caught early.
func SeedStore(tb testing.TB, st *store.Store, ads []*classad.ClassAd) {
	tb.Helper()
	for _, ad := range ads {
		t, ok := tableFor(ad)
		if !ok {
			continue
		}
		if err := st.Update(t, ad); err != nil {
			name, _ := ad.EvaluateAttrString("Name")
			tb.Fatalf("seed %s ad %q: %v", t, name, err)
		}
	}
}

func tableFor(ad *classad.ClassAd) (store.AdType, bool) {
	if pvt, ok := ad.EvaluateAttrBool("_forcePvt"); ok && pvt {
		return store.StartdPvtAd, true
	}
	mt, _ := ad.EvaluateAttrString("MyType")
	switch mt {
	case "Machine":
		return store.StartdAd, true
	case "Submitter":
		return store.SubmitterAd, true
	case "Accounting":
		return store.AccountingAd, true
	case "Negotiator":
		return store.NegotiatorAd, true
	default:
		return store.AnyAd, false
	}
}
