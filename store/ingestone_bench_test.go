package store

import "testing"

// BenchmarkIngestOneAd ingests one real AddressV1-bearing ad repeatedly (no
// MarshalOld in the timed loop), so -benchmem shows the true per-ad ingest cost.
// LastHeardFrom is stripped so this measures the production streaming fast path
// (see stripLastHeardFrom), not the errDuplicate full-reparse the corpus's
// pre-existing LastHeardFrom would otherwise trigger.
func BenchmarkIngestOneAd(b *testing.B) {
	sample := loadStartdCorpus(b)
	var text string
	for _, ad := range sample {
		if _, ok := ad.Lookup("AddressV1"); ok {
			text = stripLastHeardFrom(ad.MarshalOld())
			break
		}
	}
	if text == "" {
		b.Skip("no AddressV1 ad in corpus")
	}
	st := New()
	_ = st.UpdateOldText(StartdAd, text)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.UpdateOldText(StartdAd, text); err != nil {
			b.Fatal(err)
		}
	}
}
