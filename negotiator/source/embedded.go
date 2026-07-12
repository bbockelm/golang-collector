package source

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/store"
)

// EmbeddedSource is a negotiator.AdSource that reads a collector store.Store
// directly (in-process), for the negotiator embedded in the collector binary.
// It applies the same per-ad fixups and constraint filters as the remote
// source and publishes the negotiator's own ads straight into the store.
type EmbeddedSource struct {
	store *store.Store
	cfg   Config
	log   *slog.Logger
	slotQ *vm.Query // compiled NEGOTIATOR_SLOT_CONSTRAINT (nil = all)
	subQ  *vm.Query // compiled NEGOTIATOR_SUBMITTER_CONSTRAINT (nil = all)
}

var _ negotiator.AdSource = (*EmbeddedSource)(nil)

// Snapshot gathers slots, submitters, and claim ids from the store, one
// goroutine per table, applying fixups and filters. The store's Query iterator
// decodes each ad fresh from the compressed store on every call
// (collections.Collection.decodeAd), so the ads handed out are already
// independent copies -- mutating a snapshot ad never touches a stored ad. The
// fixups mutate these fresh copies in place; no extra deep-copy is needed.
func (s *EmbeddedSource) Snapshot(ctx context.Context) (*negotiator.PoolSnapshot, error) {
	taken := time.Now()

	var (
		wg       sync.WaitGroup
		slots    []*classad.ClassAd
		subs     []*classad.ClassAd
		claimIDs map[string]string
	)

	wg.Add(3)

	// Slots: query (with slot constraint pushed to the store) + per-slot fixup.
	go func() {
		defer wg.Done()
		for ad := range s.store.Query(store.StartdAd, s.slotQ) {
			if ctx.Err() != nil {
				return
			}
			FixupSlot(ad)
			slots = append(slots, ad)
		}
	}()

	// Submitters: query (with submitter constraint) + filter.
	go func() {
		defer wg.Done()
		for ad := range s.store.Query(store.SubmitterAd, s.subQ) {
			if ctx.Err() != nil {
				return
			}
			if KeepSubmitter(ad) {
				subs = append(subs, ad)
			}
		}
	}()

	// Private ads: build the claim-id map (never republished).
	go func() {
		defer wg.Done()
		var pvt []*classad.ClassAd
		for ad := range s.store.Query(store.StartdPvtAd, nil) {
			if ctx.Err() != nil {
				return
			}
			pvt = append(pvt, ad)
		}
		claimIDs = BuildClaimIDs(pvt)
	}()

	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &negotiator.PoolSnapshot{
		Slots:      slots,
		Submitters: subs,
		ClaimIDs:   claimIDs,
		Taken:      taken,
	}, nil
}

// PublishNegotiatorAd writes the negotiator's daemon ad into the store's
// NegotiatorAd table.
func (s *EmbeddedSource) PublishNegotiatorAd(ctx context.Context, ad *classad.ClassAd) error {
	return s.store.Update(store.NegotiatorAd, ad)
}

// PublishAccountingAds writes the per-submitter/per-group accounting ads into
// the store's AccountingAd table.
func (s *EmbeddedSource) PublishAccountingAds(ctx context.Context, ads []*classad.ClassAd) error {
	for _, ad := range ads {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.store.Update(store.AccountingAd, ad); err != nil {
			return err
		}
	}
	return nil
}
