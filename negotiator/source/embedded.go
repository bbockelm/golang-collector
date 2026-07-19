package source

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/golang-collector/negotiator"
	"github.com/bbockelm/golang-collector/store"
)

// EmbeddedSource is a negotiator.AdSource that reads a collector store.Backend
// directly (in-process), for the negotiator embedded in the collector binary.
// It applies the same per-ad fixups and constraint filters as the remote
// source and publishes the negotiator's own ads straight into the store. Reading
// through the Backend (rather than the concrete in-memory store) keeps the
// embedded negotiator working over any backend, including a persistent database.
type EmbeddedSource struct {
	store store.Backend
	cfg   Config
	log   *slog.Logger
	// Constraint expression strings (validated at construction; "" = all), handed
	// to the backend's Query, which compiles them. Kept as strings so a remote
	// backend can push them down unchanged.
	slotConstraint string
	subConstraint  string
	// defaultWeight is the parsed SLOT_WEIGHT cost expression, applied by
	// FixupSlot to slots lacking their own SlotWeight (shared read-only).
	defaultWeight *classad.Expr
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
		errs     [3]error
	)

	wg.Add(3)

	// Slots: query (with slot constraint pushed to the store) + per-slot fixup.
	go func() {
		defer wg.Done()
		ads, err := s.store.Query(ctx, store.StartdAd, s.slotConstraint, 0)
		if err != nil {
			errs[0] = err
			return
		}
		for ad := range ads {
			if ctx.Err() != nil {
				return
			}
			FixupSlot(ad, s.defaultWeight)
			slots = append(slots, ad)
		}
	}()

	// Submitters: query (with submitter constraint) + filter.
	go func() {
		defer wg.Done()
		ads, err := s.store.Query(ctx, store.SubmitterAd, s.subConstraint, 0)
		if err != nil {
			errs[1] = err
			return
		}
		for ad := range ads {
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
		ads, err := s.store.Query(ctx, store.StartdPvtAd, "", 0)
		if err != nil {
			errs[2] = err
			return
		}
		var pvt []*classad.ClassAd
		for ad := range ads {
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
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
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
	return s.store.Update(ctx, store.NegotiatorAd, ad)
}

// PublishAccountingAds writes the per-submitter/per-group accounting ads into
// the store's AccountingAd table.
func (s *EmbeddedSource) PublishAccountingAds(ctx context.Context, ads []*classad.ClassAd) error {
	for _, ad := range ads {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.store.Update(ctx, store.AccountingAd, ad); err != nil {
			return err
		}
	}
	return nil
}
