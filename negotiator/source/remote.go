package source

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	htcondor "github.com/bbockelm/golang-htcondor"

	"github.com/bbockelm/golang-collector/negotiator"
)

// RemoteSource is a negotiator.AdSource that queries a collector over CEDAR,
// the way the C++ condor_negotiator does. Machine and submitter ads use the
// htcondor collector client; startd private ads use the QUERY_STARTD_PVT_ADS
// command directly (the client has no helper for it), which requires
// NEGOTIATOR authorization at the collector. The three queries run
// concurrently.
type RemoteSource struct {
	cfg Config
	log *slog.Logger
	col *htcondor.Collector

	once sync.Once
}

var _ negotiator.AdSource = (*RemoteSource)(nil)

func (s *RemoteSource) collector() *htcondor.Collector {
	s.once.Do(func() { s.col = htcondor.NewCollector(s.cfg.CollectorAddr) })
	return s.col
}

// secCtx injects the configured CEDAR security policy into ctx so the htcondor
// collector client negotiates with it (its queries/advertises read the policy
// from the context).
func (s *RemoteSource) secCtx(ctx context.Context) context.Context {
	return htcondor.WithSecurityConfig(ctx, s.cfg.Security)
}

// Snapshot gathers machine, submitter, and private ads concurrently, pushing
// the slot/submitter constraints down to the collector at query time, then
// applies the shared fixups/filters.
func (s *RemoteSource) Snapshot(ctx context.Context) (*negotiator.PoolSnapshot, error) {
	taken := time.Now()
	qctx := s.secCtx(ctx)

	var (
		wg       sync.WaitGroup
		slots    []*classad.ClassAd
		subs     []*classad.ClassAd
		claimIDs map[string]string
		slotErr  error
		subErr   error
		pvtErr   error
	)

	wg.Add(3)

	go func() {
		defer wg.Done()
		ads, err := s.collector().QueryAdsWithProjection(qctx, "Machine", s.cfg.SlotConstraint, nil)
		if err != nil {
			slotErr = fmt.Errorf("query machine ads: %w", err)
			return
		}
		for _, ad := range ads {
			FixupSlot(ad)
		}
		slots = ads
	}()

	go func() {
		defer wg.Done()
		ads, err := s.collector().QueryAdsWithProjection(qctx, "Submitter", s.cfg.SubmitterConstraint, nil)
		if err != nil {
			subErr = fmt.Errorf("query submitter ads: %w", err)
			return
		}
		kept := ads[:0]
		for _, ad := range ads {
			if KeepSubmitter(ad) {
				kept = append(kept, ad)
			}
		}
		subs = kept
	}()

	go func() {
		defer wg.Done()
		pvt, err := s.queryPrivateAds(ctx)
		if err != nil {
			pvtErr = fmt.Errorf("query private ads: %w", err)
			return
		}
		claimIDs = BuildClaimIDs(pvt)
	}()

	wg.Wait()

	for _, err := range []error{slotErr, subErr, pvtErr} {
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

// queryPrivateAds issues QUERY_STARTD_PVT_ADS against the collector and returns
// ALL private ads (each carrying Name/MyAddress + the claim secret). The
// htcondor client exposes no private-ad query, so this drives the CEDAR
// exchange directly, mirroring the wire form the collector's own private-ad
// query test uses (server/roundtrip_test.go TestStartdPrivateAd).
//
// Unlike the public machine query, no slot constraint is applied here: private
// ads carry only Name/MyAddress + the claim secret, not the public slot
// attributes a constraint would reference, so constraining them would drop them
// all. The C++ negotiator likewise queries private ads unconstrained
// (matchmaker.cpp:3158 CondorQuery privateQuery(STARTD_PVT_AD)); the extra
// entries are harmless because the map is only consulted for slots that already
// survived the public constraint.
func (s *RemoteSource) queryPrivateAds(ctx context.Context) ([]*classad.ClassAd, error) {
	addr := firstAddr(s.cfg.CollectorAddr)

	// Copy the security policy so we can set the connection command without
	// mutating the shared config.
	sec := *s.cfg.Security
	sec.Command = commands.QUERY_STARTD_PVT_ADS

	cl, err := client.ConnectAndAuthenticate(ctx, addr, &sec)
	if err != nil {
		return nil, fmt.Errorf("connect (%s): %w", addr, err)
	}
	defer func() { _ = cl.Close() }()
	stream := cl.GetStream()

	queryAd := classad.New()
	_ = queryAd.Set("MyType", "Query")
	_ = queryAd.Set("TargetType", "Machine")
	_ = queryAd.Set("Requirements", true)

	qm := message.NewMessageForStream(stream)
	if err := qm.PutClassAd(ctx, queryAd); err != nil {
		return nil, fmt.Errorf("send query: %w", err)
	}
	if err := qm.FinishMessage(ctx); err != nil {
		return nil, fmt.Errorf("finish query: %w", err)
	}

	rm := message.NewMessageFromStream(stream)
	var ads []*classad.ClassAd
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		more, err := rm.GetInt(ctx)
		if err != nil {
			return nil, fmt.Errorf("read result marker: %w", err)
		}
		if more == 0 {
			break
		}
		ad, err := rm.GetClassAd(ctx)
		if err != nil {
			return nil, fmt.Errorf("read result ad: %w", err)
		}
		ads = append(ads, ad)
	}
	return ads, nil
}

// PublishNegotiatorAd advertises the negotiator's daemon ad with
// UPDATE_NEGOTIATOR_AD.
func (s *RemoteSource) PublishNegotiatorAd(ctx context.Context, ad *classad.ClassAd) error {
	return s.collector().Advertise(s.secCtx(ctx), ad, &htcondor.AdvertiseOptions{
		Command: commands.UPDATE_NEGOTIATOR_AD,
	})
}

// PublishAccountingAds advertises the accounting ads with UPDATE_ACCOUNTING_AD,
// reusing one connection for the batch (AdvertiseMultiple).
func (s *RemoteSource) PublishAccountingAds(ctx context.Context, ads []*classad.ClassAd) error {
	if len(ads) == 0 {
		return nil
	}
	errs := s.collector().AdvertiseMultiple(s.secCtx(ctx), ads, &htcondor.AdvertiseOptions{
		Command: commands.UPDATE_ACCOUNTING_AD,
	})
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("advertise accounting ad %d: %w", i, err)
		}
	}
	return nil
}

// firstAddr returns the first entry of a comma-separated collector address
// list (the cedar client dials a single address; the htcondor client handles
// the full list for the public queries).
func firstAddr(list string) string {
	if i := strings.IndexByte(list, ','); i >= 0 {
		return strings.TrimSpace(list[:i])
	}
	return strings.TrimSpace(list)
}
