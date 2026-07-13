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

	// defaultWeight is the parsed SLOT_WEIGHT cost expression, applied by
	// FixupSlot to slots lacking their own SlotWeight (shared read-only).
	defaultWeight *classad.Expr

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
		// Full ads, no result cap: the matchmaker evaluates arbitrary
		// Requirements/Rank references, and a truncated pool would drop slots.
		// Limit:-1 (unlimited) + Projection:["*"] reproduces the wire request of
		// the deprecated QueryAdsWithProjection(...,nil) exactly (no
		// ProjectionAttributes, no LimitResults).
		ads, _, err := s.collector().QueryAdsWithOptions(qctx, "Machine", s.cfg.SlotConstraint,
			&htcondor.QueryOptions{Limit: -1, Projection: []string{"*"}})
		if err != nil {
			slotErr = fmt.Errorf("query machine ads: %w", err)
			return
		}
		for _, ad := range ads {
			FixupSlot(ad, s.defaultWeight)
		}
		slots = ads
	}()

	go func() {
		defer wg.Done()
		ads, _, err := s.collector().QueryAdsWithOptions(qctx, "Submitter", s.cfg.SubmitterConstraint,
			&htcondor.QueryOptions{Limit: -1, Projection: []string{"*"}})
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
	// COLLECTOR_HOST may list several collectors; try each in order and fail
	// over on error, mirroring the C++ CollectorList (the htcondor client's
	// public queries already race the list, but the direct-CEDAR private-ad
	// query dials one address at a time). The first that answers wins.
	addrs := splitAddrs(s.cfg.CollectorAddr)
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no collector address configured")
	}
	var lastErr error
	for _, addr := range addrs {
		ads, err := s.queryPrivateAdsFrom(ctx, addr)
		if err == nil {
			return ads, nil
		}
		// A cancelled/expired context is terminal, not a per-collector failure:
		// stop rather than hammering the remaining addresses.
		if ctx.Err() != nil {
			return nil, err
		}
		lastErr = err
		s.log.Warn("private-ad query collector failed, trying next",
			"collector", addr, "error", err)
	}
	return nil, fmt.Errorf("all collectors failed for private-ad query: %w", lastErr)
}

// queryPrivateAdsFrom runs the QUERY_STARTD_PVT_ADS exchange against a single
// collector address.
func (s *RemoteSource) queryPrivateAdsFrom(ctx context.Context, addr string) ([]*classad.ClassAd, error) {
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

// splitAddrs parses an HTCondor-style COLLECTOR_HOST list into its individual
// collector addresses, in order, for the private-ad query's failover loop.
// Entries are separated by top-level commas or whitespace; empty entries are
// dropped.
//
// Bracket awareness: a sinful string "<host:port?k=v&...>" is an opaque blob
// whose CCB contacts are space-separated INSIDE the angle brackets, so a naive
// whitespace split would shatter it. We therefore track angle-bracket depth and
// only treat a comma or space at depth 0 as a separator (mirroring the htcondor
// client's own splitCollectorList; unbalanced brackets clamp at zero).
func splitAddrs(list string) []string {
	var (
		out   []string
		cur   strings.Builder
		depth int
	)
	flush := func() {
		if t := strings.TrimSpace(cur.String()); t != "" {
			out = append(out, t)
		}
		cur.Reset()
	}
	for _, r := range list {
		switch {
		case r == '<':
			depth++
			cur.WriteRune(r)
		case r == '>':
			if depth > 0 {
				depth--
			}
			cur.WriteRune(r)
		case depth == 0 && (r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'):
			flush()
		default:
			cur.WriteRune(r)
		}
	}
	flush()
	return out
}
