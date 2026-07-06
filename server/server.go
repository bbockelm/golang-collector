package server

import (
	"context"

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/golang-collector/store"
)

// New builds a cedar command-dispatch server that serves the collector protocol
// against st: UPDATE_*_AD commands feed the store, QUERY_*_ADS scan it, and
// INVALIDATE_*_ADS prune it. sec is the CEDAR security policy (may be a
// plaintext/no-auth config for testing). fwd, if non-nil, relays every update and
// invalidation on to the configured CONDOR_VIEW_HOST collectors (a nil fwd
// forwards nothing). The returned server is ready to Serve.
func New(st *store.Store, sec *security.SecurityConfig, fwd *Forwarder) *cedarserver.Server {
	cs := cedarserver.New(sec)
	for cmd, t := range updateCommands {
		cs.Handle(cmd, updateHandler(st, t, cmd, fwd))
	}
	// UPDATE_STARTD_AD_WITH_ACK: the sender blocks for a one-int acknowledgment
	// that the ad was stored.
	cs.Handle(commands.UPDATE_STARTD_AD_WITH_ACK, ackUpdateHandler(st, store.StartdAd, fwd))
	for cmd, t := range queryCommands {
		cs.Handle(cmd, queryHandler(st, t))
	}
	for cmd, t := range invalidateCommands {
		cs.Handle(cmd, invalidateHandler(st, t, cmd, fwd))
	}
	return cs
}

// updateHandler stores one ad update into table t. Each ad is its own command
// (one command + handshake + ad per connection), and the ad streams straight
// into the collection's wire form via UpdateOld, never materializing a
// *classad.ClassAd.
//
// For UPDATE_STARTD_AD the startd may append a second, *private* ad on the same
// connection (claim ids etc. that only the negotiator may query); it is stored
// in the StartdPvtAd table keyed the same as the public ad. Its absence (the
// common case -- most clients send only the public ad and close) is fine.
func updateHandler(st *store.Store, t store.AdType, cmd int, fwd *Forwarder) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		text, err := message.NewMessageFromStream(c.Stream).GetClassAdRaw(ctx)
		if err != nil {
			return err
		}
		if err := st.UpdateOldText(t, text); err != nil {
			return err
		}
		// Relay the public ad to any CONDOR_VIEW_HOST collectors (never the
		// private ad below -- claim ids must not leak to a monitoring collector).
		fwd.forwardText(cmd, text)
		if t == store.StartdAd {
			// Optional private ad follows; EOF (peer closed) means there is none.
			if pvt, err := message.NewMessageFromStream(c.Stream).GetClassAdRaw(ctx); err == nil && pvt != "" {
				if err := st.UpdatePvt(text, pvt); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// ackUpdateHandler stores one ad and returns a one-int acknowledgment, for the
// UPDATE_*_WITH_ACK commands whose sender blocks until the collector confirms.
func ackUpdateHandler(st *store.Store, t store.AdType, fwd *Forwarder) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		msg := message.NewMessageFromStream(c.Stream)
		text, err := msg.GetClassAdRaw(ctx)
		if err != nil {
			return err
		}
		if err := st.UpdateOldText(t, text); err != nil {
			return err
		}
		fwd.forwardText(commands.UPDATE_STARTD_AD, text)
		ack := message.NewMessageForStream(c.Stream)
		if err := ack.PutInt32(ctx, 1); err != nil {
			return err
		}
		return ack.FinishMessage(ctx)
	}
}

// queryHandler reads a query ad, evaluates its constraint against table t, and
// streams matching ads back as PutInt32(1)+PutClassAd per ad, terminated by
// PutInt32(0) -- the framing the collector query protocol expects.
func queryHandler(st *store.Store, t store.AdType) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		req := message.NewMessageFromStream(c.Stream)
		queryAd, err := req.GetClassAd(ctx)
		if err != nil {
			return err
		}
		q, projection, limit, err := parseQuery(queryAd)
		if err != nil {
			return err
		}

		resp := message.NewMessageForStream(c.Stream)
		n := 0
		for ad := range st.Query(t, q) {
			if limit > 0 && n >= limit {
				break
			}
			if err := resp.PutInt32(ctx, 1); err != nil {
				return err
			}
			if err := resp.PutClassAd(ctx, project(ad, projection)); err != nil {
				return err
			}
			n++
		}
		if err := resp.PutInt32(ctx, 0); err != nil {
			return err
		}
		return resp.FlushFrame(ctx, true)
	}
}

// invalidateHandler reads an invalidation ad and removes matching ads: by its
// Requirements constraint if present, otherwise the single ad it identifies.
func invalidateHandler(st *store.Store, t store.AdType, cmd int, fwd *Forwarder) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		msg := message.NewMessageFromStream(c.Stream)
		ad, err := msg.GetClassAd(ctx)
		if err != nil {
			return err
		}
		q, _, _, err := parseQuery(ad)
		if err != nil {
			return err
		}
		if q == nil {
			st.Invalidate(t, nil, ad)
		} else {
			st.Invalidate(t, q, nil)
		}
		// Relay the invalidation so view collectors drop the same ads.
		fwd.forward(cmd, ad)
		return nil
	}
}
