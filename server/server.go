package server

import (
	"context"
	"log/slog"

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
	// QUERY_MULTIPLE_ADS / QUERY_MULTIPLE_PVT_ADS: one query ad names several
	// target types (TargetType is a list) with optional per-type constraints; the
	// negotiator uses these every cycle to fetch Submitter + Machine ads at once.
	cs.Handle(commands.QUERY_MULTIPLE_ADS, multiQueryHandler(st))
	cs.Handle(commands.QUERY_MULTIPLE_PVT_ADS, multiQueryHandler(st))
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
		// Keep the connection open for follow-on updates on the same session:
		// HTCondor daemons stream several updates down one persistent command
		// socket (e.g. the schedd sends its schedd ad, then a submitter ad per
		// user, each as another raw command int -- no re-authentication).
		c.KeepAlive()

		msg := adMessage(c)
		text, err := msg.GetClassAdRaw(ctx)
		if err != nil {
			return err
		}
		if err := st.UpdateOldText(t, text); err != nil {
			slog.Warn("collector: dropped update", "type", t.String(), "err", err)
			return err
		}
		// Relay the public ad to any CONDOR_VIEW_HOST collectors (never the
		// private ad below -- claim ids must not leak to a monitoring collector).
		fwd.forwardText(cmd, text)
		if t == store.StartdAd {
			// The startd's public and private ads arrive in the SAME message
			// (finishUpdate puts both under one end_of_message), so read the
			// optional private ad from that same message; io.EOF means none.
			if pvt, err := msg.GetClassAdRaw(ctx); err == nil && pvt != "" {
				if err := st.UpdatePvt(text, pvt); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

// adMessage returns the message a handler should read its ad from: the message
// the (follow-on) command integer was already read from, or a fresh message for
// the first command (whose ad the peer sends as a separate message).
func adMessage(c *cedarserver.Conn) *message.Message {
	if c.Message != nil {
		return c.Message
	}
	return message.NewMessageFromStream(c.Stream)
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
		if len(projection) == 0 {
			// Fast path: no projection means whole ads, so stream them straight
			// from the stored wire form (PutClassAdRaw) without decoding each into
			// a *classad.ClassAd -- ~2x faster on realistic ads.
			for ra := range st.QueryRaw(t, q) {
				if limit > 0 && n >= limit {
					break
				}
				if err := resp.PutInt32(ctx, 1); err != nil {
					return err
				}
				if err := resp.PutClassAdRawBytes(ctx, ra.Exprs, ra.MyType, ra.TargetType); err != nil {
					return err
				}
				n++
			}
		} else {
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
		}
		if err := resp.PutInt32(ctx, 0); err != nil {
			return err
		}
		return resp.FlushFrame(ctx, true)
	}
}

// multiQueryHandler serves QUERY_MULTIPLE_ADS / QUERY_MULTIPLE_PVT_ADS: the query
// ad's TargetType is a comma-separated list of target types, each with an optional
// per-type constraint (<Type>Requirements), projection and limit. It streams the
// matching ads from every named table as one flat sequence -- PutInt32(1)+ad per
// ad, PutInt32(0) terminator -- the same framing as a single-type query.
//
// The private-attr variant is served identically: the negotiator obtains startd
// claim capabilities through the dedicated QUERY_STARTD_PVT_ADS command, so the
// multi-query only needs the public ads of each target table.
func multiQueryHandler(st *store.Store) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		queryAd, err := message.NewMessageFromStream(c.Stream).GetClassAd(ctx)
		if err != nil {
			return err
		}
		targets, _ := queryAd.EvaluateAttrString(attrTargetType)

		resp := message.NewMessageForStream(c.Stream)
		for _, target := range splitAttrs(targets) {
			adType, ok := store.AdTypeForTarget(target)
			if !ok {
				continue // unknown target type: skip it, like the C++ collector
			}
			q, projection, limit, err := parseSubQuery(queryAd, target)
			if err != nil {
				return err
			}
			n := 0
			for ad := range st.Query(adType, q) {
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
