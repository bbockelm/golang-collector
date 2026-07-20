package server

import (
	"context"
	"errors"
	"iter"
	"log/slog"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections"

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/cedar/watch"

	"github.com/bbockelm/golang-collector/store"
)

// New builds a cedar command-dispatch server that serves the collector protocol
// against st: UPDATE_*_AD commands feed the store, QUERY_*_ADS scan it, and
// INVALIDATE_*_ADS prune it. sec is the CEDAR security policy (may be a
// plaintext/no-auth config for testing). fwd, if non-nil, relays every update and
// invalidation on to the configured CONDOR_VIEW_HOST collectors (a nil fwd
// forwards nothing). The returned server is ready to Serve.
func New(st store.Backend, sec *security.SecurityConfig, fwd *Forwarder) *cedarserver.Server {
	cs := cedarserver.New(sec)
	Register(cs, st, fwd)
	return cs
}

// Register adds the collector protocol handlers -- UPDATE_*_AD (feed the store),
// QUERY_*_ADS (scan it), INVALIDATE_*_ADS (prune it), the multi-ad queries, and the
// ack'd update -- to an existing cedar command-dispatch server. A host daemon that
// wants to serve the collector on its own already-established command socket
// registers here (see the embeddable Collector's RegisterOn); New wraps this for the
// standalone case. fwd, if non-nil, relays updates/invalidations to CONDOR_VIEW_HOST
// collectors. It registers only the collector protocol; DC_* default commands (NOP,
// CONFIG_VAL, ...) are the host's responsibility.
func Register(cs *cedarserver.Server, st store.Backend, fwd *Forwarder) {
	// Every command is registered at its HTCondor authorization level (CommandLevel):
	// QUERY_*_ADS at READ, UPDATE_*/INVALIDATE_* at ADVERTISE, private-ad queries at
	// NEGOTIATOR. The cedar server uses these both to authorize a session per command
	// and (via SecurityConfigForCommand) to negotiate each command at its own level --
	// so monitoring (READ) can be permissive while publishing (ADVERTISE) requires auth.
	for cmd, t := range updateCommands {
		cs.Handle(cmd, updateHandler(st, t, cmd, fwd), CommandLevel(cmd))
	}
	// UPDATE_STARTD_AD_WITH_ACK: the sender blocks for a one-int acknowledgment
	// that the ad was stored.
	cs.Handle(commands.UPDATE_STARTD_AD_WITH_ACK, ackUpdateHandler(st, store.StartdAd, fwd), CommandLevel(commands.UPDATE_STARTD_AD_WITH_ACK))
	for cmd, t := range queryCommands {
		cs.Handle(cmd, queryHandler(st, t), CommandLevel(cmd))
	}
	// QUERY_MULTIPLE_ADS / QUERY_MULTIPLE_PVT_ADS: one query ad names several
	// target types (TargetType is a list) with optional per-type constraints; the
	// negotiator uses these every cycle to fetch Submitter + Machine ads at once.
	cs.Handle(commands.QUERY_MULTIPLE_ADS, multiQueryHandler(st), CommandLevel(commands.QUERY_MULTIPLE_ADS))
	cs.Handle(commands.QUERY_MULTIPLE_PVT_ADS, multiQueryHandler(st), CommandLevel(commands.QUERY_MULTIPLE_PVT_ADS))
	for cmd, t := range invalidateCommands {
		cs.Handle(cmd, invalidateHandler(st, t, cmd, fwd), CommandLevel(cmd))
	}
	// WatchAds: subscribe to a table's change stream (resumable, cursor-based).
	cs.Handle(watch.WatchAds, watchHandler(st), CommandLevel(watch.WatchAds))
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
func updateHandler(st store.Backend, t store.AdType, cmd int, fwd *Forwarder) cedarserver.HandlerFunc {
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
		// The startd's public and private ads arrive in the SAME message
		// (finishUpdate puts both under one end_of_message). Read the optional
		// private ad now -- before storing the public one -- so the stream stays
		// aligned for the next command even when a bad public ad is skipped below.
		// io.EOF (empty pvt) means there is none.
		pvt, havePvt := "", false
		if t == store.StartdAd {
			if p, perr := msg.GetClassAdRaw(ctx); perr == nil && p != "" {
				pvt, havePvt = p, true
			}
		}
		if err := st.UpdateOldText(ctx, t, text); err != nil {
			if isCanceled(err) {
				// The context was cancelled (the collector is shutting down, or the
				// client went away) -- the ad was not rejected, the operation was
				// aborted. End the handler cleanly instead of logging a misleading
				// "rejected ad" warning per in-flight update.
				return err
			}
			// A single unparseable/rejected ad must not tear down the persistent
			// command socket (which would drop every following update from this
			// daemon) -- log the offending ad and skip it, keeping the session up.
			slog.Warn("collector: rejected ad update; skipping (connection kept open)",
				"type", t.String(), "name", store.AdName(text), "err", err, "ad", store.AdExcerpt(text))
			return nil
		}
		// Relay the public ad to any CONDOR_VIEW_HOST collectors (never the
		// private ad -- claim ids must not leak to a monitoring collector). Only a
		// stored (well-formed) ad is forwarded, so a bad ad cannot poison the
		// downstream collector.
		fwd.forwardText(cmd, text)
		if havePvt {
			if err := st.UpdatePvt(ctx, text, pvt); err != nil {
				if isCanceled(err) {
					return err // shutting down / connection gone -- not a rejection
				}
				slog.Warn("collector: rejected private ad update; skipping (connection kept open)",
					"type", t.String(), "name", store.AdName(text), "err", err, "ad", store.AdExcerpt(pvt))
			}
		}
		return nil
	}
}

// isCanceled reports whether err is a context cancellation or deadline -- i.e. the
// operation was aborted (collector shutdown, or the peer went away), not rejected.
// Such an error is not a bad ad and must not be logged as one.
func isCanceled(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
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
// Because the sender treats the ack as "committed", the store must be durable
// before we reply -- store.DurableUpdate bypasses any Nagle buffering (which would
// otherwise ack a merely-buffered write).
func ackUpdateHandler(st store.Backend, t store.AdType, fwd *Forwarder) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		msg := message.NewMessageFromStream(c.Stream)
		text, err := msg.GetClassAdRaw(ctx)
		if err != nil {
			return err
		}
		if err := store.DurableUpdate(ctx, st, t, text); err != nil {
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
func queryHandler(st store.Backend, t store.AdType) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		req := message.NewMessageFromStream(c.Stream)
		queryAd, err := req.GetClassAd(ctx)
		if err != nil {
			return err
		}
		constraint, projection, limit := parseQuery(queryAd)

		// Redact private (secret) attributes from every public table's response.
		// The StartdPvt table is the authorized private channel (served only via
		// QUERY_STARTD_PVT_ADS to the negotiator), so its claim ids must pass
		// through unredacted -- serving them is the whole point of that table.
		redact := t != store.StartdPvtAd

		// Whole-ad (unprojected) queries take the wire-form fast path when the
		// backend offers one (the in-memory store; see store.RawQueryer): stream
		// stored bytes straight out without materializing each ad. Backends
		// without it fall through to the materialized Query path below.
		rq, rawOK := st.(store.RawQueryer)
		prq, prqOK := st.(store.ProjectedRawQueryer)

		resp := message.NewMessageForStream(c.Stream)
		n := 0
		// sendRaw streams already-wire-form RawAds (whole-ad or projected),
		// redacting private attributes for the public tables.
		sendRaw := func(raw iter.Seq[collections.RawAd]) error {
			for ra := range raw {
				if limit > 0 && n >= limit {
					break
				}
				exprs := ra.Exprs
				if redact {
					exprs = redactRawExprs(exprs)
				}
				if err := resp.PutInt32(ctx, 1); err != nil {
					return err
				}
				if err := resp.PutClassAdRawBytes(ctx, exprs, ra.MyType, ra.TargetType); err != nil {
					return err
				}
				n++
			}
			return nil
		}
		switch {
		case len(projection) == 0 && rawOK:
			// Whole-ad wire-form fast path: stream stored bytes straight out.
			raw, err := rq.QueryRaw(ctx, t, constraint, limit)
			if err != nil {
				return err
			}
			if err := sendRaw(raw); err != nil {
				return err
			}
		case len(projection) > 0 && prqOK:
			// Projected wire-form fast path: the backend applies the projection (a
			// remote database pushes it down), so only the requested attributes cross
			// the wire -- no whole-ad fetch + local project.
			raw, err := prq.QueryRawProject(ctx, t, constraint, projection, limit)
			if err != nil {
				return err
			}
			if err := sendRaw(raw); err != nil {
				return err
			}
		default:
			ads, err := st.Query(ctx, t, constraint, limit)
			if err != nil {
				return err
			}
			for ad := range ads {
				if limit > 0 && n >= limit {
					break
				}
				if err := resp.PutInt32(ctx, 1); err != nil {
					return err
				}
				if err := putAd(ctx, resp, project(ad, projection), redact); err != nil {
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
func multiQueryHandler(st store.Backend) cedarserver.HandlerFunc {
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
			constraint, projection, limit := parseSubQuery(queryAd, target)
			// Multi-queries only name public target types (the negotiator gets startd
			// claim caps through the dedicated QUERY_STARTD_PVT_ADS command), so every
			// response here is redacted.
			redact := adType != store.StartdPvtAd
			ads, err := st.Query(ctx, adType, constraint, limit)
			if err != nil {
				return err
			}
			n := 0
			for ad := range ads {
				if limit > 0 && n >= limit {
					break
				}
				if err := resp.PutInt32(ctx, 1); err != nil {
					return err
				}
				if err := putAd(ctx, resp, project(ad, projection), redact); err != nil {
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

// putAd writes ad to resp, excluding private (secret) attributes when redact is
// set (every public-table response) and sending the full ad otherwise (the
// authorized StartdPvt channel). It is the single materialized-ad write path for
// the query handlers, so a client-facing response cannot forget to redact.
func putAd(ctx context.Context, resp *message.Message, ad *classad.ClassAd, redact bool) error {
	if redact {
		return resp.PutClassAd(ctx, ad) // serialization redacts private attributes by default
	}
	// The authorized StartdPvt channel must send the claim ids it exists to serve.
	return resp.PutClassAdWithOptions(ctx, ad, &message.PutClassAdConfig{
		Options: message.PutClassAdIncludePrivate,
	})
}

// rawExprAttrName extracts the attribute name from a rendered "Name = value"
// expression line (leading whitespace trimmed, name up to the first '=' or
// space). It does not allocate beyond the returned name.
func rawExprAttrName(expr []byte) string {
	i := 0
	for i < len(expr) && (expr[i] == ' ' || expr[i] == '\t') {
		i++
	}
	start := i
	for i < len(expr) && expr[i] != '=' && expr[i] != ' ' && expr[i] != '\t' {
		i++
	}
	return string(expr[start:i])
}

// redactRawExprs returns exprs with private (secret) attributes removed. The
// [][]byte form makes redaction a cheap filter-and-count with no re-encoding.
// When nothing is private (the common case -- a public ad holds no claim ids),
// the original slice is returned unchanged, so the fast path stays allocation-free.
func redactRawExprs(exprs [][]byte) [][]byte {
	first := -1
	for i, e := range exprs {
		if message.ClassAdAttributeIsPrivateAny(rawExprAttrName(e)) {
			first = i
			break
		}
	}
	if first < 0 {
		return exprs // nothing private
	}
	out := make([][]byte, first, len(exprs)-1)
	copy(out, exprs[:first])
	for _, e := range exprs[first+1:] {
		if message.ClassAdAttributeIsPrivateAny(rawExprAttrName(e)) {
			continue
		}
		out = append(out, e)
	}
	return out
}

// invalidateHandler reads an invalidation ad and removes matching ads: by its
// Requirements constraint if present, otherwise the single ad it identifies.
func invalidateHandler(st store.Backend, t store.AdType, cmd int, fwd *Forwarder) cedarserver.HandlerFunc {
	return func(ctx context.Context, c *cedarserver.Conn) error {
		msg := message.NewMessageFromStream(c.Stream)
		ad, err := msg.GetClassAd(ctx)
		if err != nil {
			return err
		}
		constraint, _, _ := parseQuery(ad)
		if constraint == "" {
			if _, err := st.Invalidate(ctx, t, "", ad); err != nil {
				return err
			}
		} else {
			if _, err := st.Invalidate(ctx, t, constraint, nil); err != nil {
				return err
			}
		}
		// Relay the invalidation so view collectors drop the same ads.
		fwd.forward(cmd, ad)
		return nil
	}
}
