package server

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"time"

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
		start := time.Now()
		uerr := st.UpdateOldText(ctx, t, text)
		elapsed := time.Since(start)
		store.ObserveUpdate("public", elapsed)
		store.MaybeLogSlow("update", elapsed, "type", t.String(), "name", store.AdName(text))
		if uerr != nil {
			if isCanceled(uerr) {
				// The context was cancelled (the collector is shutting down, or the
				// client went away) -- the ad was not rejected, the operation was
				// aborted. End the handler cleanly instead of logging a misleading
				// "rejected ad" warning per in-flight update. The elapsed time (logged
				// at debug) distinguishes an update that was merely in flight when
				// shutdown hit (~0) from one stuck retrying against a slow/down database.
				slog.Debug("collector: ad update aborted (context cancelled)",
					"type", t.String(), "name", store.AdName(text), "elapsed", elapsed)
				return uerr
			}
			// A single unparseable/rejected ad must not tear down the persistent
			// command socket (which would drop every following update from this
			// daemon) -- log the offending ad and skip it, keeping the session up.
			slog.Warn("collector: rejected ad update; skipping (connection kept open)",
				"type", t.String(), "name", store.AdName(text), "elapsed", elapsed, "err", uerr, "ad", store.AdExcerpt(text))
			return nil
		}
		// Relay the public ad to any CONDOR_VIEW_HOST collectors (never the
		// private ad -- claim ids must not leak to a monitoring collector). Only a
		// stored (well-formed) ad is forwarded, so a bad ad cannot poison the
		// downstream collector.
		fwd.forwardText(cmd, text)
		if havePvt {
			pstart := time.Now()
			perr := st.UpdatePvt(ctx, text, pvt)
			elapsed := time.Since(pstart)
			store.ObserveUpdate("private", elapsed)
			store.MaybeLogSlow("update-pvt", elapsed, "type", t.String(), "name", store.AdName(text))
			if perr != nil {
				if isCanceled(perr) {
					// Aborted (shutdown / peer gone), not rejected. elapsed at debug
					// shows whether it was in flight (~0) or stuck retrying.
					slog.Debug("collector: private ad update aborted (context cancelled)",
						"type", t.String(), "name", store.AdName(text), "elapsed", elapsed)
					return perr
				}
				slog.Warn("collector: rejected private ad update; skipping (connection kept open)",
					"type", t.String(), "name", store.AdName(text), "elapsed", elapsed, "err", perr, "ad", store.AdExcerpt(pvt))
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
		// Keep the connection open and read from the follow-on frame (c.Message),
		// like updateHandler: a startd may stream several WITH_ACK updates down one
		// persistent socket, each a raw command int + ad, waiting for the ack
		// between them. Without this the socket closes after the first ack and the
		// follow-on frame is misread.
		c.KeepAlive()

		msg := adMessage(c)
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

		resp := message.NewMessageForStream(c.Stream)
		if err := relayMatches(ctx, resp, st, t, constraint, projection, limit, redact); err != nil {
			return err
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
			if err := relayMatches(ctx, resp, st, adType, constraint, projection, limit, redact); err != nil {
				return err
			}
		}
		if err := resp.PutInt32(ctx, 0); err != nil {
			return err
		}
		return resp.FlushFrame(ctx, true)
	}
}

// relayMatches writes every ad in table t matching constraint to resp -- PutInt32(1)
// then the ad, up to limit -- projected to projection and redacting private attributes
// when redact is set. It does NOT write the trailing PutInt32(0) terminator; the caller
// does, after its last table (so a multi-target query streams several tables as one flat
// sequence with a single terminator).
//
// It prefers the wire-form STREAMING fast path (RawStreamer / ProjectedRawStreamer --
// relay each stored ad's bytes as it arrives, no buffering, a mid-stream failure surfaces
// as an error), then the buffered wire-form path (RawQueryer / ProjectedRawQueryer), then
// the materialized Query path (decode + re-encode). Shared by the single-type and
// multi-type (negotiator) query handlers so both get the same fast paths and the same
// wire encoding.
func relayMatches(ctx context.Context, resp *message.Message, st store.Backend, t store.AdType, constraint string, projection []string, limit int, redact bool) error {
	n := 0
	var sendErr error
	// srcRedacted: the backend guaranteed no private attribute reaches sendOne (it
	// stripped them at the source via its intern table's per-id private flags), so
	// the per-ad redaction scan below is skipped.
	srcRedacted := false
	var typeAttrs rawTypeAttrs // cached MyType/TargetType prefix, reused across ads
	// sendOne relays one already-wire-form RawAd, returning false to stop the scan (limit
	// reached, or a client write failed -- then sendErr is set).
	sendOne := func(ra collections.RawAd) bool {
		if limit > 0 && n >= limit {
			return false
		}
		exprs := ra.Exprs
		if redact && !srcRedacted {
			exprs = redactRawExprs(exprs)
		}
		// Convey MyType/TargetType exactly as a C++ collector does: as ordinary ATTRIBUTES
		// in the ad body, with EMPTY trailing type strings (the C++ _putClassAdTrailingInfo
		// "always send[s] empty strings for the special-case MyType/TargetType values at the
		// end of the ad"). rawAdFromOldText lifts these out of Exprs into RawAd fields;
		// without re-adding them as attributes the C++ negotiator sees typeless ads in a
		// multi-query and buckets "0 submitter, 0 startd", matching nothing.
		exprs = typeAttrs.with(exprs, ra.MyType, ra.TargetType)
		if err := resp.PutInt32(ctx, 1); err != nil {
			sendErr = err
			return false
		}
		if err := resp.PutClassAdRawBytes(ctx, exprs, "", ""); err != nil {
			sendErr = err
			return false
		}
		n++
		return true
	}
	sendRaw := func(raw iter.Seq[collections.RawAd]) error {
		for ra := range raw {
			if !sendOne(ra) {
				break
			}
		}
		return sendErr
	}

	// Wire-row relay, the preferred path when the backend offers it: rows arrive
	// as self-contained wire-form subset ads -- projection and redaction already
	// applied at the source, many rows per transport frame -- and the ONLY
	// old-ClassAd render happens right here, at the client edge. A failure before
	// any row was relayed (an older database without the op, a wrapped backend
	// without the capability) falls through to the text paths below; a mid-stream
	// failure cannot (rows already went out) and fails the query.
	if wrs, ok := st.(store.WireRowStreamer); ok {
		var rbuf []byte
		var roffs []int
		var rexprs [][]byte
		relayed := false
		werr := wrs.QueryRawWireStream(ctx, t, constraint, projection, limit, redact, func(row []byte) bool {
			if limit > 0 && n >= limit {
				return false
			}
			var mt, tt string
			var rok bool
			rbuf, roffs, mt, tt, rok = collections.RenderRawAdInline(row, rbuf, roffs)
			if !rok {
				return true // malformed row: skip it, keep the stream
			}
			rexprs = rexprs[:0]
			for i := 0; i+1 < len(roffs); i++ {
				rexprs = append(rexprs, rbuf[roffs[i]:roffs[i+1]])
			}
			out := typeAttrs.with(rexprs, mt, tt)
			if err := resp.PutInt32(ctx, 1); err != nil {
				sendErr = err
				return false
			}
			if err := resp.PutClassAdRawBytes(ctx, out, "", ""); err != nil {
				sendErr = err
				return false
			}
			relayed = true
			n++
			return true
		})
		switch {
		case sendErr != nil:
			return sendErr
		case werr == nil:
			return nil
		case relayed:
			return werr // rows already sent: a fallback would duplicate them
		}
		// else: failed before anything was relayed -- fall through to the text paths.
	}

	if len(projection) == 0 {
		// Redaction pushdown: a backend that can guarantee no-private-attributes in
		// its raw results (source-side stripping, an O(1) per-attribute flag check)
		// spares this relay its own per-ad attribute scan. Probed at call time --
		// a wrapper (BufferedBackend) always has the method but reports
		// ErrRedactionNotSupported when what it wraps cannot deliver the guarantee.
		if redact {
			if rrq, ok := st.(store.RedactedRawQueryer); ok {
				raw, err := rrq.QueryRawRedacted(ctx, t, constraint, limit)
				if err == nil {
					srcRedacted = true
					return sendRaw(raw)
				}
				if !errors.Is(err, store.ErrRedactionNotSupported) {
					return err
				}
			}
		}
		if rs, ok := st.(store.RawStreamer); ok {
			if err := rs.QueryRawStream(ctx, t, constraint, limit, sendOne); err != nil {
				return err
			}
			return sendErr
		}
		if rq, ok := st.(store.RawQueryer); ok {
			raw, err := rq.QueryRaw(ctx, t, constraint, limit)
			if err != nil {
				return err
			}
			return sendRaw(raw)
		}
	} else {
		if redact {
			if prq, ok := st.(store.RedactedProjectedRawQueryer); ok {
				raw, err := prq.QueryRawProjectRedacted(ctx, t, constraint, projection, limit)
				if err == nil {
					srcRedacted = true
					return sendRaw(raw)
				}
				if !errors.Is(err, store.ErrRedactionNotSupported) {
					return err
				}
			}
		}
		if prs, ok := st.(store.ProjectedRawStreamer); ok {
			if err := prs.QueryRawProjectStream(ctx, t, constraint, projection, limit, sendOne); err != nil {
				return err
			}
			return sendErr
		}
		if prq, ok := st.(store.ProjectedRawQueryer); ok {
			raw, err := prq.QueryRawProject(ctx, t, constraint, projection, limit)
			if err != nil {
				return err
			}
			return sendRaw(raw)
		}
	}

	// Materialized fallback: decode each ad, project locally, re-encode via putAd (which
	// writes MyType/TargetType as attributes itself).
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
	return nil
}

// rawTypeAttrs is withRawTypeAttrs with per-response caching: every ad of one
// response nearly always carries the same MyType/TargetType ("Machine"/"Job"),
// so the rendered attribute lines are rebuilt only when the value changes and
// one prefix slice is reused across ads -- zero allocations per ad after the
// first (the old per-ad path was ~5 allocations per ad, a dominant GC source on
// large responses). The returned slice aliases the reused scratch, valid until
// the next with call -- the same contract as RawAd.Exprs itself.
type rawTypeAttrs struct {
	scratch      [][]byte
	mtVal, ttVal string
	mt, tt       []byte
}

func (r *rawTypeAttrs) with(exprs [][]byte, myType, targetType string) [][]byte {
	if myType == "" && targetType == "" {
		return exprs
	}
	r.scratch = r.scratch[:0]
	if myType != "" {
		if myType != r.mtVal || r.mt == nil {
			r.mtVal = myType
			r.mt = []byte(`MyType = "` + myType + `"`)
		}
		r.scratch = append(r.scratch, r.mt)
	}
	if targetType != "" {
		if targetType != r.ttVal || r.tt == nil {
			r.ttVal = targetType
			r.tt = []byte(`TargetType = "` + targetType + `"`)
		}
		r.scratch = append(r.scratch, r.tt)
	}
	r.scratch = append(r.scratch, exprs...)
	return r.scratch
}

// withRawTypeAttrs prepends MyType/TargetType as attribute lines to a RawAd's expressions
// so a receiver reads the ad's type from an attribute (what the C++ negotiator and modern
// tools expect), not from the old-protocol trailing type strings (which a C++ collector
// always sends empty). rawAdFromOldText lifts these out of the stored expressions into
// RawAd fields; this puts them back on the wire. Empty values are omitted.
func withRawTypeAttrs(exprs [][]byte, myType, targetType string) [][]byte {
	if myType == "" && targetType == "" {
		return exprs
	}
	out := make([][]byte, 0, len(exprs)+2)
	if myType != "" {
		out = append(out, []byte(`MyType = "`+myType+`"`))
	}
	if targetType != "" {
		out = append(out, []byte(`TargetType = "`+targetType+`"`))
	}
	return append(out, exprs...)
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
		// Keep the connection open for follow-on commands. HTCondor collectors
		// forward UPDATE_* and INVALIDATE_* interleaved down ONE persistent command
		// socket (a view collector's feed). Without this, the server closes the
		// socket after an invalidation and the sender only discovers it a few
		// buffered writes later -- "condor_write(): Socket closed ... to collector".
		c.KeepAlive()

		// Read from the follow-on message that already carried the command int
		// (c.Message), exactly as updateHandler does. A fresh NewMessageFromStream
		// would read the wrong frame on a kept-alive socket, desyncing the stream
		// (the invalidation would miss and every following forwarded ad be lost).
		msg := adMessage(c)
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
