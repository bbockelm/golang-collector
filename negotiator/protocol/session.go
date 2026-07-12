// Package protocol implements the negotiator (client) side of HTCondor's
// NEGOTIATE (416) wire protocol: it drives one matchmaking conversation with a
// schedd on behalf of one submitter, over a cached warm cedar session.
//
// The peer this must interoperate with byte-for-byte is the schedd-side handler
// in golang-ap/internal/negotiate/negotiate.go (itself a faithful port of C++
// ScheddNegotiate). The framing mirrors that server exactly:
//
//   - Begin sends the NEGOTIATE header ClassAd. On a fresh socket the NEGOTIATE
//     command int rides the security handshake (SecurityConfig.Command); the
//     header is then a standalone CEDAR message (PutClassAd + EOM), which the
//     server reads with a fresh message (c.Message == nil). On a warm socket the
//     bare command int and the header travel in ONE message (PutInt + PutClassAd
//   - EOM): cedar's server reads the follow-on command int into c.Message and
//     the handler reads the header ad from that same message.
//   - Each inner opcode is its own CEDAR message.
//   - FetchRequests sends SEND_RESOURCE_REQUEST_LIST + N (or, in legacy mode,
//     one SEND_JOB_INFO at a time) and reads JOB_INFO+ad frames until it has N
//     or hits NO_MORE_JOBS.
//   - SendMatch sends PERMISSION_AND_AD + the claim string (a secret that rides
//     the encrypted session as a plain CEDAR string, the way golang-ap's
//     readMatch GetString expects) + the enriched slot ad.
//   - Reject sends REJECTED_WITH_REASON + the reason with the
//     " |<autocluster>|<cluster>.<proc>|" suffix golang-ap's parseRejectRep reads.
//   - End sends END_NEGOTIATE and returns the socket to the factory's cache.
package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"

	"github.com/bbockelm/golang-collector/negotiator"
)

// Negotiate-header attribute names (matchmaker.cpp:3958-4125).
const (
	attrOwner             = "Owner"
	attrAutoClusterAttrs  = "AutoClusterAttrs"
	attrSubmitterTag      = "SubmitterTag"
	attrNegotiatorName    = "NegotiatorName"
	attrMatchClaimedPSlot = "MatchClaimedPSlots"
	attrMatchCaps         = "MatchCaps"
	attrNegotiatorJobCons = "NegotiatorJobConstraint"
	matchCapsDiag3        = "MatchDiag3"
)

// Request-ad attribute names read out of each JOB_INFO ad.
const (
	attrClusterID            = "ClusterId"
	attrProcID               = "ProcId"
	attrAutoClusterID        = "AutoClusterId"
	attrResourceRequestCount = "_condor_RESOURCE_COUNT"
)

// session is one NEGOTIATE conversation. Not safe for concurrent use by itself;
// the cycle owns one session per submitter and drives it serially.
type session struct {
	f           *Factory
	submitter   string
	scheddName  string
	scheddAddr  string
	submitterAd *classad.ClassAd
	legacy      bool

	hdr  *negotiator.NegotiateHeader
	key  cacheKey
	conn *warmConn

	reused     bool // this round's socket came from the cache (warm)
	progressed bool // read at least one reply since the current (re)dial
	exhausted  bool // NO_MORE_JOBS seen this round
}

var _ negotiator.ScheddSession = (*session)(nil)

// Begin opens (or reuses) the session and sends the NEGOTIATE header. A cached
// warm socket is used first; if writing the bare-command header on it fails
// (the far side dropped the kept-alive socket between cycles) it transparently
// re-dials a fresh authenticated session and re-sends the header.
func (s *session) Begin(ctx context.Context, hdr *negotiator.NegotiateHeader) error {
	s.hdr = hdr
	s.key = cacheKey{addr: s.scheddAddr, tag: hdr.SubmitterTag}
	s.exhausted = false

	if wc := s.f.take(s.key); wc != nil {
		s.conn = wc
		s.reused = true
		s.progressed = false
		if err := s.sendHeader(ctx, true); err != nil {
			// The warm socket is dead; drop it and dial anew.
			wc.close()
			s.conn = nil
			return s.dialFresh(ctx)
		}
		return nil
	}
	return s.dialFresh(ctx)
}

// dialFresh establishes a new authenticated, encrypted session (the NEGOTIATE
// command int rides the handshake) and sends the header as a standalone message.
func (s *session) dialFresh(ctx context.Context) error {
	cfg := *s.f.sec // copy: never mutate the factory's shared config
	cfg.Command = commands.NEGOTIATE

	cl, err := client.ConnectAndAuthenticate(ctx, s.scheddAddr, &cfg)
	if err != nil {
		return fmt.Errorf("negotiate: dialing schedd %s: %w", s.scheddAddr, err)
	}
	s.conn = &warmConn{client: cl, stream: cl.GetStream()}
	s.reused = false
	s.progressed = false
	return s.sendHeader(ctx, false)
}

// sendHeader writes the NEGOTIATE header ClassAd. When warm, the bare NEGOTIATE
// command int precedes it in the same message (cedar reads it as the follow-on
// command on the kept-alive session).
func (s *session) sendHeader(ctx context.Context, warm bool) error {
	m := message.NewMessageForStream(s.conn.stream)
	if warm {
		if err := m.PutInt(ctx, commands.NEGOTIATE); err != nil {
			return fmt.Errorf("negotiate: writing warm command int: %w", err)
		}
	}
	if err := m.PutClassAd(ctx, s.buildHeaderAd()); err != nil {
		return fmt.Errorf("negotiate: writing header: %w", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		return fmt.Errorf("negotiate: finishing header: %w", err)
	}
	return nil
}

func (s *session) buildHeaderAd() *classad.ClassAd {
	ad := classad.New()
	_ = ad.Set(attrOwner, s.hdr.Owner)
	_ = ad.Set(attrAutoClusterAttrs, s.hdr.AutoClusterAttrs)
	_ = ad.Set(attrSubmitterTag, s.hdr.SubmitterTag)
	name := s.hdr.NegotiatorName
	if name == "" {
		name = s.f.negotiatorName
	}
	if name != "" {
		_ = ad.Set(attrNegotiatorName, name)
	}
	_ = ad.Set(attrMatchClaimedPSlot, true)
	_ = ad.Set(attrMatchCaps, matchCapsDiag3)
	if s.hdr.JobConstraint != "" {
		_ = ad.Set(attrNegotiatorJobCons, s.hdr.JobConstraint)
	}
	return ad
}

// FetchRequests pulls up to n resource requests. An empty slice signals
// NO_MORE_JOBS (the schedd's cursor is exhausted for this round). On the first
// exchange after a warm reuse, an EOF/reset (the far side closed the kept-alive
// socket) triggers a single transparent re-dial and retry.
func (s *session) FetchRequests(ctx context.Context, n int) ([]*negotiator.Request, error) {
	if s.exhausted {
		return nil, nil
	}
	reqs, err := s.fetchOnce(ctx, n)
	if err != nil && s.reused && !s.progressed && isDropped(err) {
		// Warm socket died before yielding anything: re-dial once.
		s.f.evict(s.key)
		if s.conn != nil {
			s.conn.close()
			s.conn = nil
		}
		if derr := s.dialFresh(ctx); derr != nil {
			return nil, derr
		}
		return s.fetchOnce(ctx, n)
	}
	return reqs, err
}

func (s *session) fetchOnce(ctx context.Context, n int) ([]*negotiator.Request, error) {
	if n <= 0 {
		return nil, nil
	}
	if s.legacy {
		return s.fetchLegacy(ctx, n)
	}
	// SEND_RESOURCE_REQUEST_LIST + N in one message.
	m := message.NewMessageForStream(s.conn.stream)
	if err := m.PutInt(ctx, commands.SEND_RESOURCE_REQUEST_LIST); err != nil {
		return nil, fmt.Errorf("negotiate: sending SEND_RESOURCE_REQUEST_LIST: %w", err)
	}
	if err := m.PutInt(ctx, n); err != nil {
		return nil, fmt.Errorf("negotiate: sending request count: %w", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		return nil, fmt.Errorf("negotiate: finishing SEND_RESOURCE_REQUEST_LIST: %w", err)
	}
	return s.readRequests(ctx, n)
}

// fetchLegacy drives the pre-8.3.0 one-at-a-time SEND_JOB_INFO exchange: each
// request is a separate opcode + single reply.
func (s *session) fetchLegacy(ctx context.Context, n int) ([]*negotiator.Request, error) {
	var out []*negotiator.Request
	for i := 0; i < n; i++ {
		m := message.NewMessageForStream(s.conn.stream)
		if err := m.PutInt(ctx, commands.SEND_JOB_INFO); err != nil {
			return out, fmt.Errorf("negotiate: sending SEND_JOB_INFO: %w", err)
		}
		if err := m.FinishMessage(ctx); err != nil {
			return out, fmt.Errorf("negotiate: finishing SEND_JOB_INFO: %w", err)
		}
		reqs, err := s.readRequests(ctx, 1)
		out = append(out, reqs...)
		if err != nil || s.exhausted {
			return out, err
		}
	}
	return out, nil
}

// readRequests reads reply messages until it has collected max requests or sees
// NO_MORE_JOBS. Each JOB_INFO reply is its own CEDAR message (opcode + ad).
func (s *session) readRequests(ctx context.Context, max int) ([]*negotiator.Request, error) {
	var out []*negotiator.Request
	for len(out) < max {
		resp := message.NewMessageFromStream(s.conn.stream)
		op, err := resp.GetInt(ctx)
		if err != nil {
			return out, fmt.Errorf("negotiate: reading reply opcode: %w", err)
		}
		switch op {
		case commands.NO_MORE_JOBS:
			s.exhausted = true
			s.progressed = true
			return out, nil
		case commands.JOB_INFO:
			ad, err := resp.GetClassAd(ctx)
			if err != nil {
				return out, fmt.Errorf("negotiate: reading job ad: %w", err)
			}
			out = append(out, parseRequest(ad))
			s.progressed = true
		default:
			return out, fmt.Errorf("negotiate: unexpected reply opcode %d (%s)", op, commands.GetCommandName(op))
		}
	}
	return out, nil
}

// parseRequest extracts the group bookkeeping the negotiator echoes back from a
// JOB_INFO ad.
func parseRequest(ad *classad.ClassAd) *negotiator.Request {
	r := &negotiator.Request{Ad: ad, Count: 1}
	if v, ok := ad.EvaluateAttrInt(attrClusterID); ok {
		r.Cluster = int(v)
	}
	if v, ok := ad.EvaluateAttrInt(attrProcID); ok {
		r.Proc = int(v)
	}
	if v, ok := ad.EvaluateAttrInt(attrAutoClusterID); ok {
		r.AutoClusterID = int(v)
	}
	if v, ok := ad.EvaluateAttrInt(attrResourceRequestCount); ok && v > 0 {
		r.Count = int(v)
	}
	return r
}

// SendMatch delivers PERMISSION_AND_AD: the secret claim string followed by the
// enriched slot ad. The claim string rides the encrypted session as a plain
// CEDAR string (golang-ap readMatch reads it with GetString); extra claims for
// p-slot splitting are appended space-separated. The resource-request id is
// stamped defensively in both spellings so the wire contract holds even if the
// caller passed an un-enriched ad.
func (s *session) SendMatch(ctx context.Context, mr *negotiator.MatchResult) error {
	if s.conn == nil {
		return errors.New("negotiate: SendMatch on a closed session")
	}
	claim := mr.ClaimID
	if len(mr.ExtraClaims) > 0 {
		claim = strings.Join(append([]string{mr.ClaimID}, mr.ExtraClaims...), " ")
	}
	ad := mr.SlotAd
	if ad == nil {
		ad = classad.New()
	}
	stampResourceRequest(ad, mr.Request)

	m := message.NewMessageForStream(s.conn.stream)
	if err := m.PutInt(ctx, commands.PERMISSION_AND_AD); err != nil {
		return fmt.Errorf("negotiate: sending PERMISSION_AND_AD: %w", err)
	}
	if err := m.PutString(ctx, claim); err != nil {
		return fmt.Errorf("negotiate: sending claim id: %w", err)
	}
	if err := m.PutClassAd(ctx, ad); err != nil {
		return fmt.Errorf("negotiate: sending match ad: %w", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		return fmt.Errorf("negotiate: finishing PERMISSION_AND_AD: %w", err)
	}
	return nil
}

// Reject delivers REJECTED_WITH_REASON for a request. The reason carries the
// " |<autocluster>|<cluster>.<proc>|" suffix that identifies the rejected group
// to the schedd (golang-ap parseRejectRep, negotiate.go:413-429).
func (s *session) Reject(ctx context.Context, req *negotiator.Request, reason string) error {
	if s.conn == nil {
		return errors.New("negotiate: Reject on a closed session")
	}
	full := fmt.Sprintf("%s |%d|%d.%d|", reason, req.AutoClusterID, req.Cluster, req.Proc)
	m := message.NewMessageForStream(s.conn.stream)
	if err := m.PutInt(ctx, commands.REJECTED_WITH_REASON); err != nil {
		return fmt.Errorf("negotiate: sending REJECTED_WITH_REASON: %w", err)
	}
	if err := m.PutString(ctx, full); err != nil {
		return fmt.Errorf("negotiate: sending reject reason: %w", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		return fmt.Errorf("negotiate: finishing REJECTED_WITH_REASON: %w", err)
	}
	return nil
}

// End sends END_NEGOTIATE and returns the socket to the cache for warm reuse
// next cycle. The session must not be used again after End.
func (s *session) End(ctx context.Context) error {
	if s.conn == nil {
		return errors.New("negotiate: End on a closed session")
	}
	m := message.NewMessageForStream(s.conn.stream)
	if err := m.PutInt(ctx, commands.END_NEGOTIATE); err != nil {
		s.discard()
		return fmt.Errorf("negotiate: sending END_NEGOTIATE: %w", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		s.discard()
		return fmt.Errorf("negotiate: finishing END_NEGOTIATE: %w", err)
	}
	s.f.put(s.key, s.conn)
	s.conn = nil
	return nil
}

// Close discards the session without caching it (the protocol-error path).
func (s *session) Close() error {
	s.discard()
	return nil
}

func (s *session) discard() {
	if s.conn != nil {
		s.conn.close()
		s.conn = nil
	}
}

// isDropped reports whether err indicates the peer closed the socket (so a warm
// reuse should transparently re-dial).
func isDropped(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return true
	}
	// TCP resets / broken pipes surface as *net.OpError; match on the message
	// so we don't depend on syscall constants across platforms.
	msg := err.Error()
	return strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection closed") ||
		strings.Contains(msg, "use of closed")
}
