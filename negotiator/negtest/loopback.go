// Package negtest provides an in-process, pure-Go schedd-side NEGOTIATE server
// for exercising the negotiator's client-side protocol without a real schedd.
//
// LoopbackSchedd is a faithful re-implementation of the wire behavior in
// golang-ap/internal/negotiate/negotiate.go (which is internal to another
// module and cannot be imported): it reads the NEGOTIATE header, answers
// SEND_RESOURCE_REQUEST_LIST / SEND_JOB_INFO with batched JOB_INFO ads carrying
// _condor_RESOURCE_COUNT (one NO_MORE_JOBS at the cursor end), assigns each
// PERMISSION_AND_AD to the next member of the matched request group, records
// REJECTED_WITH_REASON suffixes, and keeps the socket alive after END_NEGOTIATE
// for warm reuse -- so the next NEGOTIATE arrives as a bare command int on the
// still-encrypted session.
//
// It is deliberately not a _test.go file so Phase 5/6 cycle and daemon tests in
// other packages can import and drive it.
package negtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
)

// Job is a (cluster, proc) job id offered as a member of a request group.
type Job struct{ Cluster, Proc int }

// J is a shorthand constructor for a Job (keeps test group literals compact
// while keeping go vet's unkeyed-fields check happy).
func J(cluster, proc int) Job { return Job{Cluster: cluster, Proc: proc} }

// Group is one resource-request group the schedd offers in a round. Only the
// representative (RepCluster/RepProc) and the count (len(Members)) go on the
// wire; Members lets the loopback assign successive PERMISSION_AND_AD matches to
// distinct jobs exactly as the real schedd's nextIdle does.
type Group struct {
	RepCluster, RepProc int
	AutoClusterID       int
	Members             []Job
	// RepAd, if set, is offered verbatim (with the id/count attrs stamped on
	// top); otherwise a minimal representative ad is built.
	RepAd *classad.ClassAd
}

// MatchRecord captures one PERMISSION_AND_AD the negotiator delivered.
type MatchRecord struct {
	ClaimID string
	Extra   string
	// RepCluster/RepProc are the representative id read back from the match ad.
	RepCluster, RepProc int
	// HasCondorSpelling / HasPlainSpelling record which resource-request id
	// attribute spellings the negotiator stamped (both must be present).
	HasCondorSpelling bool
	HasPlainSpelling  bool
	// Assigned* is the group member this match was assigned to (-1,-1 for a
	// surplus match beyond the group's members).
	AssignedCluster, AssignedProc int
	SlotName                      string
	MatchAd                       *classad.ClassAd
}

// RejectRecord captures one REJECTED / REJECTED_WITH_REASON.
type RejectRecord struct {
	Reason              string // reason with the id suffix trimmed off
	RawReason           string // full reason as received
	RepCluster, RepProc int
	HasRep              bool
}

// RoundLog is the record of one NEGOTIATE round (header + outcomes).
type RoundLog struct {
	Owner   string
	Header  *classad.ClassAd
	Matches []MatchRecord
	Rejects []RejectRecord
}

// Negotiate-protocol attribute names the schedd side reads/writes.
const (
	attrOwner                = "Owner"
	attrClusterID            = "ClusterId"
	attrProcID               = "ProcId"
	attrAutoClusterID        = "AutoClusterId"
	attrResourceRequestCount = "_condor_RESOURCE_COUNT"
	attrCondorResReqCluster  = "_condor_RESOURCE_CLUSTER"
	attrCondorResReqProc     = "_condor_RESOURCE_PROC"
	attrPlainResReqCluster   = "ResourceRequestCluster"
	attrPlainResReqProc      = "ResourceRequestProc"
	attrName                 = "Name"
	attrWantMatchDiagnostics = "WantMatchDiagnostics"
	attrWantPslotPreemption  = "WantPslotPreemption"
)

// LoopbackSchedd is a running in-process NEGOTIATE server.
type LoopbackSchedd struct {
	srv *cedarserver.Server
	ln  *countingListener

	mu             sync.Mutex
	rounds         [][]Group
	roundIdx       int
	logs           []*RoundLog
	dropAfterRound int   // if >0, close the socket (no keepalive) once this many rounds have completed
	completed      int64 // atomic: rounds whose END_NEGOTIATE has been processed
}

// Option configures a LoopbackSchedd at Start.
type Option func(*countingListener)

// WithTap records every byte the schedd reads off the wire (the negotiator->
// schedd direction, which carries the PERMISSION_AND_AD claim string) so a test
// can assert secrets never travel in cleartext.
func WithTap() Option { return func(l *countingListener) { l.tap = &bytes.Buffer{} } }

// countingListener wraps a net.Listener to count accepted connections (so tests
// can prove warm-socket reuse) and optionally tee the bytes read from each
// accepted connection into a shared buffer (the wire tap).
type countingListener struct {
	net.Listener
	conns int64
	mu    sync.Mutex
	tap   *bytes.Buffer // nil unless WithTap
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	atomic.AddInt64(&l.conns, 1)
	if l.tap == nil {
		return c, nil
	}
	return &tappedConn{Conn: c, l: l}, nil
}

// tappedConn tees Read into the listener's tap buffer.
type tappedConn struct {
	net.Conn
	l *countingListener
}

func (t *tappedConn) Read(b []byte) (int, error) {
	n, err := t.Conn.Read(b)
	if n > 0 {
		t.l.mu.Lock()
		t.l.tap.Write(b[:n])
		t.l.mu.Unlock()
	}
	return n, err
}

// ServerSecurity is the schedd-side security config: unauthenticated but
// AES-encrypted, so PERMISSION_AND_AD claim strings never hit the wire in the
// clear. The negotiator dials with a matching config (ClientSecurity).
func ServerSecurity() *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{security.AuthNone},
		Authentication: security.SecurityOptional,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Encryption:     security.SecurityRequired,
		Integrity:      security.SecurityOptional,
	}
}

// ClientSecurity is the matching negotiator-side config (Command is filled in by
// the session factory per dial).
func ClientSecurity() *security.SecurityConfig {
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{security.AuthNone},
		Authentication: security.SecurityOptional,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Encryption:     security.SecurityRequired,
		Integrity:      security.SecurityOptional,
	}
}

// Start launches a loopback schedd on a fresh 127.0.0.1 port. It serves until
// ctx is cancelled. rounds is the per-round plan: round i offers rounds[i]
// (rounds beyond the slice offer nothing -> immediate NO_MORE_JOBS).
func Start(ctx context.Context, rounds [][]Group, opts ...Option) (*LoopbackSchedd, error) {
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	ln := &countingListener{Listener: base}
	for _, o := range opts {
		o(ln)
	}
	l := &LoopbackSchedd{
		srv:    cedarserver.New(ServerSecurity()),
		ln:     ln,
		rounds: rounds,
	}
	l.srv.Handle(commands.NEGOTIATE, l.handle)
	go func() { _ = l.srv.Serve(ctx, ln) }()
	return l, nil
}

// Addr is the schedd's dial address ("127.0.0.1:port").
func (l *LoopbackSchedd) Addr() string { return l.ln.Addr().String() }

// Conns returns the number of connections accepted so far (each fresh dial is
// one; warm-socket reuse across rounds does not increment it).
func (l *LoopbackSchedd) Conns() int { return int(atomic.LoadInt64(&l.ln.conns)) }

// WireBytes returns a copy of every byte read off the wire so far (only
// populated when started WithTap).
func (l *LoopbackSchedd) WireBytes() []byte {
	l.ln.mu.Lock()
	defer l.ln.mu.Unlock()
	if l.ln.tap == nil {
		return nil
	}
	return append([]byte(nil), l.ln.tap.Bytes()...)
}

// DropWarmAfter makes the server close the connection (skip keepalive) once n
// rounds have completed on it, simulating a schedd that dropped the kept-alive
// socket between cycles. Used to exercise the negotiator's transparent re-dial.
func (l *LoopbackSchedd) DropWarmAfter(n int) {
	l.mu.Lock()
	l.dropAfterRound = n
	l.mu.Unlock()
}

// WaitRounds blocks until at least n rounds have fully completed (their
// END_NEGOTIATE processed) or ctx is done. Because matches and rejects are
// recorded before END_NEGOTIATE on the same handler goroutine, once this
// returns the first n RoundLogs are stable and safe to read.
func (l *LoopbackSchedd) WaitRounds(ctx context.Context, n int) error {
	for atomic.LoadInt64(&l.completed) < int64(n) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

// Logs returns the per-round logs in the order rounds were served.
func (l *LoopbackSchedd) Logs() []*RoundLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*RoundLog, len(l.logs))
	copy(out, l.logs)
	return out
}

// gstate tracks one offered group's match-assignment cursor for a round.
type gstate struct {
	g         Group
	assignIdx int
	rejected  bool
}

func (l *LoopbackSchedd) handle(ctx context.Context, c *cedarserver.Conn) error {
	rm := c.Message
	if rm == nil {
		rm = message.NewMessageFromStream(c.Stream)
	}
	header, err := rm.GetClassAd(ctx)
	if err != nil {
		return fmt.Errorf("loopback: reading header: %w", err)
	}
	owner, _ := header.EvaluateAttrString(attrOwner)
	if owner == "" {
		return errors.New("loopback: header missing Owner")
	}

	l.mu.Lock()
	idx := l.roundIdx
	l.roundIdx++
	var groups []Group
	if idx < len(l.rounds) {
		groups = l.rounds[idx]
	}
	rlog := &RoundLog{Owner: owner, Header: header}
	l.logs = append(l.logs, rlog)
	dropAfter := l.dropAfterRound
	l.mu.Unlock()

	cursor := 0
	byRep := map[[2]int]*gstate{}
	var last *gstate

	sendGroup := func(g Group) error {
		ad := classad.New()
		if g.RepAd != nil {
			for _, name := range g.RepAd.GetAttributes() {
				if expr, ok := g.RepAd.Lookup(name); ok {
					ad.InsertExpr(name, expr)
				}
			}
		}
		_ = ad.Set(attrClusterID, int64(g.RepCluster))
		_ = ad.Set(attrProcID, int64(g.RepProc))
		_ = ad.Set(attrAutoClusterID, int64(g.AutoClusterID))
		_ = ad.Set(attrResourceRequestCount, int64(len(g.Members)))
		_ = ad.Set(attrWantMatchDiagnostics, int64(2))
		_ = ad.Set(attrWantPslotPreemption, true)
		gs := &gstate{g: g}
		byRep[[2]int{g.RepCluster, g.RepProc}] = gs
		last = gs
		out := message.NewMessageForStream(c.Stream)
		if err := out.PutInt(ctx, commands.JOB_INFO); err != nil {
			return err
		}
		if err := out.PutClassAd(ctx, ad); err != nil {
			return err
		}
		return out.FinishMessage(ctx)
	}
	sendNoMoreJobs := func() error {
		out := message.NewMessageForStream(c.Stream)
		if err := out.PutInt(ctx, commands.NO_MORE_JOBS); err != nil {
			return err
		}
		return out.FinishMessage(ctx)
	}
	sendRequests := func(n int) error {
		for i := 0; i < n; i++ {
			if cursor >= len(groups) {
				return sendNoMoreJobs()
			}
			if err := sendGroup(groups[cursor]); err != nil {
				return err
			}
			cursor++
		}
		return nil
	}

	for {
		msg := message.NewMessageFromStream(c.Stream)
		op, err := msg.GetInt(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("loopback: reading opcode: %w", err)
		}

		switch op {
		case commands.SEND_JOB_INFO:
			if err := sendRequests(1); err != nil {
				return err
			}
		case commands.SEND_RESOURCE_REQUEST_LIST:
			n, err := msg.GetInt(ctx)
			if err != nil {
				return fmt.Errorf("loopback: reading request count: %w", err)
			}
			if err := sendRequests(n); err != nil {
				return err
			}
		case commands.PERMISSION_AND_AD:
			rec, err := readMatch(ctx, msg)
			if err != nil {
				return err
			}
			assignMatch(byRep, &rec)
			l.mu.Lock()
			rlog.Matches = append(rlog.Matches, rec)
			l.mu.Unlock()
		case commands.REJECTED:
			rec := RejectRecord{}
			if last != nil {
				last.rejected = true
				rec.RepCluster, rec.RepProc = last.g.RepCluster, last.g.RepProc
				rec.HasRep = true
			}
			l.mu.Lock()
			rlog.Rejects = append(rlog.Rejects, rec)
			l.mu.Unlock()
		case commands.REJECTED_WITH_REASON:
			reason, err := msg.GetString(ctx)
			if err != nil {
				return fmt.Errorf("loopback: reading reject reason: %w", err)
			}
			rec := RejectRecord{RawReason: reason, Reason: trimReason(reason)}
			if rep, ok := parseRejectRep(reason); ok {
				rec.RepCluster, rec.RepProc, rec.HasRep = rep[0], rep[1], true
				if gs := byRep[rep]; gs != nil {
					gs.rejected = true
				}
			} else if last != nil {
				last.rejected = true
			}
			l.mu.Lock()
			rlog.Rejects = append(rlog.Rejects, rec)
			l.mu.Unlock()
		case commands.END_NEGOTIATE:
			// All of this round's matches/rejects are now recorded; publish the
			// completion so WaitRounds callers can safely read the log.
			atomic.AddInt64(&l.completed, 1)
			// Keep the socket for the negotiator's next cycle unless we've been
			// asked to drop it after this many rounds (re-dial test).
			if dropAfter > 0 && idx+1 >= dropAfter {
				return nil
			}
			c.KeepAlive()
			return nil
		default:
			return fmt.Errorf("loopback: unexpected opcode %d (%s)", op, commands.GetCommandName(op))
		}
	}
}

// assignMatch resolves a match to the next unassigned member of its group,
// mirroring the schedd's nextIdle (all members are treated as still-idle here).
func assignMatch(byRep map[[2]int]*gstate, rec *MatchRecord) {
	rec.AssignedCluster, rec.AssignedProc = -1, -1
	gs := byRep[[2]int{rec.RepCluster, rec.RepProc}]
	if gs == nil || gs.rejected {
		return
	}
	if gs.assignIdx < len(gs.g.Members) {
		m := gs.g.Members[gs.assignIdx]
		gs.assignIdx++
		rec.AssignedCluster, rec.AssignedProc = m.Cluster, m.Proc
	}
}

func readMatch(ctx context.Context, msg *message.Message) (MatchRecord, error) {
	claimBlob, err := msg.GetString(ctx)
	if err != nil {
		return MatchRecord{}, fmt.Errorf("loopback: reading claim id: %w", err)
	}
	ad, err := msg.GetClassAd(ctx)
	if err != nil {
		return MatchRecord{}, fmt.Errorf("loopback: reading match ad: %w", err)
	}
	rec := MatchRecord{MatchAd: ad, ClaimID: claimBlob}
	if i := strings.IndexByte(claimBlob, ' '); i >= 0 {
		rec.ClaimID = claimBlob[:i]
		rec.Extra = strings.TrimSpace(claimBlob[i+1:])
	}
	cc, okcc := ad.EvaluateAttrInt(attrCondorResReqCluster)
	cp, okcp := ad.EvaluateAttrInt(attrCondorResReqProc)
	rec.HasCondorSpelling = okcc && okcp
	pc, okpc := ad.EvaluateAttrInt(attrPlainResReqCluster)
	pp, okpp := ad.EvaluateAttrInt(attrPlainResReqProc)
	rec.HasPlainSpelling = okpc && okpp
	// Resolve the representative id from the _condor_ spelling (the one
	// golang-ap's readMatch trusts), falling back to the plain spelling.
	switch {
	case rec.HasCondorSpelling:
		rec.RepCluster, rec.RepProc = int(cc), int(cp)
	case rec.HasPlainSpelling:
		rec.RepCluster, rec.RepProc = int(pc), int(pp)
	}
	rec.SlotName, _ = ad.EvaluateAttrString(attrName)
	return rec, nil
}

// parseRejectRep extracts "cluster.proc" from "reason |autocluster|cluster.proc|".
func parseRejectRep(reason string) ([2]int, bool) {
	parts := strings.Split(reason, "|")
	if len(parts) < 3 {
		return [2]int{}, false
	}
	jid := strings.TrimSpace(parts[2])
	dot := strings.IndexByte(jid, '.')
	if dot < 0 {
		return [2]int{}, false
	}
	c, err1 := strconv.Atoi(jid[:dot])
	p, err2 := strconv.Atoi(jid[dot+1:])
	if err1 != nil || err2 != nil {
		return [2]int{}, false
	}
	return [2]int{c, p}, true
}

func trimReason(reason string) string {
	if i := strings.IndexByte(reason, '|'); i >= 0 {
		return strings.TrimSpace(reason[:i])
	}
	return reason
}
