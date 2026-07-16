package protocol

import (
	"context"
	"errors"
	"fmt"

	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
)

// InformStartd sends a MATCH_INFO notification to a startd, mirroring the C++
// NotifyStartdOfMatchHandler (dc_startd.h:301-402): connect with the MATCH_INFO
// command and hand the startd the claim id (put_secret + end_of_message) so it
// can anticipate the schedd's claim request. NEGOTIATOR_INFORM_STARTD gates it
// (default off, matchmaker.cpp:826); when on, the negotiator calls this after a
// match with claiming enabled (matchmaker.cpp:5412-5426).
//
// The C++ send is best-effort and (by default) nonblocking; the negotiator does
// not treat a MATCH_INFO failure as a match failure. Callers should likewise
// run this off the decision spine and ignore/log the error.
//
// The claim id rides the encrypted session as a plain CEDAR string, the same way
// session.SendMatch delivers the schedd's claim (put_secret on an encrypted
// session is a plain string on the wire).
func (f *Factory) InformStartd(ctx context.Context, startdAddr, claimID string) error {
	if startdAddr == "" {
		return errors.New("negotiate: InformStartd with no startd address")
	}
	// Copy the security policy so we can set the command without mutating the
	// factory's shared config (as dialFresh / queryPrivateAds do).
	cfg := *f.sec
	cfg.Command = commands.MATCH_INFO

	cl, err := client.ConnectAndAuthenticate(ctx, startdAddr, &cfg)
	if err != nil {
		return fmt.Errorf("negotiate: MATCH_INFO dialing startd %s: %w", startdAddr, err)
	}
	defer func() { _ = cl.Close() }()

	m := message.NewMessageForStream(cl.GetStream())
	if err := m.PutString(ctx, claimID); err != nil {
		return fmt.Errorf("negotiate: sending MATCH_INFO claim id: %w", err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		return fmt.Errorf("negotiate: finishing MATCH_INFO: %w", err)
	}
	return nil
}
