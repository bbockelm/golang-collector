package protocol

import (
	"context"
	"net"
	"testing"

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	cedarserver "github.com/bbockelm/cedar/server"

	"github.com/bbockelm/golang-collector/negotiator/negtest"
)

// fakeStartd is a minimal cedar server that handles MATCH_INFO by reading the
// claim id the negotiator sends (the C++ startd side of NotifyStartdOfMatch).
type fakeStartd struct {
	srv    *cedarserver.Server
	ln     net.Listener
	claims chan string
}

func startFakeStartd(t *testing.T, ctx context.Context) *fakeStartd {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeStartd{
		srv:    cedarserver.New(negtest.ServerSecurity()),
		ln:     ln,
		claims: make(chan string, 4),
	}
	fs.srv.Handle(commands.MATCH_INFO, func(ctx context.Context, c *cedarserver.Conn) error {
		rm := c.Message
		if rm == nil {
			rm = message.NewMessageFromStream(c.Stream)
		}
		claim, err := rm.GetString(ctx)
		if err != nil {
			return err
		}
		fs.claims <- claim
		return nil
	})
	go func() { _ = fs.srv.Serve(ctx, ln) }()
	return fs
}

func (fs *fakeStartd) addr() string { return fs.ln.Addr().String() }

// TestInformStartd drives the MATCH_INFO notify end-to-end: the factory dials a
// fake startd and the startd receives exactly the claim id.
func TestInformStartd(t *testing.T) {
	ctx := testCtx(t)
	fs := startFakeStartd(t, ctx)

	f := NewFactory(negtest.ClientSecurity())
	t.Cleanup(f.CloseAll)

	if err := f.InformStartd(ctx, fs.addr(), "claim-xyz-123"); err != nil {
		t.Fatalf("InformStartd: %v", err)
	}

	select {
	case got := <-fs.claims:
		if got != "claim-xyz-123" {
			t.Errorf("startd received claim %q, want claim-xyz-123", got)
		}
	case <-ctx.Done():
		t.Fatal("startd never received MATCH_INFO")
	}
}

// TestInformStartdNoAddr confirms an empty address is a fast local error, not a
// dial attempt.
func TestInformStartdNoAddr(t *testing.T) {
	ctx := testCtx(t)
	f := NewFactory(negtest.ClientSecurity())
	t.Cleanup(f.CloseAll)
	if err := f.InformStartd(ctx, "", "claim"); err == nil {
		t.Fatal("InformStartd with empty address: want error, got nil")
	}
}
