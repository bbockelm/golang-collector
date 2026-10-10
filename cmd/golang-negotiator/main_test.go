package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"
)

// TestMasterSessionsReconfig: under a restrictive ALLOW_ADMINISTRATOR,
// condor_master's family and inherited parent sessions can still
// DC_RECONFIG the negotiator, as DaemonCore punches holes for them, while
// another condor identity is refused and the refusal is logged.
func TestMasterSessionsReconfig(t *testing.T) {
	condorConfig := filepath.Join(t.TempDir(), "condor_config")
	if err := os.WriteFile(condorConfig, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONDOR_CONFIG", condorConfig)
	cfg := config.NewEmpty()
	cfg.Set("ALLOW_READ", "*")
	cfg.Set("ALLOW_ADMINISTRATOR", "admin@pool")
	dlog, err := logging.New(&logging.Config{OutputPath: filepath.Join(t.TempDir(), "NegotiatorLog")})
	if err != nil {
		t.Fatal(err)
	}
	d, err := daemon.New(daemon.Options{Subsys: "NEGOTIATOR", Config: cfg, Logger: dlog})
	if err != nil {
		t.Fatal(err)
	}
	reconfigs := make(chan struct{}, 4)
	d.OnReconfig(func(*config.Config) { reconfigs <- struct{}{} })

	srv, az, err := newCommandServer(d)
	if err != nil {
		t.Fatal(err)
	}
	// The server's policies leave the session cache unset, so it keeps its
	// sessions in cedar's global cache.
	cache := security.GetSessionCache()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	records := make(chan slog.Record, 64)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = az.Serve(srv, slog.New(&recordHandler{ch: records}))(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	reconfig := func(identity string) error {
		t.Helper()
		claim, err := security.MintClaimSession(cache, security.MintClaimOptions{
			Sinful:  "<127.0.0.1:1>",
			PeerFQU: identity,
		})
		if err != nil {
			t.Fatal(err)
		}
		cc := security.NewSessionCache()
		sid, err := security.ImportClaimSession(cc, claim.ClaimID(), security.ClaimSessionOptions{PeerFQU: "negotiator@pool"})
		if err != nil {
			t.Fatal(err)
		}
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		cl, err := client.ConnectAndAuthenticate(cctx, ln.Addr().String(), &security.SecurityConfig{
			Command:        commands.DC_RECONFIG,
			AuthMethods:    []security.AuthMethod{security.AuthToken},
			Authentication: security.SecurityOptional,
			CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
			Encryption:     security.SecurityOptional,
			Integrity:      security.SecurityOptional,
			SessionID:      sid,
			SessionCache:   cc,
		})
		if err != nil {
			return err
		}
		return cl.Close()
	}

	// A resumed session gets no reply to a refused command, so the
	// refusal shows only in the log and in the missing reconfig.
	_ = reconfig("condor@pool")
	denial := waitDenial(t, records)
	for k, v := range map[string]string{"command": "DC_RECONFIG", "level": "ADMINISTRATOR", "user": "condor@pool", "allow_knob": "ALLOW_ADMINISTRATOR"} {
		if denial[k] != v {
			t.Errorf("denial %s = %q, want %q (record %v)", k, denial[k], v, denial)
		}
	}
	select {
	case <-reconfigs:
		t.Error("DC_RECONFIG from condor@pool reconfigured the negotiator")
	default:
	}

	for _, who := range []string{"condor@family", "condor@parent"} {
		if err := reconfig(who); err != nil {
			t.Errorf("DC_RECONFIG from %s: %v", who, err)
			continue
		}
		select {
		case <-reconfigs:
		case <-time.After(10 * time.Second):
			t.Errorf("DC_RECONFIG from %s did not reconfigure the negotiator", who)
		}
	}
}

// waitDenial returns the attributes of the next PERMISSION DENIED record.
func waitDenial(t *testing.T, records chan slog.Record) map[string]string {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case r := <-records:
			if r.Message != "PERMISSION DENIED" {
				continue
			}
			attrs := map[string]string{}
			r.Attrs(func(a slog.Attr) bool {
				attrs[a.Key] = a.Value.String()
				return true
			})
			return attrs
		case <-timeout:
			t.Fatal("no PERMISSION DENIED was logged")
			return nil
		}
	}
}

// recordHandler is a slog.Handler that hands every record to a channel.
type recordHandler struct{ ch chan slog.Record }

func (h *recordHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordHandler) Handle(_ context.Context, r slog.Record) error {
	select {
	case h.ch <- r.Clone():
	default:
	}
	return nil
}
func (h *recordHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordHandler) WithGroup(string) slog.Handler      { return h }
