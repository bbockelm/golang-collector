package main

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbockelm/cedar/addresses"
	"github.com/bbockelm/cedar/ccb"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	cedarversion "github.com/bbockelm/cedar/version"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"
)

// TestEmbeddedCCBServesStreaming: with ENABLE_CCB_SERVER, the collector's
// handshake for CCB_REQUEST and CCB_REGISTER advertises a version at or above
// ccb.StreamingMinVersion, below which a requester sends no streaming
// request, and a private requester's streaming CCB_REQUEST is served over an
// encrypted session: its bytes reach a target registered with the embedded
// CCB and come back.
func TestEmbeddedCCBServesStreaming(t *testing.T) {
	keyDir := t.TempDir()
	if err := security.GeneratePoolSigningKey(filepath.Join(keyDir, "POOL")); err != nil {
		t.Fatal(err)
	}
	cfg := config.NewEmpty()
	for k, v := range map[string]string{
		"CONDOR_VIEW_HOST":                   "",
		"ENABLE_CCB_SERVER":                  "true",
		"SEC_DEFAULT_AUTHENTICATION":         "REQUIRED",
		"SEC_DEFAULT_ENCRYPTION":             "REQUIRED",
		"SEC_DEFAULT_AUTHENTICATION_METHODS": "IDTOKENS",
		"SEC_DEFAULT_CRYPTO_METHODS":         "AES",
		"SEC_TOKEN_POOL_SIGNING_KEY_FILE":    filepath.Join(keyDir, "POOL"),
		"SEC_PASSWORD_DIRECTORY":             keyDir,
		"TRUST_DOMAIN":                       testTrustDomain,
		"ALLOW_READ":                         "*",
		"ALLOW_DAEMON":                       "startd@pool",
	} {
		cfg.Set(k, v)
	}
	condorConfig := filepath.Join(t.TempDir(), "condor_config")
	if err := os.WriteFile(condorConfig, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONDOR_CONFIG", condorConfig)
	dlog, err := logging.New(&logging.Config{OutputPath: filepath.Join(t.TempDir(), "CollectorLog")})
	if err != nil {
		t.Fatal(err)
	}
	d, err := daemon.New(daemon.Options{Subsys: "COLLECTOR", Config: cfg, Logger: dlog})
	if err != nil {
		t.Fatal(err)
	}

	// Build and serve the collector as run does.
	opts := authzOptions()
	opts.Subsys = "COLLECTOR"
	az, err := daemon.NewAuthz(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	sec, err := d.ServerSecurityConfig(commands.DC_NOP, "READ")
	if err != nil {
		t.Fatal(err)
	}
	c, err := newDaemonCollector(d, sec, nil, az, d.Slog())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	if err := maybeStartEmbeddedCCB(ctx, d, cfg, sec, c.Server(), addr); err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = az.Serve(c.Server(), d.Slog())(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	ac := &authzCollector{addr: addr, keyDir: keyDir}

	// The handshakes for CCB_REQUEST (at READ) and CCB_REGISTER (at DAEMON)
	// advertise a streaming-capable version.
	hctx, hcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer hcancel()
	for _, hs := range []struct {
		name, identity string
		command        int
	}{
		{"CCB_REQUEST", "user@pool", ccb.CommandRequest},
		{"CCB_REGISTER", "startd@pool", ccb.CommandRegister},
	} {
		cs := *ac.as(t, hs.identity)
		cs.Command = hs.command
		cl, err := client.ConnectAndAuthenticate(hctx, addr, &cs)
		if err != nil {
			t.Fatalf("%s handshake: %v", hs.name, err)
		}
		neg := cl.GetSecurityNegotiation()
		_ = cl.Close()
		if neg == nil || neg.ServerConfig == nil {
			t.Fatalf("%s handshake returned no server policy", hs.name)
		}
		if v, ok := cedarversion.Parse(neg.ServerConfig.RemoteVersion); !ok || !v.AtLeast(ccb.StreamingMinVersion) {
			t.Errorf("%s handshake advertises %q, want at least %s", hs.name, neg.ServerConfig.RemoteVersion, ccb.StreamingMinVersion)
		}
	}

	// A target registers (CCB_REGISTER, at DAEMON) and echoes what reaches it.
	lis := ccb.NewListener(ccb.ListenerConfig{
		BrokerAddr: addr,
		Security:   ac.as(t, "startd@pool"),
		Name:       "echo-target",
		Handler: func(conn net.Conn, _ ccb.InboundMeta) {
			defer func() { _ = conn.Close() }()
			_, _ = io.Copy(conn, conn)
		},
	})
	go func() { _ = lis.Run(ctx) }()
	contact := ""
	for deadline := time.Now().Add(10 * time.Second); contact == "" && time.Now().Before(deadline); {
		if contact = lis.Contact(); contact == "" {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if contact == "" {
		t.Fatal("target did not register with the embedded CCB")
	}

	// A private requester (its return address is itself CCB-routed) asks
	// for a streaming connection, and requires one.
	broker, ccbid, ok := addresses.SplitCCBContact(contact)
	if !ok {
		t.Fatalf("bad CCB contact %q", contact)
	}
	conn, err := ccb.Dial(hctx, []addresses.CCBContact{{BrokerAddr: broker, CCBID: ccbid, Raw: contact}}, ccb.DialOptions{
		Security:         ac.as(t, "user@pool"),
		ProxyReturnAddr:  "<10.9.8.7:0?ccbid=" + contact + ">",
		RequireStreaming: true,
		TargetDesc:       "echo-target",
		Timeout:          8 * time.Second,
	})
	if err != nil {
		t.Fatalf("streaming CCB_REQUEST: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write through the CCB: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read through the CCB: %v", err)
	}
	if string(buf) != "ping" {
		t.Errorf("echo through the CCB = %q, want ping", buf)
	}
}
