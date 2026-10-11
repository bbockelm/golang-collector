package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/security"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/golang-collector/store"
)

// TestNegotiationLevelsMatchCpp: each command negotiates under the
// SEC_<level>_* policy of the level it is registered at, and an unset
// SEC_ADVERTISE_STARTD_* falls back to SEC_DAEMON_* before SEC_DEFAULT_*, as
// in C++ (SecMan::getSecSetting). With authentication required at DAEMON
// only, an anonymous condor_status-style query succeeds while an anonymous
// UPDATE_STARTD_AD is refused before any authorization check, and an
// authenticated startd's session covers its other ADVERTISE_STARTD commands.
func TestNegotiationLevelsMatchCpp(t *testing.T) {
	keyDir := t.TempDir()
	if err := security.GeneratePoolSigningKey(filepath.Join(keyDir, "POOL")); err != nil {
		t.Fatal(err)
	}
	cfg := config.NewEmpty()
	for k, v := range map[string]string{
		"CONDOR_VIEW_HOST":                    "",
		"SEC_DEFAULT_AUTHENTICATION":          "OPTIONAL",
		"SEC_DAEMON_AUTHENTICATION":           "REQUIRED",
		"SEC_ADVERTISE_SCHEDD_AUTHENTICATION": "NEVER",
		"SEC_ADVERTISE_MASTER_ENCRYPTION":     "REQUIRED",
		"SEC_DEFAULT_AUTHENTICATION_METHODS":  "IDTOKENS",
		"SEC_DEFAULT_CRYPTO_METHODS":          "AES",
		"SEC_TOKEN_POOL_SIGNING_KEY_FILE":     filepath.Join(keyDir, "POOL"),
		"SEC_PASSWORD_DIRECTORY":              keyDir,
		"TRUST_DOMAIN":                        testTrustDomain,
		"ALLOW_READ":                          "*",
		"ALLOW_ADVERTISE_STARTD":              "*",
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

	secForLevel, err := d.SecurityLevelConfigs()
	if err != nil {
		t.Fatal(err)
	}
	for lvl, want := range map[string]security.SecurityLevel{
		"READ":             security.SecurityOptional,
		"ADMINISTRATOR":    security.SecurityOptional,
		"DAEMON":           security.SecurityRequired,
		"ADVERTISE_STARTD": security.SecurityRequired,
		"ADVERTISE_SCHEDD": security.SecurityNever,
		"ADVERTISE_MASTER": security.SecurityRequired,
	} {
		if got := secForLevel[lvl].Authentication; got != want {
			t.Errorf("%s authentication = %v, want %v", lvl, got, want)
		}
	}
	// Each setting falls back on its own.
	if got := secForLevel["ADVERTISE_MASTER"].Encryption; got != security.SecurityRequired {
		t.Errorf("ADVERTISE_MASTER encryption = %v, want %v", got, security.SecurityRequired)
	}
	if got := secForLevel["ADVERTISE_STARTD"].Encryption; got == security.SecurityRequired {
		t.Errorf("ADVERTISE_STARTD encryption = %v, from another level's setting", got)
	}

	opts := authzOptions()
	opts.Subsys = "COLLECTOR"
	az, err := daemon.NewAuthz(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	records := make(chan slog.Record, 64)
	logger := slog.New(&recordHandler{ch: records})
	c, err := newDaemonCollector(d, secForLevel["READ"], nil, az, logger)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = az.Serve(c.Server(), logger)(ctx, ln)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	ac := &authzCollector{addr: ln.Addr().String(), keyDir: keyDir, srv: c.Server(), az: az, st: c.Store(), records: records}

	if _, err := ac.query(t, ac.as(t, ""), commands.QUERY_STARTD_ADS); err != nil {
		t.Errorf("anonymous QUERY_STARTD_ADS under SEC_READ_AUTHENTICATION=OPTIONAL: %v", err)
	}
	if err := ac.advertise(t, ac.as(t, ""), commands.UPDATE_STARTD_AD, "slot1@anonymous"); err == nil {
		t.Error("anonymous UPDATE_STARTD_AD negotiated, though SEC_DAEMON_AUTHENTICATION=REQUIRED")
	}

	startd := ac.as(t, "startd@pool")
	if err := ac.advertise(t, startd, commands.UPDATE_STARTD_AD, "slot1@startd"); err != nil {
		t.Fatalf("authenticated UPDATE_STARTD_AD: %v", err)
	}
	if err := ac.advertise(t, startd, commands.UPDATE_STARTD_AD_WITH_ACK, "slot2@startd"); err != nil {
		t.Fatalf("authenticated UPDATE_STARTD_AD_WITH_ACK: %v", err)
	}
	if n := startd.SessionCache.Size(); n != 1 {
		t.Errorf("startd holds %d sessions, want 1: UPDATE_STARTD_AD's session did not cover UPDATE_STARTD_AD_WITH_ACK", n)
	}
	ac.waitStored(t, store.StartdAd, "slot1@startd")
	names := ac.adNames(t, store.StartdAd)
	if names["slot1@anonymous"] {
		t.Error("anonymous UPDATE_STARTD_AD was stored")
	}
	if !names["slot2@startd"] {
		t.Error("authenticated UPDATE_STARTD_AD_WITH_ACK was not stored")
	}
	for len(records) > 0 {
		if r := <-records; r.Message == "PERMISSION DENIED" {
			t.Errorf("anonymous UPDATE_STARTD_AD reached authorization: %v", r)
		}
	}
}
