package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/golang-collector/store"
)

// These tests drive the collector exactly as the daemon builds it
// (newCollector + daemon.Authz.Serve) over loopback, with IDTOKENS so each client
// authenticates as a distinct identity, and check that the ALLOW_/DENY_
// policy decides which identities may run which command.

const testTrustDomain = "pool"

// authzCollector is a running collector plus the means to reach it as any
// identity.
type authzCollector struct {
	addr    string
	keyDir  string
	cache   *security.SessionCache
	srv     *cedarserver.Server
	az      *daemon.Authz
	st      store.Backend
	records chan slog.Record
}

// startAuthzCollector serves a collector whose ALLOW_/DENY_ policy is knobs.
// Authentication is optional on the server, so an anonymous peer can connect
// at every level and only the authorization policy keeps it out.
func startAuthzCollector(t *testing.T, knobs map[string]string) *authzCollector {
	t.Helper()
	keyDir := t.TempDir()
	if err := security.GeneratePoolSigningKey(filepath.Join(keyDir, "POOL")); err != nil {
		t.Fatal(err)
	}
	cache := security.NewSessionCache()
	sec := &security.SecurityConfig{
		SessionCache:            cache,
		AuthMethods:             []security.AuthMethod{security.AuthToken},
		Authentication:          security.SecurityOptional,
		CryptoMethods:           []security.CryptoMethod{security.CryptoAES},
		Encryption:              security.SecurityOptional,
		Integrity:               security.SecurityOptional,
		TrustDomain:             testTrustDomain,
		TokenPoolSigningKeyFile: filepath.Join(keyDir, "POOL"),
		TokenSigningKeyDir:      keyDir,
	}

	cfg := config.NewEmpty()
	cfg.Set("CONDOR_VIEW_HOST", "")
	for k, v := range knobs {
		cfg.Set(k, v)
	}
	opts := authzOptions()
	opts.Subsys = "COLLECTOR"
	az, err := daemon.NewAuthz(cfg, opts)
	if err != nil {
		t.Fatal(err)
	}
	records := make(chan slog.Record, 64)
	logger := slog.New(&recordHandler{ch: records})
	c, err := newCollector(cfg, sec, nil, nil, az, logger)
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
	return &authzCollector{addr: ln.Addr().String(), keyDir: keyDir, cache: cache, srv: c.Server(), az: az, st: c.Store(), records: records}
}

// as returns a client security config that authenticates as identity with an
// IDTOKEN signed by the collector's pool key, or an anonymous one for "".
// Each config carries its own session cache, so no session (and so no
// identity) is reused across clients.
func (ac *authzCollector) as(t *testing.T, identity string) *security.SecurityConfig {
	t.Helper()
	return ac.limitedAs(t, identity, nil)
}

// limitedAs is as with an IDTOKEN whose authorization is limited to the
// levels limits (its condor:/<LEVEL> scopes); nil means unlimited.
func (ac *authzCollector) limitedAs(t *testing.T, identity string, limits []string) *security.SecurityConfig {
	t.Helper()
	if identity == "" {
		return &security.SecurityConfig{
			AuthMethods:    []security.AuthMethod{},
			Authentication: security.SecurityNever,
			Encryption:     security.SecurityNever,
			Integrity:      security.SecurityNever,
			SessionCache:   security.NewSessionCache(),
		}
	}
	now := time.Now().Unix()
	tok, err := security.GenerateJWT(ac.keyDir, "POOL", identity, testTrustDomain, now, now+3600, limits)
	if err != nil {
		t.Fatal(err)
	}
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{security.AuthToken},
		Authentication: security.SecurityRequired,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Encryption:     security.SecurityOptional,
		Integrity:      security.SecurityOptional,
		Token:          tok,
		TrustDomain:    testTrustDomain,
		SessionCache:   security.NewSessionCache(),
	}
}

// session returns a client security config for a peer the collector knows as
// identity through a session pre-shared with it, the way condor_master's
// family and inherited parent sessions carry condor@family and condor@parent.
func (ac *authzCollector) session(t *testing.T, identity string) *security.SecurityConfig {
	t.Helper()
	claim, err := security.MintClaimSession(ac.cache, security.MintClaimOptions{
		Sinful:  "<127.0.0.1:1>",
		PeerFQU: identity,
	})
	if err != nil {
		t.Fatal(err)
	}
	cc := security.NewSessionCache()
	sid, err := security.ImportClaimSession(cc, claim.ClaimID(), security.ClaimSessionOptions{PeerFQU: "collector@" + testTrustDomain})
	if err != nil {
		t.Fatal(err)
	}
	return &security.SecurityConfig{
		AuthMethods:    []security.AuthMethod{security.AuthToken},
		Authentication: security.SecurityOptional,
		CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
		Encryption:     security.SecurityOptional,
		Integrity:      security.SecurityOptional,
		SessionID:      sid,
		SessionCache:   cc,
	}
}

// advertise sends a startd ad (with a private companion ad carrying a
// claim id) with cmd, as sec's identity.
func (ac *authzCollector) advertise(t *testing.T, sec *security.SecurityConfig, cmd int, name string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(htcondor.WithSecurityConfig(context.Background(), sec), 10*time.Second)
	defer cancel()
	pub := mustParse(t, `[MyType="Machine"; Name="`+name+`"; MyAddress="<127.0.0.1:1>"; Cpus=8]`)
	pvt := mustParse(t, `[MyType="Machine"; Name="`+name+`"; MyAddress="<127.0.0.1:1>"; Capability="CAP-`+name+`"]`)
	return htcondor.NewCollector(ac.addr).Advertise(ctx, pub, &htcondor.AdvertiseOptions{
		Command:   commands.CommandType(cmd),
		PrivateAd: pvt,
		WithAck:   cmd == commands.UPDATE_STARTD_AD_WITH_ACK,
	})
}

// query runs a collector query command as sec's identity and returns the
// ads, or an error if the collector refused the command.
func (ac *authzCollector) query(t *testing.T, sec *security.SecurityConfig, cmd int) ([]*classad.ClassAd, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cs := *sec
	cs.Command = cmd
	cl, err := client.ConnectAndAuthenticate(ctx, ac.addr, &cs)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cl.Close() }()
	m := message.NewMessageForStream(cl.GetStream())
	if err := m.PutClassAd(ctx, mustParse(t, `[MyType="Query"; TargetType="Machine"; Requirements=true]`)); err != nil {
		return nil, err
	}
	if err := m.FinishMessage(ctx); err != nil {
		return nil, err
	}
	rm := message.NewMessageFromStream(cl.GetStream())
	var out []*classad.ClassAd
	for {
		more, err := rm.GetInt(ctx)
		if err != nil {
			return nil, err
		}
		if more == 0 {
			return out, nil
		}
		ad, err := rm.GetClassAd(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, ad)
	}
}

// adNames returns the Name of every ad in the store's table at.
func (ac *authzCollector) adNames(t *testing.T, at store.AdType) map[string]bool {
	t.Helper()
	ads, err := ac.st.Query(context.Background(), at, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for ad := range ads {
		n, _ := ad.EvaluateAttrString("Name")
		names[n] = true
	}
	return names
}

// waitStored waits until an ad called name is in table at (an update
// without an ack has no completion signal of its own).
func (ac *authzCollector) waitStored(t *testing.T, at store.AdType, name string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ac.adNames(t, at)[name] {
		if time.Now().After(deadline) {
			t.Fatalf("%s ad %q was never stored", at, name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitDenial returns the attributes of the next PERMISSION DENIED record.
func (ac *authzCollector) waitDenial(t *testing.T) map[string]string {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case r := <-ac.records:
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

// expectDenial checks the next denial names the command, level, user and
// deciding knobs.
func (ac *authzCollector) expectDenial(t *testing.T, command, level, user, allowKnob, denyKnob string) {
	t.Helper()
	got := ac.waitDenial(t)
	want := map[string]string{
		"command":    command,
		"level":      level,
		"user":       user,
		"allow_knob": allowKnob,
		"deny_knob":  denyKnob,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("denial %s = %q, want %q (record %v)", k, got[k], v, got)
		}
	}
	if got["peer"] == "" {
		t.Errorf("denial names no peer (record %v)", got)
	}
}

// TestAuthzAdvertiseStartd: an identity outside ALLOW_ADVERTISE_STARTD cannot
// store a startd ad (with either update command, or anonymously), and one in
// the list can.
func TestAuthzAdvertiseStartd(t *testing.T) {
	ac := startAuthzCollector(t, map[string]string{
		"ALLOW_READ":             "*",
		"ALLOW_ADVERTISE_STARTD": "startd@pool",
	})

	expectDenied(t, ac.advertise(t, ac.as(t, "intruder@pool"), commands.UPDATE_STARTD_AD, "slot1@intruder"))
	ac.expectDenial(t, "UPDATE_STARTD_AD", "ADVERTISE_STARTD", "intruder@pool", "ALLOW_ADVERTISE_STARTD", "")

	if err := ac.advertise(t, ac.as(t, "intruder@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot2@intruder"); err == nil {
		t.Error("UPDATE_STARTD_AD_WITH_ACK from an identity outside ALLOW_ADVERTISE_STARTD was acknowledged")
	}
	ac.expectDenial(t, "UPDATE_STARTD_AD_WITH_ACK", "ADVERTISE_STARTD", "intruder@pool", "ALLOW_ADVERTISE_STARTD", "")

	expectDenied(t, ac.advertise(t, ac.as(t, ""), commands.UPDATE_STARTD_AD, "slot3@anonymous"))
	ac.expectDenial(t, "UPDATE_STARTD_AD", "ADVERTISE_STARTD", "unauthenticated@unmapped", "ALLOW_ADVERTISE_STARTD", "")

	if err := ac.advertise(t, ac.as(t, "startd@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot1@startd"); err != nil {
		t.Fatalf("UPDATE_STARTD_AD_WITH_ACK from ALLOW_ADVERTISE_STARTD identity: %v", err)
	}
	if err := ac.advertise(t, ac.as(t, "startd@pool"), commands.UPDATE_STARTD_AD, "slot2@startd"); err != nil {
		t.Fatalf("UPDATE_STARTD_AD from ALLOW_ADVERTISE_STARTD identity: %v", err)
	}
	ac.waitStored(t, store.StartdAd, "slot2@startd")

	names := ac.adNames(t, store.StartdAd)
	for _, n := range []string{"slot1@intruder", "slot2@intruder", "slot3@anonymous"} {
		if names[n] {
			t.Errorf("startd ad %q from an identity outside ALLOW_ADVERTISE_STARTD was stored", n)
		}
	}
	for _, n := range []string{"slot1@startd", "slot2@startd"} {
		if !names[n] {
			t.Errorf("startd ad %q from startd@pool was not stored", n)
		}
	}
}

// TestAuthzPrivateAdsNeedNegotiator: QUERY_STARTD_PVT_ADS (claim ids) is
// refused to identities outside ALLOW_NEGOTIATOR -- including the startd that
// advertised them -- and answered for the negotiator, while ALLOW_READ = *
// still lets an anonymous condor_status-style query see the public ad.
func TestAuthzPrivateAdsNeedNegotiator(t *testing.T) {
	ac := startAuthzCollector(t, map[string]string{
		"ALLOW_READ":             "*",
		"ALLOW_ADVERTISE_STARTD": "startd@pool",
		"ALLOW_NEGOTIATOR":       "negotiator@pool",
	})
	if err := ac.advertise(t, ac.as(t, "startd@pool"), commands.UPDATE_STARTD_AD, "slot1@host"); err != nil {
		t.Fatalf("advertise: %v", err)
	}
	ac.waitStored(t, store.StartdPvtAd, "slot1@host")

	for _, who := range []string{"reader@pool", "startd@pool"} {
		ads, err := ac.query(t, ac.as(t, who), commands.QUERY_STARTD_PVT_ADS)
		if err == nil {
			t.Errorf("QUERY_STARTD_PVT_ADS as %s returned %d ads, want a refusal", who, len(ads))
		}
		ac.expectDenial(t, "QUERY_STARTD_PVT_ADS", "NEGOTIATOR", who, "ALLOW_NEGOTIATOR", "")
	}

	ads, err := ac.query(t, ac.as(t, "negotiator@pool"), commands.QUERY_STARTD_PVT_ADS)
	if err != nil {
		t.Fatalf("QUERY_STARTD_PVT_ADS as negotiator@pool: %v", err)
	}
	if len(ads) != 1 {
		t.Fatalf("QUERY_STARTD_PVT_ADS as negotiator@pool returned %d ads, want 1", len(ads))
	}
	if c, _ := ads[0].EvaluateAttrString("Capability"); c != "CAP-slot1@host" {
		t.Errorf("negotiator's private ad Capability = %q, want CAP-slot1@host", c)
	}

	// condor_status: anonymous READ sees the public ad, without the claim id.
	ctx := htcondor.WithSecurityConfig(context.Background(), ac.as(t, ""))
	pub, err := htcondor.NewCollector(ac.addr).QueryAds(ctx, "Machine", "")
	if err != nil {
		t.Fatalf("anonymous QUERY_STARTD_ADS: %v", err)
	}
	if len(pub) != 1 {
		t.Fatalf("anonymous QUERY_STARTD_ADS returned %d ads, want 1", len(pub))
	}
	if _, ok := pub[0].EvaluateAttrString("Capability"); ok {
		t.Error("anonymous QUERY_STARTD_ADS returned the claim id")
	}
}

// TestAuthzDenyOverridesAllow: DENY_ADVERTISE_STARTD excludes an identity
// that ALLOW_ADVERTISE_STARTD admits, and the denial names the DENY knob.
func TestAuthzDenyOverridesAllow(t *testing.T) {
	ac := startAuthzCollector(t, map[string]string{
		"ALLOW_READ":             "*",
		"ALLOW_ADVERTISE_STARTD": "*@pool",
		"DENY_ADVERTISE_STARTD":  "banned@pool",
	})
	if err := ac.advertise(t, ac.as(t, "banned@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot1@banned"); err == nil {
		t.Error("update from DENY_ADVERTISE_STARTD identity was acknowledged")
	}
	ac.expectDenial(t, "UPDATE_STARTD_AD_WITH_ACK", "ADVERTISE_STARTD", "banned@pool", "ALLOW_ADVERTISE_STARTD", "DENY_ADVERTISE_STARTD")
	if err := ac.advertise(t, ac.as(t, "startd@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot1@startd"); err != nil {
		t.Fatalf("update from an allowed identity: %v", err)
	}
	names := ac.adNames(t, store.StartdAd)
	if names["slot1@banned"] {
		t.Error("ad from DENY_ADVERTISE_STARTD identity was stored")
	}
	if !names["slot1@startd"] {
		t.Error("ad from allowed identity was not stored")
	}
}

// TestAuthzLevelsMatchCpp: ad types are authorized at the C++ collector's
// levels, not one shared ADVERTISE level. An unset ALLOW_ADVERTISE_STARTD
// falls back to ALLOW_DAEMON, and negotiator ads need NEGOTIATOR even from a
// DAEMON identity.
func TestAuthzLevelsMatchCpp(t *testing.T) {
	ac := startAuthzCollector(t, map[string]string{
		"ALLOW_READ":       "*",
		"ALLOW_DAEMON":     "daemon@pool",
		"ALLOW_NEGOTIATOR": "negotiator@pool",
	})
	if err := ac.advertise(t, ac.as(t, "daemon@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot1@daemon"); err != nil {
		t.Fatalf("UPDATE_STARTD_AD_WITH_ACK from ALLOW_DAEMON identity (ADVERTISE_STARTD falls back to DAEMON): %v", err)
	}
	if err := ac.advertise(t, ac.as(t, "negotiator@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot1@negotiator"); err == nil {
		t.Error("UPDATE_STARTD_AD_WITH_ACK from an identity outside ALLOW_DAEMON was acknowledged")
	}
	ac.expectDenial(t, "UPDATE_STARTD_AD_WITH_ACK", "ADVERTISE_STARTD", "negotiator@pool", "ALLOW_DAEMON", "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	negAd := mustParse(t, `[MyType="Negotiator"; Name="neg@pool"; MyAddress="<127.0.0.1:2>"]`)
	advNeg := func(who string) error {
		c := htcondor.WithSecurityConfig(ctx, ac.as(t, who))
		return htcondor.NewCollector(ac.addr).Advertise(c, negAd, &htcondor.AdvertiseOptions{Command: commands.UPDATE_NEGOTIATOR_AD})
	}
	expectDenied(t, advNeg("daemon@pool"))
	ac.expectDenial(t, "UPDATE_NEGOTIATOR_AD", "NEGOTIATOR", "daemon@pool", "ALLOW_NEGOTIATOR", "")
	if err := advNeg("negotiator@pool"); err != nil {
		t.Fatalf("UPDATE_NEGOTIATOR_AD as negotiator@pool: %v", err)
	}
	ac.waitStored(t, store.NegotiatorAd, "neg@pool")
}

// TestAuthzTokenLimits: a token limited to ADVERTISE_STARTD cannot run
// QUERY_STARTD_PVT_ADS (NEGOTIATOR) even for an identity in
// ALLOW_NEGOTIATOR, and the refusal is logged with the token's limits; the
// same token still advertises a startd ad, and the identity's unlimited
// token still reads the private ads.
func TestAuthzTokenLimits(t *testing.T) {
	ac := startAuthzCollector(t, map[string]string{
		"ALLOW_READ":             "*",
		"ALLOW_ADVERTISE_STARTD": "daemon@pool",
		"ALLOW_NEGOTIATOR":       "daemon@pool",
	})
	limited := ac.limitedAs(t, "daemon@pool", []string{"ADVERTISE_STARTD"})
	if err := ac.advertise(t, limited, commands.UPDATE_STARTD_AD, "slot1@host"); err != nil {
		t.Fatalf("UPDATE_STARTD_AD with an ADVERTISE_STARTD-limited token: %v", err)
	}
	ac.waitStored(t, store.StartdPvtAd, "slot1@host")

	ads, err := ac.query(t, ac.limitedAs(t, "daemon@pool", []string{"ADVERTISE_STARTD"}), commands.QUERY_STARTD_PVT_ADS)
	if err == nil {
		t.Errorf("QUERY_STARTD_PVT_ADS with an ADVERTISE_STARTD-limited token returned %d ads, want a refusal", len(ads))
	}
	got := ac.waitDenial(t)
	want := map[string]string{
		"command":              "QUERY_STARTD_PVT_ADS",
		"level":                "NEGOTIATOR",
		"authorization_limits": "ADVERTISE_STARTD",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("denial %s = %q, want %q (record %v)", k, got[k], v, got)
		}
	}
	if got["peer"] == "" {
		t.Errorf("denial names no peer (record %v)", got)
	}

	ads, err = ac.query(t, ac.as(t, "daemon@pool"), commands.QUERY_STARTD_PVT_ADS)
	if err != nil {
		t.Fatalf("QUERY_STARTD_PVT_ADS with an unlimited token: %v", err)
	}
	if len(ads) != 1 {
		t.Errorf("QUERY_STARTD_PVT_ADS with an unlimited token returned %d ads, want 1", len(ads))
	}
}

// TestAuthzMasterSessions: condor_master's family and inherited parent
// sessions hold the levels DaemonCore grants them whatever ALLOW_/DENY_
// say, so under a restrictive ALLOW_ADMINISTRATOR the master can still
// reconfigure the collector with DC_RECONFIG; another condor identity is
// decided by the lists.
func TestAuthzMasterSessions(t *testing.T) {
	ac := startAuthzCollector(t, map[string]string{
		"ALLOW_READ":          "*",
		"ALLOW_ADMINISTRATOR": "admin@pool",
		"DENY_NEGOTIATOR":     "condor@*",
	})
	// The daemon's DC_* commands, as run() registers them. DC_RECONFIG
	// rereads CONDOR_CONFIG, so point it at an empty file.
	condorConfig := filepath.Join(t.TempDir(), "condor_config")
	if err := os.WriteFile(condorConfig, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CONDOR_CONFIG", condorConfig)
	dlog, err := logging.New(&logging.Config{OutputPath: filepath.Join(t.TempDir(), "CollectorLog")})
	if err != nil {
		t.Fatal(err)
	}
	d, err := daemon.New(daemon.Options{Subsys: "COLLECTOR", Config: config.NewEmpty(), Logger: dlog})
	if err != nil {
		t.Fatal(err)
	}
	reconfigs := make(chan struct{}, 4)
	d.OnReconfig(func(*config.Config) { reconfigs <- struct{}{} })
	d.RegisterDefaultCommands(ac.srv)

	reconfig := func(sec *security.SecurityConfig) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cs := *sec
		cs.Command = commands.DC_RECONFIG
		cl, err := client.ConnectAndAuthenticate(ctx, ac.addr, &cs)
		if err != nil {
			t.Fatalf("DC_RECONFIG: %v", err)
		}
		_ = cl.Close()
	}

	reconfig(ac.session(t, "condor@pool"))
	ac.expectDenial(t, "DC_RECONFIG", "ADMINISTRATOR", "condor@pool", "ALLOW_ADMINISTRATOR", "")
	select {
	case <-reconfigs:
		t.Error("DC_RECONFIG from condor@pool, outside ALLOW_ADMINISTRATOR, reconfigured the collector")
	default:
	}

	for _, who := range []string{"condor@family", "condor@parent"} {
		reconfig(ac.session(t, who))
		select {
		case <-reconfigs:
		case <-time.After(10 * time.Second):
			t.Errorf("DC_RECONFIG from %s did not reconfigure the collector", who)
		}
	}

	// The levels each session holds: the family session's include the
	// ADVERTISE_* and NEGOTIATOR levels (over DENY_NEGOTIATOR); the parent
	// session's do not.
	for _, perm := range []string{"ADMINISTRATOR", "DAEMON", "NEGOTIATOR", "ADVERTISE_STARTD", "ADVERTISE_SCHEDD", "ADVERTISE_MASTER", "WRITE", "READ"} {
		if !ac.az.Authorize(perm, "127.0.0.1:1", "condor@family") {
			t.Errorf("condor@family not authorized at %s", perm)
		}
	}
	for _, perm := range []string{"ADMINISTRATOR", "DAEMON", "WRITE", "READ"} {
		if !ac.az.Authorize(perm, "127.0.0.1:1", "condor@parent") {
			t.Errorf("condor@parent not authorized at %s", perm)
		}
	}
	for _, perm := range []string{"NEGOTIATOR", "ADVERTISE_STARTD"} {
		if ac.az.Authorize(perm, "127.0.0.1:1", "condor@parent") {
			t.Errorf("condor@parent authorized at %s; DaemonCore grants it ADMINISTRATOR and DAEMON only", perm)
		}
	}
}

// TestAuthzReload: a reconfig that admits an identity takes effect for the
// next command.
func TestAuthzReload(t *testing.T) {
	knobs := map[string]string{
		"ALLOW_READ":             "*",
		"ALLOW_ADVERTISE_STARTD": "other@pool",
	}
	ac := startAuthzCollector(t, knobs)
	if err := ac.advertise(t, ac.as(t, "startd@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot1@startd"); err == nil {
		t.Fatal("update acknowledged before the identity was allowed")
	}
	ac.expectDenial(t, "UPDATE_STARTD_AD_WITH_ACK", "ADVERTISE_STARTD", "startd@pool", "ALLOW_ADVERTISE_STARTD", "")

	cfg := config.NewEmpty()
	for k, v := range knobs {
		cfg.Set(k, v)
	}
	cfg.Set("ALLOW_ADVERTISE_STARTD", "startd@pool")
	if err := ac.az.Reload(cfg); err != nil {
		t.Fatal(err)
	}
	if err := ac.advertise(t, ac.as(t, "startd@pool"), commands.UPDATE_STARTD_AD_WITH_ACK, "slot1@startd"); err != nil {
		t.Fatalf("update after reload: %v", err)
	}
}

// TestLogDenialIgnoresOtherErrors: only authorization refusals are logged as
// denials.
func TestLogDenialIgnoresOtherErrors(t *testing.T) {
	az, err := daemon.NewAuthz(config.NewEmpty(), daemon.AuthzOptions{Subsys: "COLLECTOR"})
	if err != nil {
		t.Fatal(err)
	}
	records := make(chan slog.Record, 4)
	if az.LogDenial(slog.New(&recordHandler{ch: records}), nil, "127.0.0.1:1",
		errors.New("cedar/server: command 0 (UPDATE_STARTD_AD) refused: session (authenticated=false encrypted=false) does not meet the command's security level")) {
		t.Error("LogDenial reported a security-level refusal as a denial")
	}
	if len(records) != 0 {
		t.Errorf("logged %d records for a security-level refusal, want 0", len(records))
	}
}

// expectDenied checks err is the collector's DENIED answer to a refused
// command.
func expectDenied(t *testing.T, err error) {
	t.Helper()
	var ae *security.AuthorizationError
	if !errors.As(err, &ae) || ae.ReturnCode != "DENIED" {
		t.Errorf("refused command returned %v, want an AuthorizationError DENIED", err)
	}
}

func mustParse(t *testing.T, s string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return ad
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
