package negotiator_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/message"
	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
)

// setPriorityFactor sends SET_PRIORITYFACTOR as sec's identity and returns
// the reply ad's ErrorCode.
func setPriorityFactor(t *testing.T, ctx context.Context, addr string, sec *security.SecurityConfig, submitter string, factor float64) int64 {
	t.Helper()
	cs := *sec
	cs.Command = commands.SET_PRIORITYFACTOR
	cs.RemoteVersion = testVersion
	cl, err := client.ConnectAndAuthenticate(ctx, addr, &cs)
	if err != nil {
		t.Fatalf("dialing SET_PRIORITYFACTOR: %v", err)
	}
	defer func() { _ = cl.Close() }()
	m := message.NewMessageForStream(cl.GetStream())
	if err := m.PutString(ctx, submitter); err != nil {
		t.Fatal(err)
	}
	if err := m.PutDouble(ctx, factor); err != nil {
		t.Fatal(err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatal(err)
	}
	reply, err := message.NewMessageFromStream(cl.GetStream()).GetClassAd(ctx)
	if err != nil {
		t.Fatalf("SET_PRIORITYFACTOR: reading reply ad: %v", err)
	}
	code, ok := classad.GetAs[int64](reply, "ErrorCode")
	if !ok {
		t.Fatalf("SET_PRIORITYFACTOR reply has no ErrorCode: %s", reply)
	}
	return code
}

// TestSetPriorityFactorAuthorizationLimits: SET_PRIORITYFACTOR is registered
// at WRITE but sets the factor only for a peer holding ADMINISTRATOR, which
// must also be within its credential's authorization limits. A token limited
// to WRITE is refused even for an identity the policy grants ADMINISTRATOR;
// the same identity's unlimited token is not.
func TestSetPriorityFactorAuthorizationLimits(t *testing.T) {
	ctx := testCtx(t)
	keyDir := t.TempDir()
	if err := security.GeneratePoolSigningKey(filepath.Join(keyDir, "POOL")); err != nil {
		t.Fatal(err)
	}
	const trustDomain = "pool"
	serverSec := &security.SecurityConfig{
		AuthMethods:             []security.AuthMethod{security.AuthToken},
		Authentication:          security.SecurityRequired,
		CryptoMethods:           []security.CryptoMethod{security.CryptoAES},
		Encryption:              security.SecurityRequired,
		Integrity:               security.SecurityOptional,
		TrustDomain:             trustDomain,
		TokenPoolSigningKeyFile: filepath.Join(keyDir, "POOL"),
		TokenSigningKeyDir:      keyDir,
	}
	adminOnly := func(_, _, user string) bool { return user == "admin@"+trustDomain }
	addr, acct, _ := newDaemonWithSecurity(t, ctx, adminOnly, serverSec, nil)

	token := func(limits []string) *security.SecurityConfig {
		now := time.Now().Unix()
		tok, err := security.GenerateJWT(keyDir, "POOL", "admin@"+trustDomain, trustDomain, now, now+3600, limits)
		if err != nil {
			t.Fatal(err)
		}
		return &security.SecurityConfig{
			AuthMethods:    []security.AuthMethod{security.AuthToken},
			Authentication: security.SecurityRequired,
			CryptoMethods:  []security.CryptoMethod{security.CryptoAES},
			Encryption:     security.SecurityRequired,
			Integrity:      security.SecurityOptional,
			Token:          tok,
			TrustDomain:    trustDomain,
			SessionCache:   security.NewSessionCache(),
		}
	}
	const bob = "bob@pool.test"

	if code := setPriorityFactor(t, ctx, addr, token([]string{"WRITE"}), bob, 50); code == 0 {
		t.Error("SET_PRIORITYFACTOR with a WRITE-limited token replied ErrorCode 0")
	}
	if got := acct.GetPriorityFactor(bob); approx(got, 50) {
		t.Error("SET_PRIORITYFACTOR with a WRITE-limited token changed the factor")
	}

	if code := setPriorityFactor(t, ctx, addr, token(nil), bob, 60); code != 0 {
		t.Fatalf("SET_PRIORITYFACTOR with an unlimited token replied ErrorCode %d, want 0", code)
	}
	if got := acct.GetPriorityFactor(bob); !approx(got, 60) {
		t.Errorf("factor after SET_PRIORITYFACTOR with an unlimited token = %v, want 60", got)
	}
}

// TestAuthorizeUnauthenticatedPeer: a peer that did not authenticate is
// authorized as unauthenticated@unmapped, the identity cedar's own dispatch
// check and HTCondor use, not as "" (which HTCondor's ALLOW_ matching treats
// as the wildcard user).
func TestAuthorizeUnauthenticatedPeer(t *testing.T) {
	ctx := testCtx(t)
	var mu sync.Mutex
	var users []string
	record := func(_, _, user string) bool {
		mu.Lock()
		defer mu.Unlock()
		users = append(users, user)
		return false
	}
	addr, acct, _ := newDaemon(t, ctx, record, nil)
	const bob = "bob@pool.test"

	cl := dialCmd(t, ctx, addr, commands.SET_CEILING)
	m := message.NewMessageForStream(cl.GetStream())
	if err := m.PutString(ctx, bob); err != nil {
		t.Fatal(err)
	}
	if err := m.PutInt(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if err := m.FinishMessage(ctx); err != nil {
		t.Fatal(err)
	}
	waitClosed(ctx, cl)

	if got := acct.GetCeiling(bob); got != -1 {
		t.Errorf("ceiling after refused SET_CEILING = %v, want -1 (unset)", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(users) == 0 {
		t.Fatal("the authorizer was not consulted")
	}
	for _, u := range users {
		if u != cedarserver.UnauthenticatedFQU {
			t.Errorf("authorizer saw user %q, want %q", u, cedarserver.UnauthenticatedFQU)
		}
	}
}
