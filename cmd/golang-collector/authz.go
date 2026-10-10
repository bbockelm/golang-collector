package main

import (
	"context"
	"log/slog"
	"net"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"

	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/golang-htcondor/authz"

	"github.com/bbockelm/golang-collector/server"
)

// collectorAuthz is the ALLOW_<level>/DENY_<level> authorization policy for
// every command served on the collector's command socket: the collector
// protocol, the DC_* defaults, and the embedded CCB and negotiator when they
// are enabled. It is built once from the configuration and rebuilt on
// reconfig, as the C++ daemons rebuild IpVerify.
type collectorAuthz struct {
	cur atomic.Pointer[authzState]
}

type authzState struct {
	policy *authz.Policy
	cfg    authz.ConfigGetter
}

// authzSubsys is the subsystem whose ALLOW_<level>_<subsys> variants apply.
const authzSubsys = "COLLECTOR"

func newCollectorAuthz(cfg authz.ConfigGetter) (*collectorAuthz, error) {
	a := &collectorAuthz{}
	if err := a.reload(cfg); err != nil {
		return nil, err
	}
	return a, nil
}

// reload rebuilds the policy from cfg; commands dispatched afterwards are
// checked against the new one.
func (a *collectorAuthz) reload(cfg authz.ConfigGetter) error {
	p, err := authz.NewPolicy(cfg, authzSubsys)
	if err != nil {
		return err
	}
	a.cur.Store(&authzState{policy: p, cfg: cfg})
	return nil
}

// Authorize reports whether user, connecting from peerAddr, holds the
// authorization level perm. It has the signature of cedar's
// Server.Authorizer.
func (a *collectorAuthz) Authorize(perm, peerAddr, user string) bool {
	return a.cur.Load().policy.Authorize(perm, peerAddr, user)
}

// policyKnob returns the setting that supplies prefix+perm (prefix "ALLOW_"
// or "DENY_"), or "" if none is set. An unset level falls back to the next
// one HTCondor consults (ADVERTISE_* to DAEMON, every level to DEFAULT), and
// at each level the _COLLECTOR variant is consulted first, as in authz.Policy.
func (a *collectorAuthz) policyKnob(prefix, perm string) string {
	cfg := a.cur.Load().cfg
	chain := []string{perm}
	if strings.HasPrefix(perm, "ADVERTISE_") {
		chain = append(chain, string(authz.PermDaemon))
	}
	chain = append(chain, string(authz.PermDefault))
	for _, p := range chain {
		for _, k := range []string{prefix + p + "_" + authzSubsys, prefix + p} {
			if _, ok := cfg.Get(k); ok {
				return k
			}
		}
	}
	return ""
}

// refusedRE matches the error cedar's Server.ServeConn returns when the
// Authorizer refuses a command: it names the command and the identity. The
// denial tests in authz_test.go fail if cedar changes that wording.
var refusedRE = regexp.MustCompile(`command (\d+) \([^)]*\) refused: identity ("(?:[^"\\]|\\.)*") is not authorized`)

// logDenial logs err if it is a command the authorization policy refused,
// with the command, its authorization level, the peer, the identity, and
// the ALLOW_/DENY_ settings that decide that level.
func (a *collectorAuthz) logDenial(log *slog.Logger, srv *cedarserver.Server, peerAddr string, err error) {
	m := refusedRE.FindStringSubmatch(err.Error())
	if m == nil {
		return
	}
	cmd, convErr := strconv.Atoi(m[1])
	if convErr != nil {
		return
	}
	user, convErr := strconv.Unquote(m[2])
	if convErr != nil {
		return
	}
	if user == "" {
		user = "unauthenticated"
	}
	perms := srv.CommandPerms(cmd)
	var allow, deny []string
	for _, perm := range perms {
		k := a.policyKnob("ALLOW_", perm)
		if k == "" {
			k = "ALLOW_" + perm + " (unset)"
		}
		allow = append(allow, k)
		if k := a.policyKnob("DENY_", perm); k != "" {
			deny = append(deny, k)
		}
	}
	name := server.CommandName(cmd)
	if name == "" {
		name = m[1]
	}
	log.Warn("PERMISSION DENIED",
		"command", name,
		"command_id", cmd,
		"level", strings.Join(perms, ","),
		"peer", peerAddr,
		"user", user,
		"allow_knob", strings.Join(allow, ","),
		"deny_knob", strings.Join(deny, ","),
		"destination", "security")
}

// serveCommands returns a serve loop for srv: like cedar's Server.Serve, it
// accepts connections on a listener and serves each on srv until ctx is
// cancelled, and it also logs every command the authorization policy refused.
// cedar closes a refused connection without logging it, so this is the
// operator's record of a denial.
func serveCommands(srv *cedarserver.Server, az *collectorAuthz, log *slog.Logger) func(context.Context, net.Listener) error {
	return func(ctx context.Context, ln net.Listener) error {
		go func() {
			<-ctx.Done()
			_ = ln.Close()
		}()
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			_ = srv.KeepAlive.Apply(conn)
			go func() {
				peer := conn.RemoteAddr().String()
				defer func() {
					if r := recover(); r != nil {
						log.Error("panic in connection handler; recovered",
							"panic", r, "remote", peer, "stack", string(debug.Stack()))
						_ = conn.Close()
					}
				}()
				if err := srv.ServeConn(ctx, conn); err != nil {
					az.logDenial(log, srv, peer, err)
				}
			}()
		}
	}
}
