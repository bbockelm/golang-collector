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

	"github.com/bbockelm/cedar/commands"
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

// punchedHoles are the levels C++ DaemonCore grants the identities of the
// sessions condor_master shares with its children whatever ALLOW_/DENY_ say
// (IpVerify::PunchHole in daemon_core.cpp: the inherited parent session at
// ~9364-9366, the family session at ~9421-9427), with the levels each one
// implies. Without them a restrictive ALLOW_ADMINISTRATOR would refuse
// condor_master's own DC_RECONFIG and DC_OFF. cedar attributes these
// identities only to a peer resuming the inherited session.
var punchedHoles = map[string][]authz.Perm{
	"condor@family": {
		authz.PermAdministrator, authz.PermDaemon, authz.PermNegotiator,
		authz.PermAdvertiseMaster, authz.PermAdvertiseSchedd, authz.PermAdvertiseStartd,
		authz.PermWrite, authz.PermRead,
	},
	"condor@parent": {authz.PermAdministrator, authz.PermDaemon, authz.PermWrite, authz.PermRead},
}

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
	for _, p := range punchedHoles[user] {
		if string(p) == perm {
			return true
		}
	}
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
// Authorizer refuses a command: it names the command and the identity, which
// is unauthenticated@unmapped for a peer that did not authenticate.
// limitedRE matches the one it returns when the command's levels are outside
// the authorization limits of the peer's credential (an IDTOKEN's
// condor:/<LEVEL> scopes), which cedar checks before the Authorizer. The
// denial tests in authz_test.go fail if cedar changes either wording.
var (
	refusedRE = regexp.MustCompile(`command (\d+) \([^)]*\) refused: identity ("(?:[^"\\]|\\.)*") is not authorized`)
	limitedRE = regexp.MustCompile(`command (\d+) \([^)]*\) refused: authorization levels \[([^\]]*)\] are outside the session's authorization limits \[([^\]]*)\]`)
)

// logDenial logs err if it is a command refused for authorization: by the
// ALLOW_/DENY_ policy, with the command, its authorization level, the peer,
// the identity and the settings that decide that level; or by the peer's
// authorization limits, with the command, level, peer and those limits.
func (a *collectorAuthz) logDenial(log *slog.Logger, srv *cedarserver.Server, peerAddr string, err error) {
	msg := err.Error()
	if m := limitedRE.FindStringSubmatch(msg); m != nil {
		cmd, convErr := strconv.Atoi(m[1])
		if convErr != nil {
			return
		}
		log.Warn("PERMISSION DENIED",
			"command", commandName(cmd),
			"command_id", cmd,
			"level", strings.Join(strings.Fields(m[2]), ","),
			"peer", peerAddr,
			"authorization_limits", strings.Join(strings.Fields(m[3]), ","),
			"destination", "security")
		return
	}
	m := refusedRE.FindStringSubmatch(msg)
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
	log.Warn("PERMISSION DENIED",
		"command", commandName(cmd),
		"command_id", cmd,
		"level", strings.Join(perms, ","),
		"peer", peerAddr,
		"user", user,
		"allow_knob", strings.Join(allow, ","),
		"deny_knob", strings.Join(deny, ","),
		"destination", "security")
}

// dcCommandNames names the DaemonCore commands this socket serves (see
// daemon.RegisterDefaultCommands) that cedar's command table does not.
var dcCommandNames = map[int]string{
	commands.DC_NOP:            "DC_NOP",
	commands.DC_NOP_READ:       "DC_NOP_READ",
	commands.DC_NOP_WRITE:      "DC_NOP_WRITE",
	commands.DC_NOP_NEGOTIATOR: "DC_NOP_NEGOTIATOR",
	commands.DC_RECONFIG:       "DC_RECONFIG",
	commands.DC_RECONFIG_FULL:  "DC_RECONFIG_FULL",
	commands.DC_OFF_GRACEFUL:   "DC_OFF_GRACEFUL",
	commands.DC_OFF_PEACEFUL:   "DC_OFF_PEACEFUL",
	commands.DC_OFF_FAST:       "DC_OFF_FAST",
}

// commandName returns cmd's HTCondor name, or its number if it has none.
func commandName(cmd int) string {
	if n := server.CommandName(cmd); n != "" {
		return n
	}
	if n, ok := dcCommandNames[cmd]; ok {
		return n
	}
	return strconv.Itoa(cmd)
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
