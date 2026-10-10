// Package seclevel negotiates each command on a cedar command server under
// the security policy of the authorization level the command is registered
// at, as DaemonCore does: the server side of the handshake takes its policy
// from the command's DCpermission (daemon_command.cpp,
// FillInSecurityPolicyAdFromCache(m_comTable[m_cmd_index].perm)), and each
// SEC_<level>_* setting falls back along DCpermissionHierarchy::nextConfig
// (condor_secman.cpp, SecMan::getSecSetting): an ADVERTISE_* level to DAEMON,
// every level to DEFAULT.
package seclevel

import (
	"strings"

	"github.com/bbockelm/cedar/security"
	cedarserver "github.com/bbockelm/cedar/server"
	"github.com/bbockelm/golang-htcondor/authz"
	"github.com/bbockelm/golang-htcondor/config"
)

// Levels are the authorization levels a command can be registered at.
var Levels = []authz.Perm{
	authz.PermAllow, authz.PermRead, authz.PermWrite, authz.PermNegotiator,
	authz.PermAdministrator, authz.PermConfig, authz.PermDaemon,
	authz.PermAdvertiseStartd, authz.PermAdvertiseSchedd, authz.PermAdvertiseMaster,
}

// Build returns the security policy of every level in Levels, keyed by level
// name. build returns the server policy of the SEC_<secContext>_* settings,
// each falling back to SEC_DEFAULT_* (as htcondor.GetServerSecurityConfig and
// daemon.Daemon.ServerSecurityConfig do). For an ADVERTISE_* level Build
// inserts DAEMON into that fallback, setting by setting: a SEC_ADVERTISE_*_X
// left unset takes SEC_DAEMON_X before SEC_DEFAULT_X.
func Build(cfg *config.Config, build func(secContext string) (*security.SecurityConfig, error)) (map[string]*security.SecurityConfig, error) {
	byLevel := make(map[string]*security.SecurityConfig, len(Levels))
	var daemonSec *security.SecurityConfig
	for _, lvl := range Levels {
		s, err := build(string(lvl))
		if err != nil {
			return nil, err
		}
		if lvl == authz.PermDaemon {
			daemonSec = s
		}
		byLevel[string(lvl)] = s
	}
	for _, lvl := range []authz.Perm{authz.PermAdvertiseStartd, authz.PermAdvertiseSchedd, authz.PermAdvertiseMaster} {
		s := byLevel[string(lvl)]
		set := func(feature string) bool {
			v, ok := cfg.Get("SEC_" + string(lvl) + "_" + feature)
			return ok && strings.TrimSpace(v) != ""
		}
		if !set("AUTHENTICATION") {
			s.Authentication = daemonSec.Authentication
		}
		if !set("ENCRYPTION") {
			s.Encryption = daemonSec.Encryption
		}
		if !set("INTEGRITY") {
			s.Integrity = daemonSec.Integrity
		}
		if !set("AUTHENTICATION_METHODS") {
			s.AuthMethods = daemonSec.AuthMethods
		}
		if !set("CRYPTO_METHODS") {
			s.CryptoMethods = daemonSec.CryptoMethods
		}
	}
	return byLevel, nil
}

// Install makes srv negotiate each command under byLevel's policy for the
// first level the command is registered at; a command registered at no level
// (or at one byLevel lacks) uses srv.SecurityConfig. It applies to commands
// registered on srv later, too. Each policy takes srv.SecurityConfig's
// post-authentication policy, which cedarserver.New installed there, so a
// session negotiated at any level advertises the commands its peer is
// authorized for and maps its identity as the base policy does. Call it after
// cedarserver.New.
func Install(srv *cedarserver.Server, byLevel map[string]*security.SecurityConfig) {
	for _, s := range byLevel {
		s.PostAuthPolicy = srv.SecurityConfig.PostAuthPolicy
		s.CommandPerms = srv.SecurityConfig.CommandPerms
	}
	srv.SecurityConfigForCommand = func(command int) *security.SecurityConfig {
		perms := srv.CommandPerms(command)
		if len(perms) == 0 {
			return nil
		}
		return byLevel[perms[0]]
	}
}
