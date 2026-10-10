// Package server maps the collector's CEDAR command protocol onto a store.Store,
// using the cedar command-dispatch server for the wire/security layer.
package server

import (
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/golang-htcondor/authz"

	"github.com/bbockelm/golang-collector/store"
)

// CommandPerm returns the HTCondor authorization level (DCpermission) a
// collector command is registered at, the level an identity must hold in the
// ALLOW_<level>/DENY_<level> policy to run it. The levels are those of the C++
// condor_collector's Register_CommandWithPayload calls (see collectorCommands).
// Commands outside that table (the Go-only WatchAds) are read-only and run at
// READ.
func CommandPerm(cmd int) string {
	if c, ok := collectorCommands[cmd]; ok {
		return string(c.perm)
	}
	return string(authz.PermRead)
}

// CommandNames returns the name of every collector command, by number, for
// naming the commands in denial logs (daemon.AuthzOptions.CommandNames).
func CommandNames() map[int]string {
	names := make(map[int]string, len(collectorCommands))
	for cmd, c := range collectorCommands {
		names[cmd] = c.name
	}
	return names
}

// commandInfo is a collector command's name and authorization level.
type commandInfo struct {
	name string
	perm authz.Perm
}

// collectorCommands names every collector command and gives its authorization
// level, copied from CollectorDaemon::Init in the C++ collector
// (src/condor_collector.V6/collector.cpp): queries at READ except the private
// ones at NEGOTIATOR; startd, schedd/submitter and master ads at their
// ADVERTISE_* level; negotiator and accounting ads at NEGOTIATOR; every other
// ad type at DAEMON. Updates and invalidations of one ad type share a level.
var collectorCommands = map[int]commandInfo{
	commands.QUERY_STARTD_ADS:       {"QUERY_STARTD_ADS", authz.PermRead},
	commands.QUERY_STARTD_PVT_ADS:   {"QUERY_STARTD_PVT_ADS", authz.PermNegotiator},
	commands.QUERY_SCHEDD_ADS:       {"QUERY_SCHEDD_ADS", authz.PermRead},
	commands.QUERY_MASTER_ADS:       {"QUERY_MASTER_ADS", authz.PermRead},
	commands.QUERY_CKPT_SRVR_ADS:    {"QUERY_CKPT_SRVR_ADS", authz.PermRead},
	commands.QUERY_SUBMITTOR_ADS:    {"QUERY_SUBMITTOR_ADS", authz.PermRead},
	commands.QUERY_LICENSE_ADS:      {"QUERY_LICENSE_ADS", authz.PermRead},
	commands.QUERY_COLLECTOR_ADS:    {"QUERY_COLLECTOR_ADS", authz.PermRead},
	commands.QUERY_STORAGE_ADS:      {"QUERY_STORAGE_ADS", authz.PermRead},
	commands.QUERY_ACCOUNTING_ADS:   {"QUERY_ACCOUNTING_ADS", authz.PermRead},
	commands.QUERY_NEGOTIATOR_ADS:   {"QUERY_NEGOTIATOR_ADS", authz.PermRead},
	commands.QUERY_HAD_ADS:          {"QUERY_HAD_ADS", authz.PermRead},
	commands.QUERY_ANY_ADS:          {"QUERY_ANY_ADS", authz.PermRead},
	commands.QUERY_GRID_ADS:         {"QUERY_GRID_ADS", authz.PermRead},
	commands.QUERY_MULTIPLE_ADS:     {"QUERY_MULTIPLE_ADS", authz.PermRead},
	commands.QUERY_MULTIPLE_PVT_ADS: {"QUERY_MULTIPLE_PVT_ADS", authz.PermNegotiator},

	commands.INVALIDATE_STARTD_ADS:     {"INVALIDATE_STARTD_ADS", authz.PermAdvertiseStartd},
	commands.INVALIDATE_SCHEDD_ADS:     {"INVALIDATE_SCHEDD_ADS", authz.PermAdvertiseSchedd},
	commands.INVALIDATE_MASTER_ADS:     {"INVALIDATE_MASTER_ADS", authz.PermAdvertiseMaster},
	commands.INVALIDATE_CKPT_SRVR_ADS:  {"INVALIDATE_CKPT_SRVR_ADS", authz.PermDaemon},
	commands.INVALIDATE_SUBMITTOR_ADS:  {"INVALIDATE_SUBMITTOR_ADS", authz.PermAdvertiseSchedd},
	commands.INVALIDATE_LICENSE_ADS:    {"INVALIDATE_LICENSE_ADS", authz.PermDaemon},
	commands.INVALIDATE_COLLECTOR_ADS:  {"INVALIDATE_COLLECTOR_ADS", authz.PermDaemon},
	commands.INVALIDATE_STORAGE_ADS:    {"INVALIDATE_STORAGE_ADS", authz.PermDaemon},
	commands.INVALIDATE_ACCOUNTING_ADS: {"INVALIDATE_ACCOUNTING_ADS", authz.PermNegotiator},
	commands.INVALIDATE_NEGOTIATOR_ADS: {"INVALIDATE_NEGOTIATOR_ADS", authz.PermNegotiator},
	commands.INVALIDATE_HAD_ADS:        {"INVALIDATE_HAD_ADS", authz.PermDaemon},
	commands.INVALIDATE_GRID_ADS:       {"INVALIDATE_GRID_ADS", authz.PermDaemon},

	commands.UPDATE_STARTD_AD:          {"UPDATE_STARTD_AD", authz.PermAdvertiseStartd},
	commands.UPDATE_STARTD_AD_WITH_ACK: {"UPDATE_STARTD_AD_WITH_ACK", authz.PermAdvertiseStartd},
	commands.UPDATE_SCHEDD_AD:          {"UPDATE_SCHEDD_AD", authz.PermAdvertiseSchedd},
	commands.UPDATE_SUBMITTOR_AD:       {"UPDATE_SUBMITTOR_AD", authz.PermAdvertiseSchedd},
	commands.UPDATE_LICENSE_AD:         {"UPDATE_LICENSE_AD", authz.PermDaemon},
	commands.UPDATE_MASTER_AD:          {"UPDATE_MASTER_AD", authz.PermAdvertiseMaster},
	commands.UPDATE_CKPT_SRVR_AD:       {"UPDATE_CKPT_SRVR_AD", authz.PermDaemon},
	commands.UPDATE_COLLECTOR_AD:       {"UPDATE_COLLECTOR_AD", authz.PermDaemon},
	commands.UPDATE_STORAGE_AD:         {"UPDATE_STORAGE_AD", authz.PermDaemon},
	commands.UPDATE_NEGOTIATOR_AD:      {"UPDATE_NEGOTIATOR_AD", authz.PermNegotiator},
	commands.UPDATE_HAD_AD:             {"UPDATE_HAD_AD", authz.PermDaemon},
	commands.UPDATE_GRID_AD:            {"UPDATE_GRID_AD", authz.PermDaemon},
	commands.UPDATE_ACCOUNTING_AD:      {"UPDATE_ACCOUNTING_AD", authz.PermNegotiator},
}

// updateCommands maps each UPDATE_*_AD command to the table it feeds.
var updateCommands = map[int]store.AdType{
	commands.UPDATE_STARTD_AD:     store.StartdAd,
	commands.UPDATE_SCHEDD_AD:     store.ScheddAd,
	commands.UPDATE_MASTER_AD:     store.MasterAd,
	commands.UPDATE_SUBMITTOR_AD:  store.SubmitterAd,
	commands.UPDATE_COLLECTOR_AD:  store.CollectorAd,
	commands.UPDATE_NEGOTIATOR_AD: store.NegotiatorAd,
	commands.UPDATE_LICENSE_AD:    store.LicenseAd,
	commands.UPDATE_STORAGE_AD:    store.StorageAd,
	commands.UPDATE_CKPT_SRVR_AD:  store.CkptSrvrAd,
	commands.UPDATE_ACCOUNTING_AD: store.AccountingAd,
	commands.UPDATE_GRID_AD:       store.GridAd,
	commands.UPDATE_HAD_AD:        store.HadAd,
}

// queryCommands maps each QUERY_*_ADS command to the table it scans.
var queryCommands = map[int]store.AdType{
	commands.QUERY_STARTD_ADS: store.StartdAd,
	// Private startd ads (claim ids): registered at NEGOTIATOR (collectorCommands).
	commands.QUERY_STARTD_PVT_ADS: store.StartdPvtAd,
	commands.QUERY_SCHEDD_ADS:     store.ScheddAd,
	commands.QUERY_MASTER_ADS:     store.MasterAd,
	commands.QUERY_SUBMITTOR_ADS:  store.SubmitterAd,
	commands.QUERY_COLLECTOR_ADS:  store.CollectorAd,
	commands.QUERY_NEGOTIATOR_ADS: store.NegotiatorAd,
	commands.QUERY_LICENSE_ADS:    store.LicenseAd,
	commands.QUERY_STORAGE_ADS:    store.StorageAd,
	commands.QUERY_CKPT_SRVR_ADS:  store.CkptSrvrAd,
	commands.QUERY_ACCOUNTING_ADS: store.AccountingAd,
	commands.QUERY_GRID_ADS:       store.GridAd,
	commands.QUERY_HAD_ADS:        store.HadAd,
	commands.QUERY_ANY_ADS:        store.AnyAd,
}

// invalidateCommands maps each INVALIDATE_*_ADS command to the table it prunes.
var invalidateCommands = map[int]store.AdType{
	commands.INVALIDATE_STARTD_ADS:     store.StartdAd,
	commands.INVALIDATE_SCHEDD_ADS:     store.ScheddAd,
	commands.INVALIDATE_MASTER_ADS:     store.MasterAd,
	commands.INVALIDATE_SUBMITTOR_ADS:  store.SubmitterAd,
	commands.INVALIDATE_COLLECTOR_ADS:  store.CollectorAd,
	commands.INVALIDATE_NEGOTIATOR_ADS: store.NegotiatorAd,
	commands.INVALIDATE_LICENSE_ADS:    store.LicenseAd,
	commands.INVALIDATE_STORAGE_ADS:    store.StorageAd,
	commands.INVALIDATE_CKPT_SRVR_ADS:  store.CkptSrvrAd,
	commands.INVALIDATE_ACCOUNTING_ADS: store.AccountingAd,
	commands.INVALIDATE_GRID_ADS:       store.GridAd,
	commands.INVALIDATE_HAD_ADS:        store.HadAd,
}
