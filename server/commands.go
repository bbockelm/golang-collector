// Package server maps the collector's CEDAR command protocol onto a store.Store,
// using the cedar command-dispatch server for the wire/security layer.
package server

import (
	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/watch"

	"github.com/bbockelm/golang-collector/store"
)

// HTCondor DCpermission authorization levels the collector serves commands at.
const (
	LevelRead       = "READ"       // public monitoring: QUERY_*_ADS
	LevelAdvertise  = "ADVERTISE"  // publishing ads: UPDATE_*/INVALIDATE_*
	LevelNegotiator = "NEGOTIATOR" // private/claim ads: the *_PVT_ADS queries
)

// CommandLevel returns the HTCondor authorization level a collector command is
// served at, matching the C++ condor_collector: ordinary QUERY_*_ADS at READ
// (monitoring is public, so condor_status works without daemon credentials),
// the private-ad queries at NEGOTIATOR (claim ids are secret), and updates and
// invalidations at ADVERTISE (only authenticated daemons may publish or expire
// ads). Unknown commands default to READ (the least-privileged, read-only
// level). Callers use this both to register per-command authorization and to
// select the per-command security policy to negotiate.
func CommandLevel(cmd int) string {
	switch cmd {
	case commands.QUERY_STARTD_PVT_ADS, commands.QUERY_MULTIPLE_PVT_ADS:
		return LevelNegotiator
	case commands.UPDATE_STARTD_AD_WITH_ACK:
		return LevelAdvertise
	case commands.QUERY_MULTIPLE_ADS, watch.WatchAds:
		return LevelRead
	}
	if _, ok := updateCommands[cmd]; ok {
		return LevelAdvertise
	}
	if _, ok := invalidateCommands[cmd]; ok {
		return LevelAdvertise
	}
	if _, ok := queryCommands[cmd]; ok {
		return LevelRead
	}
	return LevelRead
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
	commands.QUERY_STARTD_ADS:     store.StartdAd,
	// Private startd ads. NOTE: in a real pool this must be gated on NEGOTIATOR
	// authorization; it is registered unconditionally here (see server.New).
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
