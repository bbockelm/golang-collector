package server

import (
	"testing"

	"github.com/bbockelm/cedar/commands"
	"github.com/bbockelm/cedar/watch"

	"github.com/bbockelm/golang-collector/store"
)

// TestCommandPermsMatchCpp pins the authorization level of every collector
// command to the level CollectorDaemon::Init registers it at in the C++
// collector (src/condor_collector.V6/collector.cpp), and checks the server
// registers each command at exactly that level.
func TestCommandPermsMatchCpp(t *testing.T) {
	want := map[int]string{
		// Queries (collector.cpp:279-312): READ, private ones NEGOTIATOR.
		commands.QUERY_STARTD_ADS:       "READ",
		commands.QUERY_STARTD_PVT_ADS:   "NEGOTIATOR",
		commands.QUERY_SCHEDD_ADS:       "READ",
		commands.QUERY_MASTER_ADS:       "READ",
		commands.QUERY_CKPT_SRVR_ADS:    "READ",
		commands.QUERY_SUBMITTOR_ADS:    "READ",
		commands.QUERY_LICENSE_ADS:      "READ",
		commands.QUERY_COLLECTOR_ADS:    "READ",
		commands.QUERY_STORAGE_ADS:      "READ",
		commands.QUERY_ACCOUNTING_ADS:   "READ",
		commands.QUERY_NEGOTIATOR_ADS:   "READ",
		commands.QUERY_HAD_ADS:          "READ",
		commands.QUERY_ANY_ADS:          "READ",
		commands.QUERY_GRID_ADS:         "READ",
		commands.QUERY_MULTIPLE_ADS:     "READ",
		commands.QUERY_MULTIPLE_PVT_ADS: "NEGOTIATOR",

		// Invalidations (collector.cpp:315-350).
		commands.INVALIDATE_STARTD_ADS:     "ADVERTISE_STARTD",
		commands.INVALIDATE_SCHEDD_ADS:     "ADVERTISE_SCHEDD",
		commands.INVALIDATE_MASTER_ADS:     "ADVERTISE_MASTER",
		commands.INVALIDATE_CKPT_SRVR_ADS:  "DAEMON",
		commands.INVALIDATE_SUBMITTOR_ADS:  "ADVERTISE_SCHEDD",
		commands.INVALIDATE_LICENSE_ADS:    "DAEMON",
		commands.INVALIDATE_COLLECTOR_ADS:  "DAEMON",
		commands.INVALIDATE_STORAGE_ADS:    "DAEMON",
		commands.INVALIDATE_ACCOUNTING_ADS: "NEGOTIATOR",
		commands.INVALIDATE_NEGOTIATOR_ADS: "NEGOTIATOR",
		commands.INVALIDATE_HAD_ADS:        "DAEMON",
		commands.INVALIDATE_GRID_ADS:       "DAEMON",

		// Updates (collector.cpp:353-380, 395-399).
		commands.UPDATE_STARTD_AD:          "ADVERTISE_STARTD",
		commands.UPDATE_STARTD_AD_WITH_ACK: "ADVERTISE_STARTD",
		commands.UPDATE_SCHEDD_AD:          "ADVERTISE_SCHEDD",
		commands.UPDATE_SUBMITTOR_AD:       "ADVERTISE_SCHEDD",
		commands.UPDATE_LICENSE_AD:         "DAEMON",
		commands.UPDATE_MASTER_AD:          "ADVERTISE_MASTER",
		commands.UPDATE_CKPT_SRVR_AD:       "DAEMON",
		commands.UPDATE_COLLECTOR_AD:       "DAEMON",
		commands.UPDATE_STORAGE_AD:         "DAEMON",
		commands.UPDATE_NEGOTIATOR_AD:      "NEGOTIATOR",
		commands.UPDATE_HAD_AD:             "DAEMON",
		commands.UPDATE_GRID_AD:            "DAEMON",
		commands.UPDATE_ACCOUNTING_AD:      "NEGOTIATOR",

		// Go-only change stream: read-only.
		watch.WatchAds: "READ",
	}

	// Every command the collector serves has a C++ level here.
	for _, tbl := range []map[int]store.AdType{updateCommands, queryCommands, invalidateCommands} {
		for cmd := range tbl {
			if _, ok := want[cmd]; !ok {
				t.Errorf("command %d (%s) has no expected level", cmd, commands.GetCommandName(cmd))
			}
		}
	}

	cs := New(store.New(), plaintextSec(), nil)
	for cmd, perm := range want {
		if got := CommandPerm(cmd); got != perm {
			t.Errorf("CommandPerm(%s) = %s, want %s", commands.GetCommandName(cmd), got, perm)
		}
		if got := cs.CommandPerms(cmd); len(got) != 1 || got[0] != perm {
			t.Errorf("%s registered at %v, want [%s]", commands.GetCommandName(cmd), got, perm)
		}
	}
}
