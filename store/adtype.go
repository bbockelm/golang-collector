// Package store is the core of the Go collector: a set of ClassAd tables (one
// per ad type) built on the classad collections engine, with the collector's
// update / query / invalidate / expire semantics layered on top.
package store

// AdType identifies one collector table. The values mirror the roles of the
// C++ collector's per-type hash tables; queries and updates are routed to the
// table matching their command.
type AdType int

const (
	AnyAd AdType = iota // query-only: search every table
	StartdAd
	StartdPvtAd
	ScheddAd
	MasterAd
	SubmitterAd
	CollectorAd
	NegotiatorAd
	LicenseAd
	StorageAd
	CkptSrvrAd
	AccountingAd
	GridAd
	HadAd
	GenericAd
	numAdTypes
)

// myType is the ATTR_MY_TYPE string the collector associates with each table,
// e.g. what a GENERIC query's TargetType matches and what an ad advertises.
var myType = map[AdType]string{
	StartdAd:     "Machine",
	StartdPvtAd:  "Machine",
	ScheddAd:     "Scheduler",
	MasterAd:     "DaemonMaster",
	SubmitterAd:  "Submitter",
	CollectorAd:  "Collector",
	NegotiatorAd: "Negotiator",
	LicenseAd:    "License",
	StorageAd:    "Storage",
	CkptSrvrAd:   "CkptServer",
	AccountingAd: "Accounting",
	GridAd:       "Grid",
	HadAd:        "HAD",
	GenericAd:    "Generic",
}

var adTypeName = map[AdType]string{
	AnyAd: "Any", StartdAd: "Startd", StartdPvtAd: "StartdPvt", ScheddAd: "Schedd",
	MasterAd: "Master", SubmitterAd: "Submitter", CollectorAd: "Collector",
	NegotiatorAd: "Negotiator", LicenseAd: "License", StorageAd: "Storage",
	CkptSrvrAd: "CkptSrvr", AccountingAd: "Accounting", GridAd: "Grid",
	HadAd: "HAD", GenericAd: "Generic",
}

// MyType returns the ATTR_MY_TYPE string for a table, or "" if it has none.
func (t AdType) MyType() string { return myType[t] }

// String returns a short human-readable name for the table.
func (t AdType) String() string {
	if n, ok := adTypeName[t]; ok {
		return n
	}
	return "AdType(?)"
}

// stored reports whether t is a real storage table (not the query-only AnyAd).
func (t AdType) stored() bool { return t > AnyAd && t < numAdTypes }
