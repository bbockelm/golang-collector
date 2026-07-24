package store

// ClassAd attribute names the collector reads or writes. These mirror the
// condor_attributes.h constants used by the C++ collector.
const (
	attrName            = "Name"
	attrMachine         = "Machine"
	attrMyAddress       = "MyAddress"
	attrMyType          = "MyType"
	attrTargetType      = "TargetType"
	attrSlotID          = "SlotID"
	attrStartdIPAddr    = "StartdIpAddr"
	attrScheddName      = "ScheddName"
	attrScheddIPAddr    = "ScheddIpAddr"
	attrLastHeardFrom   = "LastHeardFrom"
	attrClassAdLifetime = "ClassAdLifetime"
	attrRequirements    = "Requirements"
	attrAbsent          = "Absent"
	// Update ordering: a daemon bumps UpdateSequenceNumber each publish and jumps
	// DaemonStartTime on restart (so it dominates a wrapped sequence). Used to resolve a
	// write-write conflict in favor of the newer ad (see seqversion.go).
	attrUpdateSequenceNumber = "UpdateSequenceNumber"
	attrDaemonStartTime      = "DaemonStartTime"
)
