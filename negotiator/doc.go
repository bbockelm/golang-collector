// Package negotiator implements the HTCondor negotiator in Go: accounting
// (user priorities, usage with half-life decay, hierarchical group quotas),
// matchmaking (the pie-spin fair-share algorithm over slot and submitter ads),
// and the NEGOTIATE wire protocol toward schedds.
//
// It is designed to be embedded in the collector binary -- reading the
// collector's collections store directly through an AdSource -- or run
// standalone (cmd/golang-negotiator), periodically querying a collector the
// way the C++ condor_negotiator does.
//
// Behavioral reference and full specification: docs/NEGOTIATOR_DESIGN.md.
// The concurrency contract is "concurrency for speed, determinism for
// decisions": parallel ad gathering, RRL prefetch, and candidate scans, but
// match decisions identical to a serial run (enforced by tests comparing
// compat mode against fast mode).
package negotiator
