// Package negtest holds the negotiator's shared test harnesses:
//
//   - Fixture pools: testdata/*.ads hold machine and submitter ClassAds (old
//     syntax, one blank-line-separated ad per block) that unit and
//     differential tests load into a PoolSnapshot.
//
//   - Differential oracle: tests that compare against the C++ negotiator run
//     condor_negotiator / condor_userprio from a real HTCondor build (skipped
//     when not in PATH, like golang-ccb's TestGoCCBUnderCondorMaster). The Go
//     negotiator runs in compat (serial) mode; match lists and accountant
//     state must agree.
//
//   - Loopback schedd: a pure-Go NEGOTIATE peer built on golang-ap's
//     internal/negotiate Handler served over an in-process cedar conn pair, so
//     protocol tests need no external daemons.
//
// Phase owners extend this package; see docs/NEGOTIATOR_DESIGN.md section 7.
package negtest
