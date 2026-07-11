# Go negotiator: next features (post-MVP roadmap)

The MVP (Phases 0-6) is done: accounting + hierarchical group quotas,
matchmaking, the NEGOTIATE protocol, embedded/standalone AdSources, the pie-spin
cycle (compat==fast proven), the daemon + userprio handlers, and an end-to-end
integration test (Go negotiator matches a C++ schedd's job to a C++ startd under
`condor_master`).

This is the prioritized backlog of what's missing, for the next agent. Each item
gives what/why, the Go touchpoints, the C++ reference, and scope. Read
[`NEGOTIATOR_CPP_DIFFERENCES.md`](NEGOTIATOR_CPP_DIFFERENCES.md) first — several
of these are the deferrals it calls out.

**Progress:** #1 differential harness (`e14353b`), #2 preemption (`a6ce298`),
#3 concurrency limits (`68861e5`), and #4 Accountantnew.log importer (`c241d31`)
are all **DONE**. Next up: #5 (perf benchmarks) and #6-#9. The differential
harness (#1) is available to validate later features against C++, including a
preemption-*on* config.

---

## 1. C++ differential test harness  ✅ DONE (`e14353b`)

Delivered: an accountant differential (identical `condor_userprio` SET_*
sequences into real `condor_negotiator` vs Go, comparing `-l -modular` output --
exact agreement on all submitters + decay-to-floor) and a fair-share allocation
differential (same pool under C++ vs Go -> identical slots-per-submitter split).
Both in `negtest/differential.go` + `integration/negotiator_differential_test.go`,
pinned to the MVP feature set. Extend it to preemption-on configs to validate #2
against C++.

_Original brief:_ C++ differential test harness (the oracle).

**What:** run the real `condor_negotiator` and the Go negotiator against the same
static fixture pool and assert they make the same matches and the same accountant
state. Today we only prove *compat==fast* (Go vs Go) and one live e2e.

**Why:** it's the oracle every later feature is checked against. Without it,
"matches C++" is an assertion, not a test.

**Where:** `negotiator/negtest/` + `integration/`. Two flavors:
- *Component:* seed identical machine+submitter ad files; run C++ `condor_userprio`
  vs Go `ReportState` after N seeded matches (compare priorities within fp
  tolerance); feed the same job/slot pair to C++ match logic vs `matchmaker.Match`.
- *Cycle:* a real pool (harness like `integration/negotiator_master_test.go`) run
  once with C++ and once with the Go negotiator, comparing NegotiatorLog match
  decisions. **Config the C++ side into the MVP feature set**
  (`NEGOTIATOR_CONSIDER_PREEMPTION = FALSE`, no concurrency limits) or it's an
  unfair comparison (see the differences doc §2).

**Scope:** medium. No production code — pure test infrastructure. Highest ROI.

## 2. Preemption  ✅ DONE (`a6ce298`)

Delivered: the full C++ per-candidate preemption pipeline in the matchmaker
(RANK/PRIO tiers, `PREEMPTION_REQUIREMENTS`/`PREEMPTION_RANK`, rank conditions,
`only_for_startdrank`), cycle-side `addRemoteUserPrios` + claimed-slot retention
+ full/unclaimed submitter-limit split, and **`NEGOTIATOR_CONSIDER_PREEMPTION`
flipped to default `true`** (C++ parity), preemption-off byte-identical.
`compat==fast` holds with preemption on. Not ported (roadmap #6 / documented):
`NEGOTIATOR_CROSS_SLOT_PRIOS`, `pslotMultiMatch`.

_Original brief:_ preemption (the biggest default-behavior gap).

**What:** `PREEMPTION_REQUIREMENTS` / `PREEMPTION_RANK`, the three preemption
tiers (NO_PREEMPTION > RANK > PRIO), remote-user determination, the rank
conditions `rankCondStd` (`MY.Rank > MY.CurrentRank`) and `rankCondPrioPreempt`
(`>=`), and `NEGOTIATOR_CONSIDER_PREEMPTION` (default **true** in C++, currently
forced **false** in Go).

**Why:** most C++ pools run with preemption on by default; until this lands, Go
only matches C++ on preemption-off pools.

**Where:** the seams are already stubbed.
- `negotiator/matchmaker`: `Candidate.PreemptTier`/`PreemptRank` fields exist and
  `Better()` already orders on them; fill them in `evalCandidate`
  (`shard.go`) — remoteUser lookup, the rank conditions, `PREEMPTION_REQUIREMENTS`
  gate, `PREEMPTION_RANK` eval. Add the reject counters
  (`rejPreemptForPolicy`/`rejPreemptForRank` in `RejectInfo`).
- `negotiator/cycle/spin.go`: the spin-1 `ignore_submitter_limit` bypass for
  rank-preferred jobs; the *unclaimed* vs full submitter-limit variants
  (`MatchLimits` already carries both concepts).
- `negotiator/source`: stop trimming claimed non-pslot slots when preemption is
  on (fixups currently trims them, matching preemption-off).
- `negotiator/accountant`: the `addRemoteUserPrios` equivalent (stamp
  `RemoteUserPrio` etc. on slot ads) so `PREEMPTION_*` expressions can reference
  them; `GetPriority(remoteUser)`.

**C++:** matchmaker.cpp `matchmakingAlgorithm` :4934-5072, `calculateRanks`
:5192-5246, rank-cond consts :419-423, `addRemoteUserPrios` :5674,
`trimStartdAds_PreemptionLogic` :2989-3011, defaults :820-821.

**Scope:** large. This is the marquee Phase-7 feature. Flip
`GroupConfig.ConsiderPreemption`/matchmaker default only when complete.

## 3. Concurrency limits  ✅ DONE (`68861e5`)

Delivered: matchmaker gate on `ConcurrencyLimits` (comma list, `name:weight`)
vs `<NAME>_LIMIT` / `CONCURRENCY_LIMIT_DEFAULT[_<PREFIX>]` maxes; cross-cycle
counts store-backed under a `ConcurrencyLimit.` namespace (rebuilt in
CheckMatches), in-cycle live consumption via a per-cycle tracker; pure gate so
compat==fast holds. Not ported: the per-candidate expression form
(`evaluate_limits_with_match`).

_Original brief:_

**What:** enforce `CONCURRENCY_LIMIT` during matchmaking — a match consumes a
limit; a candidate that would exceed a limit is rejected
(`rejForConcurrencyLimit`, already a field in `RejectInfo`).

**Where:** `negotiator/matchmaker` (evaluate `ConcurrencyLimits` with the match,
gate), `negotiator/accountant` (the limit counters — C++ `GetLimit`/`GetLimitMax`,
`MatchedConcurrencyLimits` on the Resource record, which the store already
tracks). The match-ad enrichment already sets `MatchedConcurrencyLimits`.

**C++:** matchmaker.cpp :5074-5079, :4606-4608; Accountant limit map.

**Scope:** medium; self-contained.

## 4. `Accountantnew.log` importer  ✅ DONE (`c241d31`)

Delivered: a ClassAdLog format adapter (`accountant/import.go`) parsing the C++
transaction journal (opcodes 101-107, commit/abort/truncation semantics) into
the native store; `Config.ImportFrom` + `cmd -import`/`ACCOUNTANT_IMPORT_LOG`
with a one-shot idempotency guard (imports only when the native store has no
Customer records). C++ attr names map verbatim.

_Original brief:_

**What:** read the C++ `ClassAdLog` accountant journal so a Go negotiator can
take over a running pool's accumulated usage/priorities in place.

**Why:** without it, deploying the Go negotiator resets everyone's fair-share
history. Blocks drop-in replacement of a live pool.

**Where:** `negotiator/accountant/store.go`. The native store already uses the
**C++ key/attribute shapes** (`Accountant.`/`Customer.`/`Resource.` namespaces,
exact attr names), so this is a **format adapter**: parse the ClassAdLog
transaction records into the existing record model. Add a one-shot import path
(config `ACCOUNTANT_DATABASE_FILE` pointing at an `Accountantnew.log`, or a
`-import` flag on `cmd/golang-negotiator`).

**C++:** `ClassAdLogAccountantDB.cpp`, `classad_log.h` (the log format).

**Scope:** medium; isolated to the store.

## 5. Performance benchmarks + matchmaker allocation reduction  (P1)

**What:** quantify the concurrency thesis — Go vs C++ negotiation-cycle wall-clock
on large synthetic pools (10k-100k slots, many schedds, deep RRLs) — and cut the
matchmaker's per-request allocations.

**Why:** "Go is faster because it overlaps I/O and shards the scan" is currently
unmeasured. And `BenchmarkMatch` is ~1.7ms/request at 10k slots, **alloc-dominated
by the classad evaluator** (~10 allocs/candidate for Symmetry + 3 rank evals).

**Where:** `negotiator/cycle` + `negotiator/matchmaker` benchmarks; a synthetic
pool generator in `negtest`. For allocations, look at reusing evaluator scratch
across candidates and the `NEGOTIATOR_MATCHLIST_CACHING` idea (C++ caches the
sorted candidate list per autocluster; Go re-scans every request).

**Scope:** medium.

## 6. Partitionable-slot consumption policies (optional negotiator-side split)  (P2)

**What:** the C++ modes where the negotiator itself packs multiple jobs into one
p-slot — consumption policies (`CP_MATCH_COST`, `cp_deduct_assets`, deducting
`RequestCpus` from the p-slot ad in-cycle) and `NEGOTIATOR_DEPTH_FIRST`
(skip `jobsInSlot` subsequent identical requests). The MVP intentionally does
NOT split at the negotiator (the EP carves dslots via claim leftovers).

**Where:** `negotiator/matchmaker/slotview.go` (deduct/persist p-slot capacity),
`negotiator/accountant` (the `_cp_match_%03d` Resource-key shape is already
noted). Make it opt-in so default behavior (hand the whole p-slot to the schedd)
is unchanged.

**C++:** matchmaker.cpp cp_ functions :4902-4929, :5481-5499; DEPTH_FIRST :4497.

**Scope:** medium; opt-in, low risk to the default path.

## 7. Full NegotiatorAd stats + userprio-locate polish  (P2)

**What:** the C++ NegotiatorAd keeps a ring of the last N cycles (suffixes
`0..N-1`) and publishes CpuTime/MatchRate/Pies/ScheddsOutOfTime/
SubmittersFailed/OutOfTime/ShareLimit; Go publishes only the last cycle and a
counter subset. Also: `condor_userprio` prints "Can't locate negotiator in local
pool" before the table (the NegotiatorAd hasn't propagated / the locate falls
back), even though the query works.

**Where:** `negotiator/negotiator.go` `publishCycleStats`, `negotiator/cycle/stats.go`
`CycleStats` (add the missing counters + CPU timing), and check the NegotiatorAd
is advertised promptly/queryably so `condor_userprio`'s locate succeeds.

**C++:** matchmaker.cpp `publishNegotiationCycleStats` :6455-6544, `updateCollector`
:6197-6218.

**Scope:** small-medium.

## 8. Userprio completeness: GET_RESLIST, leases, PRIORITY_FACTOR_AUTHORIZATION  (P2)

**What:** the userprio surface still missing:
- `GET_RESLIST` (463) — `condor_userprio -getreslist`. **Its command int isn't in
  `cedar/commands` yet** — add the constant there first (a cedar PR), then the
  handler (returns per-submitter `Name<i>`/`StartTime<i>`).
- The ceiling/floor/priority-factor **lease** commands (a local HTCondor
  extension; `MANAGE_*`).
- `PRIORITY_FACTOR_AUTHORIZATION` user-map for `SET_PRIORITYFACTOR` at WRITE (Go
  currently requires ADMINISTRATOR; the user-map path is stubbed with the
  versioned `{ErrorCode}` reply).

**Where:** `negotiator/negotiator.go` handlers; `cedar/commands` for GET_RESLIST.

**Scope:** small each.

## 9. Multi-pool / job-prio / match-expr knobs  (P2)

Smaller cycle-side C++ features not yet ported, each independent:
- `USE_GLOBAL_JOB_PRIOS` — submitter-ad fan-out per `JobPrioArray`, the
  `want_globaljobprio` key in `submitterLessThan`, and `JOBPRIO_MIN/MAX` in the
  NEGOTIATE header (matchmaker.cpp :817, :3455-3477).
- `NEGOTIATOR_MATCH_EXPRS` (`MatchExprX` injected into match ads),
  `NEGOTIATOR_JOB_CONSTRAINT`, `STARTD_AD_REEVAL_EXPR`.
- `NEGOTIATOR_INFORM_STARTD` (`MATCH_INFO` to the startd; default off).
- Multi-collector query with failover for `RemoteSource` (`CollectorList`),
  and flocking `SubmitterTag` handling across pools.

**Where:** `negotiator/cycle`, `negotiator/source`, `negotiator/protocol`.

**Scope:** small each; pick as needed by target deployments.
