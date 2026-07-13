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
compat==fast holds. The per-candidate expression form
(`evaluate_limits_with_match`) is now ALSO ported: when `ConcurrencyLimits` is a
match-referencing expression (only evaluates to a string with a TARGET),
`Match` flags it and `evalCandidate` evaluates it per candidate against the
match, so a per-CPU license `strcat("license:", TARGET.Cpus)` consumes the
matched slot's Cpus. The check is read-only over the usage view, so the sharded
scan stays deterministic.

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

## 5. Performance benchmarks + matchmaker allocation reduction  ✅ DONE

Delivered: a deterministic synthetic-pool generator (`negtest.Generate`) and
scale benchmarks (`BenchmarkMatchScale` 10k/100k × serial/sharded,
`BenchmarkPieSpin`). The sharded scan measures 1.5× (10k) to **5.5× (100k)**
faster than serial, confirming the concurrency thesis (§1). The matchmaker hot
path (`shard.go`) now writes the rank tuple into a caller-owned scratch
`Candidate` and keeps the winner in one `best` value — O(1) `Candidate` allocs
per scan range instead of one per matching candidate (~19% fewer bytes/op, ~10%
fewer allocs/op), worker-local so the sharded winner stays byte-identical to
serial. Remaining ~9 allocs/candidate live inside the `PelicanPlatform/classad`
evaluator (a library-boundary follow-up, as is `NEGOTIATOR_MATCHLIST_CACHING`).

_Original brief:_

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

## 7. Full NegotiatorAd stats + userprio-locate polish  ✅ DONE

Delivered: the NegotiatorAd now carries a **ring of the last N cycles**
(`NEGOTIATOR_CYCLE_STATS_LENGTH`, default 3, cap 100), newest-first suffix
`0..N-1`, with the full C++ attribute set — Period, MatchRate/MatchRateSustained,
Pies, SlotShareIter, NumSchedulers, ActiveSubmitterCount, ScheddsOutOfTime,
SubmittersFailed/OutOfTime, and aggregate CpuTime (getrusage). The
userprio-locate issue was fixed separately (CondorVersion on the ad; see
NEGOTIATOR_CPP_DIFFERENCES.md). Remaining deviations: CpuTime is whole-process
not per-phase; SubmittersShareLimit stays 0 (not classified).

_Original brief:_

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

## 8. Userprio completeness: GET_RESLIST, leases, PRIORITY_FACTOR_AUTHORIZATION  (partial)

- **`GET_RESLIST` (463) — ✅ DONE.** `condor_userprio -getreslist`: the handler
  (`handleGetResList`) reads the submitter and replies with the per-submitter
  resource list (`Name<i>`/`StartTime<i>`) via a new `Accountant.ResList`
  (mirrors `Accountant::ReportState(customer)`, Accountant.cpp:1344). Registered
  at READ by `SCHED_VERS+63` — cedar/commands does not name it yet, so it is
  referenced by offset rather than blocking on a cedar tag. Covered by
  `TestResList` + `TestGetResListWire`.
- **Ceiling/floor/priority-factor lease commands (`MANAGE_*`) — DEFERRED.** They
  need new cedar command ints (`MANAGE_CEILING`/`MANAGE_FLOOR`/
  `MANAGE_PRIORITY_FACTOR`) *and* lease storage on the accountant record; a niche
  local HTCondor extension. Do the cedar command-int PR first, then the
  accountant `SetCeilingLease` / expiry.
- **`PRIORITY_FACTOR_AUTHORIZATION` user-map — DEFERRED (needs infra).** The C++
  path (matchmaker.cpp:1107-1140) maps the authenticated identity through the
  `CLASSAD_USER_MAP` mechanism (`NEGOTIATOR_CLASSAD_USER_MAP_NAMES`, regex map
  files) and authorizes at WRITE when the mapped output prefixes the submitter.
  The Go stack has **no user-map facility yet** — that is a general HTCondor
  feature that belongs in `golang-htcondor/authz`, not the negotiator. Until it
  exists, `SET_PRIORITYFACTOR` stays ADMINISTRATOR-only (the versioned
  `{ErrorCode}` reply already in place).

**Where:** `negotiator/negotiator.go` handlers; `negotiator/accountant` (ResList,
future leases); a future `golang-htcondor/authz` user-map for the WRITE path.

**Scope:** GET_RESLIST small (done); leases + user-map each need cross-cutting
infra first.

## 9. Multi-pool / job-prio / match-expr knobs  (mostly DONE)

Smaller cycle-side C++ features, each independent:
- **`NEGOTIATOR_MATCH_EXPRS` — ✅ DONE.** Config list of bare macro names
  (resolved via the knob getter, like C++ `param`) or inline `name=expr`,
  injected into every match ad as `NegotiatorMatchExpr<name>` (unevaluated
  expression, string-literal fallback). Ordered → deterministic
  (matchmaker.cpp:728-746, :5268-5274).
- **`NEGOTIATOR_JOB_CONSTRAINT` — ✅ DONE (now enforced).** Previously only
  forwarded in the NEGOTIATE header + folded into significant attrs; now compiled
  in `cycle.New` and evaluated against each returned request in `nextRequest`
  (non-matching requests silently skipped), so an older/looser schedd cannot slip
  through excluded jobs. Deterministic; runs in both compat and fast modes.
- **`NEGOTIATOR_INFORM_STARTD` (default false) — ✅ DONE.** Full `MATCH_INFO`
  wire send (`put_secret(claimID)+EOM`, dc_startd.h:301-402) via a
  `startdInformer` type-assertion on the SessionFactory (no `interfaces.go`
  change); best-effort, ordered after schedd delivery, never fails the match.
  Default-off keeps behavior byte-identical.
- **Multi-collector failover — ✅ DONE.** The direct-CEDAR private-ad query now
  iterates a bracket-aware `COLLECTOR_HOST` list and fails over on error (context
  cancellation is terminal); the htcondor client already races the list for
  public queries.
- **`USE_GLOBAL_JOB_PRIOS` — DEFERRED.** The meatiest item (submitter-ad fan-out
  per `JobPrioArray`, `want_globaljobprio` in the submitter sort, `JOBPRIO_MIN/MAX`
  in the NEGOTIATE header, matchmaker.cpp:817, :3455-3477). Touches the submitter
  loop + header; left rather than risk half-breaking the spin.
- `STARTD_AD_REEVAL_EXPR` and flocking `SubmitterTag` handling remain unported.

**Where:** `negotiator/cycle`, `negotiator/source`, `negotiator/protocol`.
