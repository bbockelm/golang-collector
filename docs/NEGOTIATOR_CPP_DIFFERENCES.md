# Go negotiator vs. the C++ negotiator: differences that matter

Orientation for future agents extending `negotiator/` or differential-testing it
against `condor_negotiator`. The Go negotiator is a faithful port of
`src/condor_negotiator.V6/` (matchmaker.cpp, Accountant.cpp, GroupEntry.cpp) and
its *decisions* are meant to match C++ on a static pool — but several things are
deliberately different. Read this before assuming a divergence is a bug.

The normative spec is [`NEGOTIATOR_DESIGN.md`](NEGOTIATOR_DESIGN.md); this doc is
just the delta.

---

## 1. Concurrency: "concurrency for speed, determinism for decisions"

**C++** is a single-threaded event loop: for each submitter (in priority order)
it synchronously RPCs the schedd for its resource requests, matches them serially
against the slot set, and RPCs matches back — every schedd round-trip stalls the
whole cycle (`NEGOTIATOR_NUM_THREADS` only parallelizes one `IsAMatch`).

**Go** overlaps the I/O and CPU without changing the decision sequence:
- concurrent RRL **prefetch** across all schedds (read-only; reserves nothing),
- a **sharded** candidate scan with a deterministic reduce (`Candidate.Better`
  tie-breaks on `ScanIndex`, so N-worker results are byte-identical to serial),
- **async per-schedd match delivery** off the decision spine.

The decision *spine* stays serial and priority-ordered, so a given pool yields
the same matches either way. This is enforced by a test, not just asserted:
`negotiator/cycle` `TestCompatFastEquality` runs 20 seeded pools in
`CompatMode` (fully serial) and fast mode and asserts the per-submitter ordered
match lists and reject counts are identical.

**Implication:** if you add logic to the matchmaking hot path, it must be a pure
function of `(request, candidate, limits)` — no cross-candidate state — or you
break the sharded-equals-serial invariant. Keep `CompatMode` working as the
oracle.

## 2. Deferred behaviors that change *decisions* (the differential-test trap)

The MVP intentionally omits three things the C++ negotiator does by default.
Until Phase 7 lands them, Go and C++ will disagree on any pool that exercises
them:

- **Preemption.** `NEGOTIATOR_CONSIDER_PREEMPTION` defaults **true** in C++ but
  the Go accountant/matchmaker default it **false** (`GroupConfig.ConsiderPreemption`,
  `matchmaker` pins `PreemptTier = NO_PREEMPTION`). No `PREEMPTION_REQUIREMENTS`/
  `PREEMPTION_RANK`, no rank/prio preemption tiers. **To differential-test, set
  `NEGOTIATOR_CONSIDER_PREEMPTION = FALSE` on the C++ side** (and avoid claimed
  slots), or you are comparing against behavior Go does not implement yet.
- **Concurrency limits.** `CONCURRENCY_LIMIT` is not enforced during matchmaking.
- **Negotiator-side p-slot splitting.** Go **matches** a partitionable slot and
  hands the whole slot + claim to the schedd; the EP carves dynamic slots via the
  claim-leftovers protocol (this was the design intent). C++'s consumption
  policies (`CP_MATCH_COST`, `cp_deduct_assets`) and `NEGOTIATOR_DEPTH_FIRST`
  that let one cycle pack many jobs into a p-slot are **not** implemented. On a
  p-slot pool without those knobs the outcomes agree; with them they will not.

**Implication:** the first differential harness must config the C++ negotiator
into the MVP feature set (preemption off, no concurrency limits, default p-slot
handling). Widening scope is Phase 7.

## 3. Accountant persistence: native log, not `Accountantnew.log`

**C++** persists priority/usage state in `$(SPOOL)/Accountantnew.log`, a
`ClassAdLog` transaction journal, and can resume a running pool's accumulated
usage in place.

**Go** writes a **native JSONL transaction log** (`$(SPOOL)/GoAccountant.log` by
default; memory-only when unset). It deliberately keeps the **C++ key/attribute
shapes** — three namespaces `Accountant.` / `Customer.<name>` / `Resource.<startd@ip>`
with the exact attr names — precisely so the deferred `Accountantnew.log`
importer is a pure format adapter, not a re-modeling.

**Implication:** a Go negotiator **cannot take over a C++ pool's history yet** —
it starts fresh. Point `ACCOUNTANT_DATABASE_FILE` at a new path when migrating.
Don't hand it an `Accountantnew.log` expecting it to parse (Phase 7). The
write-on-read side effects of `GetPriority`/`GetPriorityFactor` ARE replicated
(see §5).

## 4. Determinism where C++ is unspecified (stable sorts)

**C++** uses `std::sort` (unstable) for the submitter negotiation order
(`submitterLessThan`) and the group round-robin index sort; on **equal keys** the
resulting order is unspecified and can vary run to run.

**Go** uses **stable sorts with an explicit final tie-break** — snapshot/
breadth-first index for submitters and groups, `ScanIndex` for candidates. So Go
is 100% reproducible, and on a genuine key tie it may pick a different (but
equally valid) order than any particular C++ run.

**Implication:** a strict "identical output" differential test can spuriously
fail on ties. Compare on the *set* of matches and the fair-share numbers, or
construct fixtures with no tied keys, rather than demanding identical ordering of
tied entries. This is a determinism **improvement**, not a semantic change — do
not "fix" it back to unstable.

## 5. Embeddable in the collector

**C++** `condor_negotiator` is always a standalone daemon that CEDAR-queries the
collector for ads.

**Go** runs **either** standalone (`cmd/golang-negotiator`, `RemoteSource`
querying a collector, like C++) **or embedded in the collector binary**
(`NEGOTIATOR_EMBEDDED`), where `EmbeddedSource` reads the collector's
collections store **directly, in-process** — no serialization, no query
round-trip — via the same `RegisterOn(server)` / `StartBackground` seam the
embedded CCB uses. The `AdSource` interface is the only thing the cycle sees, so
the two modes are otherwise identical.

**Implication:** an embedded negotiator shares the collector's process, security
config, and command server. Claim ids read from private ads are consumed
in-process and **must never be republished**. When adding ad-gathering logic, put
it behind `AdSource` so both modes get it.

---

## Smaller differences (worth knowing, unlikely to bite)

- **Unimplemented userprio bits.** `GET_RESLIST` (463) is skipped — the command
  int isn't in `cedar/commands` yet. `SET_PRIORITYFACTOR` requires `ADMINISTRATOR`
  (the C++ `PRIORITY_FACTOR_AUTHORIZATION` user-map path is deferred). The
  ceiling/floor/priority-factor **lease** commands (a local HTCondor extension)
  are not ported.
- **ClassAd cycle handling.** A mutually-recursive cross-ad reference in a
  `Requirements`/`Rank` expression yields **ERROR** in the Go classad library vs.
  **UNDEFINED** in C++ (a pre-existing library-wide convention, not introduced by
  the negotiator). Both mean "no match," so matchmaking outcomes agree.
- **NegotiatorAd cycle stats.** Go publishes only the **last** cycle (suffix `0`)
  and the subset of counters `CycleStats` tracks; C++ keeps a ring of the last N
  cycles and adds CpuTime/MatchRate/Pies/etc. Enough for liveness and basic
  dashboards, not yet a full stats history.
- **Collector discovery (standalone).** The Go daemon prefers the collector's
  **address file** (`$(LOG)/.collector_address`) over `COLLECTOR_HOST`, so a
  co-located negotiator finds a collector on a shared/ephemeral port. C++ resolves
  `COLLECTOR_HOST` via `CollectorList`. Same result for a normal CM on port 9618.
- **`condor_userprio` locate.** For `DT_NEGOTIATOR`, `condor_daemon_client`
  locates strictly through the collector's NegotiatorAd (never the address file:
  `_is_local` is false for negotiators, daemon.cpp:1290), and `getInfoFromAd`
  (daemon.cpp:2052) fails the locate unless the ad carries an address, `Machine`,
  **and `CondorVersion`** (ATTR_VERSION, :2098-2101). The real negotiator gets
  `CondorVersion` free from `daemonCore->publish()`; the Go ad must set it
  explicitly. It now does (`buildNegotiatorAd`, guarded by
  `TestNegotiatorAdLocateContract`) — without it, a stock `condor_userprio`
  reports "Can't locate negotiator in local pool" even though the query would
  otherwise work. (A lenient/locally-built `condor_userprio` may locate without
  it, which masked the gap until a distro build ran it in CI.)

## Faithfully replicated — do NOT "simplify" these

These look like quirks but are exact C++ parity; changing them will cause
divergence:

- **Write-on-read.** `GetPriority`/`GetPriorityFactor` persist the value they
  compute (a fresh submitter's factor; a below-`MinPriority` real priority)
  (Accountant.cpp:320-399). The store must accept writes on read paths.
- **Per-cycle weighted-usage rebuild.** `CheckMatches` zeroes every
  `WeightedResourcesUsed` and re-adds from the claimed slot ads each cycle; only
  the integer `ResourcesUsed` is incremental (Accountant.cpp:1260-1338).
- **`SetAccumUsage` writes the *weighted* attr** (`WeightedAccumulatedUsage`),
  not the unweighted one (Accountant.cpp:784).
- **Effective priority = real × factor**, `MinPriority = 0.5` floor and
  new-submitter init; the pie-spin `submitterShare = maxPrio/(prio·normalFactor)`
  and `SubmitterLimitPermits` round-up rule (`used<=0 && allowed>0`). The
  matchmaker does **not** re-check `pieLeft` (the C++ parameter is commented out;
  the outer loop pre-caps the submitter limit) and does **not** enforce the
  ceiling (the cycle loop does).
