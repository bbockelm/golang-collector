# Go Negotiator Design

A pure-Go HTCondor negotiator, wire-compatible with the C++ daemons, living as a
`negotiator/` package in `golang-collector` so it can be **embedded** in the
collector binary (reading the collections store directly) or run **standalone**
(`cmd/golang-negotiator`, periodically querying a collector like the C++
negotiator). Behavioral reference: `condor_negotiator` from the HTCondor source
at `~/projects/htcondor` (file:line references below are into that tree).

Scope decisions (project owner):
- **MVP includes** accounting groups + hierarchical group quotas.
- **Deferred to hardening**: preemption, concurrency limits, `Accountantnew.log`
  import (native state store first), negotiator-side p-slot splitting.
- **P-slots**: the negotiator matches jobs against partitionable slots and hands
  the match to the schedd; the EP carves dslots as jobs start (claim leftovers).
  The negotiator only adjusts its *in-cycle* view of a matched p-slot.

---

## 1. Architecture

Five frozen interfaces (Phase 0 contract; the parallel workstreams build against
these and only these):

```go
// AdSource yields the negotiator's view of the pool for one cycle, and accepts
// the ads the negotiator publishes back.
type AdSource interface {
    // Snapshot returns the machine (startd public), submitter, and (if
    // authorized) startd-private ads for a cycle. Implementations: embedded
    // (direct store read) and remote (CEDAR queries to a collector).
    Snapshot(ctx context.Context) (*PoolSnapshot, error)
    // PublishNegotiatorAd / PublishAccountingAds push the negotiator's own ads.
    PublishNegotiatorAd(ctx context.Context, ad *classad.ClassAd) error
    PublishAccountingAds(ctx context.Context, ads []*classad.ClassAd) error
}

// Accountant owns priorities, usage, and (MVP) hierarchical group quotas.
type Accountant interface {
    UpdatePriorities(now time.Time)                       // half-life decay tick
    GetPriority(submitter string) float64                 // effective = real x factor
    GetPriorityFactor(submitter string) float64
    AddMatch(submitter string, slotAd *classad.ClassAd, now time.Time)
    RemoveMatch(resourceName string, now time.Time)
    CheckMatches(claimedAds []*classad.ClassAd)           // per-cycle reconcile
    // Group tree access for the cycle (quotas assigned, usage rolled up).
    GroupTree() *GroupEntry
    // Userprio protocol surface (GET_PRIORITY/SET_* handlers call these).
    ReportState(rollup bool) *classad.ClassAd
    SetPriorityFactor(submitter string, f float64) error
    // ... (SET_PRIORITY, RESET_USAGE, etc.)
}

// Matchmaker finds the best slot for one request against the cycle's slot set.
type Matchmaker interface {
    // Match returns the best candidate (or nil) for the request ad, honoring
    // the full C++ ranking order. Deterministic: same inputs -> same winner.
    Match(req *Request, view *SlotView) (*Candidate, error)
}

// ScheddSession is the negotiator side of the NEGOTIATE protocol against one
// schedd/submitter pair, over a cached (warm) cedar session.
type ScheddSession interface {
    Begin(ctx context.Context, submitter, scheddAddr string) error
    FetchRequests(ctx context.Context, n int) ([]*Request, error) // RRL prefetch
    SendMatch(ctx context.Context, m *MatchResult) error          // PERMISSION_AND_AD
    Reject(ctx context.Context, req *Request, reason string) error
    End(ctx context.Context) error                                // END_NEGOTIATE, keep socket
}

// Cycle orchestrates one negotiation cycle (the pie spin).
type Cycle interface {
    Run(ctx context.Context) (*CycleStats, error)
}
```

**Embedding seam** (mirrors the collector's embedded CCB): the negotiator object
exposes `New(cfg Config) (*Negotiator, error)`, `RegisterOn(cs *cedarserver.Server)`
(userprio + RESCHEDULE handlers), and `StartBackground(ctx)` (cycle timer). The
collector main constructs it with an embedded `AdSource` wrapping
`collector.Store()`; `cmd/golang-negotiator` constructs it with the remote
`AdSource` (via `htcondor.NewCollector(addr).QueryAds*`).

## 2. Concurrency model — "concurrency for speed, determinism for decisions"

The C++ negotiator is single-threaded; every schedd round-trip stalls the cycle.
We parallelize I/O and scans but keep *decisions* byte-identical to a serial
run:

1. **Parallel gather**: machine/submitter/private-ad queries concurrently; in
   embedded mode this is a direct snapshot.
2. **Concurrent RRL prefetch**: after the submitter list is fixed, open all
   schedd sessions and prefetch each submitter's resource-request list in
   parallel (read-only; reserves nothing).
3. **Deterministic spine**: submitters are negotiated in strict sorted order
   (group order, then effective priority) exactly as C++ does. Within a
   request, the candidate scan over the slot set is sharded across workers with
   a deterministic reduce (identical tie-break to serial). The winner is
   consumed serially before the next request.
4. **Async match delivery**: PERMISSION_AND_AD writes stream on each schedd's
   socket from a per-schedd goroutine, overlapping matchmaking of the next
   requests; a failed delivery returns the slot to the pool view (same as C++
   failed-notify handling).
5. **compat mode**: a config switch forces fully-serial execution. Test
   invariant: compat and fast mode produce identical match lists on a fixture
   pool.

## 3. Accountant specification (from C++ recon)

Source: `src/condor_negotiator.V6/Accountant.cpp`, `GroupEntry.cpp` (structure
mirrored; our store is native Go, not the C++ ClassAdLog — import is deferred).

### 3.1 Priority math
- Effective priority = `RealPriority x PriorityFactor` (Accountant.cpp:320).
- Constants: `MinPriority=0.5` (also new-submitter init), `PriorityDelta=0.5`,
  min factor `1.0`, `DEFAULT_PRIO_FACTOR=1000`, `NICE_USER_PRIO_FACTOR=1e10`,
  `REMOTE_PRIO_FACTOR=1e7` (forced to 1 when `ACCOUNTANT_LOCAL_DOMAIN` empty).
- Factor selection order (Accountant.cpp:372-399): stored factor if >= 1.0, else
  nice-user (name begins `nice-user`) -> `GROUP_PRIO_FACTOR_<group>` if nonzero
  -> remote factor if `@domain` differs from local domain -> default. Chosen
  value **written back** (read side effect; the store must accept writes on
  read paths or we diverge on first query).
- Decay tick (per cycle, elapsed-time based; Accountant.cpp:1094-1253):
  - `AgingFactor = 0.5^(TimePassed/HalfLife)` (`PRIORITY_HALFLIFE`, default 86400s)
  - `RecentUsage = ResourcesUsed + UnchargedTime/TimePassed`
  - `WeightedRecentUsage = WeightedResourcesUsed + WeightedUnchargedTime/TimePassed`
    (group nodes: use `HierWeightedResourcesUsed` when > 0)
  - **`Priority = Priority*AgingFactor + WeightedRecentUsage*(1-AgingFactor)`**,
    clamped to `>= MinPriority`.
  - `AccumulatedUsage += ResourcesUsed*TimePassed + UnchargedTime` (and weighted
    analog); zero the uncharged buckets after.
  - GC: purge a record with `Priority<MinPriority && ResourcesUsed==0 &&
    AccumulatedUsage==0` and no explicitly-set factor.

### 3.2 Usage tabulation
- Weight = `SlotWeight` attr (1.0 if `NEGOTIATOR_USE_SLOT_WEIGHTS=false` or
  missing/negative). Parallel tallies: `ResourcesUsed` (int) and
  `WeightedResourcesUsed` (float).
- `AddMatch`: `ResourcesUsed+=1`, `WeightedResourcesUsed+=w`, *pre-charge*
  `UnchargedTime -= (T-LastUpdateTime)` (weighted analog x w); write Resource
  record `{RemoteUser, SlotWeight, StartTime}` keyed `startdName@ip`.
- `RemoveMatch`: decrement (floored 0); charge `UnchargedTime += T-max(StartTime,
  LastUpdateTime)` (weighted x w); delete Resource record.
- Only Claimed/Preempting slot states count; suspended excluded when
  `NEGOTIATOR_DISCOUNT_SUSPENDED_RESOURCES`.
- **Per-cycle `CheckMatches` reconcile**: zero all `WeightedResourcesUsed`,
  re-AddMatch every currently-claimed slot ad (authoritative rebuild each
  cycle); `ResourcesUsed` stays incremental with a startup sanity check.
- Rollup: matches also update the submitter's assigned-group record, and
  increment `HierWeightedResourcesUsed` up the ancestor chain.
- CP p-slot matches (`_cp_match_cost`): recorded under pseudonym
  `<resource>_cp_match_%03d` with weight = match cost (deferred with
  consumption policies, but keep the key shape).

### 3.3 Groups + hierarchical quotas (MVP)
- Tree from `GROUP_NAMES` (dotted paths, parents first); root `"<none>"` always
  exists with `accept_surplus=true`. Per group: `GROUP_QUOTA_<g>` (static slots)
  XOR `GROUP_QUOTA_DYNAMIC_<g>` (fraction 0..1); `GROUP_ACCEPT_SURPLUS[_<g>]`,
  `GROUP_AUTOREGROUP[_<g>]`.
- Submitter -> group: strip `@domain`; split on **last dot**; no dot -> root;
  unknown subgroup -> deepest matched ancestor. Group records share the
  Customer namespace keyed by bare group name (no `@` = group).
- Quota normalization `hgq_assign_quotas(total)` (GroupEntry.cpp:539-615):
  static children first (`min(sum_static, quota)` unless oversubscription),
  dynamic children share the remainder with fractions renormalized only if they
  sum > 1; node keeps the leftover; root gets `quota - sum(children)`.
- Allocation (GroupEntry.cpp:341-758), up to `GROUP_QUOTA_MAX_ALLOCATION_ROUNDS`
  (default 3) rounds:
  - `hgq_fairshare`: `allocated = min(requested, quota)`; surplus recurses up.
  - `hgq_allocate_surplus`: parent competes as a *peer of its children* (last);
    cornucopia (surplus >= demand) satisfies everyone; scarcity runs two
    proportional loops — weight by `subtree_quota`, then weight 1 for
    zero-quota groups — iterated to convergence; recurse downward.
  - Unweighted pools: remainder recovery + round-robin whole slots ordered by
    `rr_time` (fairness across cycles).
  - `NEGOTIATOR_STRICT_ENFORCE_QUOTA` (default true): cap each group by every
    non-surplus ancestor's `quota - subtree_usage`.
- Demand: `requested = WeightedIdleJobs + WeightedRunningJobs` (submitter ads)
  when `NEGOTIATOR_USE_WEIGHTED_DEMAND` (default true).
- Negotiation order within a round: `GROUP_SORT_EXPR` (default: fraction of
  quota in use, ascending; root last).
- HGQ path only when > 1 group; flat pools use the traditional single-root path.

### 3.4 State store
Native Go persistent store (Phase 1): a transaction-logged map of records in
three namespaces mirroring the C++ keys — `Accountant.` (singleton:
`LastUpdateTime`), `Customer.<name>` (per submitter/group: `Priority`,
`PriorityFactor`, `ResourcesUsed`, `WeightedResourcesUsed`,
`HierWeightedResourcesUsed`, `UnchargedTime`, `WeightedUnchargedTime`,
`AccumulatedUsage`, `WeightedAccumulatedUsage`, `BeginUsageTime`,
`LastUsageTime`, `Ceiling`, `Floor`), `Resource.<startd@ip>` (per match:
`RemoteUser`, `SlotWeight`, `StartTime`). Keeping the C++ key/attr shape makes
the deferred `Accountantnew.log` importer a pure format adapter.

### 3.5 Userprio protocol (RegisterOn surface)
Command ints already in `cedar/commands`: GET_PRIORITY(451),
GET_PRIORITY_ROLLUP(514), SET_PRIORITY(449), SET_PRIORITYFACTOR(459),
RESET_USAGE(458), RESET_ALL_USAGE(460), DELETE_USER(482), SET_ACCUMUSAGE(494),
SET_BEGINTIME(495), SET_LASTTIME(496), SET_CEILING(525)/SET_FLOOR(530).
Missing from cedar (add): GET_RESLIST(463). Wire: setters read
`string submitter [+ value]` + EOM (ADMINISTRATOR; SET_PRIORITYFACTOR at WRITE
with reply ad `{ErrorCode}` for peers >= 8.9.9); GET_PRIORITY[_ROLLUP] return
the `ReportState` ad (numbered attrs `Name<i>`, `Priority<i>` (effective),
`PriorityFactor<i>`, `ResourcesUsed<i>`, `WeightedResourcesUsed<i>`,
`AccumulatedUsage<i>`, `WeightedAccumulatedUsage<i>`, `SubmitterShare<i>`,
`SubmitterLimit<i>`, `BeginUsageTime<i>`, `LastUsageTime<i>`,
`IsAccountingGroup<i>`, `AccountingGroup<i>`, `Ceiling<i>`, `Floor<i>`; groups
add `EffectiveQuota<i>`, `ConfigQuota<i>`, `SubtreeQuota<i>`, `GroupSortKey<i>`,
`SurplusPolicy<i>`, `Requested<i>`; top-level `LastUpdate`, `NumSubmittors`).

### 3.6 Accounting ads to the collector
`UPDATE_ACCOUNTING_AD` with `MyType="Accounting"`, end of every cycle when
`NEGOTIATOR_ADVERTISE_ACCOUNTING` (default true): per-submitter = full Customer
ad + `Name`, `NegotiatorName`, `Priority` (effective), `IsAccountingGroup`,
`AccountingGroup`, `LastUpdate` (only if `ResourcesUsed` present); per-group ads
add the quota fields. NegotiatorAd on `NEGOTIATOR_UPDATE_INTERVAL` (default
300s).

## 4. Matchmaking cycle specification

Source: `src/condor_negotiator.V6/matchmaker.cpp` (`negotiationTime()`
:1861-2177, `negotiateWithGroup()` :2434-2845, `matchmakingAlgorithm()`
:4692-5182, `matchmakingProtocol()` :5283-5510).

### 4.1 Cycle order (negotiationTime)
Timer-driven at `NEGOTIATOR_INTERVAL` (60s); `RESCHEDULE` (421) fires it
immediately. Guards: skip if `now - completedLastCycle < NEGOTIATOR_CYCLE_DELAY`
(20s) or `now - startedLastCycle < NEGOTIATOR_MIN_INTERVAL` (5s).

1. **Obtain ads** (Phase 1): CONDOR_QUERY to the collector(s) — startd private
   ads (claim ids -> `claimIds` map keyed `name+addr`), then a multi-query for
   submitter ads (`NEGOTIATOR_SUBMITTER_CONSTRAINT`) + slot ads
   (`NEGOTIATOR_SLOT_CONSTRAINT`). Per-slot fixups: drop
   `RemoteAdminCapability`; swap `NegotiatorRequirements`->`Requirements`
   (saving `SavedRequirements`); `MachineMatchCount=0`; default `SlotWeight`
   expr if missing. Per-submitter: require `Name`+`ScheddIpAddr`; drop if
   `RunningJobs+IdleJobs <= 0`. Drop submitters with `SkipMatchmaking==true`
   BEFORE fair-share normalization.
2. **Accounting** (Phase 2): `compute_significant_attrs(startdAds)` (union of
   job attr references -> `AutoClusterAttrs`); `accountant.UpdatePriorities()`;
   `accountant.CheckMatches(startdAds)`.
3. **Poolsize for quotas**: `weightedPoolsize` = sum SlotWeights (optionally
   under `NEGOTIATOR_SLOT_POOLSIZE_CONSTRAINT`); `effectivePoolsize` counts a
   p-slot as its `Cpus`.
4. `trimStartdAds` (shutdown-threshold + preemption-logic trims; with
   preemption deferred we trim claimed non-pslot ads, matching
   `NEGOTIATOR_CONSIDER_PREEMPTION=false` semantics),
   `insertNegotiatorMatchExprs` (`NEGOTIATOR_MATCH_EXPRS`).
5. **Dispatch**: flat pool (<=1 group) -> floor round (submitters under their
   `Floor`) then main `negotiateWithGroup` over all submitters; HGQ (>1 group)
   -> `hgq_prepare_for_matchmaking` + `hgq_negotiate_with_all_groups` with a
   per-group callback (floor round then full round per group), quota base =
   weighted (or effective) poolsize.
6. **Publish**: accounting ads (`NEGOTIATOR_ADVERTISE_ACCOUNTING`, default on)
   each cycle; NegotiatorAd on its own timer. Reset timer to
   `max(cycle_delay, interval - duration)`.

### 4.2 Pie spin math (negotiateWithGroup)
Outer do/while, `spin_pie` from 1. Per spin:

- Spin 1 with preemption enabled sets `ignore_submitter_limit=true` (rank-
  preferred jobs bypass fair share); with preemption deferred this is false.
- Normalization (`calculateNormalizationFactor`):
  `maxPrio = max_s prio(s)`; `normalFactor = sum_uniqueName maxPrio/prio(s)`
  (and the abs/factor analogs).
- `slotWeightTotal = min(untrimmedSlotWeightTotal, groupQuota)`.
- **Submitter limit** (`calculateSubmitterLimit`):
  ```
  submitterShare = maxPrioValue / (prio(s) * normalFactor)
  submitterLimit = max(0, submitterShare*slotWeightTotal - weightedUsage(s))
  # unclaimed variant additionally capped by max(0, groupQuota - groupUsage)
  # preemption-off => submitterLimit = unclaimed variant
  # floor round => submitterLimit = min(Floor(s), submitterLimit)
  ```
- **pieLeft** = sum over submitters of submitterLimit (also stamps
  `SubmitterStarvation` = usage/(usage+limit)).
- Spin 1 only: sort submitters (`submitterLessThan`: group order then effective
  priority, deterministic); stamp `SubmitterShare`/`SubmitterLimit` into the
  accounting ad; prefetch RRLs.
- Per submitter: cap `submitterLimit` at `pieLeft`; ceiling headroom =
  `Ceiling(s) - weightedUsage(s)` (-1 => unlimited); enforce per-cycle time
  budgets (`NEGOTIATOR_MAX_TIME_PER_{CYCLE,SUBMITTER,SCHEDD,PIESPIN}`); skip to
  next spin if `submitterLimit < minSlotWeight` (spins > 1); call `negotiate`.
  `MM_RESUME` (hit limit) keeps the submitter for the next spin; `MM_DONE`
  removes it (satisfied); errors invalidate the cached socket + remove.
- Per match: `match_cost = SlotWeight(offer)` (or CP match cost);
  `limitUsed += cost; pieLeft -= cost`.
- **Continue** while progress was made (`pieLeft < pieLeftOrig ||
  submitters.size() < origCount`) and submitters+slots remain; floor rounds
  never spin more than once.

### 4.3 Ranking order (matchmakingAlgorithm)
Per request, scan candidates (MatchList cache when
`NEGOTIATOR_MATCHLIST_CACHING`, keyed by autocluster+prio+submitter, re-checking
submitter limits on pop):

1. Bilateral `Requirements` (IsAMatch; our `classad.MatchClassAd.Symmetry`).
2. Submitter-limit gate: `SubmitterLimitPermits` — permit if
   `used + match_cost <= allowed` OR the round-up rule (`used<=0 && allowed>0`,
   so every submitter with any share can get at least one weighted slot).
3. Rank values (`calculateRanks`):
   `PreJobRank = eval NEGOTIATOR_PRE_JOB_RANK (else -MaxFloat64)`;
   `Rank = eval job Rank against candidate (else 0.0)`;
   `PostJobRank = eval NEGOTIATOR_POST_JOB_RANK (else -MaxFloat64)`.
4. **Lexicographic best (more is better)**: PreJobRank, then Rank, then
   PostJobRank, then preemption tier (NO_PREEMPTION=2 beats RANK=1 beats
   PRIO=0 — constant 2 for us until preemption lands), then PREEMPTION_RANK.
   Deterministic final tie-break: first-seen wins (C++ keeps the incumbent on
   ties), so our sharded scan must reduce by (rank tuple, then original scan
   index).
5. No candidate -> `REJECTED_WITH_REASON`, reason from the dominant reject
   counter (`rejForSubmitterLimit`, `rejForConcurrencyLimit`, ...), with the
   ` |<autocluster>|<cluster>.<proc>|` suffix when diagnostics requested.

Preemption-off simplifications (MVP, mirrors
`NEGOTIATOR_CONSIDER_PREEMPTION=false`): claimed non-pslot ads trimmed
up-front, unclaimed submitter limit everywhere, no remoteUser/preemption
branches, `addRemoteUserPrios` skipped.

### 4.4 P-slot matching
The negotiator NEVER splits a p-slot:

- A request matches the p-slot ad directly (its `Requirements` check
  `RequestCpus <= Cpus` etc. against the p-slot's advertised totals).
- On match the p-slot ad **stays in the candidate set** (it can match further
  requests this cycle); with consumption policies the C++ deducts assets from
  the ad in place (`cp_deduct_assets`) — deferred for us along with CP support.
  MVP behavior matches stock non-CP HTCondor: hand the whole p-slot + claim to
  the schedd once per request group, optionally skipping
  `jobsInSlot = min(availCpus/reqCpus, availMem/reqMem)` subsequent identical
  requests when `NEGOTIATOR_DEPTH_FIRST` (default false).
- Header ad advertises `MatchClaimedPSlots=true`; the schedd + startd do the
  dslot carve via the claim-leftovers protocol.

## 5. NEGOTIATE wire protocol (negotiator = client)

Ground truth on the Go side: `golang-ap/internal/negotiate/negotiate.go` (the
schedd server we must interoperate with) — a faithful port of C++
`ScheddNegotiate`. Command ints (all already in `cedar/commands`):

```
NEGOTIATE                 416  neg -> schedd   header ClassAd {Owner, AutoClusterAttrs, SubmitterTag,...} + EOM
  SEND_JOB_INFO           417  neg -> schedd   -> JOB_INFO(419)+ad+EOM | NO_MORE_JOBS(418)+EOM
  SEND_RESOURCE_REQUEST_LIST 518 + int N       -> up to N JOB_INFO ads, then NO_MORE_JOBS at cursor end
  PERMISSION_AND_AD       472  neg -> schedd   claim-id string (secret, encrypted session) + match ad
  REJECTED                426 / REJECTED_WITH_REASON 476
  END_NEGOTIATE           425  round over; SOCKET STAYS OPEN (warm reuse, next
                               NEGOTIATE is a bare command int, no re-handshake)
```

Request ads received carry: full flattened job ad + `ClusterId`/`ProcId`
(representative), `_condor_RESOURCE_COUNT` (group size — we may return up to
that many matches), `AutoClusterId`, `WantMatchDiagnostics=2`,
`WantPslotPreemption=true`. The match ad we send back MUST carry
`_condor_RESOURCE_CLUSTER`/`_condor_RESOURCE_PROC` echoing the representative
job id, and `Name` (slot); the claim id travels as a CEDAR string (optionally
`"<claim> <extra>..."` for pslot splits). Reject reasons append
`" |<autocluster>|<cluster>.<proc>|"` when diagnostics are on. Sessions are
NEGOTIATOR-authorized (schedd registers the handler at NEGOTIATOR/WRITE).
Socket cache: one warm session per (schedd, submitter-tag), reused across
cycles; EOF on the far side simply re-dials next cycle.

C++-side confirmation (matchmaker.cpp:3958-4125, matchmaker_negotiate.cpp):

- **Session**: socket from a per-schedd SocketCache; a cache miss dials the
  schedd and `startCommand(NEGOTIATE)` under the match-password session derived
  from the submitter ad's `Capability` (SEC_ENABLE_MATCH_PASSWORD_AUTHENTICATION);
  a warm socket just writes the bare command int. Invalidate on error.
- **Header ad**: `Owner` (submitter's `OriginalName` if present else name),
  `AutoClusterAttrs` (= computed significant attrs), `SubmitterTag`,
  `MatchClaimedPSlots=true`, `MatchCaps="MatchDiag3"`, `NegotiatorName`,
  optional `NegotiatorJobConstraint`, then EOM.
- **RRL depth**: schedd version >= 8.3.0 and `USE_RESOURCE_REQUEST_COUNTS`
  (default true) -> `SEND_RESOURCE_REQUEST_LIST` +
  `NEGOTIATOR_RESOURCE_REQUEST_LIST_SIZE` (default 200); else one-at-a-time
  `SEND_JOB_INFO`. Prefetch across all schedds is already parallel-ish in C++
  (`NEGOTIATOR_PREFETCH_REQUESTS`, default true) — ours is truly concurrent.
- **Request-ad enrichment before matching** (C++ inserts into the request):
  `SubmitterUserPrio`, `SubmitterUserResourcesInUse`, and when grouped
  `SubmitterGroup`, `SubmitterGroupResourcesInUse`, `SubmitterGroupQuota`,
  `SubmitterNegotiatingGroup`, `SubmitterAutoregroup` — job/machine expressions
  may reference these.
- **Match-ad enrichment before PERMISSION_AND_AD** (on the offer/slot ad):
  restore `Requirements` from `SavedRequirements`; `MatchedConcurrencyLimits`;
  `RemoteGroup`/`RemoteNegotiatingGroup`/`RemoteAutoregroup` (from the
  request); `ResourceRequestCluster`/`ResourceRequestProc` (representative job
  id — golang-ap reads the `_condor_`-prefixed aliases, verify both spellings
  in the loopback); `NegotiatorMatchExprXXX` copies. Claim id(s) travel as
  `put_secret` (encrypted string), "null" when claiming is off. After a match:
  `accountant.AddMatch(submitter, offer)`.
- **Timeouts**: `NEGOTIATOR_TIMEOUT` (30s) on connect/commands;
  END_NEGOTIATE (425) + EOM ends the round, socket kept cached.

## 6. AdSource details

- **Embedded**: read `store.StartdAd` / `store.SubmitterAd` / `store.StartdPvtAd`
  via `store.Query` iterators (snapshot semantics from collections' O(1)
  snapshots). Private ads carry claim ids (`ClaimId`/`Capability`) — the
  negotiator consumes them in-process and must never republish them.
- **Remote**: `htcondor.NewCollector(addr)` + `QueryAdsWithProjection` for
  machine+submitter ads and `QUERY_STARTD_PVT_ADS` for private ads (NEGOTIATOR
  authz at the collector), on `NEGOTIATOR_INTERVAL`.
- Constraints: `NEGOTIATOR_SLOT_CONSTRAINT`, `NEGOTIATOR_SUBMITTER_CONSTRAINT`
  applied at query time in both modes.

## 7. Testing strategy

1. **Unit**: accountant math (decay/factor/GC golden vectors), group quota
   allocation (pseudocode-derived tables incl. surplus/cornucopia/scarcity,
   round-robin), matchmaker ranking/tie-break tables, protocol codec.
2. **Loopback (pure Go)**: drive `golang-ap`'s `negotiate.Handler` over an
   in-process cedar conn pair — full NEGOTIATE rounds, RRL prefetch, batched
   match assignment, reject paths, warm-socket reuse across cycles.
3. **Differential vs C++**: fixture pool (static machine+submitter ad files) ->
   run C++ negotiator (`~/projects/htcondor-build/release_dir`) and Go
   negotiator in compat mode -> identical match lists and (within fp tolerance)
   identical accountant state; `condor_userprio -l` output vs our ReportState.
4. **Integration**: Go negotiator under condor_master with C++
   collector/schedd/startd runs a real job end-to-end (harness precedent:
   golang-ccb's `TestGoCCBUnderCondorMaster`); mixed stack with golang-ap
   schedd + C++ startd (golang-ep does not exist yet, so claims land on C++
   EPs).
5. **Determinism**: compat == fast decision equality on randomized fixture
   pools; `-race` on everything.

## 8. Phases and fleet assignment

- **Phase 0** (done by supervisor): this doc, frozen interfaces, package
  skeleton, harness skeleton.
- **Phase 1 — Accountant** (2 agents: 1a flat priority/usage/store+userprio,
  1b group tree/quota/surplus): `negotiator/accountant/`.
- **Phase 2 — Matchmaker** (1 agent): `negotiator/matchmaker/` (uses
  `classad.MatchClassAd` with `ReplaceRightAd` reuse; sharded scan).
- **Phase 3 — Protocol** (1 agent): `negotiator/protocol/` (ScheddSession,
  session cache, RRL prefetch) + the pure-Go loopback harness against
  golang-ap.
- **Phase 4 — AdSource** (1 agent): `negotiator/source/` embedded + remote.
- **Phase 5 — Cycle** (supervisor + 1 agent): `negotiator/cycle.go`, pie spin,
  concurrency wiring, compat mode.
- **Phase 6 — Daemon/embedding/e2e** (1 agent + supervisor): `RegisterOn`,
  `StartBackground`, `cmd/golang-negotiator`, master integration test.
- **Phase 7 — Hardening**: preemption, concurrency limits, Accountantnew.log
  import, negotiator-side p-slot consumption modes, benchmarks vs C++.

## 9. Config knobs (MVP set)

Accounting: `PRIORITY_HALFLIFE` (86400), `DEFAULT_PRIO_FACTOR` (1000),
`NICE_USER_PRIO_FACTOR` (1e10), `REMOTE_PRIO_FACTOR` (1e7),
`ACCOUNTANT_LOCAL_DOMAIN` (""), `NEGOTIATOR_USE_SLOT_WEIGHTS` (true),
`NEGOTIATOR_DISCOUNT_SUSPENDED_RESOURCES` (false), `ACCOUNTANT_DATABASE_FILE`
($(SPOOL)/Accountantnew.log — native format initially).
Groups: `GROUP_NAMES`, `GROUP_QUOTA_<g>`, `GROUP_QUOTA_DYNAMIC_<g>`,
`GROUP_PRIO_FACTOR_<g>`, `GROUP_ACCEPT_SURPLUS[_<g>]` (false),
`GROUP_AUTOREGROUP[_<g>]` (false), `GROUP_SORT_EXPR` (default expr),
`NEGOTIATOR_ALLOW_QUOTA_OVERSUBSCRIPTION` (false),
`NEGOTIATOR_STRICT_ENFORCE_QUOTA` (true),
`GROUP_QUOTA_MAX_ALLOCATION_ROUNDS` (3), `NEGOTIATOR_USE_WEIGHTED_DEMAND`
(true).
Cycle: `NEGOTIATOR_INTERVAL` (60), `NEGOTIATOR_MIN_INTERVAL` (5),
`NEGOTIATOR_CYCLE_DELAY` (20), `NEGOTIATOR_TIMEOUT` (30),
`NEGOTIATOR_MAX_TIME_PER_CYCLE` (1200), `NEGOTIATOR_MAX_TIME_PER_SUBMITTER`
(60), `NEGOTIATOR_MAX_TIME_PER_SCHEDD` (120), `NEGOTIATOR_MAX_TIME_PER_PIESPIN`
(120), `NEGOTIATOR_MATCHLIST_CACHING` (true), `NEGOTIATOR_UPDATE_INTERVAL`
(300), `NEGOTIATOR_ADVERTISE_ACCOUNTING` (true), `NEGOTIATOR_SLOT_CONSTRAINT`,
`NEGOTIATOR_SUBMITTER_CONSTRAINT`, `NEGOTIATOR_JOB_CONSTRAINT`,
`NEGOTIATOR_MATCH_EXPRS`, `NEGOTIATOR_PRE_JOB_RANK`, `NEGOTIATOR_POST_JOB_RANK`,
`NEGOTIATOR_RESOURCE_REQUEST_LIST_SIZE` (200), `USE_RESOURCE_REQUEST_COUNTS`
(true), `NEGOTIATOR_PREFETCH_REQUESTS` (true), `NEGOTIATOR_DEPTH_FIRST` (false),
`NEGOTIATOR_IGNORE_USER_PRIORITIES` (false), `NEGOTIATOR_NAME`.
Go-specific: `NEGOTIATOR_GO_COMPAT_MODE` (false = fast), worker counts.
