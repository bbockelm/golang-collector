# Pool partitioning (Go negotiator — a feature C++ does not have)

## Motivation

The CMS global pool wants each of its ~50 sites to independently split its slots
between the `production` and `analysis` accounting groups (e.g. 50/50). In C++
the only way to get a per-site fair-share split is to run **one negotiator per
site** (~50 negotiators), each with its own `GROUP_NAMES`/quotas over that site's
slots. Every one of those negotiators independently opens a NEGOTIATE session to
**every schedd** and pulls its resource-request list (RRL). The result is ~50×
the negotiation load on the schedds — the dominant scaling pain.

**Pool partitioning** runs the equivalent of those 50 per-site negotiation cycles
inside **one** negotiator that queries each schedd **once**: the RRL is fetched a
single time and matched against each partition's slots in turn. Accounting stays
**pool-wide** (a submitter's usage/priority is aggregated across all sites), while
**allocation is per-partition** (each site divides its own slots among the groups
by its own quotas).

## Definitions

- **Partition key**: a config attribute, `NEGOTIATOR_PARTITION_ATTR` (e.g.
  `SiteName`). Every slot ad is assigned to the partition named by the value of
  that attribute; slots missing it fall into a single default partition
  (`NEGOTIATOR_PARTITION_DEFAULT`, default `""`).
- **Partition**: the set of slots sharing one partition-key value, plus its own
  group tree/quotas derived from that partition's weighted slot total.
- Off by default: no `NEGOTIATOR_PARTITION_ATTR` ⇒ today's single-cycle behavior,
  byte-identical.

## What is shared vs per-partition

| Concern | Scope |
|---|---|
| Accountant (priority = real×factor, usage, decay) | **Pool-wide** (one accountant, shared) |
| Submitter set + effective priorities | **Pool-wide** (computed once per cycle) |
| Schedd sessions + RRL (resource requests) | **Fetched once**, re-presented per partition |
| Slots / SlotView | **Per-partition** (each partition owns its slot subset) |
| Group tree + quotas (`GROUP_NAMES`, `GROUP_QUOTA_*`) | **Per-partition** (each partition's weighted pool size drives its quotas) |
| Pie spin (fair-share loop) | **Per-partition** (one spin per partition over that partition's slots) |
| Concurrency limits (`CONCURRENCY_LIMIT`) | **Pool-wide** (a limit is global; the tracker spans partitions) |
| Match delivery (PERMISSION_AND_AD to schedd) | Per match, whichever partition produced it |
| Accounting-ad publish | Once, pool-wide, after all partitions |

## Cycle restructuring

Today `cycle.Run` (cycle.go) is: snapshot → accounting → trim → build one
`SlotView` → dispatch (flat `negotiateWithGroup` or HGQ `NegotiateAllGroups`) →
publish. Partitioning inserts a partition loop around the dispatch:

```
snapshot()                       # slots + submitters + claim ids, ONCE
acct.UpdatePriorities/CheckMatches   # pool-wide accounting, ONCE
partitions := partitionSlots(trimmed, cfg.PartitionAttr)   # map[value][]slot, deterministic order
wrap submitters ONCE (shared subStates: sessions, RRL queue, concurrency tracker)
for _, p := range partitions (stable key order):           # deterministic
    view_p := NewSlotView(p.slots)
    tree_p, quotas_p := BuildGroupTree(cfg.Group) with p's weighted pool size
    run the existing flat/HGQ pie-spin against view_p, tree_p
    #   - submitter priorities come from the shared accountant (pool-wide)
    #   - matches update the shared accountant usage -> later partitions see it
    #   - requests come from the shared per-submitter RRL queue (see below)
drain async match deliveries
publish accounting ads ONCE
```

Determinism: iterate partitions in **sorted key order** (like the submitter/group
stable sorts) so the whole cycle stays reproducible and `compat==fast` holds.

## The RRL-once mechanism (the crux)

Each submitter's `subState` (concurrency.go) already owns the schedd `sess`
(NEGOTIATE session) and a `queue []*Request` of fetched-but-unmatched requests.
Today the pie-spin pulls requests via `nextRequest` and matches them against the
single view; a request's `Count` (group size) is decremented as matches are made.

For partitioning, that request stream must survive **across** partition spins:

1. **Fetch once, hold in the shared `subState`.** A submitter is wrapped once for
   the whole cycle (not per partition), so its `sess`/`queue` persist. `nextRequest`
   already fetches lazily from the session and buffers in `queue`.
2. **Re-present unmatched requests to each partition.** A request not fully
   satisfied in partition A (Count remaining) is still in the queue for partition
   B. A request is only dropped when its Count reaches 0 (fully matched) or it is
   rejected by *every* partition (no slot anywhere satisfies it).
3. **Per-partition rejected-autocluster set.** The existing `rejectedACs` tracking
   (spin.go `nextRequest`) becomes per-(submitter,partition): an autocluster
   rejected in partition A may still match in B. A request rejected by ALL
   partitions is finally rejected to the schedd (REJECTED).
4. **End the session once**, after the last partition, with END_NEGOTIATE — so the
   schedd sees exactly one negotiation, not 50.

Net effect on the schedd: one SEND_RESOURCE_REQUEST_LIST / SEND_JOB_INFO exchange
per submitter per cycle, regardless of partition count — the 50× → 1× win.

## Fair share across partitions

Within partition P, submitters compete for P's slots using their **pool-wide**
effective priorities (from the shared accountant), and P's own group quotas.
Because matches update the shared accountant's usage immediately, a submitter that
wins heavily in early partitions has higher usage → worse priority → smaller
share in later partitions. That is the intended pool-wide fairness with
per-partition allocation. (Partition iteration order therefore has a second-order
fairness effect on ties; the sorted-key order keeps it deterministic. If order
bias proves material, a future refinement is to compute each partition's
submitter-limits against a *snapshot* of usage taken before the partition loop —
documented as a knob, not MVP.)

## Config knobs (`param_info.in`, tag=negotiator)

- `NEGOTIATOR_PARTITION_ATTR` (string, default unset ⇒ feature off)
- `NEGOTIATOR_PARTITION_DEFAULT` (string, default `""`): partition for slots
  missing the attribute.
- Group config (`GROUP_NAMES`, `GROUP_QUOTA_*`) is reused **per partition** as-is;
  a later extension could allow per-partition overrides
  (`SITE_<name>_GROUP_QUOTA_*`), deferred.

## Touchpoints

- `negotiator/cycle/cycle.go` — the partition loop; `partitionSlots` helper;
  per-partition group-tree build from per-partition weighted pool size.
- `negotiator/cycle/spin.go` — `nextRequest`/`rejectedACs` become
  per-(submitter,partition); a request is finally rejected only when every
  partition rejects it; session END once after the loop.
- `negotiator/cycle/concurrency.go` — `subState` lifetime spans the whole cycle
  (already true); the concurrency tracker spans partitions (already pool-wide).
- `negotiator/cycle/config.go` / `knobs.go` — the two new knobs.
- `negotiator/cycle/stats.go` / types — per-partition counters folded into the
  cycle stats (Pies already counts group pies; add a Partitions count).

## Testing

- Unit: `partitionSlots` groups deterministically; slots missing the attr land in
  the default partition.
- Cycle: a 2-partition pool with `GROUP_NAMES=production,analysis` and a 50/50
  quota — assert each partition's slots split ~50/50 between the groups, and that
  a submitter's pool-wide usage after the cycle equals the sum across partitions.
- **Schedd-load invariant**: a loopback schedd (negtest) records how many
  SEND_JOB_INFO / RRL exchanges it received; assert it is one per submitter per
  cycle regardless of partition count (the whole point).
- `compat==fast` must still hold with partitioning on (the pie-spin per partition
  is the same deterministic spine).
- Differential: not applicable (C++ has no equivalent); instead assert that a
  single-partition run is byte-identical to the non-partitioned cycle.

## Status

Design only (2026-07-13). Implementation is the next step — a self-contained,
cycle-scoped feature (no protocol wire changes; the NEGOTIATE session is reused,
not extended). Independent of the eval-perf work (see
NEGOTIATOR_CPP_DIFFERENCES.md and the classad tree-walk perf PRs).
