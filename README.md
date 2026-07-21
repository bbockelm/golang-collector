# golang-collector

A pure-Go reimplementation of HTCondor's **`condor_collector`** (and, optionally,
**`condor_negotiator`**), shipped as two daemons:

| Binary | Replaces | Role |
| --- | --- | --- |
| `htc-collector` | `condor_collector` | Receives daemon ClassAd updates, answers queries, expires/invalidates ads; optional CONDOR_VIEW forwarding, embedded CCB, and embedded negotiator. |
| `htc-negotiator` | `condor_negotiator` | Matchmaking cycle over the collector's ad store (also available embedded inside `htc-collector`). |

It reads a normal HTCondor configuration, speaks the CEDAR wire protocol, runs
under `condor_master` like any other daemon (shared-port endpoint, privilege drop,
`DC_*` commands), and is backed by the [classad collections
engine](https://github.com/bbockelm/golang-classads) for compact ad storage.

> **Status: experimental.** The intended first production use is as a **CONDOR_VIEW
> host** observing an existing pool (see [Deploying alongside an existing
> condor_collector](#deploying-htc-collector-alongside-an-existing-condor_collector)).

## Building

```sh
make build          # -> build/htc-collector, build/htc-negotiator (version-stamped)
make test           # go test ./...
make test-short     # go test -short ./...
make install PREFIX=/usr/local   # installs both into $(PREFIX)/sbin
```

Binaries report their version:

```sh
$ build/htc-collector -version
htc-collector v0.3.0
```

Cross-compile for a Linux deployment target from any host:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 make build
```

Tagged releases publish prebuilt `linux/amd64` + `linux/arm64` binaries, tarballs,
and checksums to the GitHub release.

## Running

### Under condor_master (production)

Point `condor_master` at the binary the same way you would any daemon. To run the
Go collector as the pool's collector, set:

```
COLLECTOR = /usr/local/sbin/htc-collector
```

Started as root under the master, `htc-collector` drops its effective uid/gid to
the `condor` account, inherits the shared-port endpoint, writes its address file,
and answers `condor_status` / `condor_who` / `condor_ping` like the C++ collector.
Keep `USE_SHARED_PORT = True` (the default and the recommended mode).

To instead run it *next to* the stock collector as a view host — the recommended
first rollout — see [Deploying htc-collector alongside an existing
condor_collector](#deploying-htc-collector-alongside-an-existing-condor_collector).

### Standalone (testing)

```sh
htc-collector -listen 127.0.0.1:9618
```

Reads `CONDOR_CONFIG` for its security and forwarding policy but does not need a
`condor_master`. Useful for local experimentation.

Flags: `-listen` (fallback bind when not under shared port), `-metrics`
(`:9720`-style address for a Prometheus `/metrics` endpoint), `-version`, and the
`-local-name`/`-sock` flags `condor_master` passes automatically.

## Deploying htc-collector alongside an existing condor_collector

The lowest-risk way to run `htc-collector` against a live pool is as a **CONDOR_VIEW
host**: a second collector that receives a *copy* of every ad your existing
(C++) collector receives. Your production `condor_collector` stays the source of
truth; `htc-collector` just mirrors it, so you can validate it against real pool
traffic with zero impact on matchmaking.

Run it as a **second, differently-named DaemonCore daemon under the same
`condor_master`** as the stock collector — not as a hand-started process. The
master supervises it, drops it to `condor`, and hands it a shared-port endpoint,
exactly like every other daemon.

```
                       condor_master
                        ├── condor_collector   (C++, unchanged) ──┐
                        └── htc-collector       (HTC_COLLECTOR)  ◀─┘ CONDOR_VIEW_HOST
                                                                     (a copy of every ad)
```

### Minimal example

Drop this into a new config file on the central manager — e.g.
`/etc/condor/config.d/50-htc-view.conf` — and `condor_reconfig` (or restart the
master). It adds the view host without changing anything about the existing
collector.

```
# --- htc-collector as a CONDOR_VIEW host, under the existing condor_master ---

# Define a second daemon. Do NOT call it COLLECTOR (that is the stock collector);
# a distinct name + a -local-name is what keeps the two from colliding.
HTC_COLLECTOR       = /usr/local/sbin/htc-collector
HTC_COLLECTOR_ARGS  = -local-name HTCVIEW
DAEMON_LIST         = $(DAEMON_LIST), HTC_COLLECTOR
DC_DAEMON_LIST      = +HTC_COLLECTOR     # append to the built-in DaemonCore list

# Its own log and address file. Do NOT reset LOG or reuse COLLECTOR_ADDRESS_FILE
# (those belong to the stock collector). The "HTCVIEW." prefix scopes these knobs
# to this daemon only.
HTCVIEW.COLLECTOR_LOG          = $(LOG)/HTCViewCollectorLog
HTCVIEW.COLLECTOR_ADDRESS_FILE = $(LOG)/.htcview_collector_address
# It is a view *sink*; make sure it never forwards onward (avoids a loop).
HTCVIEW.CONDOR_VIEW_HOST       =

# Point the stock collector at the view host. Under shared port the daemon shares
# the collector's port and is addressed by its socket name, which is derived from
# the local name ("HTCVIEW" -> "htcview"). Use your collector's actual port.
CONDOR_VIEW_HOST              = <$(CONDOR_HOST):9618?sock=htcview>
# REQUIRED: htc-collector speaks TCP only, but view forwarding defaults to UDP
# (UPDATE_VIEW_COLLECTOR_WITH_TCP is false), which would be silently dropped.
UPDATE_VIEW_COLLECTOR_WITH_TCP = True
# A collector forwards only Machine (startd) ads to a view host by default (the
# classic CondorView utilization use); opt every daemon type in to mirror the pool.
CONDOR_VIEW_CLASSAD_TYPES      = Machine, Scheduler, Negotiator, DaemonMaster
```

Notes:

- **Shared port stays on** (`USE_SHARED_PORT = True`, the default). The view host
  needs no port of its own — the master gives it the socket name `htcview` on the
  collector's shared port, so its address is `<host:9618?sock=htcview>`.
- **Leave security alone.** `htc-collector` inherits the pool's `SEC_*` policy; it
  interoperates on a normal pool (AES with `IDTOKENS`/`FS`). Do not hand-craft a
  separate security config for it.
- **Nothing about the stock collector changes** except the three
  `CONDOR_VIEW_HOST` / `UPDATE_VIEW_COLLECTOR_WITH_TCP` / `CONDOR_VIEW_CLASSAD_TYPES`
  lines, which are read only by the `COLLECTOR` subsystem; the `HTCVIEW.`-scoped
  overrides above keep the view daemon from acting on them.
- The `HTCVIEW.`-prefixed knobs rely on HTCondor subsystem/local-name config
  scoping. Build `htc-collector` against a `golang-htcondor` new enough to
  implement it; an older build ignores the prefix and the two daemons collide on
  `COLLECTOR_LOG` / `COLLECTOR_ADDRESS_FILE`.

### Verify

The view daemon logs to `HTCViewCollectorLog` and writes its address to
`.htcview_collector_address`. Query it directly — it mirrors the pool's daemon ads:

```sh
condor_status -pool '<CM-HOST:9618?sock=htcview>'                # machine ads
condor_status -pool '<CM-HOST:9618?sock=htcview>' -schedd
condor_status -pool '<CM-HOST:9618?sock=htcview>' -negotiator
condor_status -pool '<CM-HOST:9618?sock=htcview>' -master
```

All four daemon ad types (Startd, Schedd, Negotiator, Master) should appear.

### The reverse direction

`htc-collector` can also be the **source** and forward to a C++ view host — set
`CONDOR_VIEW_HOST` in the Go collector's own (scoped) config. It forwards every ad
type over TCP (no `CONDOR_VIEW_CLASSAD_TYPES` needed on the Go side). Note the C++
*sink* then needs `ALLOW_NEGOTIATOR` to accept forwarded negotiator/accounting ads.

## Configuration

`htc-collector` reads standard HTCondor knobs; the notable ones:

| Knob | Purpose |
| --- | --- |
| `COLLECTOR_HOST` | Well-known bind address/port (default `:9618`). |
| `CONDOR_VIEW_HOST` | Comma/space-separated view collectors to forward every ad to (TCP). |
| `COLLECTOR_STORE` | Ad store backend: `memory` (default), `embedded`, or `db` (see [Ad store backends](#ad-store-backends)). |
| `COLLECTOR_DB_PATH` | `embedded` store location (default `$(LOCAL_DIR)/collector-db`). |
| `COLLECTOR_DB_ENCRYPTION` | Encrypt the `embedded` store at rest under the pool signing key. Default `True`. |
| `COLLECTOR_DB_HOST` | `db` store: address of the external database daemon (CEDAR). |
| `COLLECTOR_BATCH_WINDOW_MS` | Ad-update batching window for the persistent backends. Default `100`; `0` disables. |
| `COLLECTOR_BATCH_MAX_ADS` | Flush the batch early once this many ads buffer. Default `2048`. |
| `COLLECTOR_PERSIST_SESSIONS` | Persist the CEDAR session cache across restarts (see below). Default off. |
| `COLLECTOR_SESSION_CACHE_FILE` | Path to the encrypted session DB (default `$(SPOOL)/collector_sessions.db`). |
| `SEC_PASSWORD_DIRECTORY` | Pool signing keys — required for session persistence, embedded-store encryption, and IDTOKENS. |
| `ENABLE_CCB_SERVER` | Run an embedded CCB server on the collector's command socket. |
| `NEGOTIATOR_EMBEDDED` | Run the negotiator inside `htc-collector` (reads the store directly). |
| `COLLECTOR_METRICS_ADDRESS` / `-metrics` | Serve Prometheus metrics at `/metrics`. |
| `COLLECTOR_UPDATE_INTERVAL` | Ad expiry sweep interval. |
| `COLLECTOR_DICT_RETRAIN_INTERVAL`, `COLLECTOR_DICT_SAMPLE_SIZE` | Compression-dictionary retraining cadence and sample size. |
| `COLLECTOR_SLOW_OP_MS` | Log a WARN when a single ad update or batch flush blocks at least this long. Default `2000`; `0` disables. |
| `COLLECTOR_DEBUG_PPROF` | Serve Go `pprof` endpoints under `/debug/pprof/` on the metrics listener. Default off (debugging only). |
| `SEC_*` | Authentication/encryption policy, exactly as for the C++ collector. |

### Ad store backends

`COLLECTOR_STORE` selects where advertised ads live:

- **`memory`** (default) — ads are held in the in-memory collections engine
  (compressed, footprint-tuned). Fastest, but the pool is lost on restart.
- **`embedded`** — ads are persisted to a local database under `COLLECTOR_DB_PATH`,
  so a restart resumes with the pool intact (stale ads are pruned by the startup
  expiry sweep, not lost). Encrypted at rest by default (see below).
- **`db`** — ads are stored in an external database daemon reached over CEDAR at
  `COLLECTOR_DB_HOST`, so several collectors can share one durable store.

**Encryption at rest (`embedded`).** With `COLLECTOR_DB_ENCRYPTION = True` (the
default), the embedded database is sealed under the pool signing key(s) in
`SEC_PASSWORD_DIRECTORY` — the same key source and root-read mechanism as the
[session cache](#encrypted-session-persistence-and-the-privilege-model), so the
on-disk ads (including private ads such as claim capabilities) are unreadable
without a key. As with session persistence, enabling encryption with no signing
key available is a fatal misconfiguration rather than a silent fall back to
plaintext; set `COLLECTOR_DB_ENCRYPTION = False` to store ads unencrypted (e.g. for
testing).

**Update batching.** For the persistent backends, ad updates are buffered for
`COLLECTOR_BATCH_WINDOW_MS` (default `100`), deduplicated per ad, and committed in
one transaction — collapsing a daemon's rapid re-advertises and coalescing startup
storms into far fewer commits/round-trips. Reads flush the buffer first, so the
window bounds only write-visibility latency (well under the C++ collector's update
cadence, and imperceptible to `condor_status`); `UPDATE_*_AD_WITH_ACK` updates
bypass the buffer entirely, so an acknowledgment always follows durability. Set the
window to `0` to disable batching. Batching does not apply to the `memory` store.

### Encrypted session persistence and the privilege model

With `COLLECTOR_PERSIST_SESSIONS = True`, `htc-collector` persists its CEDAR
security-session cache to an at-rest-encrypted SQLite database so a restart resumes
sessions instead of triggering a re-authentication storm. The database's data key
is wrapped by the pool signing key(s) in `SEC_PASSWORD_DIRECTORY`.

This exercises HTCondor's privilege model end to end:

- Started as root under `condor_master`, the collector **drops** its effective
  uid/gid to `condor`.
- Its log file and the session database are opened *after* the drop, so they are
  **owned by `condor`**.
- The pool signing key (root-owned, `0600`) is read by **re-elevating to root**,
  then dropping back — so a stolen database is useless without a signing key, and
  the daemon never runs privileged longer than the key read.

`integration/privilege_drop_test.go` verifies all three properties under a real
`condor_master` (root-gated; it runs in the `test-root` CI job).

### Metrics

```sh
htc-collector -metrics :9720    # or COLLECTOR_METRICS_ADDRESS = :9720
curl -s localhost:9720/metrics
```

Reports the compressed storage footprint per ad type plus standard Go/process
metrics, so a pool can be sized from live numbers.

### Debugging stalls

When updates back up — e.g. a C++ collector forwarding to `htc-collector` hits its
write timeout — the cause is usually a slow store operation blocking the per-connection
handler. Three layered aids surface it without leaving a profiler exposed in production:

- **Operational metrics** (always on `/metrics`): `condor_collector_update_seconds`,
  `condor_collector_batch_flush_seconds`, `condor_collector_backoff_seconds`, and
  `condor_collector_retries_total{cause}` — the `cause` label
  (`no_txn`/`deadline`/`conn_closed`/`transport`/`dial`) attributes retry storms against
  the `db` backend. A rising `backoff_seconds` relative to `batch_flush_seconds` means the
  database, not the collector, is the bottleneck.
- **Slow-op WARN** (`COLLECTOR_SLOW_OP_MS`, default 2s): any update/flush that blocks past
  the threshold logs a WARN with the ad type/name — so a stall shows up in the log even
  though `withRetry` eventually succeeds and would otherwise hide it as latency. The same
  path also rate-limits a WARN naming the transient error each retry backs off on.
- **`kill -USR1 <pid>`**: dumps every goroutine's stack to the collector log — the
  on-demand way to see exactly which lock/syscall/channel a handler is parked on during a
  live stall, with no HTTP surface and no restart.
- **`COLLECTOR_DEBUG_PPROF = True`**: when the above is not enough, exposes the standard Go
  `pprof` endpoints under `/debug/pprof/` on the metrics listener for a full CPU/heap/block
  profile. Off by default; enable only while investigating.

## Testing

```sh
make test                       # unit + integration (integration tests skip if
                                # condor_master is not on PATH)
go test -run TestGoCollectorAsView ./integration/    # the CONDOR_VIEW round trips
```

CI runs the suite through `gotestsum`; the root-gated privilege-drop test runs in a
separate `test-root` job (installing HTCondor provides the `condor` account).

## Repository layout

```
cmd/golang-collector/   htc-collector daemon (condor_master glue, CCB, metrics, sessions)
cmd/golang-negotiator/  htc-negotiator daemon
collector.go            embeddable collector library (New / RegisterOn / Serve)
server/                 CEDAR command handlers (UPDATE_*/QUERY_*/INVALIDATE_*, forwarding)
store/                  ad store backed by the classad collections engine
negotiator/             matchmaking engine (also embeddable via NEGOTIATOR_EMBEDDED)
metrics/                Prometheus exporter
integration/            live tests under condor_master (condor-view, privilege drop)
docs/                   negotiator design, roadmap, and C++ differences
```

### Related repositories

- [`golang-htcondor`](https://github.com/bbockelm/golang-htcondor) — config, daemon
  core, privilege drop, security, session cache.
- [`golang-classads`](https://github.com/bbockelm/golang-classads) — the ClassAd
  collections/compression engine backing the store.
- [`cedar`](https://github.com/bbockelm/cedar) — the CEDAR wire/security library.
- [`golang-ccb`](https://github.com/bbockelm/golang-ccb) — the embedded CCB server.
