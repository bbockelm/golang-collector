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
> condor_collector](#deploying-htc-collector-alongside-an-existing-condor_collector)),
> not as a drop-in replacement for a central manager.

## Compatibility

- **TCP only, AES-GCM only.** CEDAR here does not implement UDP or the legacy
  Blowfish/3DES ciphers. This has direct configuration consequences — see the view
  host setup below.
- Authentication methods supported: `FS` (same host) and `IDTOKENS`/`TOKEN`
  (tokens signed by the pool signing key). Peers should be HTCondor 25.x.
- Pure Go (`CGO_ENABLED=0`): cross-compiles to `linux/amd64` and `linux/arm64`
  with no C toolchain.

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

Tagged releases (`git tag v0.3.0 && git push origin v0.3.0`) publish prebuilt
`linux/amd64` + `linux/arm64` binaries, tarballs, and checksums to the GitHub
release via `.github/workflows/release.yml`.

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
Keep `USE_SHARED_PORT = True` (the default); non-shared-port launch is not yet
supported (bbockelm/golang-htcondor#119).

### Standalone (testing)

```sh
htc-collector -listen 127.0.0.1:9618
```

Reads `CONDOR_CONFIG` for its security and forwarding policy but does not need a
`condor_master`. Useful as a view sink (below) and for local experimentation.

Flags: `-listen` (fallback bind when not under shared port), `-metrics`
(`:9720`-style address for a Prometheus `/metrics` endpoint), `-version`, and the
`-local-name`/`-sock` flags `condor_master` passes automatically.

## Deploying htc-collector alongside an existing condor_collector

The lowest-risk way to run `htc-collector` against a live pool is as a **CONDOR_VIEW
host**: a second collector that receives a *copy* of every ad your existing
(C++) collector receives. Your production `condor_collector` remains the source of
truth; `htc-collector` just mirrors it, so you can validate it against real pool
traffic with zero impact on matchmaking. This is exactly the arrangement the
integration tests exercise (`integration/condor_view_test.go`).

```
   daemons ──update──▶  condor_collector (C++, unchanged)
                              │  CONDOR_VIEW_HOST (a copy of every ad, over TCP)
                              ▼
                        htc-collector  (view sink — observe, query, scrape metrics)
```

### 1. Run htc-collector as a standalone view sink

Give it its own config and port (here `127.0.0.1:9620`; use a routable address for
a separate host). A view sink only receives updates and answers queries — it needs
no `condor_master`:

```
# htc-view.config
COLLECTOR_HOST = 0.0.0.0:9620
LOG            = /var/log/htc-view
COLLECTOR_LOG  = $(LOG)/CollectorLog
COLLECTOR_ADDRESS_FILE = $(LOG)/.htc_view_address
USE_SHARED_PORT = False
DAEMON_LIST    = COLLECTOR

# htc-collector is TCP + AES-GCM only. Same host as the source? FS auth works.
# Different host? Use IDTOKENS with a token signed by the pool signing key.
SEC_DEFAULT_AUTHENTICATION         = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS, IDTOKENS
SEC_DEFAULT_CRYPTO_METHODS         = AES
SEC_PASSWORD_DIRECTORY             = /etc/condor/passwords.d   # for IDTOKENS
```

```sh
CONDOR_CONFIG=/etc/condor/htc-view.config htc-collector -listen 0.0.0.0:9620
```

### 2. Point the existing collector at it

On the host running your **existing `condor_collector`**, add the view host. Two
settings are load-bearing:

```
CONDOR_VIEW_HOST = <view-sink-host>:9620

# REQUIRED: htc-collector is TCP-only, but view forwarding defaults to UDP
# (UPDATE_VIEW_COLLECTOR_WITH_TCP defaults false). Without this the forwarded ads
# are silently dropped.
UPDATE_VIEW_COLLECTOR_WITH_TCP = True

# By default a collector forwards only Machine (startd) ads to a view host (the
# classic CondorView utilization use). To mirror every daemon, opt each type in:
CONDOR_VIEW_CLASSAD_TYPES = Machine, Scheduler, Negotiator, DaemonMaster
```

Reconfigure the C++ collector (`condor_reconfig -collector`). It now authenticates
to the view sink and forwards a copy of each ad.

> **Gotchas, from experience:**
> - Omitting `UPDATE_VIEW_COLLECTOR_WITH_TCP = True` → the C++ collector forwards
>   over UDP, which `htc-collector` never receives. Nothing arrives, no error.
> - Omitting `CONDOR_VIEW_CLASSAD_TYPES` → only startd (`Machine`) ads are
>   forwarded; schedd/negotiator/master ads never appear on the view host.
> - Cross-host: the C++ collector must authenticate to `htc-collector`, so provision
>   an IDTOKEN (`condor_token_create`) signed by the pool signing key rather than
>   relying on same-host `FS`.

### 3. Verify

Query the view sink directly and confirm the usual daemon ads mirror across:

```sh
condor_status -pool <view-sink-host>:9620            # machine ads
condor_status -pool <view-sink-host>:9620 -schedd    # scheduler ads
condor_status -pool <view-sink-host>:9620 -negotiator
condor_status -pool <view-sink-host>:9620 -master
```

All four daemon ad types (Startd, Schedd, Negotiator, Master) should be present.

### The reverse direction

`htc-collector` can also be the **source** and forward to a C++ view host — set
`CONDOR_VIEW_HOST` in the Go collector's config. It forwards every ad type over TCP
(no `CONDOR_VIEW_CLASSAD_TYPES` needed on the Go side). Note the C++ *sink* then
needs `ALLOW_NEGOTIATOR` to accept forwarded negotiator/accounting ads.

## Configuration

`htc-collector` reads standard HTCondor knobs; the notable ones:

| Knob | Purpose |
| --- | --- |
| `COLLECTOR_HOST` | Well-known bind address/port (default `:9618`). |
| `CONDOR_VIEW_HOST` | Comma/space-separated view collectors to forward every ad to (TCP). |
| `COLLECTOR_PERSIST_SESSIONS` | Persist the CEDAR session cache across restarts (see below). Default off. |
| `COLLECTOR_SESSION_CACHE_FILE` | Path to the encrypted session DB (default `$(SPOOL)/collector_sessions.db`). |
| `SEC_PASSWORD_DIRECTORY` | Pool signing keys — required for session persistence and IDTOKENS. |
| `ENABLE_CCB_SERVER` | Run an embedded CCB server on the collector's command socket. |
| `NEGOTIATOR_EMBEDDED` | Run the negotiator inside `htc-collector` (reads the store directly). |
| `COLLECTOR_METRICS_ADDRESS` / `-metrics` | Serve Prometheus metrics at `/metrics`. |
| `COLLECTOR_UPDATE_INTERVAL` | Ad expiry sweep interval. |
| `COLLECTOR_DICT_RETRAIN_INTERVAL`, `COLLECTOR_DICT_SAMPLE_SIZE` | Compression-dictionary retraining cadence and sample size. |
| `SEC_*` | Authentication/encryption policy, exactly as for the C++ collector. |

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
