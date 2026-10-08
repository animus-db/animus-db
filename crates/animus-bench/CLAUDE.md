# CLAUDE.md — animus-bench

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

B-01 (docs/roadmap.md): the **load generator** behind published benchmarks.
It drives an AnimusDB cluster **as a client over the real, SigV4-signed
DynamoDB wire** (HTTP/1.1 JSON, `X-Amz-Target`) — not in-process — with YCSB
A-F, **open-loop arrivals and coordinated-omission-corrected latency**
(HdrHistogram), phases (load / warm-up / steady / degraded / recovery) and a
results JSON (`"schema": "animus-bench/v1"`) that discloses the topology,
environment and methodology. Library + binary (`animus-bench`); the library is
the reusable surface (C-17 builds scale scenarios on it without touching the
CLI).

`crates/animusd/benches/cluster_bench.rs` is the *in-process smoke* it grew
from (unauthenticated, sequential, closed-loop, no CO correction). It is kept
as a developer tool; **its numbers are not publishable and are not comparable
to this crate's.**

## Design decisions (the ADR is written separately — these are its inputs)

- **SigV4: reuse `animus_dynamo::sigv4::sign`**, not the AWS SDK. It was
  already `pub` (ADR 0057's test signer; AWS's own test-vector suite checks
  it), needs no new dependency, and keeps client overhead legible. No change
  to `animus-dynamo` was needed.
- **HTTP client: hand-rolled keep-alive HTTP/1.1 over a tokio `TcpStream`**
  (`client.rs`), one connection per worker. The server speaks a tiny fixed
  subset (`POST /`, `Content-Length`); nothing in `Cargo.lock` (hyper is only
  there for the OTLP/S3 paths) is needed.
- **TLS (ADR 0064 server-only; `tls.rs`)**: `--tls-ca PATH` (+ optional
  `--tls-server-name NAME`) makes every DynamoDB **and admin** dial a rustls
  client handshake, modelled on `animus-cli`: CA-only root store, no client
  cert, workspace `ring` provider (no new crypto backend, nothing new in
  `Cargo.lock`). `Conn` wraps `animus_env::MaybeTlsStream` (an enum: plain
  path unchanged, TLS = one `match` per poll). Endpoints are `SocketAddr`s,
  so the verified name is the node **IP** unless `--tls-server-name` is given
  (the node cert needs an IP SAN, or pass the DNS name). **The handshake is
  at connection setup, never in a measured op:** `Conn::connect` does it and
  `engine::run_phase` calls `Cluster::prewarm(connections)` *before* the phase
  clock starts, so workers begin with handshaken pooled connections; only a
  redial after a broken connection pays a handshake inside an op (as a TCP
  connect always did). Launch modes: `--launch processes|in-process` serve TLS
  when given `--tls-cert/--tls-key` (one leaf for all nodes, SAN `127.0.0.1`)
  plus `--tls-ca` (also the nodes' mutual-TLS CA) via `cluster::LaunchTls`;
  `--nodes` takes `--tls-ca` only. The report records the real
  `environment.tls` (+ `tls_note`) and `topology.tls`; schema stays
  `animus-bench/v1` (the existing field just stops being always false).
- **New dependency: `hdrhistogram` 7 (default-features off)** — MIT/Apache-2.0,
  plus its `byteorder`/`num-traits` deps. `rand`/`rand_chacha` (already in the
  workspace) seed the op/key stream.
- **CO method (`schedule.rs`, `recorder.rs`, `engine.rs`)**: op `i` has the
  *intended* send time `start + i/rate`, computed from the index (no drift). A
  dispatcher task never skips or delays an arrival: it sleeps to the intended
  time (or sends at once if late) and pushes `(intended, op)` onto an
  unbounded queue; `--connections` workers pull and execute. Latency is
  recorded from `intended` (headline) **and** from `started` (service time,
  what a closed-loop client sees); both are in the report. Queueing in the
  client (offered rate above what the pool can drive) is charged to latency,
  and ops still unstarted `drain_timeout` after the window are `abandoned`
  (flagged, never silently dropped). Errors are counted by kind
  (`throttled`/`timeout`/`connection`/`other`), **not** timed.
- **Key layout (workload.rs docs, repeated in every results file)**: one table
  per workload run, `pk` S HASH + `sk` N RANGE; record `i` → `pk =
  user<10-digit i/100>`, `sk = i%100`; item `{pk, sk, version:N, data:S}`.
  Workload E = `Query pk=:p AND sk>=:s Limit=len` within one 100-record
  partition (a DynamoDB `Query` cannot cross partition keys — disclosed
  difference from YCSB's `scan`). The table is created with **no provisioned
  throughput**, so it starts as one tablet; tablet count / replica placement
  are captured into `params.table_topology_after_load`.
- **Workload F** = `GetItem` then `UpdateItem SET data, version=old+1` with
  `ConditionExpression version = :old` (`attribute_not_exists(version)` if the
  read found nothing); one logical op spanning both calls; a lost race
  (`ConditionalCheckFailedException`) is **not retried and not an error** — it
  is `condition_failed`, recorded as completed. Workload D reads are
  zipfian-distance-from-newest, inserts sequential new keys.
- **`ConsistentRead`**: every workload runs once per `--consistent-read` mode
  (default `both`), as separate `RunResult`s with the same seed so the op
  stream is identical across modes. **Each mode gets its own freshly created
  and loaded table** (`scenario::table_name`: `<prefix>_<w>_cr` / `_ev`), so
  the second mode never measures a table the first mutated or warmed;
  `params.table_state` says so. Reads that find nothing are counted as
  `empty_reads` (legitimate under `false`).
- **Launch modes (`cluster.rs`)**: `external` (`--nodes D@A,...`; the
  publishable shape), `processes` (spawns real `animusd --config FILE --node I
  --dir DIR` children on this host; kill = SIGKILL, restart = respawn on the
  same dir), `in-process` (`animusd::Node` inside this process via
  `Node::bind` + `run_bound_node`; kill = `shutdown_graceful`, **not a crash**,
  no restart; used by the smoke test). Launched clusters sign with a default
  credential (`dynamo_auth` is put in the generated config) so SigV4 is always
  exercised. **Any bench-launched cluster, or an all-loopback external one, is
  `client_and_server_colocated: true` ⇒ `publishable: false` with a reason.**
  `publishable` (`report::publishability`) is true only if the servers are
  off-host (not bench-launched, not all-loopback) **and** a `degraded` phase
  whose fault was injected (`fault.ok`) is in the report; the reason lists
  every unmet condition. Necessary, not sufficient (instance types, RF,
  disk/network disclosure are the publisher's checklist, ADR 0076 §8).
- **Degraded run**: one, last, on a fresh table (it damages the cluster):
  warm-up → baseline (healthy, the comparison) → degraded (fault fires at the
  phase's start) → recovery (killed node restarted at its start when the
  cluster supports it: `processes`, or `--restart-cmd`). Victim = the
  tablet's leader (`/admin/status` finds the table's first tablet, each node's
  `/admin/raftkv` `is_leader` finds the node), a follower (a node hosting a
  verifiably non-leader replica), or `node:N` (index N, whatever it hosts —
  may be the leader or hold no replica; out-of-range is an error). A bare
  `--degraded node` is rejected (it used to silently mean follower).
  `--drain-secs` (default 30) is the post-phase grace after which unstarted
  ops are `abandoned`; recorded in `params.drain_secs`. For an
  external cluster the fault is a `--kill-cmd` `sh -c` template (`{node}`,
  `{host}`, `{dynamo}`, `{admin}`); with none, the degraded run is skipped and
  a `notes` entry says so.
- **Not discoverable → stated, never invented**: encryption at rest and
  `--shared-wal` are not reported by `/admin` (only `quiesce_after_ms`,
  `auth_enabled`, split/throttle thresholds are); the topology block says
  "unknown: not reported" for an external cluster.

## Entry points / library API (what C-17 reuses)

- `cluster::Cluster` — `external(nodes, creds, tls, kill, restart)`,
  `launch_in_process(n, dir, creds, Option<LaunchTls>)`,
  `launch_processes(.., Option<LaunchTls>, extra_args)`, `tls()`, `prewarm(n)`; `nodes()`, `dynamo_endpoints()`, `await_ready`,
  `tablet_roles(table)`, `apply_fault(FaultAction)`, `shutdown()`.
- `engine::run_phase(cluster, clock, &PhaseSpec, &mut impl OpSource,
  Arc<impl OpExecutor>) -> PhaseResult` — the open-loop shell. A scenario
  supplies an `OpSource` (seeded op stream, one task) and an `OpExecutor`
  (`class(op)` + `async execute(conn, op) -> Outcome`).
- `scenario::{run_steady, run_degraded}` (+ `PhasePlan`, `DegradedPlan`) —
  workload-agnostic sequencing; `scenario::run_ycsb` is the YCSB glue.
- `report::{Report, RunResult, SweepPoint}` — plain serde; push your own
  `RunResult { name, params: Value, load, phases, sweep }` into a `Report`.
- `envinfo::{HostInfo, git_state, capture_topology}`; `ycsb::{create_table,
  load_table, drop_table}`; `client::Conn` (`connect(addr, creds, Option<&TlsClient>)`, `call(op, json)`, SigV4-signed); `tls::TlsClient`.
- **Add a scenario**: implement `OpSource` + `OpExecutor`, create/load your
  tables with `Conn`, call `run_steady`/`run_degraded` (or `run_phase`
  directly), wrap the phases in a `RunResult`, add it to a `Report`. No CLI
  change needed; wire a flag in `cli.rs` only if you want a command.

## A/B comparison and the `bench` workflow

`animus-bench compare [--threshold-pct P] [--out FILE] --base A1.json A2.json
--head B1.json B2.json` (`compare.rs`, pure) prints a markdown table of
median / spread / delta per `(run, phase, metric)` (corrected p50/p99/p99.9
and achieved rate; sweep points too). A row is flagged only when `|delta| >
P` **and** the base/head [min,max] ranges are disjoint; otherwise a big
delta is `noisy`. Reporting only: exit 0 for any well-formed input (2 usage,
1 unreadable file). `.github/workflows/bench.yml` is `workflow_dispatch`
only: builds the generator once (head's) plus `animusd` for `base_ref` and
head, runs them interleaved base/head/base/head on one runner, uploads the
JSON + summaries, and appends the table to the step summary. Hosted-runner
numbers are colocated, non-publishable, and not a baseline.

## Lint posture

A real process boundary, modelled on `animus-cli`: **no package-level
exemption**. The pure core (`schedule`, `recorder`, `dist`, `workload`,
`report`) has no clock and no allow. All real time / spawn / wall-clock access
goes through four tiny wrappers in `rt.rs` (`Clock::start`, `sleep`,
`timeout`, `spawn`, `wall_epoch_secs`), each with its own
`#[allow(clippy::disallowed_methods, reason = "...")]`; everything else (tests
included) calls those. `BTreeMap` only. `std::net`/`std::fs` are unlinted but
used only at the edges (`cluster.rs` port probing/config files, `envinfo.rs`).

## What the tests prove (and don't)

- `recorder.rs` tests: **the CO property** — on a fake-clock FIFO server with a
  2 s stall on one request at 100 req/s, the corrected p99/p90 show the stall
  and the service-time and closed-loop views hide it (see
  `docs/lessons/testing/2026-10-04-test-coordinated-omission-correction-on-a-fake-clock-fifo-model.md`);
  no stall ⇒ the views agree; client-side backlog is charged. `schedule.rs`:
  intended times, no drift. `dist.rs`: zipfian/uniform/latest sanity (seeded,
  loose bounds). `workload.rs`: op-mix proportions per workload, seeded
  reproducibility, key layout. `ycsb.rs`: request shapes, error classification.
- `tests/smoke.rs` (second test): the same 3-node in-process shape with
  **server-only TLS on every port** (throwaway rcgen CA + leaf); workloads
  A/E/F through the TLS client with zero errors, the report says `tls: true`,
  the admin port is reachable over TLS, and three negative controls: an
  untrusting CA fails the handshake, and a plain-TCP client is not served by
  a TLS port. Same `prod-heavy` tier as the first test.
- `tests/smoke.rs` (first test): a real 3-node in-process cluster **with SigV4 on**; A-F x
  both read modes at ~300 ops each plus a follower-kill run. Asserts **wire
  shapes and correctness only** (zero unexpected errors, every arrival
  completes, JSON round-trips) — **never a latency or rate**. It is a
  `prod-heavy` target (`[[test]] required-features`, like animus-control /
  animus-cp-data / animus-storage's real-thread tests): a whole ProdEnv
  cluster must not run beside other tests, so it is **not** in the per-push
  `gates` tier (`--workspace --exclude animusd` skips it structurally) and
  runs exactly once, in CI's `prod-liveness-scattered` job
  (`cargo test -p animus-bench --features prod-heavy --test smoke --
  --test-threads=1`); ~45 s. Locally: `cargo test -p animus-bench --features
  prod-heavy`; a plain `cargo test -p animus-bench` runs only the unit tests.
- `compare.rs` tests: verdict logic of `animus-bench compare` (threshold
  respected and disclosed, a delta inside the run-to-run range is `noisy`
  not a regression, single-run groups never call a regression, throughput
  direction inverted, one-sided series reported, arg parsing).
- Nothing here validates absolute performance. Numbers from a colocated run
  are labelled non-publishable; the first publishable numbers need a
  dedicated client host and an external cluster (an operations task).

## Gotchas

- **A fat colocated tail is server-side, not the generator** (investigated
  2026-10-04; see `docs/lessons/testing/2026-10-04-a-suspicious-benchmark-
  tail-is-diagnosed-by-periodicity-and-an-independent-client.md`): ~200-300
  ms all-connection stalls every few seconds under a *write* workload, and a
  ~22 ms floor on `ConsistentRead: true` gets, reproduce with a plain Python
  client. Don't "fix" them here. Consider `--workloads C` or
  `--consistent-read false` when you need a clean read baseline.
- `serde_json` float parsing is not bit-exact: don't `assert_eq!` a `Report`
  against its re-parsed file (compare ints/strings).
- Sleep granularity is ~1 ms (tokio timer): at high rates arrivals are sent in
  tiny bursts; *intended* times are unaffected, which is the point.
- A killed node costs each worker at most one failed op (it redials to the
  next endpoint); the failures show up as `connection` errors in the
  `degraded` phase. The generator never retries.
- `--launch processes` needs a built `animusd` (default: next to the
  `animus-bench` binary, else `--animusd-bin`). Ports are probe-and-release;
  a lost race retries the whole launch.
- Table names: `ycsb<epoch>_<w>`; tables are dropped at the end unless
  `--keep-tables`.
- Record count is per workload table and loaded each time (D and E mutate
  theirs by inserting); reads under `false` right after a load may miss
  records on a lagging replica — they are counted, not failed.
