# ADR 0076 — Published benchmarks: an open-loop, coordinated-omission-corrected load generator over the real DynamoDB wire, and the rules a publishable result must meet

- **Status:** Accepted
- **Date:** 2026-10-04
- **Origin:** roadmap item B-01 ("Published benchmarks with disclosed
  methodology"). `website/performance.html` committed, before any number
  existed, to a methodology (disclosed hardware, tail percentiles, both read
  modes apart, a failure case in every run, reproducible, no DynamoDB
  comparison charts) and `website/index.html` listed the benchmarks as
  Planned, but the only measuring tool in the tree was
  `crates/animusd/benches/cluster_bench.rs`: an in-process cluster on one
  host, **unauthenticated** (no SigV4), sequential connect-per-request
  latency classes, a **closed-loop** concurrency sweep, percentiles over
  sorted samples — so no coordinated-omission correction — no workload mix
  and no warm-up/steady-state split. That is a developer smoke, not a
  publishable suite.
- **Amends:** none. **Depends on:** [ADR 0003](0003-deterministic-simulation.md)
  (why a load generator is a real-socket process boundary and not
  `Env`-generic), [ADR 0006](0006-dual-cql-dynamo-adapters.md) (the DynamoDB JSON/HTTP
  wire it drives), [ADR 0055](0055-eventually-consistent-reads.md) (the two
  `ConsistentRead` paths that must be reported apart),
  [ADR 0057](0057-sigv4-client-auth.md) and
  [ADR 0066](0066-sigv4-hardening.md) (the SigV4 edge the generator signs
  for), [ADR 0064](0064-tls-on-every-port.md) (why "no TLS client" is a stated
  limit), [ADR 0067](0067-throughput-derived-minimum-tablet-count.md) (why the
  bench tables start as one tablet), [ADR 0072](0072-dynamodb-service-limits.md)
  (the generator runs under the same compiled-in limits as any client).
- **Consumers:** the library API of `crates/animus-bench` is the reusable
  surface for C-17 Tier 2 (scale/density scenarios) and for the R-01 soak and
  capacity-planning sub-tracks; none of them needs to touch the CLI.

## Context

Benchmark numbers for a distributed database are easy to produce and hard to
make mean anything. The failure modes this ADR exists to pre-empt, each of
which the old `cluster_bench` has:

1. **Closed-loop clients hide stalls (coordinated omission).** A client that
   sends the next request only when the previous one returned stops
   offering load exactly while the server is slow, so the slow period
   contributes one sample instead of the hundreds that arrivals would have
   produced. Tail percentiles from such a client under-report stalls —
   leader elections, compactions, fsync spikes — by orders of magnitude.
2. **Same-host numbers are not server numbers.** A client and three servers
   sharing four cores compete for CPU, disk and loopback; the figure
   measures the box.
3. **A number from a different host or day is not a baseline**
   (`docs/engineering-lessons.md`: "a historical bench figure from a
   different host is not a baseline"). Shared CI runners make absolute
   numbers meaningless.
4. **Unstated configuration.** Item size, key count, key distribution,
   read-consistency mode, replica placement, durability settings, what was
   discarded as warm-up: a result that does not carry these cannot be
   reproduced or compared.

## Decision

### 1. A separate client-side crate, over the real wire

The load generator is a new workspace member, **`crates/animus-bench`**
(library + `animus-bench` binary). It drives a cluster **as a client** over
the real DynamoDB JSON/HTTP wire with SigV4-signed requests; it never links
into the server's request path.

It is a **real-socket process boundary** in the same sense as `animus-cli`
(ADR 0003): not `Env`-generic, and **not** covered by a package-level lint
exemption either. The pure core (`schedule`, `recorder`, `dist`, `workload`,
`report`) has no clock and no allow; all real time, task spawning and
wall-clock access goes through four small wrappers in `rt.rs`, each with its
own individually justified `#[allow(clippy::disallowed_methods)]`. This is
the lint posture `animus-cli` uses, not `animusd`'s.

The deterministic-simulation guarantee does not extend to it: a benchmark
measures real hardware, so its numbers are intrinsically non-reproducible.
What *is* reproducible is the **operation stream** (seeded, §3) and the
**disclosed configuration** (§8).

### 2. SigV4 via `animus_dynamo::sigv4::sign`, not the AWS SDK

Requests are signed by reusing the already-`pub`
`animus_dynamo::sigv4::sign` (ADR 0057's test signer, validated against
AWS's own test-vector suite). Rejected: the AWS SDK as a dependency.

- **No new dependency tree.** The SDK would add a large TLS/HTTP/runtime
  stack to a workspace under `cargo deny`, for a tool that needs one
  signing function.
- **Client overhead stays legible.** The point of the tool is to attribute
  latency to the server; an SDK's retry policy, connection pooling,
  endpoint discovery, checksum and clock-skew logic all sit between the
  intended send time and the wire and cannot be turned fully off. Under
  open-loop CO accounting, a hidden client-side retry is a correctness bug
  in the measurement, not a convenience. The generator **never retries**.
- **Same bytes the server verifies.** The signer is the one the server's
  verifier tests already exercise.

Cost: the generator speaks only the subset of the wire it needs (§3); it is
not an SDK-compatibility test. SDK-shaped behaviour is the compatibility
suite's job (`website/compatibility.html`), not the benchmark's.

### 3. A hand-rolled keep-alive HTTP/1.1 client — and its consequence

The client (`client.rs`) is a minimal keep-alive HTTP/1.1 client over a tokio
`TcpStream`, one connection per worker. The server's DynamoDB edge speaks a
tiny fixed subset (`POST /`, `Content-Length`, `X-Amz-Target`), and nothing
already in `Cargo.lock` is needed to speak it (hyper is present only for the
OTLP/S3 paths). A worker redials the next endpoint after a socket error.

**Consequence: there is no TLS client yet.** The generator dials the DynamoDB
and admin ports in plain TCP, and the results file says so (`environment.tls`
is always `false` with an explanatory `tls_note`). The cluster under test
must therefore have TLS off on those ports (it is off by default, ADR 0064).
Benchmarking the TLS posture is a named follow-up ("What is *not* done"); `animus-env`'s
`MaybeTlsStream`, which `animus-cli` already uses, is the intended
mechanism. Until then no published result may be read as a TLS-on number.

### 4. Workloads: YCSB A–F mapped onto DynamoDB operations

| Workload | Mix | DynamoDB operations |
|---|---|---|
| A | 50% read / 50% update | `GetItem` / `UpdateItem SET data` (unconditional) |
| B | 95% read / 5% update | as A |
| C | 100% read | `GetItem` |
| D | 95% read-latest / 5% insert | `GetItem` of recently inserted keys / `PutItem` of a new key |
| E | 95% scan / 5% insert | `Query pk = :p AND sk >= :s`, `Limit` = scan length / `PutItem` |
| F | 50% read / 50% read-modify-write | `GetItem`; or `GetItem` then conditional `UpdateItem` |

**Key layout (repeated in every results file).** One table per workload run,
`pk` S HASH + `sk` N RANGE. Record `i` is `pk = "user" + zero-padded-10(i /
100)`, `sk = i % 100`; the item is `{pk, sk, version: N, data: S}` with `data`
exactly `--value-bytes` long. Record count and value size are CLI flags
(defaults 10,000 and 256 B) and are echoed into every run's `params`.

**Workload E is not YCSB's scan.** A DynamoDB `Query` cannot cross partition
keys, so E's "short scan" is a `Query` within one 100-record partition
starting at a chosen sort key, with `Limit` uniform in `1..=max_scan_len`
(default 100). It exercises a range read on one tablet, **not** a
cross-partition ordered scan, and the layout string in every results file says
so. A cross-partition `Scan` workload is not part of B-01.

**Workload F** is `GetItem`, then `UpdateItem SET data = :v, version = :old+1`
with `ConditionExpression version = :old` (`attribute_not_exists(version)` if
the read found no item). It is **one logical operation** whose latency spans
both calls. A lost race (`ConditionalCheckFailedException`) is *not retried
and not an error*: it is counted as `condition_failed` and the op is recorded
as completed. This uses a conditional `UpdateItem` rather than
`TransactWriteItems`: it is the idiomatic optimistic read-modify-write on this
wire and keeps F a single-tablet workload; a transactional F variant is a
possible later scenario on the library API.

**Distributions.** `--distribution zipfian` (default) is YCSB's *scrambled*
zipfian (theta = 0.99, rank hashed onto the key space); `uniform` is the
alternative. Workload D always reads *latest* (zipfian distance back from the
newest insert; inserts are sequential new keys), regardless of the flag.

**Table shape.** The table is created with **no provisioned throughput**, so
it starts as one tablet (ADR 0067's throughput-derived split applies only to
provisioned tables); byte-driven auto-split may change that during a run. The
tablet count, replica placement and leaders after the load are recorded
(`params.table_topology_after_load`), and the topology is re-captured at the
end (`topology_end`), because splits and failovers change it. Whether a result
is "a single-tablet" or "multi-tablet" number is thereby visible, not
assumed.

### 5. `ConsistentRead: true` and `false` are separate runs

Every workload with reads runs **once per `--consistent-read` mode** (default
`both`) as separate results with the same seed, so the op stream is identical
across modes. **Each mode runs on its own freshly created and loaded table**
(`ycsb<epoch>_<w>_cr` / `_ev`, dropped afterwards), so the second mode never
measures a table the first already mutated, compacted or warmed; the results
file says so in `params.table_state`. They are never blended (ADR 0055: `true` is the linearizable
ReadIndex read, `false` — the wire default — the replica-local one). A read
that finds nothing is a completed op counted separately as `empty_reads`:
under `false` a lagging replica may legitimately miss a just-loaded record.

### 6. Open-loop generation and the coordinated-omission correction

- **Schedule.** Operation `i` of a phase has the *intended* send time
  `start + i / rate`, computed from the index, so the schedule cannot drift.
- **Dispatch.** A single dispatcher never skips or delays an arrival: it
  sleeps to the intended time (or sends at once if already late) and queues
  `(intended, op)`; `--connections` workers pull and execute. If the server
  stalls or the offered rate exceeds what the worker pool can drive, ops wait
  in the client queue and that wait **is charged to latency**.
- **Measurement.** Headline latency is `completed − intended`. A separate
  **service-time** histogram records `completed − started` (what a
  closed-loop client would have measured). Both are in the results so a
  reader can see how much the correction matters. Histograms are
  HdrHistogram, microseconds, **3 significant figures**, 1 µs to 1 h
  (out-of-range values are saturated, never dropped). Reported:
  min/mean/p50/p90/p99/p99.9/p99.99/max per latency class and overall.
- **Errors are counted, not timed.** By kind: `throttled`, `timeout`,
  `connection`, `other` (a timeout is not a latency sample). A bounded
  number of error samples is kept verbatim.
- **Abandoned ops.** After the phase window the workers drain the queue for
  up to a grace (`--drain-secs`, default 30 s, recorded in `params.drain_secs`); ops still unstarted by then are `abandoned` — flagged
  in the result, never silently dropped. A non-zero `abandoned` or an
  `achieved_rate` below `target_rate` means the offered rate exceeded what
  the system (or the client pool) could carry; the point is past the knee.
- **Sweep.** `--sweep R1,R2,...` runs one measured phase per rate; the
  table of target rate vs achieved rate vs corrected percentiles is how the
  **knee** is found, in place of a single closed-loop number.
- **Known granularity limit.** The tokio timer has ~1 ms resolution, so at
  high rates arrivals are sent in tiny bursts. *Intended* times are
  unaffected, which is exactly why latency is measured from them.

The CO property is tested on a fake-clock FIFO server model (a 2 s stall at
100 req/s): corrected percentiles show the stall, service-time and closed-loop
views hide it. See `crates/animus-bench/CLAUDE.md`.

### 7. Phases

Per workload run: **load** (bulk `BatchWriteItem`, 25 per call; reported, not
a latency measurement) → **warm-up** (`--warmup-secs`, run, counted, and
**discarded**: no histograms in the result) → **steady** (`--steady-secs`,
measured) or **sweep**.

A **degraded run** follows, once, last, on a fresh table (it damages the
cluster): warm-up → **baseline** (healthy, the comparison) → **degraded** (the
fault fires at the start of the phase) → **recovery** (the killed node is
restarted at its start when the cluster supports it). The victim is the
tablet's **leader**, a **follower**, or an explicit `node:N`; leader/follower
are resolved from `/admin/status` (the table's first tablet) and each node's
`/admin/raftkv` `is_leader`. By default it runs one workload (the first
selected) at `ConsistentRead: true`, on the whole op mix of that workload —
not only two op classes as `cluster_bench` did.

How the fault is applied depends on how the cluster was obtained (§8):
`processes` kills the `animusd` child with SIGKILL (a real crash) and restarts
it on the same data dir; `in-process` calls a graceful shutdown (**not** a
crash, and no restart; dev/smoke only); `external` runs an operator-supplied
`--kill-cmd` (and `--restart-cmd`) `sh -c` template. With no kill mechanism the
degraded run is skipped and a `notes` entry says so. Because the generator
never retries, a killed node costs each worker at most one failed op; the
failures appear as `connection` errors in the `degraded` phase. A publishable
result must include the degraded run (§8); the tool does not enforce this
(see "publishable" below), the publication process does.

### 8. Topology and the `publishable` flag

**Launch modes** (`cluster.rs`):

- `--nodes D@A,...` (**`external`**): attach to a running cluster by each
  node's DynamoDB address and admin address. This is the only shape a
  published number may come from.
- `--launch processes`: spawn real `animusd` children on the generator's
  host. **Development only.**
- `--launch in-process`: run `animusd::Node` inside the generator process
  (what the smoke test uses). **Development only.**

**What a publishable result requires** (the contract, split into what the tool
checks and what the publication process checks):

- *Checked by the tool:* the load generator and the servers are on **different
  hosts**. `environment.client_and_server_colocated` is true — and
  `publishable` is false, with `publishable_reason` — if the bench launched
  the cluster itself, or if every endpoint address is loopback.
  `publishable` additionally requires that the report contain a **degraded
  run whose fault was actually injected**; `publishable_reason` lists every
  unmet condition. It does not check that TLS posture is acceptable, that the
  cluster has three nodes, or that the hardware is named. Treat it as a
  necessary condition, not a certificate.
- *Required by this ADR and checked at publication time:* three nodes at
  RF 3 as the baseline plus **one scale-out point** (more nodes, same
  workload), a **named instance type, disk model and network** for both the
  servers and the client host, replica placement across failure domains
  stated, the degraded run (leader kill, and a follower-kill variant) present,
  the raw results JSON committed or attached to a release, and the
  `animusd` build/flags recorded. The servers' own hardware is **not**
  captured by the tool (it only sees `/proc` on the client host); the
  publisher states it alongside the file.
- The tool records: git SHA (and dirty flag; `ANIMUS_BENCH_GIT_SHA`
  overrides), the exact argv, the seed, the client host (hostname, kernel, CPU
  model and count, memory), `launch_mode`, the target endpoints, whether
  requests were SigV4-signed, TLS state (always off), the cluster topology
  before and after the run from `/admin` (`/admin/config`, `/admin/status`,
  `/admin/raftkv`: membership, per-node `auth_enabled`/`quiesce_after_ms`/
  split and throttle thresholds, per-table tablet count, replication-factor
  policy, replica placement and leaders), and a fixed `methodology` block.
- **Stated, never invented.** Facts `/admin` does not report are written as
  such: **encryption at rest** and **`--shared-wal`** are not exposed by
  `/admin`, so for an external cluster they are recorded as `"unknown: not
  reported by /admin"`. For a cluster the bench launched they are recorded as
  disabled (no `--encryption-key`) and "animusd default" respectively. A
  publisher states both for an external cluster by hand until `/admin`
  reports them ("What is *not* done", item 5).

The results schema is `animus-bench/v1` (`report.rs`); see
`docs/benchmarks.md` for every field. The schema tag is bumped, and the old
shape kept readable, on a breaking change.

### 9. Regression tracking: manual, same-host, interleaved A/B, never an assertion

- A **`workflow_dispatch`** workflow (`.github/workflows/bench.yml`) builds a
  base ref and a candidate ref and runs the generator against both **in the
  same job on the same host**, interleaved, uploading the raw JSON as
  workflow artifacts. The generator is built once from the dispatched ref so
  both sides share it; inputs are `runner`, `base_ref`, `workloads`, `rates`,
  `records`, `value_bytes`, `warmup_secs`, `steady_secs`, `consistent_read`,
  `degraded`, `repeats` and `threshold_pct`. It is manual: it is **not** a per-push gate, and on a
  GitHub-hosted runner it is a smoke of the harness, not a source of numbers.
- Comparison is **only** between runs executed together on one host. The
  workflow reports run-to-run **spread** (it repeats rather than trusting one
  run) and flags a change only beyond a disclosed threshold; a single
  outlier is a rerun, not a verdict. The flags are advisory output.
  `animus-bench compare` builds the table (median, spread, delta per run,
  phase and metric): a row is flagged only when `|delta|` exceeds the
  threshold **and** the base and head [min, max] ranges are disjoint;
  otherwise it is `noisy` (also for a single-run group). It exits 0 on any
  well-formed input.
- **A benchmark never becomes a latency or throughput assertion in a test or a
  required check.** `crates/animus-bench/tests/smoke.rs` asserts wire shapes
  and correctness only (zero unexpected errors, every arrival completes,
  JSON round-trips) — never a rate or a latency. A latency threshold in a
  gate is a flaky test by construction (root `CLAUDE.md`, item 4) and would
  be tuned until green.
- A **dedicated, fixed runner** (self-hosted or pinned bare metal) is what
  turns this from "A/B on a noisy host" into a trend line. Provisioning it is
  an **operations task, not done by this ADR**; until it exists nothing here
  is a time series, and no absolute number from a CI run may be quoted.

### 10. Comparison rules (should a comparison ever be wanted)

None is planned. If the maintainer ever wants one it must be **like for
like**: the same instance types and counts; the same item and key shape; the
same durability (fsync-acked writes against the other system's equivalent
setting); `ConsistentRead` matched to the other system's read consistency;
replication factor matched; the other system's configuration published in
full and ideally reviewed by its own community; versions pinned; and the
other system driven by the same open-loop, CO-corrected generator, not its
own bundled tool.

**The existing commitment stands unchanged:** *no comparison charts against
DynamoDB*, and no managed-service-versus-self-hosted charts of any kind
(`website/performance.html`'s commitments table). The generator's results
file states `"no comparison against any other database is made or implied"`.
Reversing this takes an explicit amendment to this ADR.

## What is *not* done (explicit follow-ups)

This ADR and its implementation land the **method and the harness**. They do
not land results.

1. **No numbers are published.** The first curated results run needs a
   dedicated client host and a real multi-host cluster; the development
   container this was built in is colocated and the tool marks its own output
   `publishable: false`. Numbers from such a run must not appear on the
   website, in the README or in a PR description as AnimusDB performance.
   The website says "no results yet" for this reason.
2. **The separate-hosts topology is supported (`--launch external`) but has
   never been run.** The code path exists (`Cluster::external`, the
   `--kill-cmd`/`--restart-cmd` templates) but the smoke test attaches to
   nothing external, so no multi-host run, and no external kill, has been
   exercised end to end. The first real multi-host run is the first test of
   it and may surface defects.
3. **No dedicated runner.** The regression workflow is manual with artifacts
   only (§9).
4. **No TLS client** (§3): a TLS-on result cannot be produced yet.
5. **Encryption at rest and `--shared-wal` are not reported by `/admin`**, so
   an external run records them as unknown (§8). Surfacing both in
   `/admin/config` is a small `animusd` change (the website's performance
   page and this ADR should then be updated together).
6. **The scale-out point and the follower-kill variant** are procedure, not
   automation: the operator runs the tool again against the larger cluster
   and with `--degraded follower`; there is no single command that executes
   the whole publication matrix.
7. **No server-side hardware capture.** The tool records the client host
   only.
8. **Not measured:** cross-partition `Scan`, transactions
   (`TransactWriteItems`/`TransactGetItems`), batch operations, GSI/LSI
   reads and writes, Streams/TTL/backup overhead, TLS and encryption-at-rest
   overhead, multi-tablet fan-out beyond what the byte-driven auto-split
   produces. Each is a possible scenario on the library API (C-17 Tier 2
   builds the scale/density ones); none is claimed.

## Alternatives considered

- **The AWS SDK** — §2.
- **Extend `cluster_bench`** — it is in-process and unauthenticated by
  design; making it a client over the wire is a rewrite, and keeping it as the
  fast developer smoke is useful. It is retained, labelled non-publishable and
  not comparable to this crate's numbers.
- **`criterion`** — a micro-benchmark framework; it measures closed-loop
  iterations and has no open-loop arrival model.
- **An existing YCSB or `wrk2`-style tool** — none speaks SigV4-signed
  DynamoDB JSON with ADR-0055 read modes and a per-phase degraded run; the
  custom surface is small, and the CO method needs to be ours to be
  verifiable.
- **A latency assertion in CI** — §9: a gate that fails on noise gets
  quarantined or widened (forbidden by root `CLAUDE.md` item 4).

## Consequences

- A repeatable, disclosed, coordinated-omission-corrected measurement exists
  and ships with a fake-clock test of the correction itself.
- Honest limits are in the file the reader gets: `publishable`, `tls: false`,
  "unknown: not reported", `notes` for a skipped degraded run, `abandoned`
  and `achieved_rate` for saturation.
- `animus-bench` is a new workspace member and brings one new dependency,
  `hdrhistogram` 7 (default features off; MIT/Apache-2.0) plus its
  `byteorder`/`num-traits` dependencies.
- The generator's results are not a regression signal until the dedicated
  runner exists; the A/B workflow is the interim.
- `website/performance.html` and `docs/benchmarks.md` describe this method and
  say plainly that no results are published.

## Known server-side findings from the harness's first colocated runs

Observations from development runs on a 4-vCPU colocated dev box (generator
and three `animusd` processes sharing it). **They are not results**, and no
figure from such a run is publishable; they are recorded because the harness
surfaced them and they bear on how future numbers should be read.

- **Periodic write-path stalls** (#1196). Under any workload that writes, all
  operations on the tablet freeze together for roughly 200-700 ms every few
  seconds; the stall length grows with table size and its period shrinks with
  write rate, and read-only workloads show none. A plain sequential Python
  HTTP client with no SigV4 against an unauthenticated cluster reproduces it,
  so it is not a generator artifact (service-time and corrected percentiles
  agree, all connections stall in the same instant). Suspected cause, not
  profiled: inline LSM flush/compaction on the apply path
  (`background_maintenance: false`).
- **~21 ms `ConsistentRead: true` floor** (#1197). A linearizable GetItem takes
  about 21-23 ms even at 50 ops/s on an idle cluster, against about 1 ms for
  `ConsistentRead: false`; latencies cluster at multiples of about 21 ms,
  which looks tick-quantised.

Until these are understood, p99 and above of write-bearing workloads and every
`ConsistentRead: true` latency on a published run should be read with them in
mind, and a regression flagged by the A/B workflow in the stall tail may be the
stall's phase rather than a change.

## Testing

- `recorder.rs`: the CO property on a fake-clock FIFO model; the views agree
  without a stall; client-side backlog is charged. `schedule.rs`: intended
  times, no drift. `dist.rs`: zipfian/uniform/latest sanity with loose bounds.
  `workload.rs`: op-mix proportions, seeded reproducibility, key layout.
  `ycsb.rs`: request shapes, error classification.
- `tests/smoke.rs`: a real 3-node in-process cluster with SigV4 on; workloads
  A–F × both read modes and a follower-kill run; asserts wire shapes and
  correctness only, never a rate or latency. A `prod-heavy` test
  (`required-features`): it runs once, in CI's `prod-liveness-scattered` job
  (`--test-threads=1`), not in the per-push `gates` tier, because it is a real
  `ProdEnv` cluster. `compare.rs`, `report.rs` (publishability), `cli.rs`
  (degraded victim parsing, `--drain-secs`) and `scenario.rs` (per-mode table
  names) have pure unit tests.
- Nothing validates absolute performance; that is by design.
