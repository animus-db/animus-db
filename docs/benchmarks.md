# Benchmarks: methodology and how to reproduce a run

The design record is [ADR 0076](adr/0076-published-benchmarks.md); the
tool's own guide is [`crates/animus-bench/CLAUDE.md`](../crates/animus-bench/CLAUDE.md).
This page is the procedure.

**Status, plainly: there are no published results.** The method and the load
generator exist; the first curated run on dedicated, multi-host hardware has
not been done. Numbers produced on a development box (the generator and the
servers on one host) are labelled `"publishable": false` by the tool itself
and must not be quoted as AnimusDB performance. See
[What is not done](#what-is-not-done).

## What the tool is

`animus-bench` (`crates/animus-bench`) is a load generator that drives an
AnimusDB cluster **as a client over the real DynamoDB wire**: HTTP/1.1 JSON
with `X-Amz-Target`, SigV4-signed with `animus_dynamo::sigv4::sign`. It runs
YCSB workloads A–F mapped onto DynamoDB operations, with **open-loop
arrivals** and **coordinated-omission-corrected latency**, and writes a
results file (`"schema": "animus-bench/v1"`) that records the topology,
environment and methodology next to the numbers.

It is not `cargo bench -p animusd` (`crates/animusd/benches/cluster_bench.rs`):
that is an in-process, unauthenticated, closed-loop developer smoke. Its
numbers are **not comparable** to this tool's and are not publishable.

## Build

```sh
cargo build --release -p animus-bench -p animusd
cargo run --release -p animus-bench -- --help      # the flag reference
```

`--launch processes` looks for an `animusd` binary next to the `animus-bench`
binary (so build both in the same profile), else take `--animusd-bin PATH`.
The tool does not run on the cluster hosts for a publishable result; build it
on, or copy it to, the **client host**.

## Topologies

| Mode | Flag | Servers | Publishable? |
|---|---|---|---|
| External | `--nodes D@A,D@A,...` | A cluster you started; `D` is each node's DynamoDB address, `A` its admin address | **Yes, if the client host is a different host** |
| Processes | `--launch processes` | `animusd` children spawned on this host | No — colocated |
| In-process | `--launch in-process` | `animusd::Node` inside this process | No — colocated; the "kill" is a graceful shutdown, not a crash |

`processes` and `in-process` are for development, smoke runs and CI. For both,
`publishable` is `false` with a `publishable_reason`. An external cluster whose
every endpoint is a loopback address is also marked colocated.

### The publishable shape

1. Provision **separate hosts**: a client host running `animus-bench`, and the
   cluster nodes. Name the instance type (or machine model), disk model and
   network for both, and the failure domain of each node. The tool records
   only the **client** host's `/proc` facts (hostname, kernel, CPU model and
   count, memory); the servers' hardware is yours to state next to the file.
2. Start a **three-node cluster at RF 3** as the baseline. Run a second
   measurement against a larger cluster (the **scale-out point**) with the
   same workload.
3. TLS is optional. To benchmark a cluster running server-only TLS on the
   `dynamo` and `admin` ports (ADR 0064), pass `--tls-ca PATH` (the CA that
   signed the nodes' certificates; add `--tls-server-name NAME` if the
   certificates carry a DNS SAN rather than an IP SAN, since endpoints are
   addresses). The client presents no certificate. The results file records
   `tls: true`. The TLS handshake happens when a connection is set up, before
   each phase starts (the connection pool is pre-dialled), never inside a
   measured operation; only a redial after a broken connection pays it, as a
   TCP connect always did. State TLS-on or TLS-off next to any number you
   publish: they are different measurements.
4. Configure the cluster with SigV4 (`--dynamo-auth`; ADR 0057/0066) and pass
   the same credentials with `--access-key/--secret-key` (or
   `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`). `--no-auth` skips signing;
   an external cluster with no credentials given is not signed.
5. For the degraded run, give the tool a way to kill (and restart) a node:
   `--kill-cmd` and `--restart-cmd`, `sh -c` templates with `{node}` (the
   index in the `--nodes` list), `{host}`, `{dynamo}` and `{admin}`
   substituted, e.g. `--kill-cmd 'ssh {host} sudo systemctl kill -s KILL animusd'`.
   With no `--kill-cmd` on an external cluster the degraded run is **skipped**
   and a `notes` entry says so; a publishable result must include it.
6. State encryption at rest and `--shared-wal` yourself. `/admin` does not
   report them, so an external run records `"unknown: not reported by
   /admin"`.

> The external, multi-host path (`--nodes`, `--kill-cmd`, `--restart-cmd`) has
> **not yet been run end to end**; the smoke test uses an in-process cluster.
> Expect to find defects in it on the first real run.

### Example (external)

```sh
animus-bench --nodes 10.0.1.11:8000@10.0.1.11:9000,10.0.1.12:8000@10.0.1.12:9000,10.0.1.13:8000@10.0.1.13:9000 \
  --access-key "$KEY" --secret-key "$SECRET" \
  --workloads A,B,C,D,E,F --consistent-read both \
  --records 1000000 --value-bytes 256 --distribution zipfian \
  --rate 5000 --warmup-secs 60 --steady-secs 120 --connections 128 \
  --degraded leader --kill-cmd 'ssh {host} sudo systemctl kill -s KILL animusd' \
  --restart-cmd 'ssh {host} sudo systemctl start animusd' \
  --seed 42 --out run-3node.json
```

(The addresses and ports above are placeholders.) To find the knee instead of
measuring one rate, pass `--sweep 1000,2000,4000,8000,...`.

### Example (development smoke, not publishable)

```sh
cargo build --release -p animus-bench -p animusd
target/release/animus-bench --launch processes --workloads A --records 2000 \
  --rate 200 --warmup-secs 3 --steady-secs 10 \
  --baseline-secs 5 --degraded-secs 8 --recovery-secs 8
```

## Flags that matter

From `animus-bench --help`. Defaults in parentheses.

**Cluster (exactly one of):** `--nodes D@A[,D@A...]` attach; `--launch
processes|in-process` (+ `--cluster-size N` (3), `--animusd-bin PATH`,
`--animusd-arg ARG` repeatable for extra `animusd` flags such as
`--animusd-arg --no-shared-wal`, `--data-dir DIR`).

**TLS:** `--tls-ca PATH` [`--tls-server-name NAME`] for `--nodes`; `--tls-ca
PATH --tls-cert PATH --tls-key PATH` for `--launch`.
**Auth:** `--access-key`/`--secret-key` (or the `AWS_*` env vars); `--no-auth`.
A launched cluster signs with a built-in default credential unless `--no-auth`.

**Workload:** `--workloads A,B,..|all` (A); `--records N` (10000, per table);
`--value-bytes N` (256); `--distribution zipfian|uniform` (zipfian; D always
reads latest); `--max-scan-len N` (100, workload E); `--consistent-read
true|false|both` (both); `--seed N` (42).

**Load shape:** `--rate R` ops/s (1000); `--sweep R1,R2,...` (replaces the
single steady phase); `--warmup-secs` (10, discarded); `--steady-secs` (30,
each sweep point); `--connections N` (64, client concurrency cap);
`--op-timeout-secs` (10); `--drain-secs` (30, grace after each phase for queued
ops to start; unstarted ops are `abandoned`).

**Degraded run:** `--degraded none|leader|follower|node:N` (leader when a kill
mechanism exists, else none). `leader` kills the node leading the table's first
tablet, `follower` a node hosting a non-leader replica of it, `node:N` node
index N whatever it hosts (it may be the leader or hold no replica); a bare
`node` is rejected; `--degraded-workload W` (first of
`--workloads`); `--degraded-consistent-read true|false` (true);
`--baseline-secs/--degraded-secs/--recovery-secs` (15/20/20); `--kill-cmd`,
`--restart-cmd`, `--no-restart`.

**Output:** `--out FILE` (`./animus-bench-<epoch>.json`); `--table-prefix P`;
`--keep-tables` (otherwise tables are dropped at the end).

`ANIMUS_BENCH_GIT_SHA` overrides the recorded git SHA (useful when running
from a copied binary outside the checkout).

## What a run does

Per workload, on its own freshly created table:

1. **Create and load.** `CreateTable` (`pk` S HASH, `sk` N RANGE, no
   provisioned throughput, so it starts as one tablet), wait for `ACTIVE`,
   then bulk-load `--records` with `BatchWriteItem` (25 per call). The load is
   reported but is **not** a latency measurement. The table's tablet count,
   replica placement and leaders are captured into
   `params.table_topology_after_load`.
2. **Per `ConsistentRead` mode** (`true`, then `false`, as separate results
   with the same seed): **warm-up** (discarded: only counts are kept), then
   **steady** (or one **sweep** phase per rate). Both modes run on the same
   loaded table, one after the other, so for workloads that write (A, B, D,
   E, F) the second mode runs on a table the first one already modified and
   warmed. A write-only comparison between the two modes is not what this
   measures; the read-latency comparison is.
3. Drop the table (unless `--keep-tables`).

After all workloads, the **degraded run** (if there is a kill mechanism and
`--degraded` is not `none`), once, on a fresh table: warm-up, **baseline**
(healthy; the comparison), **degraded** (the fault fires at the start of this
phase), **recovery** (the killed node is restarted at its start where the
cluster supports it). The victim: the tablet's leader (`/admin/status` finds
the table's first tablet; each node's `/admin/raftkv` `is_leader` finds the
node), a follower, or `node:N`.

### Workload mapping

| W | Mix | Operations |
|---|---|---|
| A | 50 read / 50 update | `GetItem` / `UpdateItem SET data` |
| B | 95 read / 5 update | as A |
| C | 100 read | `GetItem` |
| D | 95 read-latest / 5 insert | `GetItem` skewed to newest keys / `PutItem` of new sequential keys |
| E | 95 scan / 5 insert | `Query pk=:p AND sk>=:s Limit=len` / `PutItem` |
| F | 50 read / 50 read-modify-write | `GetItem` / `GetItem` + conditional `UpdateItem` |

- Record `i`: `pk = "user" + zero-padded-10(i/100)`, `sk = i%100`; item
  `{pk, sk, version: N, data: S}`, `data` exactly `--value-bytes` long.
- **E is a `Query` inside one 100-record partition**, length uniform in
  `1..=--max-scan-len`. A DynamoDB `Query` cannot cross partition keys, so
  this is **not** YCSB's cross-key scan. Do not describe E results as "scan
  throughput" in the YCSB sense.
- **F** counts as one operation spanning two calls (latency covers both). The
  update is conditional on the version just read; a lost race is not retried
  and is not an error: it is `condition_failed`, recorded as completed.
- Zipfian is YCSB's scrambled zipfian, theta 0.99.
- Reads that find nothing are `empty_reads` (legitimate under
  `ConsistentRead: false`).

## The results file (`animus-bench/v1`)

One JSON document (`--out`); `animus-bench` also prints a text summary.
Top level:

| Field | Meaning |
|---|---|
| `schema` | `"animus-bench/v1"`; bumped on a breaking change |
| `generated_at_epoch_secs`, `tool_version` | when, and the crate version |
| `git_sha`, `git_dirty` | the checkout's HEAD and whether it had changes; `ANIMUS_BENCH_GIT_SHA` overrides; `"unknown"` if neither is available |
| `args` | the exact command line |
| `seed` | seeds the op/key stream |
| `publishable`, `publishable_reason` | `true` only if the servers are off the generator's host (not bench-launched, not all-loopback) **and** a degraded run whose fault was actually injected is in the report; otherwise `false` with every unmet condition listed. Necessary, not sufficient; see below |
| `environment` | `client_host` (hostname, kernel, cpu_model, cpu_count, memory_total_kb), `client_and_server_colocated`, `launch_mode` (`external`/`processes`/`in-process`), `target_endpoints`, `node_count`, `sigv4`, `tls` (`true` when the DynamoDB and admin ports were dialled over TLS) and `tls_note` (mode, server name verified, when the handshake happens) |
| `methodology` | fixed disclosures: load model, latency definition, histogram, error handling, phases, retries (none), comparison (none) |
| `topology_start`, `topology_end` | what `/admin` reports before and after: node count, per-node `auth_enabled`/`quiesce_after_ms`/auto-split and throttle thresholds, membership, `encryption_at_rest`, `shared_wal`, `tls` |
| `notes` | anything skipped or not possible, e.g. no degraded run |
| `runs[]` | one per workload × read mode, plus the degraded run |

Each `runs[]` entry: `name` (`ycsb-A/consistent_read=true`,
`ycsb-A/degraded/consistent_read=true`), `params` (workload, mix, table,
record count, value size, distribution, scan length, `consistent_read`, seed,
arrival rate, sweep rates, warm-up/steady seconds, connections, op timeout,
`drain_secs`, `table_state`,
`key_layout`, `table_topology_after_load` with tablet count, RF policy,
replica placement and leaders; a degraded run adds `degraded` with the
victim, phase lengths and whether the node was restarted), `load` (records,
value_bytes, elapsed, records/s, retried batches), `phases[]`, and `sweep[]`.

Each phase (`warmup`, `steady`, `sweep@RATE`, `baseline`, `degraded`,
`recovery`): `discarded`, `target_rate`, `achieved_rate`, `dispatched`,
`completed`, `errors` (`throttled`, `timeout`, `connection`, `other`),
`condition_failed`, `empty_reads`, `abandoned`, a few `error_samples`, `fault`
(the action, node, offset into the phase, outcome and detail), and latency
summaries `overall` and `classes[<read|update|insert|scan|read_modify_write>]`.
A latency summary has `corrected` and `service`, each `count, min_us, mean_us,
p50_us, p90_us, p99_us, p99_9_us, p99_99_us, max_us`. A warm-up phase has
counts only. `sweep[]` rows (`target_rate`, `achieved_rate`, `completed`,
`errors`, `abandoned`, `p50/p99/p99.9/p99.99/max`, and `service_p99_us`) are
the knee table.

`serde_json` float round-tripping is not bit-exact; compare integers and
strings, not whole documents.

### Reading corrected versus service-time percentiles

- **`corrected`** (the headline) is measured from each operation's *intended*
  send time, `start + i/rate`. If the server stalls, or the client's own queue
  backs up, every arrival scheduled in the stall is charged the wait it would
  have had.
- **`service`** is measured from the actual send: what a closed-loop client
  would report. It is there so you can see how much the correction matters.
  When there is no stall the two agree. When they diverge, the system (or the
  `--connections` pool) was stalled or saturated, and the corrected figure is
  the honest one.
- Histograms are HdrHistogram, microseconds, 3 significant figures, up to 1 h.
- `achieved_rate` below `target_rate`, or any `abandoned` ops (queued and not
  started within the `--drain-secs` grace, default 30 s, after the phase), means the offered rate is
  past what was sustainable: that point is beyond the knee, and its latencies
  describe a queue.
- Errors are **counted, not timed**; a run with timeouts and a clean latency
  histogram is not a clean run. Read the `errors` before the percentiles.
- The generator never retries.
- Timer granularity is about 1 ms, so at high rates arrivals are sent in small
  bursts; intended times are unaffected.

### What `publishable` does and does not mean

`publishable` is `true` only when the servers are not on the generator's host
(the bench did not launch them and the endpoints are not all loopback) **and**
the report contains a degraded run whose fault was successfully injected;
`publishable_reason` lists every unmet condition. It does **not** check that
there are three nodes, that hardware is named, or what TLS or encryption is
configured. It is a necessary condition, not a certificate. The rest is the publisher's checklist (ADR 0076
§8): named instance types, disk and network for both sides, replica
placement, the leader-kill and follower-kill variants, a scale-out point, the
raw JSON committed or attached to a release, and encryption/`--shared-wal`
stated by hand.

## Regression tracking (A/B)

The manual `workflow_dispatch` workflow `.github/workflows/bench.yml` (no
schedule, no push/PR trigger) builds the generator once from the dispatched
ref, builds `animusd` for an optional `base_ref` and for the dispatched ref in
the same job, runs them **interleaved** (base, head, base, head, ...) on one
runner, uploads the raw JSON and text summaries as artifacts and appends the
comparison table to the job summary. Inputs: `runner` (runs-on label, default
`ubuntu-latest`), `base_ref` (empty = measure the dispatched ref only),
`workloads`, `rates` (one value = steady, a comma list = sweep), `records`,
`value_bytes`, `warmup_secs`, `steady_secs`, `consistent_read`
(both/true/false), `degraded` (none/leader/follower), `repeats` (interleaved
pairs, default 2) and `threshold_pct` (default 10).

The table comes from `animus-bench compare [--threshold-pct P] [--out F]
--base A1.json A2.json --head B1.json B2.json`: per run, phase and metric
(corrected p50/p99/p99.9 and achieved rate; sweep points too) the median, the
spread across repeats (`(max-min)/median`) and the median delta. **Flagging
rule:** a row is flagged `REGRESSION`/`improvement` only when `|delta|` exceeds
the threshold **and** the base and head [min, max] ranges are disjoint; a
delta beyond the threshold with overlapping ranges, or from a group with a
single run, is shown as `noisy`. It is reporting only: the command exits 0 for
any well-formed input and no latency number can fail the job. Its rules, from
ADR 0076 §9:

- Compare only runs made together on one host. A figure from another host or
  another day is not a baseline.
- Run-to-run spread is reported; a change is flagged only beyond a disclosed
  threshold; a single outlier is a rerun, not a verdict.
- It is advisory. A benchmark is never a latency or throughput assertion in
  a test or required check, and `crates/animus-bench/tests/smoke.rs` asserts
  only wire shapes and correctness. The smoke is a `prod-heavy` test and runs
  once, in CI's `prod-liveness-scattered` job, not in the per-push `gates`
  tier.
- A GitHub-hosted runner is shared and noisy: its output validates that the
  harness runs, not what the server's absolute performance is. A trend line
  needs a dedicated, fixed runner, which does not exist yet.

To run an A/B by hand: build both refs, run the same `animus-bench` command
against each (alternating, several times), then `animus-bench compare` the
files; read the spread between repeats before the difference between sides.

## Comparing with another system

Not done, and not planned. The website commits to **no comparison charts
against DynamoDB** and none between a managed service and self-hosted
hardware. Were a comparison ever wanted, ADR 0076 §10 sets the rules: like for
like (instance types and counts, item and key shape, fsync-acked durability,
matched read consistency and replication factor), the other system's
configuration published in full, versions pinned, and the other system driven
by this same open-loop generator.

## Caveats

- Colocated results (`processes`, `in-process`, loopback) measure the box.
- The default table is a single tablet unless byte-driven auto-split fires
  during the run (a byte threshold set on `animusd` itself; check which
  flags your `animusd` mode accepts). Check `topology_end` against `params.table_topology_after_load`
  before calling a result "one tablet" or "multi-tablet".
- Workload E is a within-partition `Query`, not a cross-partition scan.
- Workload F's conditional updates lose races at high skew; `condition_failed`
  is not an error but is not free either.
- Reads right after a load under `ConsistentRead: false` may miss records on a
  lagging replica; they are counted in `empty_reads`.
- `--launch in-process`'s degraded run is a graceful shutdown, not a crash,
  and does not restart the node.
- A killed node costs each worker at most one failed op, as a `connection`
  error in the `degraded` phase.
- Tables are named `ycsb<epoch>_<workload>`; a crashed run can leave tables
  behind.

## What is not done

- **No published results.** The first curated run needs a dedicated client
  host and a real multi-host cluster. Until then the website says "no results
  yet".
- **The multi-host external topology has never been run** (only
  in-process/processes clusters have).
- **TLS is server-only.** Mutual TLS on the DynamoDB port is not supported (the
  server does not ask for a client certificate there). `--launch
  processes|in-process` serve TLS only with `--tls-cert/--tls-key/--tls-ca`
  (one shared leaf certificate, SAN `127.0.0.1`); like every launched
  cluster these are colocated and non-publishable.
- **Encryption at rest and `--shared-wal`** are not reported by `/admin`, so an
  external run records them as unknown.
- **No server-side hardware capture**, and no single command that runs the whole
  publication matrix (3-node baseline, scale-out point, leader and follower
  kill).
- **No dedicated regression runner**; the A/B workflow is manual.
- **Not measured at all:** cross-partition `Scan`, transactions, batch
  operations, secondary indexes, Streams/TTL/backup overhead and
  encryption-at-rest overhead (TLS-on is now measurable, but no TLS-on versus
  TLS-off comparison has been published). The library API (`animus_bench::{engine,
  scenario, report, cluster}`) is the extension point; C-17 builds scale
  scenarios on it.
