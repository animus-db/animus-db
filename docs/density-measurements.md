# Per-node density measurements (issue #1295)

Measurements for animus-db#1295 (per-node density and background-work
budget). Every number below was measured on one 4-core, 15 GB Linux sandbox
(Intel Xeon @ 2.8 GHz, release build) on 2026-10-12, at main `2b44a69` plus
the two small knob-gated test additions in this change. Each ProdEnv figure is
a **single run** (ProdEnv is not seed-reproducible), so treat them as
order-of-magnitude, not as a benchmark.

Sampling: `/proc/<pid>` of the test binary every 5 s (CPU ticks at 100/s, RSS,
open fds, threads). "Cores" = delta of CPU ticks over the last 60 s of the run
(the hold phase) / 100 / 60.

## Rig (read this first)

`density_repro_g3000_holds_led_count`
(`crates/animus-cp-data/tests/election_backpressure_liveness.rs`, `#[ignore]`d):
G single-tablet RF-3 groups on **three ProdEnv nodes inside one process and one
tokio runtime with `worker_threads = 2`**, real loopback TCP, the production
election-backpressure assembly (governor K=32, load monitor, host stagger),
heartbeat batching, and `MemoryEngine` (no disk). All G groups are hosted at
once, then the led count must hold for 120 s.

Caveats that apply to everything below:

- Three nodes share the process, so per-node cost is about one third of the
  process total (inferred, not measured per node).
- The runtime has **2 worker threads**, so process CPU can never exceed about 2
  cores. A run that sits at ~1.9 cores is saturated, not "using 1.9 cores of
  need": the real demand is higher.
- `MemoryEngine`: no LSM memtables, SSTables, WAL files or compaction. Disk,
  fd and memory cost per tablet of `LsmEngine` is **not covered**.
- There is no client traffic. Awake groups only exchange heartbeats and
  elections; write cost is not measured.

## A. Awake-group ceiling

New knob: `ANIMUS_BP_LIVE_QUIESCE=0` disables quiescence in that ignored test
only (default unchanged).

```sh
cargo test -p animus-cp-data --release --features prod-heavy \
  --test election_backpressure_liveness --no-run
ANIMUS_BP_LIVE_QUIESCE=0 ANIMUS_BP_LIVE_GROUPS=<G> \
  target/release/deps/election_backpressure_liveness-<hash> \
  density_repro_g3000_holds_led_count --ignored --nocapture
```

(Sampled on the test binary directly: sampling `cargo test` measures cargo.)

| G (groups) | replicas | all-led | max_term | min led in 120 s hold | cores (hold) | RSS end | verdict |
|---|---|---|---|---|---|---|---|
| 1000 | 3000 | 3.7 s | 2 | 1000 | 1.55 | 79 MB | pass |
| 1250 | 3750 | 5.3 s | 5 | 1250 | 1.80 | 100 MB | pass |
| 1500 | 4500 | 7.3 s | 3 -> 7 | 1496 | 1.93 | 128 MB | **fail** (led count dipped by 4) |
| 2000 | 6000 | 29.3 s | 16 | 1999 | 1.89 | 292 MB | **fail** (dipped by 1) |
| 3000 | 9000 | never (2651/3000 after the 600 s bound) | 3 | n/a | 1.92 | 604 MB | **fail** (not all led) |

Other runs of the same cells (invalid CPU sampling, but the protocol outcome is
valid): G=1000 all-led 3.1 s, held; G=2000 all-led 57.0 s, max_term 45,
min_led 1999. So G=2000's all-led time varied 29-57 s between two runs; the
failure mode (a short led-count dip) repeated. Logs of the failing runs are
kept only in the session scratchpad; each failure is the test's own
`assert_eq!(min_led, g)` / `host: only N/G groups led` message, there is no
seed (ProdEnv).

Findings:

- The awake ceiling of this rig is **between 1250 and 1500 groups (3750-4500
  replicas) with 2 worker threads**, where CPU saturates (1.8 -> 1.93 cores)
  and elections start to churn (max_term, led-count dips).
- Cost: 1.55 cores for 3000 awake replicas and 1.80 for 3750 =
  **about 0.48-0.52 cores per 1000 awake replicas**, process-wide. The
  third-share inference: each node uses about 0.5 core to keep 1000 of its own
  replicas awake (= about 1900-2000 awake replicas per node-core), idle of any
  client traffic.
- Memory awake is no larger than quiesced up to 2000 groups (see B), but grows
  once the rig falls behind (604 MB at G=3000, queued work).
- Awake vs quiesced at G=3000: ~1.9 cores (and never converging) vs 0.10
  cores. Quiescence is the density mechanism; the awake ceiling is the number
  that bounds the *simultaneously active* working set.

## B. Quiesced cost per hosted replica

Same command without `ANIMUS_BP_LIVE_QUIESCE=0` (production default, quiesce
after 5 s). Engine: `MemoryEngine` per tablet.

| G | replicas | all-led | max_term | cores (hold, all quiesced) | RSS end | fds end |
|---|---|---|---|---|---|---|
| 1000 | 3000 | 3.1 s | 2 | 0.04 | 79 MB | 24 |
| 2000 | 6000 | 8.2 s | 3 | 0.07 | 146 MB | 24 |
| 3000 | 9000 | 14.8 s | 4 | 0.10 | 216 MB | 25 |

(G=3000 is the earlier run, all 9000 replicas quiesced in the hold phase; 5907
governor deferrals, hwm 32.) All three held the led count for 120 s.

- RSS slope: (216 - 79) MB / 6000 replicas = **about 23 KB per hosted
  quiesced replica** (MemoryEngine, empty tablets); base about 10 MB.
  Extrapolation to 100k replicas per node-equivalent is about 2.3 GB: linear
  by the three points, not measured beyond 9000.
- fds: flat at 24 (transient peak 143-147 during hosting from connection
  setup). MemoryEngine opens no files, so **per-tablet fds of `LsmEngine` are
  not measured here**.
- Idle CPU: 0.04 -> 0.10 cores for 3000 -> 9000 replicas, about
  0.011 core per 1000 quiesced replicas (process-wide), i.e. roughly linear and
  small.

## C. Node-loss re-replication burst (sim, not ProdEnv)

New `#[ignore]`d test `sim_cluster_scale_node_loss_repair_burst`
(`crates/animusd/src/sim_cluster_scale.rs`, ~70 lines, structural assert only
that the dead node held a replica): 6-node, RF-3 `SimCluster`, G single-tablet
tables, settle 60 virtual s, crash node 5, run until every tablet has three
replicas none on node 5.

```sh
ANIMUS_SCALE_REPAIR_TABLES=300 cargo test -p animusd --lib --release \
  sim_cluster_scale_node_loss_repair_burst -- --ignored --nocapture
```

| tables | tablets on dead node | converge (virtual) | peak proposals/s | reconfigure accepted / rejected | snapshot ships / installs / triggers |
|---|---|---|---|---|---|
| 60 | 6 | 7 s | 7 | 11 / 1 | 0 / 0 / 0 |
| 300 | 12 | 7 s | 25 | 28 / 0 | 0 / 0 / 0 |

(Seed `893798268308609673` = `name_seed`, deterministic.)

Caveats, and what this does **not** show: the placement of
`create_table_with_replication` leaves only 4-10 % of tablets on node 5, so the
burst is tiny; the tablets are empty so no snapshot is ever needed (all
snapshot counters are 0); the sim has no CPU model, so convergence time is not
a capacity number. Roughly 2.3 accepted reconfigurations per affected tablet
(add learner, promote, drop). A harness that measures the real burst needs:
tablets pre-loaded with data above the snapshot threshold, a placement that
spreads tablets evenly (or a larger node count), the in-flight learner and
snapshot-stream counts sampled each virtual second (they are not exposed as
gauges today, only the cumulative `CpSnapshot*`/`CpReconfigure*` counters), and
a ProdEnv or CPU-modelled run (`Simulator::set_cpu_cost`) for CPU and disk cost.

## D. Auto-split storm (sim, existing cell)

No code change: `sim_cluster_scale_split_storm_corpus` already runs concurrent
in-place splits under a node restart and a control-leader transfer.

```sh
ANIMUS_SCALE_MAX_TABLETS=10000 cargo test -p animusd --lib --release \
  sim_cluster_scale_split_storm_corpus -- --nocapture
```

| cell | splits completed | final tablets | control proposals | virtual s | control msgs/s | tablet msgs/s |
|---|---|---|---|---|---|---|
| 3 nodes, 4 tables | 8 | 12 | 17 | 182 | 170 | 78 |
| 5 nodes, 10 tables | 17 | 27 | 76 | 302 | 411 | 356 |

Caveat: sim virtual time, 4-10 tablets, a ~2 KB split threshold on 16 small
writes per table. It proves splits complete and acked writes survive
concurrency; it does not give a safe concurrent-split count under real load.
Missing: a CPU-modelled or ProdEnv run with realistic tablet sizes (split cost
is dominated by the fork's data movement and the follow-up directed Placing).

## Implications for #1295

Suggested starting values, each marked.

| Quantity | Starting value | Basis |
|---|---|---|
| Awake replicas per core | about 1500 (conservative; rig measured about 1900-2000 per node-core with no client load, up to saturation at 3750-4500 total) | measured (rig) / inferred (per node, 1/3 split, no client I/O, so halve it for a loaded node) |
| Hosted-replica capacity (quiesced) | memory: about 25 KB per replica for the Raft/host layer, before engine cost; idle CPU about 0.01 core per 1000 | measured up to 9000 replicas with MemoryEngine; beyond that and LsmEngine inferred |
| Hosted capacity cap | choose from the LsmEngine per-tablet fd and memtable cost, not measured here | not measured |
| Campaign concurrency K | keep 32; it was at its high-water mark in every run and 29-21034 deferrals bounded the churn | measured |
| Snapshot concurrency | no data (0 snapshots in C); start low (2 per node) | inferred |
| Concurrent splits | no real-load data; start at 1 per node and raise from a ProdEnv storm run | inferred |
| Compaction concurrency | not covered (MemoryEngine) | not measured |
| Admission signal | led-count dips and max_term growth appeared first (G=1500), CPU saturation just before; a lag-based back-off (the existing load monitor) is the right trigger | measured |

Follow-ups this implies: an LsmEngine variant of the rig (fds, memory,
compaction), a loaded variant with client writes, a larger `worker_threads` to
find the true per-core figure, and a data-bearing node-loss cell.
