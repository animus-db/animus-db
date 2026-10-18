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

---

# 2026-10-13: ADR 0079 step 1 calibration

Same box (4-core, 15 GB, release build), main `50e1edc` plus the test knobs of
this change. Every ProdEnv row is a **single run** (not seed-reproducible):
order of magnitude, and a row at a boundary (a one-group led dip, `max_term`
in the 4-10 range) can flip on a repeat. G=1500 at 2 workers passed here with
a 60 s hold and failed in the 2026-10-12 run with a 120 s hold. The sim rows
(section F) are seeded and deterministic but have no CPU model. "Measured" =
printed by the run; "inferred" = arithmetic on measured rows, labelled so.

The ignored test samples itself (`/proc/self`, so the lesson of
`docs/lessons/testing/2026-10-12-sample-the-test-binary-not-cargo-test.md` cannot
recur). Knobs, all default to the old behaviour (`election_backpressure_liveness.rs`):
`ANIMUS_BP_LIVE_WORKERS` (runtime worker threads, 2), `_ENGINE=lsm`
(`LsmEngine<ProdEnv>`, real files), `_WRITE_RPS`, `_WRITE_GROUPS`, `_VALUE_BYTES`
(client puts at a fixed rate, round-robin over the first N groups), `_HOLD_SECS`
(120), `_CONVERGE_SECS` (600), `_ASSERT=0` (print a `verdict` line instead of
failing). "cores" = CPU ticks over the hold phase / hold seconds; "hold" below is
60 s unless stated. Logs: session scratchpad only.

```sh
cargo test -p animus-cp-data --release --features prod-heavy --test election_backpressure_liveness --no-run
ANIMUS_BP_LIVE_ASSERT=0 ANIMUS_BP_LIVE_QUIESCE=0 ANIMUS_BP_LIVE_WORKERS=1 ANIMUS_BP_LIVE_GROUPS=1100 \
  ANIMUS_BP_LIVE_HOLD_SECS=60 target/release/deps/election_backpressure_liveness-<hash> \
  density_repro_g3000_holds_led_count --ignored --nocapture
```

## E1. Awake ceiling vs worker threads (MemoryEngine, no client load, `QUIESCE=0`)

| workers | G | cores (hold) | max_term | deferrals | verdict |
|---|---|---|---|---|---|
| 1 | 500 | 0.51 | 2 | 12 | held |
| 1 | 700 | 0.71 | 2 | 0 | held |
| 1 | 900 | 0.86 | 3 | 541 | held |
| 1 | 1100 | 0.98 | 3 | 1820 | held |
| 1 | 1300 | 1.02 | 4 | 1347 | held (saturated) |
| 1 | 1500 | 1.02 | 9 | 4228 | held, churning |
| 1 | 1800 | 1.03 | 22 | 8865 | held, churning |
| 1 | 2200 | 1.04 | 70 | 44207 | **fail** (not all led in 120 s; dip of 4) |
| 2 | 1250 | 1.50 | 2 | 139 | held |
| 2 | 1500 | 1.75 | 3 | 279 | held (fail in the 2026-10-12 120 s run) |
| 3 | 1500 | 2.08 | 2 | 0 | held |
| 3 | 1900 | 2.53 | 3 | 32 | held |
| 3 | 2300 | 2.76 | 4 | 2747 | held |
| 3 | 2700 | 2.77 | 9 | 6496 | held, churning |
| 3 | 3000 | 2.75 | 47 | 55223 | held, heavy churn |
| 3 | 3400 | 2.75 | 8 | 170910 | **fail** (min led 3252) |

Findings (measured unless marked):

- **The per-core cost is lowest with one worker.** Below saturation: 0.9-1.0
  process-core per 1000 groups at 1 worker, 1.2 at 2, 1.2-1.4 at 3 (cross-thread
  contention). The 2026-10-12 inference of 0.5 core per 1000 own replicas
  (1900-2000 per node-core) was a 2-worker number; at 1 worker it is about
  0.30-0.33.
- **Clean ceiling** (all led, led count held, `max_term` single digit, no
  deferral storm): about 1100-1300 groups at 1 worker (1.0 core), 1500 at 2 (1.75
  cores), 2300-2700 at 3 (2.76 cores). Past it the process pins at its worker
  count, `max_term` and deferrals climb steeply, and the verdict degrades from
  "held, churning" to a led-count dip, to non-convergence.
- **Inferred per node-core** (three nodes share the process, each hosts all G
  replicas, so one core carries 3 x G replicas): about **3300-3900 awake
  replicas per core** at the 1-worker clean ceiling, about 2600-2700 at 2-3
  workers. No client load. The ADR's provisional `A` = 1500 is 40-60 % of that.

## E2. `LsmEngine<ProdEnv>` per tablet (real files, 2 workers)

| mode | G | replicas | all-led | host CPU | cores (hold) | RSS | fds | files / sstables | notes |
|---|---|---|---|---|---|---|---|---|---|
| quiesced | 300 | 900 | 5.4 s | 5.6 core-s | 0.04 | 35 MB | 25 | 3600 / 900 | |
| quiesced | 1000 | 3000 | **54 s** | 58 core-s | 0.06 | 95 MB | 25 | 12000 / 3000 | |
| quiesced | 2000 | 6000 | **not in 180 s** (1400 led at 180 s) | 184 core-s | 1.19 (still hosting) | 148 MB | 25 | 16800 / 4200 | stopped |
| awake | 300 | 900 | 7.2 s | 8 core-s | 0.42 | 36 MB | 25 | 3600 / 900 | |
| awake | 700 | 2100 | 24.8 s | 35 core-s | 0.91 | 73 MB | 25 | 8400 / 2100 | |
| awake | 1100 | 3300 | 66 s | 108 core-s | 1.41 | 112 MB | 25 | 13200 / 3300 | |

- **Steady state is close to MemoryEngine**: awake 1100 groups costs 1.41 cores
  (MemoryEngine at 2 workers: about 1.3 interpolated), quiesced 0.04-0.06 core,
  about 28 KB RSS per hosted replica (95 MB / 3000; MemoryEngine 23 KB). Each
  replica holds 4 files, one of them an sstable (the layout-marker flush), so
  **about 4 inodes per hosted replica** (12000 files at 3000 replicas). **fds are flat at 25 idle**: engine files are
  opened per operation, not held.
- **Hosting cost is superlinear, and that looks like a product defect, not
  overload.** Host CPU per replica is 6 ms at G=300, 19 ms at 1000, at least 30 ms
  at 2000 (184 core-s over at most 6000 replicas), so total hosting CPU is roughly quadratic in the tablets already on
  the node. Cause (read, not profiled): the reconciler calls
  `EngineFactory::probe` for every tablet it hosts (`host.rs`, around line 2228),
  and `LsmTabletFactory::probe` (and `local_tablets`, `destroy`) does a full
  `env.list()` of the node's flat data directory, which `ProdEnv::list`
  implements as `read_dir` plus one `file_type().await` (a blocking-pool hop)
  per entry. At 12000 entries that is 12000 hops per probe. The test factory
  mirrors the product's. Per-push quiesced density through `LsmEngine` is
  therefore bounded by hosting time (G=2000 did not converge in 180 s with
  idle machinery), not by steady-state cost. **Not fixed here**; to be filed and
  handled separately (a per-tablet probe by a prefix-targeted stat, or one
  listing per reconciler tick). Until then the hosting-time-bound
  `max_replicas` below is the safe figure.
- Awake-vs-quiesced per replica matches E1 (no new steady-state cost class).

## E3. Loaded variant: client writes at a fixed rate (MemoryEngine, 2 workers, 128 B values)

Writes are raw tablet puts through the leader (`RaftKvNode::put`), no DynamoDB
wire, `Accepted` means appended on the leader. Rate is requested; the sender is
a single task in the polling loop.

| mode | G | requested puts/s | writes sent / accepted | cores | RSS | fds | min led | verdict |
|---|---|---|---|---|---|---|---|---|
| awake | 1000 | 0 | 0 | 1.25 | 81 MB | 25 | 1000 | held |
| awake | 1000 | 1000 | 59899 / 59899 | 2.05 | 555 MB | 1154 | 1000 | held |
| awake | 1000 | 3000 | 179684 / 179684 | 2.15 | 1.22 GB | 1602 | 1000 | held (saturated) |
| awake | 1000 | 6000 | 359643 / 359298 | 2.12 | 1.55 GB | 1746 | 999 | **dip of 1** |
| awake | 1500 | 1000 | 59978 / 59963 | 2.02 | 802 MB | 1575 | 1499 | **dip of 1** (1.75 cores unloaded, held) |
| awake | 1500 | 3000 | 179853 / 179630 | 2.16 | 1.18 GB | 2199 | 1498 | **dip of 2** |
| quiesced, 100 hot groups | 2000 | 1000 | 59946 / 59946 | 1.21 | 292 MB | 25 | 2000 | held |
| quiesced, writes over all groups | 1000 | 500 | 29972 / 29972 | 1.74 | 195 MB | 25 | 1000 | held |
| LSM 4 KiB, awake, 50 hot groups | 700 | 600 | 35987 / 35961 | 2.11 (0.91 unloaded) | 2.67 GB | 298 | 693 | **dip of 7**, 10951 deferrals |

- **CPU per write** (inferred from the 1000/s rows, a whole RF-3 write: leader
  append, two follower appends, three applies, the network): about **0.8-1.0 ms**
  of process CPU with MemoryEngine and 128 B values, about **2 ms** with
  `LsmEngine` and 4 KiB. At 2 workers (about 2 cores) that is a pinned ceiling
  of roughly 2000-2500 RF-3 writes/s in this rig before the awake groups starve.
- **Load moves the clean awake ceiling.** Unloaded 1500 groups held at 1.75 cores;
  with 1000 puts/s it pinned at 2.0 and lost a led group. So at 2 workers the
  loaded clean ceiling is below 1500 groups at 1000 puts/s (inferred: about
  1000-1250). Load costs about the same core fraction as 800-1000 awake
  replicas per 1000 puts/s.
- **Writes keep a quiesced group awake**: 500 puts/s spread over 1000 groups
  (one write per group every 2 s, under the 5 s quiesce timer) costs 1.74 cores,
  about the same as 1000 awake groups without writes. A hot set of 100 groups at
  1000 puts/s on 2000 groups costs 1.21 cores, of which about 0.1 is the idle
  floor. The quiesce saving needs the *write working set*, not just the reads,
  to be small.
- **fds and memory under write load are per written replica, not per hosted
  one**: a per-replica raft WAL file stays open once written (about 0.35-0.5 fd
  per hosted replica at 1000 and 1500 groups, 25 idle). A 1024 soft `nofile`
  limit would be exceeded at about 2000 written replicas (inferred);
  `LimitNOFILE` must follow `max_replicas`.
- **LSM write amplification** (single run): 35961 x 4 KiB x 3 replicas = 430 MB
  logical became 1.5 GB on disk in 60 s (about 3.5x; WAL segments and sstables,
  sstable count 2447-2571 then falling to 2364 from a 2100 baseline, so compaction
  ran and kept up, inferred). **The RSS growth (71 MB to 2.7 GB in 60 s) was
  explained on 2026-10-14 (issue #1304, section H): it was not the engine.**

## F. Data-bearing node-loss burst (sim, `sim_cluster_scale_node_loss_repair_burst`)

6 nodes, RF 3, tables placed round-robin (every node holds half the tablets;
the old fixture put every tablet on nodes 0-2), each preloaded with `puts`
writes (more than the 64-entry compaction threshold), settle 60 s, node 5
crashed. "metadata converged" = three live replicas on every tablet;
"data settled" = also every replica hosted and the reconfigure/snapshot counters
still for 10 s. Seed `name_seed` = `893798268308609673`, deterministic.

| tables x puts x bytes | tablets on dead node | metadata converged | data settled | peak reconfigure/s | reconfigures accepted / rejected | snapshot triggers / ships (chunk msgs) / installs | tablet traffic |
|---|---|---|---|---|---|---|---|
| 12 x 80 x 1 KiB | 6 | 6 s | 17 s | 8 | 30 / 0 | 18 / 54 / 9 | 5.2 MB |
| 60 x 80 x 1 KiB | 30 | 6 s | 54 s | 30 | 129 / 30 | 86 / 273 / 43 | 28 MB |
| 60 x 600 x 1 KiB | 30 | 6 s | 57 s | 30 | 129 / 0 | 86 / 711 / 43 | 72 MB |
| 120 x 200 x 4 KiB | 60 | 6 s | 102 s | 61 | 255 / 0 | 167 / 1850 / 85 | 161 MB |
| 300 x 80 x 1 KiB | 150 | 6 s | 243 s | 151 | 633 / 1 | 423 / 1322 / 211 | 216 MB |

- **Without a pacing mechanism the metadata flips for every affected tablet in
  one second** (peak reconfigure/s = the affected-tablet count: 151 for 150
  tablets), then the data catches up serially: **about 1.6 virtual s per affected
  tablet** (243 s / 150), independent of the tablet's size over 80-600 puts and
  1-4 KiB values. 4.2 accepted reconfigurations and 1.4 snapshot installs per
  affected tablet (the earlier empty-tablet figure was 2.3 and 0 installs).
- The burst is 1:1 with tablets on the dead node, which on a balanced cluster is
  `replicas_per_node / 1` tablets: at 3000 hosted replicas per node, 3000
  proposals in the first second. That is the case `R` (per-tick repair
  proposals) is for. Virtual time and no CPU model: it bounds the work, not the
  CPU cost; the snapshot bytes are tablet-message bytes (0.9-2.7 MB of tablet messages per affected tablet, 1-4x its data, including
  ordinary heartbeats).
- The small form of this cell (12 x 80 x 1 KiB, about 1 s in release) is now
  per-push as `sim_cluster_scale_node_loss_with_data_installs_snapshots`
  (structural: dead node held replicas, snapshot installed, every affected
  tablet saw an accepted reconfigure). The scalable cell stays `#[ignore]`d.
  Harness note: `SimCluster::put` retains about 5 MB of sim trace per call, so
  the preload uses the new `put_many_fast` (see
  `docs/lessons/testing/2026-10-13-let-the-test-sample-itself-and-sweep-worker-threads-for-a-per-core-figure.md`).

## G. ProdEnv split storm (`inplace_split_bench.rs`, `bench_inplace_split_storm_cost`)

Three real nodes in one process (LSM, 4 workers), T single-tablet tables, every
split kicked off back to back via `/admin/tablet/split`, a separate `probe`
table read and written throughout.

| T x rows x bytes | total | per-split done (first / median / last) | cores | probe get p50 / p99 | probe put p50 / p99 / max | probe retries |
|---|---|---|---|---|---|---|
| 1 x 2000 x 256 | 0.7 s | 0.7 s | 2.26 | 0.5 / 3.0 ms | 3.3 / 7.3 / 7.6 ms | 0 |
| 4 x 2000 x 256 | 1.8 s | 0.7 / 1.5 / 1.8 s | 2.21 | 1.0 / 5.2 ms | 3.2 / 7.1 / 8.3 ms | 0 |
| 8 x 4000 x 1 KiB | 15.4 s | 1.9 / 9.6 / 15.4 s | 2.62 | 1.8 / 8.4 ms | 2.5 / 7.9 / 16.7 ms | 0 |

- **Splits finish one after another** (1.9 s, 9.6 s, 15.4 s for 8 started
  together): about 1.7 s per 4 MB split regardless of concurrency, and the
  process runs at 2.2-2.6 of 4 cores while it does. Neighbouring-table reads and
  writes were unaffected (no retries, put p99 under 10 ms), so these sizes show
  no starvation from concurrency; the per-split cost is the unit.
- Not measured: splits of tablets near the real split size (hundreds of MB; the
  runs fork 0.5-4 MB), splits under an awake-saturated node, and the follow-up
  directed Placing. The doubling from 4 to 8 tables with 2x larger tablets and
  4x larger rows (15 s vs 1.8 s) says cost tracks bytes, not split count.

## H. LSM RSS under write load (issue #1304, 2026-10-14)

Same box, same rig and command as E3's LSM row (`ANIMUS_BP_LIVE_ENGINE=lsm`,
`_GROUPS=700`, `_WORKERS=2`, `_QUIESCE=0`, `_WRITE_RPS=600`, `_WRITE_GROUPS=50`,
`_VALUE_BYTES=4096`, `_HOLD_SECS=60`), `cargo test --release`. New rig output:
`log_len_sum` / `apply_lag_sum` / `apply_lag_max` (retained Raft log entries and
committed-but-not-engine-applied entries over every replica) and `writes_shed`;
new knob `ANIMUS_BP_LIVE_WRITE_WINDOW=N` (0, the old open loop; N sheds a put while
its leader's apply backlog exceeds N, the way the wire's per-request wait does).
Every row is one run (RSS repeats within about +-25%; the attribution counters
were temporary and are not in the tree).

**Attribution** (before the fix, 600 puts/s open loop): not memtables (64 KiB
x about 150 written replicas is 10 MB), not a leak. Measured with temporary
counters and `valgrind --tool=massif`:

| quantity | before | note |
|---|---|---|
| bytes handed to `send_stream` | 2.3 GB at 10 s, 11.5 GB at 30 s | the entries written are 0.22 GB: about 30 `AppendEntries` entry-sends per entry |
| encoded frames queued in `send_stream` tasks | 450-700 MB, 11-17k tasks | each task holds its payload until the send finishes |
| inbox frames/bytes | under 35 MB | the per-stream cap works |
| committed-but-unapplied backlog | 80k entries at 60 s | each entry sits in the log, the apply buffer and clones |
| massif peak (scaled run) | 70% `encode_for_send` -> `put_bytes` | then decoded received entries, apply buffers, LSM flush/compaction buffers |

Root cause: `RaftCore::replicate_to` sent `next_index..` on every propose,
every success ack and every heartbeat, and `next_index` only moves on an ack, so
the traffic was quadratic in the ack lag and a saturated peer made it worse
(ADR 0009's 2026-10-14 amendment). It is a bounded-but-too-large steady state
only at a rate the node keeps up with; past capacity the rig's open loop (a
proposer with no admission control on `ProposeResult::Accepted`) grows the apply
backlog linearly, which is a rig property: the wire is bounded by
`max_inflight_requests` (2048).

| row | RSS at 60 s before | RSS at 60 s after | min led (of 700) before / after | note |
|---|---|---|---|---|
| 600 puts/s, open loop | 2.62 GB (80.7k backlog, dip of 10) | 0.81 GB (35.7k backlog, held) | 690 / 700 | the documented command |
| 600 puts/s, `WRITE_WINDOW=64` | 2.03 GB (plateau) | 0.55 GB (plateau) | 692 / 694 | closed loop, 31% / 39% of puts shed |
| 300 puts/s, open loop | 1.69 GB | 0.55 GB | 697 / 700 | below capacity |
| sent bytes, 600 puts/s, 20-30 s | 8.7 GB | 0.13 GB (instrumented run of a first, unconditional version of the fix; the shipped depth-4 version was not instrumented) | | 30x to about 1.1x per entry |

Not fixed, and why (needs a decision, not done here): (1) the 150 s open-loop run
at 600 puts/s still grows (about 1.4 GB at 150 s): 600 puts/s of 4 KiB is above
this node's LSM apply capacity (about 400-450 puts/s at 2 workers), so any
open-loop proposer queues; a leader-side admission limit on the apply backlog
(`ProposeResult::Busy`) is the missing bound and is a new knob and a new return
variant for every proposer. (2) The closed-loop steady state of 0.4-0.55 GB is
allocator retention plus LSM compaction and flush buffers (a compaction
materialises its input tables; `decode_block`, `partition_records`,
`flatten_memtable` in massif) and the Raft log plus apply clones; a node-wide
memtable/compaction memory budget is a design decision.

## I. LSM write stall and compaction IO budget (ADR 0081, issue #1300, 2026-10-18)

Method. `animus-cp-data` `tests/it/lsm_stall_measure.rs`, an `#[ignore]`d sweep: `LsmEngine<SimEnv>`
tablet engines under one node `BackgroundBudget` (pool of 2 compaction permits), 1 KiB values,
60 s of virtual time per cell, writers in a closed loop, the disk slowed only by
`DiskConfig::set_sync_delay` (ADR 0079 decision 7; no disk-cost model). Run with `cargo test -p
animus-cp-data --test it lsm_stall_measure -- --ignored --nocapture` (`MEASURE_SECS`,
`MEASURE_CELL` narrow it). Modes: everything off (factors 0, the gate at the old post-append
position), the stall on, and the stall plus the budget at 100 %, 50 % and 25 % of the sustained
compaction rate the off run measured. The grid is one writer per engine (finding 3 explains why).
This is a simulation: it ranks mechanisms and shows shapes, it is not a throughput claim.

| Cell (engines x writers, sync delay) | Mode | acked/s | L0 high-water | compactions | compaction MiB/s | flush blocked (ms) |
|---|---|---|---|---|---|---|
| 1 x 1, 5 ms | off and stall | 90 (p99 20 ms) | 4 | 22 | - | - |
| 1 x 1, 5 ms | budget | 90 | 8 | 16-20 (about 3,600 deferrals) | - | 130-170 |
| 8 x 1, 5 ms | off | 705 | 8 | 154 | 0.036 | 26,051 |
| 8 x 1, 5 ms | budget 100 / 50 / 25 % | about 716 | 8 | - | 0.021 | 2,379 / 1,270 / 1,264 |
| 32 x 1, 5 ms | off | 2,844 | 8 | - | - | 11,376 |
| 32 x 1, 5 ms | budget | - | 8 | - | - | about 5,056 |
| 32 x 1, 30 ms | off | 516 | 8 | - | - | 44,622 |
| 8 x 8, 5 ms | off | memtable stalls about 2,014 s in 60 s | - | - | - | - |

Findings.

1. Level 0 never exceeded 8 (twice the trigger of 4) in any cell with the stall off. The urgent
   bypass and the memtable cap hold the line first; the 3T/5T levels are a backstop for a
   compaction that cannot run, which a slow disk alone does not produce. Under a slow disk the
   active stall cause is the memtable.
2. The budget costs no goodput here and cuts flush-behind-compaction wait 2x to 20x: a deferred
   compaction does not hold the maintenance lock. The sustained compaction rate is only
   0.036 MiB/s in this model, so there is no honest default number; the budget ships off.
3. Eight concurrent writers per engine collapse goodput (124 acked/s for one engine) with L0
   low: `flush` defers while `applies_in_flight > 0` and with several writers one is always in
   flight, so the memtable hits its hard cap. Pre-existing flush starvation, exposed by the stall;
   recorded in `docs/lessons/code-patterns/` and filed as a follow-up.
4. ProdEnv baseline (the sandbox volume, `volume_sequential_fsync_write_baseline`, 128 MiB with an fsync per MiB): 170.5 MiB/s. That is the order of figure `compactionIoMiBPerSec` is sized as a fraction of; it is a sandbox number, not a recommendation. The real-thread stall test (`lsm_stall_prod.rs`) held a withheld-compaction writer stalled with L0 bounded at the stop level and resumed it with no `Err` once compaction returned (6.6 s).
5. The provisional defaults (3T/5T, 16 MiB/s to 256 KiB/s, failure limit 8, 1 s burst, 4 MiB/s
   lag floor) were not contradicted and are unchanged.

## Proposed final defaults (recommendations for review; ADR 0079's knob table is not edited)

| Knob | ADR provisional | Recommendation | Basis |
|---|---|---|---|
| `A` awake replicas per core | 1500 | **keep 1500** | Measured clean no-load ceiling is 2600-3900 per core (E1); 1000 puts/s costs the equivalent of about 800-1000 awake replicas (E3, inferred); 1500 leaves 0.4-0.55 core for writes (about 1500 RF-3 writes/s per node-core at 0.8-1 ms per write, inferred) and still sits below the loaded dip seen at 2 workers (E3). Raising it would eat the write budget. |
| `cores` | operator, from requests | **keep**; document that `A` is per *configured* core, not per worker thread | E1: per-core efficiency differs by 1.3x across worker counts |
| snapshot send / recv pools | 2 / 2 | **keep 2 / 2** | F: 1.4 installs per affected tablet, serial at about 1.6 virtual s each; no CPU data to justify more. Revisit with a CPU-modelled run. |
| catch-up learners pool | 4 | **keep 4** | inferred; F shows catch-up is not the binding stage, the snapshot is |
| `bg-splits` | 1 | **keep 1**; reconsider 2 only with a near-real-size split run | G: concurrency 1 vs 8 gives the same per-split time (they serialise already); a pool of 1 costs nothing measured |
| `bg-compactions` | `max(1, cores/2)` | **keep** | E3 LSM: sstable count stayed flat under 600 puts/s; no starvation seen. Single run, 60 s. |
| `R` (repair proposals/tick) | 8 | **keep 8**, `R_inflight` 16 | F: unpaced, all affected tablets flip in 1 s while the data stage runs at about 0.6 tablets/s, so the proposal stage is not the bottleneck; `R` caps the metadata/Raft storm, not the bytes. 3000 proposals at once is the case to pace. |
| `max_replicas` (operator) | derived | **min(** 0.5 x memory request / 64 KiB, `nofile` / 2, 1000 **)**. Measured: about 28 KB RSS per idle replica (E2), so 64 KiB is a 2x margin; fds 0 idle, 0.35-0.5 per *written* replica (E3); with `LsmEngine` hosting G=1000 per node took 54 s and G=2000 did not finish in 180 s (E2, the `probe` defect). | the constant 1000 is the hosting-time cap while the `probe` defect stands (about 1 min to host 1000 per node at 2/3 core each); drop that term after the fix. Memory under write load is data- and rate-proportional, not replica-proportional (section H): about 0.4-0.55 GB at 150 written replicas of 4 KiB values at a rate the node keeps up with, so it is not covered by a per-replica term; do not tie `max_replicas` to a memory limit under load. |
| `LimitNOFILE` | n/a | at least `max_replicas` | E3: fds scale with written replicas |

## Not measured, and why

- **CPU-modelled snapshot/repair cost** (F is virtual time only); would need
  `Simulator::set_cpu_cost` plumbed into `SimCluster`, or a ProdEnv node-kill run
  with data, a larger harness than this step.
- **Per-node (not per-process) CPU**: the rig puts three nodes in one process;
  the 1/3 split is inferred. The sandbox has 4 cores, so w=4 (a free core
  for the sampler) was not run.
- **Real-size splits and compaction under sustained load** (G only reaches 4 MB
  tablets; E3 LSM ran 60 s once).
- **DynamoDB wire write cost** (E3 uses raw tablet puts; the wire adds parsing,
  item encoding and index maintenance on top of the 0.8-2 ms).
- **LSM RSS growth under load**: explained and mostly fixed, section H.
- **A repeat of any ProdEnv cell**: all single runs; the 2026-10-12 and 2026-10-13
  G=1500 / 2-worker results disagree (fail vs pass), which is the noise floor.

# 2026-10-14: issue #1303, LsmEngine hosting cost (the `probe` defect, fixed)

Same rig as E2: `election_backpressure_liveness`, `ANIMUS_BP_LIVE_ENGINE=lsm`,
3 `ProdEnv` nodes in one process, 2 workers, `HOLD_SECS=1`. **Dev profile**
(unoptimized, `debug=0`), so absolute times are not comparable with the release
runs in E2; before and after are the same binary profile on the same machine.
Single runs each. "CPU/replica" is `host_cpu_ticks` x 10 ms / (3 x G).

| G (per cluster) | Before: all-led | Before: CPU/replica | After: all-led | After: CPU/replica |
|---|---|---|---|---|
| 300 | 11.2 s | 13 ms | 17.2 s | 15 ms |
| 1000 | 120.6 s | 40 ms | 42.7 s | 13.5 ms |
| 2000 | not run (E2 release: did not finish in 180 s) | - | 78.9 s | 12.8 ms |

Before, 3.3x the groups cost 9.8x the CPU (quadratic). After, CPU per replica is
flat (13-15 ms) from G=300 to G=2000. The G=300 row is within run-to-run noise.

Root cause: every `EngineFactory::probe` (once per hosted tablet), `destroy`
and `local_tablets` did a whole-directory `Disk::list` of the node's flat data
dir (about 4 files per hosted replica), and `LsmEngine::open` of an existing
engine did one more listing in `remove_orphan_wal_segments`. The issue's guess
that `ProdEnv::list` awaits a `file_type()` per entry is not the cost: tokio
1.53 caches the type from the buffered `readdir`. The cost is the O(entries)
allocate-and-sort per call.

Fix (no layout change): `animus_cp_data::tablet_files::TabletFileIndex` answers
`probe` from one cached listing, kept exact by recording every engine open or
clone; `remove_orphan_wal_segments` probes the (at most 16) segment numbers below
the live set directly and only lists past that. The deterministic proof is
`tests/it/tablet_file_index.rs`: hosting 100 and 1000 fresh tablets costs exactly
one listing each, against 100 and 1000 for the old shape (a negative control that
scans about 100x the entries at 10x N). `destroy` still lists once per reclaimed
tablet (O(N) each, so a drop of N tablets is O(N^2) in directory entries, not on
the hosting path); the `max_replicas` 1000 hosting-time cap can be revisited (dev profile only here; confirm in release first).

# 2026-10-17: ADR 0080 overload admission calibration

Provisional defaults held; nothing was retuned. What was measured:

- **Tier 1 corpus** (`overload_admission_corpus`, 8 seeds, `ApplyEngine` = `MemoryEngine`
  plus 1 ms of virtual apply cost per op): with the bound on, goodput is at least 80 % of the
  capacity measured with the bound off (0.4 of it for the fault cells), the backlog never
  passes the bound, and no acked write is lost. Controls (a)-(c), (g), (h) each fail their oracle.
- **Tier 2** (`sim_cluster_overload`): a leaderless tablet is refused at 2.25 s (the 2 s
  `leaderless_wait` plus one tick) and a storm of requests to healthy groups in the same run
  completes in 50 ms; without the fast-fail (control d) the same request parks 10.25 s. A burst
  against a tiny bound sees 6 ok / 90 `ThrottlingException`; with the bound off 96 ok, 0 refused.
- **Tier 3** (`animusd/tests/overload.rs`, real TCP, one node, `queued_bytes_budget` = 2 MiB,
  16 clients x 200 KB items for 8 s): 91 admitted, 2963 refused (only 200 / 503 / `ThrottlingException`),
  `queued_bytes_hwm` 410 KB, the node serves again at once.
- **Not run (done 2026-10-18, see the section below):** the 150 s density-rig RSS measurement with the budget on. The ADR 0079 floor
  (0.4-0.55 GB) plus the 256 MiB budget is the sizing rule until it is.

# 2026-10-18: `QueuedBytes` budget vs RSS (issue #1296, ADR 0080 as-built item 11)

Question: is the 256 MiB default `QueuedBytes` budget (ADR 0080 section 4) sound
against the measured RSS, and does RSS stay bounded and flat with it on? ADR 0080
shipped it without the 150 s RSS run. This is that run. Same box (4 cores, 15 GB,
release build), main `089e61f0` plus the rig knobs below. Every row is **one
ProdEnv run** (not seed-reproducible; RSS repeats within about +-25 %).

## Method

Rig: section H's (`election_backpressure_liveness`, LSM, G=700 groups, 2 workers,
`QUIESCE=0`, raw tablet puts through the leaders, 4 KiB values, 600 puts/s round-robin over
the first 50 groups, hold 150 s unless stated). 600 puts/s is above this rig's LSM
apply capacity (about 400-450/s), so the queues fill. New knobs (all default to the old
behaviour), which wire the production assembly into the rig:

- `ANIMUS_BP_LIVE_QUEUED_BYTES=B`: each node gets a `NodeAdmission` (propose bound
  512 entries / 4 MiB per group, the lag feed from the `LoadMonitor`) over a
  `QueuedBytes::new(B)` that `ProdEnv::set_queued_bytes` also hands to the transport
  outboxes, exactly as `animusd` wires it. `0` = unlimited (counted, never refused).
  Unset = no admission (the section H shape). The rig prints `writes_busy` (puts refused
  `Busy`), `queued_used` (sum over the 3 nodes), `queued_hwm_max` (largest single-node
  high-water mark) and `queued_refused` (budget refusals only; the rest of `writes_busy`
  is the propose bound).
- `ANIMUS_BP_LIVE_BACKLOG=off`: also switch the propose bound off (budget alone).
- `ANIMUS_BP_LIVE_STALL_PEER=i`: every other node's address book points node `i` at a
  listener that accepts and never reads, so their sends to it back up (a stalled peer
  whose TCP connection stays up; node `i` receives nothing).

```sh
cargo test -p animus-cp-data --release --features prod-heavy --test election_backpressure_liveness --no-run
ANIMUS_BP_LIVE_ASSERT=0 ANIMUS_BP_LIVE_QUIESCE=0 ANIMUS_BP_LIVE_WORKERS=2 ANIMUS_BP_LIVE_GROUPS=700 \
  ANIMUS_BP_LIVE_ENGINE=lsm ANIMUS_BP_LIVE_WRITE_RPS=600 ANIMUS_BP_LIVE_WRITE_GROUPS=50 \
  ANIMUS_BP_LIVE_VALUE_BYTES=4096 ANIMUS_BP_LIVE_HOLD_SECS=150 ANIMUS_BP_LIVE_QUEUED_BYTES=268435456 \
  target/release/deps/election_backpressure_liveness-<hash> density_repro_g3000_holds_led_count --ignored --nocapture
```

**Caveat that governs every RSS figure:** three nodes share one process, so RSS is the
**process** total; per node is about one third (inferred; the harness base is 67 MB).
`queued_*` are per node or summed as labelled. Goodput = puts accepted in the 150 s hold
(sent 89.9k in every 4 KiB row at 600/s).

## Results: 4 KiB values, 600 puts/s, 50 hot groups (overload)

| run | budget | propose bound | peer | RSS 60 s / 100 s / 150 s (process, MB) | accepted | `Busy` (of which budget) | `queued_bytes_hwm`, largest node | summed `queued_bytes` at end |
|---|---|---|---|---|---|---|---|---|
| r0 control | none | off | ok | 802 / 1828 / 2011 | 89 897 | 0 | n/a | n/a |
| r1 **default** | 256 MiB | on | ok | 1017 / 1278 / 2258 | 77 510 | 12 428 (0) | 72 MiB | 97 MiB |
| r2 | unlimited | on | ok | 937 / 1155 / 2057 | 77 389 | 12 590 (0) | 79 MiB | 29 MiB |
| r3 | 32 MiB | on | ok | 890 / 1398 / 1493 | 71 936 | 17 977 (10 080) | 33 MiB | 39 MiB |
| r4 | 256 MiB | **off** | ok | 674 / 1607 / 1774 | 89 914 | 0 (0) | 81 MiB | 91 MiB |
| s1 | 256 MiB | on | node 2 stalled | 653 / 930 / 1109 | 88 766 | 1 210 (0) | 48 MiB | 52 MiB |
| s2 | unlimited | on | node 2 stalled | 774 / 1038 / 1255 | 87 662 | 2 332 (0) | 68 MiB | 62 MiB |
| s3 | 32 MiB | on | node 2 stalled | 778 / 879 / 999 | 80 550 | 9 414 (6 852) | 36 MiB | 41 MiB |
| f1 | 256 MiB | on | ok, **300 puts/s** (below capacity) | 413 / 641 / 987 | 44 992 | 0 | 27 MiB | 22 MiB |

All rows: all 700 groups led after about 13 s, `max_term` 2, fds 25-38. Led-count dips
in r0-r4 (min 695-699 of 700) are the known loaded-ceiling behaviour (E3), not new; the
stalled-peer rows held 700.

Longer run, same as r1 but a 420 s hold (l1): RSS 1017 MB at 60 s, 1241 at 100 s,
**2294 MB at 150 s and exactly 2295 MB for every sample from 150 s to 420 s**; accepted
148 047, `Busy` 103 883 (all propose-bound, 0 budget), `queued_bytes_hwm` 96 MiB (largest
node), summed `queued_bytes` 58-100 MiB for the whole plateau.

8 KiB values, 300 puts/s over 300 hot groups (m1 256 MiB / m2 unlimited / m3 32 MiB):
RSS 1845 / 1793 / 1793 MB at 150 s, flat from 110 s; accepted 44 313 / 43 848 / 42 747;
hwm 46 / 50 / 39 MiB; budget refusals 0 / 0 / 863. The 32 MiB budget bit only after
110 s and RSS did not move.

## Findings

1. **At the 256 MiB default the budget never binds on this workload.** The largest
   single-node `queued_bytes_hwm` in any 4-8 KiB run with the default was 96 MiB (l1, 420 s),
   with zero budget refusals; every `Busy` came from the per-group propose bound
   (512 entries), which stops each hot group at about 2 MiB of 4 KiB entries
   (`50 groups x 512 x 4 KiB` = 100 MiB, matching the 97-100 MiB the budget reads). The
   budget is a backstop for many hot groups or large entries, not the working limit.
2. **The small budget does bite**, and the refusals are the budget's own (32 MiB: 10 080
   and 6 852 `queue-budget` refusals, hwm held at 33-36 MiB against a 32 MiB budget plus the
   forced Internal/Liveness charge). Goodput cost at 32 MiB: 71.9k vs 77.5k accepted
   (-7 %). So the mechanism works; the 256 MiB value is simply above this workload.
3. **The budget does not bound RSS, because RSS is mostly state the budget does not
   charge.** The charged bytes were 4-5 % of RSS (about 100 MiB of 2.0-2.3 GB, summed over the
   three nodes), and RSS with the budget at 256 MiB, unlimited, or 32 MiB is the same
   order (2.26 / 2.06 / 1.49 GB at 150 s; the single runs differ by less than the +-25 %
   noise, except 32 MiB which is lower in both the healthy and the stalled-peer pair).
   ADR 0080 section 4 states this by design (LSM memtables and compaction/flush buffers
   are not charged); the measurement says how large the uncharged part is.
4. **RSS is bounded and flat once it has climbed, but at a plateau set by uncharged
   state.** It ratchets up in steps of 0.2-0.5 GB (each step coincides with a compaction
   or flush burst: `dir_bytes` peaks at the same sample) and stops: l1 held 2295 MB for 270 s;
   the 8 KiB rows are flat from 110 s. About 0.76 GB per node (inferred, process / 3) at
   600 puts/s over capacity; 0.33 GB per node (987 / 3) at 300 puts/s below it. The 4 KiB
   overload plateau is first reached between 100 s and 150 s, so a 60 s run (section H)
   understates it by 2-3x: **use 150 s or more**.
5. **A stalled peer does not grow the outboxes.** With node 2 never reading, the other
   two nodes' charged bytes were no larger than in the healthy rows (48-68 MiB hwm) and RSS
   was lower (1.1-1.25 GB), as the stalled node did no work. The per-peer outbox caps
   (8 MiB Replicate/Normal, 4 MiB Bulk, drop-oldest) hold; the stalled peer's
   queue is bounded at a few tens of MiB per node, not the thing that sizes memory.
6. **The propose bound caps the leader's backlog, not the followers'.** In l1 the
   cluster-wide apply lag (committed minus engine-applied, summed over all replicas)
   kept growing after 150 s (72k to 128k entries by 420 s, about +200 entries/s) while
   the leader-side bound was refusing 100k puts: the bound reads the leader's
   own apply watermark, a follower that applies more slowly is not charged or limited.
   RSS did not follow (flat at 2295 MB), so the entries are not memory-resident, but the
   follower lag (and with it log retention on disk and restart replay) is unbounded
   under sustained overload. Issue below.
7. **Large values (64 KiB, 300 hot groups, 150 puts/s) are not measured cleanly.** The
   disk filled (`dir_bytes` pinned at 5.1 GB, the box's free space) at about 45 s, so
   RSS after that is a disk-full result. Up to that point, and independent of the budget
   (256 MiB / unlimited / 32 MiB), RSS rose with the bytes written: 0.7, 1.5, 2.3, 2.7 GB at
   10, 20, 30, 40 s with `log_len_sum` 6k-19k entries of 64 KiB (about 1.2 GB of retained
   Raft log tail, which is charged nowhere), and the budget read at most 198 MiB summed
   (144 MiB largest node). After the disk filled, RSS kept growing (13.0 GB at 150 s with
   256 MiB, 9.5 GB with 32 MiB) while accepted puts were flat at 9 335, and with the budget
   unlimited the process was killed by the container's OOM killer at about 14 GB. The
   disk-full part is a separate question from the budget (ENOSPC has its own corpus,
   issue #1218) but the memory growth under ENOSPC with load is filed, below.

## Verdict

- **Is 256 MiB sound?** Yes as a backstop, and the data does not support changing it:
  it was never reached by a workload that stays inside the propose bound (hwm 96 MiB at
  most), a 32 MiB budget costs about 7 % goodput and bites cleanly, and 25 % of a pod
  limit (the operator rule) keeps the *charged* part inside the container. No default or
  operator change is made. Raising it would only matter for a node with many more than 50
  hot groups; lowering it is not justified by an OOM seen with the budget on.
- **Does it make RSS bounded?** No: it bounds the charged queues, which are about 5 % of
  RSS here. The sizing statement in ADR 0080 ("a pod needs floor + B + working set") is
  right but the uncharged term is larger than the "0.4-0.55 GB" quoted for the floor:
  **about 0.75 GB per node at 4 KiB / 600 puts/s overload, 150 s or more** (that figure
  is also per process, so the old floor was a whole-process number at 60 s). With B = 256 MiB
  a write-heavy node needs about 1 GiB; a 1 GiB limit (B = 256 MiB under the operator rule,
  RSS about 0.76 GB + the charged part) has no headroom, and the operator's
  `< 1 GiB` warning threshold is the right minimum, not a comfortable one. Recommend
  **2 GiB** as the practical floor for a node taking sustained writes with large values
  (the 64 KiB rows show RSS scaling with value size, uncharged).
- **Open:** a rig with the DynamoDB wire (the wire holds request bodies and charges them)
  was not run; the per-node RSS is inferred as process / 3; each row is a single run.

Issues filed from this run: #1330 (follower apply lag grows without bound under sustained overload, finding 6) and #1331 (RSS grows without bound under ENOSPC with load, finding 7).
