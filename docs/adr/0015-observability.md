# ADR 0015 — Deterministic-safe observability seam (metrics)

- **Status:** Accepted
- **Date:** 2026-08-02

## Context

AnimusDB has rich internal behavior — leader elections, log replication,
chunked snapshot installs, heartbeat-driven failure detection (ADR 0012) — but
no way to *observe* it at runtime. An operator running `animusd` cannot tell how
often elections happen, whether a follower is rejecting `AppendEntries`, whether
snapshots are being shipped, or how often the failure detector flips a member
`Down`/`Active`.

The hard constraint is determinism (ADR 0003): **all** nondeterminism flows
through the `Env` seam, and the deterministic simulator is the source of truth
for correctness. A naive metrics layer breaks this in three classic ways — it
reads the wall clock to timestamp samples, it keys counters by string in a
`HashMap` (nondeterministic iteration order), and it does its own I/O (a
`/metrics` HTTP handler, a background flush). Any of those would make a sim run
no longer a pure function of its seed, or leak `HashMap` ordering into logic.

We also must not disturb the carefully-built `Env` supertrait
(`Clock + Rng + Network + Disk + Spawner`) or any of the many components generic
over `E: Env`: adding a required method would force a change to every `Env` impl
(`SimEnv`, `ProdEnv`) and ripple through call sites.

## Decision

Add a **minimal, deterministic-safe metrics seam** in `animus-env`
(`metrics.rs`), wired into the control-plane Raft driver, and exposed from
`ProdEnv` as structured text. The data-plane live endpoint is wired at
integration; the seam itself does **no** HTTP.

- **Closed `Metric` enum + fixed atomic array.** The recording sink
  (`MetricSink`) is a fixed-size array of `AtomicU64` counters (one slot per
  `Metric` variant) plus one `AtomicI64` leadership gauge. Recording is a single
  relaxed atomic add — lock-free, allocation-free, no map lookup. A closed enum
  (vs. arbitrary string keys) keeps recording O(1) and makes the exported metric
  names a small, reviewable, stable surface. **No `HashMap`/`HashSet`**; a
  snapshot collects into a `BTreeMap`, so export order is deterministic.

- **No wall clock, no I/O, no randomness in the seam.** Recording touches only
  atomics; it never reads time. Any metric that needs a timestamp takes one
  derived from `Clock::now` (the `Env` clock), never `std::time`. Export
  (`MetricSnapshot::to_text`) is a pure read rendered as stable `name value`
  lines with **no embedded timestamp** (a scrape adds its own), so equal
  snapshots are byte-identical — which makes "same seed ⇒ same metrics" testable.

- **Additive `Env::metrics`, with a no-op default.** `Env` gains one method,
  `fn metrics(&self) -> MetricsHandle`, **with a default** that returns a shared
  no-op handle. Every existing `Env` impl keeps compiling and behaving
  identically with no change — `SimEnv` included (it does not override it). The
  supertrait set is untouched. Returning a handle (not `Option`) means recording
  sites need no `if let Some(..)` guard: the no-op handle is a real sink that is
  recorded into and simply never read.

- **`ProdEnv` records for real; `start_with_metrics` for sim observability.**
  `ProdEnv` overrides `metrics()` to return its own recording handle, so an
  assembled production node accumulates counters with no extra wiring, and
  `ProdEnv::metrics_text()` renders the current snapshot. Because `SimEnv` uses
  the no-op default, a *test* that wants to read counters constructs a recording
  `MetricsHandle` and threads it into the component via the additive
  `RaftNode::start_with_metrics(env, nodes, handle)` — so the deterministic suite
  observes metrics **without changing `animus-sim` at all**. `RaftNode::start`
  forwards `env.metrics()`, so production keeps recording into the env's sink.

- **What the data plane records (ADR 0001/0010/0005).** *(Audit note
  2026-08-06: the `data_*` counters below are **dead in v1** — their recording
  sites left with the deleted AP plane (ADR 0019); the enum keeps the variants
  by the append-only rule, so they render as permanent zeros. Meanwhile the
  **CP data plane that v1 actually ships records no metrics at all** — no
  `cp_*`/raftkv variants exist. Adding per-tablet-group counters
  (proposals/applies/read-barriers/compactions, apply-batch sizes, snapshot
  ships) via the same additive pattern is the standing observability follow-up;
  the v1 serving path is currently observable only through the ADR 0020 admin
  snapshots.)* The leaderless-AP data
  plane records, all from `Env`-supplied inputs so recording stays a deterministic
  function of the run, at the real coordinator/replica sites:
  `data_quorum_writes_attempted`/`_succeeded`/`_failed` and
  `data_quorum_reads_attempted`/`_succeeded`/`_failed` (the `DataClient`
  coordinator — a write/delete is a quorum mutation counted under the *write*
  counters, succeeded iff `W` acked, failed otherwise; a read succeeds iff `R`
  responded); `data_read_repair_triggered` + `data_read_repair_keys_repaired` (a
  divergent quorum read pushes the winner back, one repair / one key);
  `data_hints_stored` (a committed write/delete buffers a residency-admitted hint
  per unreached replica) + `data_hints_delivered` (a hint-handoff/replay loop
  re-sends a buffered batch to a returning target); and
  `data_anti_entropy_rounds` (each background round that emits a non-empty segment
  digest). Names are `data_*`-prefixed. The coordinator's handle defaults to
  `env.metrics()`; the background loops (`serve_anti_entropy`,
  `serve_hint_handoff`, `serve_hint_replay`) gain additive
  `*_with_metrics` variants (the originals forward `env.metrics()`), so a sim test
  threads a recording handle in to read counters back without changing `SimEnv`.
  Recording is observe-only — it changes **no** quorum semantics, and all public
  signatures stay additive/stable.

- **What the storage engine records (ADR 0004/0008).** The on-disk `LsmEngine`
  records, at the real LSM site that knows the *outcome* (not merely a schedule),
  all observe-only and deterministic (counters only, no wall clock): `storage_flushes`
  (one per memtable flush, counted *after* the manifest swap commits the new table);
  `storage_compactions` + `storage_compaction_tables_merged` +
  `storage_compaction_bytes_merged` (one per compaction whose manifest swap
  committed, plus the input tables it merged away — the "segments compacted" — and
  their on-disk `file_size` bytes); `storage_tombstones_reclaimed` (the records a
  compaction's tombstone-GC physically dropped below the GC floor — the drop in the
  merged record count); `storage_sstable_block_reads` (one per block fetched from
  disk, recorded in `SsTableReader::read_block` — the read-amplification counter);
  `storage_bloom_hits`/`storage_bloom_misses` (the per-table Bloom verdict for a
  point lookup whose key was inside the table's key range — a "miss" is a block
  read the Bloom saved, so it is counted at the gate, before any block read); and
  `storage_wal_segment_rotations` (one per WAL segment rotation, counted at the
  group-commit site via a monotonic coordinator counter whose delta the engine
  records around each `commit`). Names are `storage_*`-prefixed. The engine's handle
  defaults to `env.metrics()` (the no-op under `SimEnv`, the recording one under
  `ProdEnv`); a sim test threads a recording handle in via the additive
  `LsmEngine::open_with_metrics`, and the `SsTableReader` takes it via the additive
  `with_metrics` — so the deterministic suite reads storage counters back without
  changing `animus-sim`, and all public signatures stay additive/stable.

- **What the control plane records.** The `RaftNode` driver loops record, all
  from `Env`-supplied or core-derived inputs (so recording is a deterministic
  function of the run): `elections_started`/`elections_won` (from role/term
  transitions: candidate at a higher term, then entering `Leader`); a leadership
  **gauge** (a *level*, not an event); `append_entries_sent` and
  `append_entries_rejected` (read off the messages the core emits — a rejection
  surfaces as an outbound `AppendEntriesResp { success: false }`);
  `snapshot_installs` (a completed follower-side `InstallSnapshotResp`); and
  `failure_detector_down`/`failure_detector_up` (the `Active`↔`Down` edges the
  `detect_loop` proposes, ADR 0012). Names are `control_*`-prefixed.

## Consequences

- The data plane is **observable** at runtime with zero determinism risk: every
  data counter is exercised under the deterministic simulator and asserted to move
  by the expected amount under a known workload
  (`animus-data/tests/metrics.rs` — a quorum write+read moves the
  attempted/succeeded counters; a two-replica crash makes a write fail
  sub-quorum; an `R=3` read against a deliberately-stale replica triggers exactly
  one read-repair / one repaired key; a crash-then-return replica buffers a hint
  and the replay loop delivers it; a seeded replica's anti-entropy loop counts its
  rounds), and the recorded snapshot is asserted byte-identical across two runs of
  the same seed.
- The storage engine is **observable** at runtime with zero determinism risk:
  every storage counter is exercised under the deterministic simulator and asserted
  to move under a known workload (`animus-storage/tests/lsm_metrics.rs` — a write
  workload forces several flushes, an L0→L1 compaction (tables + bytes merged), WAL
  segment rotations, and on-disk point reads; a proven-absent in-range key is a
  Bloom *miss* that reads **zero** blocks; an aged tombstone is counted reclaimed),
  and the recorded snapshot is asserted byte-identical across two runs of the same
  seed. The counters fire on the cooperative write/read path, so the single-threaded
  `SimEnv` exercises them all — no `ProdEnv` multi-thread test is required for
  coverage (recording is a relaxed atomic add at sites already proven correct).
- The control plane is **observable** at runtime with zero determinism risk:
  every counter is exercised under the deterministic simulator and asserted to
  move under known events (`animus-control/tests/metrics.rs` — a forced election
  bumps `elections_started`/`elections_won` and the gauge; a crashed
  heartbeating member bumps `failure_detector_down`, its recovery bumps
  `failure_detector_up`), and the recorded snapshot is asserted byte-identical
  across two runs of the same seed.
- The seam is **additive and supertrait-preserving**: no `Env` impl or `E: Env`
  call site needed changing; `SimEnv` is untouched.
- The export is **timeless structured text** (`name value` lines). It is not
  Prometheus exposition format, but is trivially adaptable to it; choosing a
  timeless, dependency-free format keeps the seam pure and avoids pulling a
  metrics crate into the determinism-critical core.
- **The live endpoint is wired in `animusd`.** A running node serves
  `GET /metrics` on its **HTTP endpoint** (the same hand-rolled HTTP/1.1 listener
  as the DynamoDB JSON wire — `Node::dynamo_addr()`, so no seventh port or config
  field), returning the text export as `text/plain`. No HTTP lives in
  `animus-env`; the edge is production-only I/O like the rest of that listener.
  - **It aggregates the node's three role sinks.** A node runs three internal
    `ProdEnv` roles on distinct ids (control / data / coord), each recording into
    its **own** sink (`RaftNode::start` records into the control env's sink; the
    replica and the coordinator into theirs). The handler
    (`ClientCtx::metrics_text`) snapshots all three **at request time** — so the
    export reflects live activity, not a cached value — sums the counters
    counter-by-counter, and takes the max of the leadership gauge (leadership is
    the control plane's, recorded only in the control sink). So both control- and
    data-plane counters surface from one endpoint. Proven over real TCP in
    `animusd/tests/metrics_endpoint.rs` (a 3-node cluster elects a leader,
    `GET /metrics` returns the text format with `control_elections_won >= 1` and
    `control_is_leader 1` on the leader, `0` on a follower).
- **Deferred:** histograms/latency (would need a deterministic bucketing scheme).
  Storage-engine counters (flushes, compactions + bytes/segments merged, tombstone
  GC, SSTable block reads, Bloom hits/misses, WAL segment rotations), data-plane
  counters (quorum reads/writes, read-repair, hinted handoff, anti-entropy), and the
  live `/metrics` endpoint are now **done** (see above). The storage-engine counters
  are recorded only into the env/handle the engine was opened with; wiring them into
  `animusd`'s aggregated `/metrics` endpoint (alongside the control/data role sinks)
  is a thin follow-up at the assembly point and is not part of this change.
