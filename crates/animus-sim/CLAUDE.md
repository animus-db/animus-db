# CLAUDE.md — animus-sim

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The deterministic simulator: a seeded `SimEnv` implementing the `animus-env`
seam, plus a `Simulator` that drives an entire distributed run as a pure
function of one seed. This is the substrate every distributed test runs on.

## Entry points

- `Simulator::new(seed)`, `sim.env(node_id) -> SimEnv`.
- Driving: `run()` (to quiescence), `run_for(dur)` / `run_until(deadline)`
  (bounded virtual time), `run_until_quiescent(max_steps)`.
- Faults: `partition`/`partition_pair`/`heal`, `crash`/`restart`, `stop`
  (process exit), `set_net_config(NetConfig)` (delay/jitter/drop),
  `set_disk_config(DiskConfig)` / `set_disk_config_for(node, ..)` (disk faults:
  per-op injected `io::Error`s on `append`/`sync`/`read`/`read_at`/`replace`/
  `link` (ADR 0058 rung 2 — modelled as a snapshot copy into an independent
  map slot, no inode/directory concept, since that's indistinguishable from
  a real hard link for the trait's sanctioned use: sharing an already-fully-
  synced, never-mutated-in-place file), torn un-synced tails on crash, byte
  corruption of the torn region, and
  `DiskConfig::set_sync_delay` — a fixed extra virtual-time latency on every
  `append`/`sync`, issue #279's slow-disk livelock repro; unlike the other
  knobs it draws no RNG, so it's a plain fixed cost, not a seed-sampled
  fault), and `corrupt_durable(node, file, offset)` (flip one durable byte —
  at-rest corruption of synced data, e.g. to hit an SSTable's per-block CRC).
- Observability: `trace()` / `trace_lines()`, `now()`, `seed()`.
- Clock skew (ADR 0018 §2 sim support): `set_clock_skew_for(node, skew_nanos)`
  — a per-node signed-nanosecond offset applied only to that node's own
  `Clock::now()` reads (mirrors the `set_disk_config_for` per-node-override
  shape). **Opt-in and default-zero**: with no call, every node's `now()` is
  byte-identical to the global clock, so this changes nothing for any
  existing test. Clamped so a reading never underflows below 0 or overflows
  `u64::MAX`. Deliberately **read-side only** — `sleep`'s timers still fire
  against the single global timeline, since a per-node skewed *timeline*
  would reorder the shared event loop and break determinism; skew models a
  node's clock *reading* wrong (exactly what an HLC has to tolerate), not a
  different flow of time for that node.

## What's non-obvious

- **`SimSegmentStore` (`segment_store.rs`, ADR 0043 §A7) is `animus-env`'s
  `SegmentStore` seam's deterministic corpus impl** — a seeded,
  fault-injectable in-memory `BTreeMap<String, Vec<u8>>` store, built from a
  `SimEnv` handle (`SimSegmentStore::new(env)`) rather than pulled off the
  `Simulator` the way `sim.env(node)` hands out an `Env` handle, because
  `SegmentStore` is deliberately **not** part of the `Env` supertrait (F5) —
  a consumer threads the store explicitly, same as `StorageEngine`.
  Constructing it from *any* node's `SimEnv` handle is fine for determinism:
  every node's `Rng` draws off the same single `SimState`-shared stream (see
  `next_u64`'s `self.shared.lock().rng.next_u64()` above), so "which node's
  handle" never changes the draw sequence — only *whether* a draw happens at
  all changes it.
  - **Fault knobs mirror `DiskConfig`'s own discipline exactly**: `roll`
    draws RNG only when its threshold is non-zero, so a store with no
    configured `SegmentFaultConfig` perturbs neither the RNG stream nor any
    other test's determinism — a fault schedule is opt-in per test, just
    like `NetConfig`/`DiskConfig`.
  - **Ack-lost is the deliberate *opposite* of `DiskConfig`'s "no state
    change on a fault" rule.** `inject_disk_fault` guarantees a failed op
    changed nothing; `SimSegmentStore`'s `put_ack_lost`/`delete_ack_lost`
    faults do the op for real (the object lands / is removed) and *then*
    return an injected error — modeling the exact ambiguity ADR 0043 §A3's
    seal step must tolerate (a crash or network error between the store
    acking and the caller's proposal), not a clean failure. Don't reuse
    `inject_disk_fault`'s "no state change" shape for a fault whose whole
    point is that the state *did* change.
  - **Unavailability windows are a plain `Option<Nanos>` deadline compared
    against `Clock::now()`**, not a new timeline/RNG mechanism — no new
    `Simulator` machinery needed since `SegmentStore` isn't part of `Env`'s
    own event loop. `set_unavailable_until`/`clear_unavailable` are ordinary
    setters a test calls directly (no RNG draw: the deadline is
    caller-chosen, not sampled), and healing happens for free once virtual
    time (driven by `sim.run_for`/`run_until`, as always) passes it.
  - **Tests spawn the async workload via `env.spawn_task` and drive it with
    `Simulator::run_until_quiescent`/`run_for`, not `#[tokio::test]`** — this
    crate has no `tokio` dependency at all (async here is driven by the
    simulator's own cooperative executor). None of `SegmentStore`'s methods
    genuinely suspend, so a spawned task completes within the first drain
    under `run_until_quiescent`; a test that calls `Clock::sleep` (to check
    an unavailability window healing) needs `run_for(dur)` instead, bounded
    past the sleep. A panicking assertion inside the spawned block
    propagates out of the polling call exactly like any other panic (the
    simulator polls synchronously on the test's own thread), so no
    completion flag is needed to catch a task that silently never ran its
    assertions.

- **`Simulator` is `Clone`** (added for ADR 0031 PR5's reconciler corpus): it
  hands out another handle to the SAME shared world (clones the inner `Arc`),
  exactly like `SimEnv`'s own `Clone`, not a fork. This is what lets a test's
  spawned "driver" task carry its own `Simulator` handle to call the `&self`
  fault-injection methods (`stop`/`crash`/`partition_pair`/`heal`/`env`) from
  *inside* an async scenario script, while the outer synchronous test code
  keeps its own handle to drive `run_for`/`run_until` (the only `&mut self`
  methods — no field either handle touches is exclusive to one of them).
- **`run()` never returns for protocols with perpetual timers** (Raft
  heartbeats). Use `run_for`/`run_until` whenever the control plane is involved.
- The core is a single shared `SimState` behind a `Mutex`, plus a custom
  waker-driven executor: the loop drains ready tasks, then fires the earliest
  event on a unified `(time, seq)` timeline (timers + message deliveries). The
  `seq` tiebreaker is what makes ordering total and reproducible.
- A future is *checked out* of the task map while polled so it can re-enter the
  state lock (e.g. via `env.send`) without deadlocking. Wakers hold a `Weak` to
  avoid a reference cycle.
- `crash(node)` drops un-synced disk + the inbox **and mutes the node's
  outbound sends** (a dead node emits nothing); deliveries to a crashed node are
  dropped until `restart`.
- `restart(node)` **re-arms** the node's tasks: it clears `crashed` *and* marks
  every task the node owns ready so the run loop re-polls them. This is load-
  bearing — crashing drops the waker of any task parked on `recv()` (the inbox is
  volatile), so without the re-poll a later delivery would find no registered
  recv waker and the task would never wake again. Re-polling a parked `Recv` on
  an empty inbox is side-effect-free (no RNG, no timeline event), and tasks are
  re-armed in ascending id order, so determinism holds. Regression test:
  `determinism.rs::restart_resumes_a_parked_recv`.
- `stop(node)` models a **process exit**: it removes the node's tasks (each
  spawned task is tagged with its owner node id) and volatile state (inbox,
  un-synced disk), keeping durable disk. Start a fresh node on the same id
  afterward and it recovers from disk — the real restart-and-rejoin path (see
  `animus-control/tests/restart.rs`). `crash` keeps the tasks running but mute;
  `stop` ends them.
- Determinism invariants to preserve when editing: only `BTreeMap`/`BTreeSet`,
  RNG drawn only in deterministic order, no wall clock. Disk ops add no timeline
  events and — under the **default** `DiskConfig` — draw no RNG, so they don't
  perturb traces; every existing test is byte-identical with the fault model
  present. With a non-default `DiskConfig` the disk *does* draw RNG (error
  sampling per op; tear point + corrupted byte on crash, files in `BTreeMap`
  name order), which is deterministic but seed-shifting — so a fault config is
  strictly **opt-in per test**. `error_rate` never fires on `size`/`remove`/
  `list` (metadata ops stay reliable so discovery/GC paths don't need fault
  handling to be testable). A `crash` tear keeps a seed-chosen **strict**
  prefix of each file's buffered bytes and makes it durable (it reached the
  platter); `corrupt_on_crash` flips one byte only inside that retained
  region, never in previously-durable bytes. Tests: `tests/disk_faults.rs`
  (default-off byte-identity is asserted against a run with no config at all).

- **Multiplexed `(node, stream)` addressing (ADR 0026).** The inbox/waker maps
  are keyed `(NodeId, u64)` instead of `NodeId`, so a node can be addressed on
  more than one stream; `crash`/`stop` now node-prefix-scan both maps (the same
  pattern `Disk::list`'s node-prefix scan already used) to clear *every* stream
  of a crashed/stopped node, not just its primary one. `Simulator::env` still
  only pre-registers `PRIMARY_STREAM`'s inbox entry — any other stream is
  created lazily on first send/recv. No new RNG draw or timeline event shape,
  so the determinism argument (trace = pure function of the seed) is unchanged;
  `tests/determinism.rs::multiplexed_streams_are_isolated_and_deterministic`
  proves it directly (two streams to one node don't cross-talk, and the run —
  trace included — reproduces byte-for-byte from the seed).

## Tests

`cargo test -p animus-sim` — `tests/determinism.rs` asserts byte-identical
traces across runs, reproducible partitions, and the crash/disk model;
`tests/disk_faults.rs` asserts the opt-in disk fault model is default-off
byte-identical and seed-reproducible when enabled, including `link`
(ADR 0058 rung 2): its own basic hard-link-semantics test (`dst` reads
`src`'s bytes, survives a `remove` of `src`, an already-linked `dst` is
safely overwritten on relink, a missing `src` is a clean `NotFound`) and a
reproducibility test proving it participates in the same seeded
error-injection schedule as every other disk op. The storage-facing fault
corpus (LSM torn-tail recovery, injected-error write paths, CRC on corrupted
blocks) lives in `animus-storage/tests/lsm_disk_faults.rs`; the SSTable-clone
crash-safety corpus (`LsmEngine::clone_to`) lives in
`animus-storage/tests/lsm_clone.rs`. `tests/clock_skew.rs`
(ADR 0018 §2 sim support) proves the clock-skew knob: per-node `now()` offsets
by exactly its configured skew while an unskewed node tracks the global clock;
a large negative skew clamps at 0 near time zero instead of underflowing; and
the same seed + skew script reproduces an identical observed `now()` sequence
(the determinism guarantee holds with skew configured, not just by default).
The HLC-specific causality-under-skew property (a behind-clock node's mint
still exceeds an ahead-clock node's) is tested in
`animus-cp-data/tests/hlc_skew.rs`, since it needs both this crate and
`animus_cp_data::hlc`.

`segment_store.rs`'s own `#[cfg(test)] mod tests` (unit tests, not a
`tests/*.rs` integration file — small enough to sit beside the impl) covers
`SimSegmentStore`: the shared `animus_env::test_support::
assert_segment_store_contract`; an ack-lost `put`/`delete` leaves the actual
state change intact while the caller sees an error (checked from a second
clone of the store, not just the original handle); an unavailability window
fails every op with no state change until virtual time (advanced via
`Clock::sleep` + driving the simulator) passes the deadline, then heals on
its own; `clear_unavailable` heals early regardless of virtual time; and a
determinism regression comparing two runs of the same seed + fault schedule
for byte-identical outcome sequences (plus a different seed diverging), with
a sanity check that the configured fault probability actually produces both
outcomes over the run.
