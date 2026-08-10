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
  per-op injected `io::Error`s on `append`/`sync`/`read`/`read_at`/`replace`,
  torn un-synced tails on crash, byte corruption of the torn region), and
  `corrupt_durable(node, file, offset)` (flip one durable byte — at-rest
  corruption of synced data, e.g. to hit an SSTable's per-block CRC).
- Observability: `trace()` / `trace_lines()`, `now()`, `seed()`.

## What's non-obvious

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
- `SimEnv` still implements `Coresident::sibling` (mint a co-resident inbox at
  runtime, ADR 0017 D), but the mechanism is **vestigial**: production co-hosting
  moved to multiplexed `(node, stream)` addressing (ADR 0026, below) and nothing
  live calls `sibling` anymore. It touches only the `nodes`/`inboxes` maps (no
  RNG draw, no timeline event), so keeping it costs determinism nothing.
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
  created lazily on first send/recv, exactly like `Coresident::sibling` lazily
  registers a whole new node id today. No new RNG draw or timeline event shape,
  so the determinism argument (trace = pure function of the seed) is unchanged;
  `tests/determinism.rs::multiplexed_streams_are_isolated_and_deterministic`
  proves it directly (two streams to one node don't cross-talk, and the run —
  trace included — reproduces byte-for-byte from the seed).

## Tests

`cargo test -p animus-sim` — `tests/determinism.rs` asserts byte-identical
traces across runs, reproducible partitions, and the crash/disk model;
`tests/disk_faults.rs` asserts the opt-in disk fault model is default-off
byte-identical and seed-reproducible when enabled. The storage-facing fault
corpus (LSM torn-tail recovery, injected-error write paths, CRC on corrupted
blocks) lives in `animus-storage/tests/lsm_disk_faults.rs`.
