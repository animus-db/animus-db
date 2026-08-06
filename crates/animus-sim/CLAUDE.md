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
  (process exit), `set_net_config(NetConfig)` (delay/jitter/drop).
- Observability: `trace()` / `trace_lines()`, `now()`, `seed()`.

## What's non-obvious

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
- **`Coresident::sibling` mints a co-resident inbox at runtime** (ADR 0017 D): it
  registers the new id in the shared `nodes`/`inboxes` maps exactly as
  `Simulator::env` does (idempotent `entry(..).or_default()`) and returns a `SimEnv`
  bound to it — sharing the same `Arc<Shared>` (clock, disk, executor). Touches
  only those maps: no RNG draw, no timeline event, so a trace stays a pure function
  of the seed. This is what lets a split's apply spawn the new tablet's group on the
  same simulated node without the test pre-allocating its id.
- Determinism invariants to preserve when editing: only `BTreeMap`/`BTreeSet`,
  RNG drawn only in deterministic order, no wall clock. Disk ops add no timeline
  events and draw no RNG, so they don't perturb traces.

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
traces across runs, reproducible partitions, and the crash/disk model.
