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
- Determinism invariants to preserve when editing: only `BTreeMap`/`BTreeSet`,
  RNG drawn only in deterministic order, no wall clock. Disk ops add no timeline
  events and draw no RNG, so they don't perturb traces.

## Tests

`cargo test -p animus-sim` — `tests/determinism.rs` asserts byte-identical
traces across runs, reproducible partitions, and the crash/disk model.
