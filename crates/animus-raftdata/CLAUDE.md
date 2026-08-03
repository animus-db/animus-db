# CLAUDE.md — animus-raftdata

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The **leaderful, strongly-consistent (CP) data plane** (ADR 0016, ADR 0017): each
tablet is its own Raft group with a single leader serving **linearizable**
single-tablet reads/writes, durable on a real `StorageEngine`. It is the CP
counterpart to the leaderless AP `animus-data` plane, built **additively** — AP is
untouched, no dual-mode seam yet. The control plane (`animus-control`) remains the
metadata authority (tablet map, placement, failure detection) in both modes.

## How it reuses the control-plane Raft

It instantiates `animus-control`'s generic, sync, I/O-free `RaftCore<C, S>`
(ADR 0009) with `C = KvCommand` and a **`DRIVER_APPLIED` state machine**
(`KvState`, a unit placeholder): the core agrees the order of commands but does
**not** apply them in-core (a `StorageEngine` apply is async I/O the sync core
can't do). Instead the core buffers committed-and-durable commands as effects;
this crate's **async driver** drains them (`RaftCore::drain_apply`) and applies to
the engine — the `AccordCore` sync-core/async-driver split.

## Entry points

- `KvCommand` (`Put`/`Delete`/`NoOp`), `KvState` (the `DRIVER_APPLIED` SM).
- `RaftKvNode<E, S>` — a running tablet-group node: `start(env, all_nodes,
  storage)`, `put`/`delete` (proposed via Raft, honored on the leader), `is_leader`.
  `local_get` reads this replica's engine (**not** linearizable — that is ReadIndex,
  Stage B.2; it is a test/observability aid).

## What's non-obvious

- **The driver mirrors the control-plane `RaftNode` driver, minus reconcile +
  failure-detection (control-plane only), plus engine apply.** Each loop:
  `flush_and_apply` (drain WAL records → append + `fsync` → `mark_durable_through`
  → `drain_apply` → `merge`/`merge_tombstone` into the engine, in commit order),
  then `select(recv, timer)`, step the core, `flush_and_apply` again (durability
  before shipping), then send. Heartbeat ticks re-replicate (no separate retry).
- **The Raft log index is the MVCC version.** Apply uses `index` as the engine
  `version`, so per-key LWW reproduces the agreed Raft total order, and re-applying
  on recovery is idempotent.
- **Durable-before-visible holds** (ADR 0009): effects are only drained for fsynced
  entries, and the engine write follows the WAL `fsync`.
- Distinct WAL file (`raftkv.wal`) from the control plane's `raft.wal`, so a node
  can host both planes.

## Stage status (ADR 0017)

- **B.1 (done)** — single-group driver + write path; `tests/single_tablet.rs`
  (writes replicate + apply on every replica; survive a leader kill + rejoin
  catch-up; trace reproducibility).
- **B.2** — linearizable **ReadIndex** reads.
- **A.2** — engine-as-snapshot + streaming `InstallSnapshot` (follower catch-up).
- **C** — per-tablet hosting + single-server Raft membership change (reconfigure on
  node failure, driven by the control plane).
- **D** — tablet split on cluster growth.

## Tests

`cargo test -p animus-raftdata` — `tests/single_tablet.rs` (SimEnv; drive with
`run_for`, never `run()` — the driver has perpetual heartbeat/election timers).
