# CLAUDE.md — custos-control

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The strongly-consistent control plane: an in-house Raft (ADR 0009, *not*
openraft) replicating cluster metadata — membership and the tablet map — with
epoch compare-and-swap transactions.

## Entry points

- `meta.rs` — `Metadata` (members + tablet map) and `MetaCommand`
  (`UpsertMember`, `CreateTablet`, `CasTabletReplicas`, `SplitTablet`,
  `MergeTablets`). `Metadata::apply` is the deterministic state machine.
- `raft.rs` — `RaftCore`: a **synchronous, I/O-free** Raft state machine. Time
  and randomness are parameters (`now`, `entropy`); it returns outbound messages
  and emits WAL records.
- `persist.rs` — `WalRecord`, `PersistedState` (durability/recovery).
- `node.rs` — `RaftNode<E>`: the `Env` driver wrapping the core.

## What's non-obvious

- The split is deliberate: **all consensus logic is in the sync `RaftCore`**
  (unit-testable, deterministic); the driver only does I/O. When changing
  protocol behavior, change the core and keep it I/O-free — don't reach for the
  env inside it.
- The driver races `env.recv()` against a timer via `futures::select`. It draws
  `entropy` every iteration (deterministic) and passes it in for randomized
  election timeouts.
- Durability: the core emits `WalRecord`s at log-append/truncate/apply sites;
  `drain_persist` also folds in any hard-state (term/vote) change, so a granted
  vote is persisted before it's sent. The state machine is checkpointed so
  recovery does **not** re-apply committed commands (which would double-apply a
  CAS). The driver compacts the WAL to `RaftCore::wal_image()` (latest
  checkpoint + hard state + current log) on a threshold, via atomic
  `Disk::replace`, so it stays bounded by the live state.
- `CasTabletReplicas` applies only if the tablet's epoch matches, then bumps it
  — evaluated identically on every replica, so accept/reject is consistent.
- Commit advances only for **current-term** entries via majority `matchIndex`
  (the Raft safety rule). Don't relax this.
- Not implemented: truncating the committed log prefix in memory (true log
  compaction) + `InstallSnapshot` to catch up far-behind followers; full in-sim
  restart-and-rejoin (recovery is validated at the `RaftCore` level — see
  `tests/persistence.rs` and `tests/wal_compaction.rs`).

## Tests

`cargo test -p custos-control` — election/replication/leader-kill +
multi-seed convergence (`control_raft.rs`), durability/recovery
(`persistence.rs`), split/merge (`tablet_split_merge.rs`). Use `run_for`, never
`run()` (perpetual heartbeats).
