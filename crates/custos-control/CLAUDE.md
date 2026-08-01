# CLAUDE.md — custos-control

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The strongly-consistent control plane: an in-house Raft (ADR 0009, *not*
openraft) replicating cluster metadata — membership and the tablet map — with
epoch compare-and-swap transactions.

## Entry points

- `meta.rs` — `Metadata` (members + tablet map) and `MetaCommand`
  (`UpsertMember`, `CreateTablet`, `CasTabletReplicas`, `SplitTablet`,
  `MergeTablets`, `SetTabletPolicy`). `Metadata::apply` is the deterministic
  state machine; `Metadata::reconcile` is the pure placement decision (see
  below).
- `raft.rs` — `RaftCore`: a **synchronous, I/O-free** Raft state machine. Time
  and randomness are parameters (`now`, `entropy`); it returns outbound messages
  and emits WAL records.
- `persist.rs` — `WalRecord`, `PersistedState` (durability/recovery). The WAL
  write/compact/recover flow is diagrammed in `docs/wal.md`.
- `node.rs` — `RaftNode<E>`: the `Env` driver wrapping the core, plus
  `reconcile_loop` (the leader's automatic placement reconciler).

## What's non-obvious

- The split is deliberate: **all consensus logic is in the sync `RaftCore`**
  (unit-testable, deterministic); the driver only does I/O. When changing
  protocol behavior, change the core and keep it I/O-free — don't reach for the
  env inside it.
- The driver races `env.recv()` against a timer via `futures::select`. It draws
  `entropy` every iteration (deterministic) and passes it in for randomized
  election timeouts.
- Durability: the core emits `WalRecord`s at log-append/truncate sites;
  `drain_persist` also folds in any hard-state (term/vote) change, so a granted
  vote is persisted before it's sent. The log is offset by a snapshot:
  `snapshot()` truncates the committed prefix it covers, and on a threshold the
  driver snapshots + rewrites the WAL to `wal_image()` (snapshot + hard + log
  tail) via atomic `Disk::replace` — bounding both. Recovery restores the
  snapshot and **re-applies the tail** (commit re-advances), so a CAS lands once.
  A follower behind the leader's compacted prefix is caught up via
  `InstallSnapshot`. See `docs/wal.md`.
- `CasTabletReplicas` applies only if the tablet's epoch matches, then bumps it
  — evaluated identically on every replica, so accept/reject is consistent.
- **Automatic placement (ADR 0005).** Policies are replicated in `Metadata`
  (`SetTabletPolicy` → `policies` map). The decision lives in the pure
  `Metadata::reconcile` (runs `custos_placement::replan` over `Active` members,
  emits a `CasTabletReplicas` only for tablets whose set violates the policy —
  idempotent). The **leader** drives it: `node.rs`'s `reconcile_loop` ticks on a
  slow `env.sleep` timer and proposes the result. Keep the *timing* in the
  driver and the *decision* pure — don't put a clock or RNG in `reconcile`, and
  don't reconcile off-leader (a non-leader `propose` is dropped; a stale CAS is
  epoch-rejected). `custos-placement` is a **normal** dependency now (no cycle).
- Commit advances only for **current-term** entries via majority `matchIndex`
  (the Raft safety rule). Don't relax this.
- Not implemented: chunked snapshot transfer (snapshots ship whole).

## Tests

`cargo test -p custos-control` — election/replication/leader-kill +
multi-seed convergence (`control_raft.rs`), durability/recovery
(`persistence.rs`), split/merge (`tablet_split_merge.rs`), snapshot truncation
(`wal_compaction.rs`), a partitioned follower catching up via `InstallSnapshot`
(`install_snapshot.rs`), process restart-and-rejoin (`restart.rs`, using
`Simulator::stop`), caller-driven placement reconcile through Raft under a
replica death + follower crash (`placement_reconcile.rs`, driving
`custos-placement`), and **leader-driven automatic** reconcile from a replicated
policy (`placement_auto_reconcile.rs` — no test-side `replan`/CAS). Use
`run_for`, never `run()` (perpetual heartbeats).
