# CLAUDE.md — custos-consensus

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Accord-style **leaderless transaction consensus** (ADR 0011). Each transaction
gets a unique, totally-ordered timestamp; a coordinator agrees with the replicas
on an *execution* timestamp and a *dependency* set via PreAccept → (fast path)
Commit, or PreAccept → Accept → Commit (slow path), then each replica
**executes** the transaction in agreed order, **durably** (WAL + recovery). No
leader. This is the layer that will eventually give the AP data plane multi-key
atomicity and a strict serialization order.

## Entry points

- `timestamp.rs` — `Timestamp { logical, node }` (totally ordered, unique) and
  the per-node `LogicalClock` (`witness` to advance past a peer; `mint` for a
  strictly-greater fresh stamp).
- `core.rs` — `AccordCore`: a **synchronous, I/O-free** state machine mirroring
  `custos-control`'s `RaftCore`. `submit(keys)` starts a transaction this node
  coordinates; `handle(from, msg)` processes an inbound message. Both return
  `Vec<Out>` and never touch `Env`. Holds the replica view (`txns`), the
  coordinator view (`coordinating`), reached `decisions`, the executed `store` +
  `applied_order`, and a `pending` buffer of `WalRecord`s. `drain_persist` hands
  the records to the driver; `recovered` rebuilds the core from a
  `PersistedState`.
- `message.rs` — `AccordMsg` (PreAccept/PreAcceptOk/Accept/AcceptOk/Commit),
  (de)serialized with `serde_json` over the `Network`'s `Vec<u8>` payloads.
  Execution/Apply is *local* — no new wire message.
- `persist.rs` — `WalRecord` (PreAccepted/Accepted/Committed/Applied) and
  `PersistedState` (replay/decode/encode), mirroring `custos-control::persist`.
- `node.rs` — `AccordNode<E>`: the thin `Env` driver. `persist_then_ship` drains
  the core's `WalRecord`s, appends + `fsync`s them to `accord.wal`, then ships
  outbound (durable before action); `drive` recovers from the WAL on startup. A
  plain `recv` loop — still no perpetual timers.

## What's non-obvious

- **All protocol logic is in the sync `AccordCore`**; the driver only does I/O.
  Keep it that way — don't reach for `Env` inside the core.
- Unlike `RaftCore`, the core takes **no `now`/`entropy`**: timestamps are
  logical (not wall-clock) and there are no timers, so determinism rests purely
  on logical clocks + `BTreeMap`/`BTreeSet` iteration order. Don't introduce a
  `HashMap`/`HashSet` (lint-enforced) or any time source.
- A node is **both coordinator and replica**: `submit` applies the transaction
  to the node's own replica state and seeds the coordinator's reply set with the
  node's own PreAcceptOk, so the coordinator counts itself toward the quorum.
- **Fast-path gating is the subtle part** (`advance_from_pre_accept`): the fast
  path needs a fast quorum agreeing on `t0` *and* reporting identical deps. Do
  not escalate to the slow path while the fast path is still reachable — but DO
  escalate once every replica has answered and the fast path didn't fire (the
  all-agree-on-`t0`-but-deps-differ case), or it stalls forever. That bug bit
  during development; the multi-seed test guards it.
- The **fast quorum is a placeholder `ceil(3N/4)`**, not Accord's exact bound.
  See ADR 0011.
- Conflict = intersecting key sets. `Key` is a bare `u64` for now (the real
  system keys by partition/range).
- **Execution gating is the subtle part** (`conflicts_clear_for` /
  `next_applicable`): a committed txn executes only once every *conflicting* txn
  that could order before it has applied. "Could order before" is judged against
  **every conflicting txn known in any phase**, not just the recorded deps and
  not only committed ones — a not-yet-committed conflict blocks because its final
  timestamp might land lower. Gating only on the deps set (or only on committed
  txns) lets two concurrent same-timestamp-but-different-node txns execute in
  arrival order and diverge across replicas; the seed sweep guards this.
- **Recovery sets phase to `Applied` when `PersistedTxn.applied`** even though
  the phase-bearing records stop at `Committed` — the separate `Applied` WAL
  record carries the executed bit. The store is rebuilt by replaying
  `applied_order` against the recovered key sets.
- WAL replay is **order-insensitive** (per-record merge is commutative for our
  fields: `max` on timestamp/phase, union on deps, single `Committed`/`Applied`
  per txn), so the driver may flush from either `submit` or the recv loop.

## Deferred (see ADR 0011)

The full transitive dependency wait-graph (the execution wait is conflict +
timestamp based), WAL snapshotting/log truncation (the WAL is the full
per-txn history — contrast `RaftCore`), coordinator failover (a *replica*
restart is recovered; a dead coordinator still strands its txn), full
data-plane/`StorageEngine` integration (the executed store is a stand-in),
timeouts/retries/livelock handling, sharding/placement (one global replica
set), and wiring the Elle cycle checker (`custos-test`) — now natural since a
real execution history exists. The sync-core boundary is where each slots in.

## Tests

`cargo test -p custos-consensus` — unit tests on the timestamp/clock, plus two
`SimEnv` test files:

- `tests/accord_commit.rs`: single-transaction fast-path commit on all replicas,
  two conflicting transactions committing in a consistent timestamp order
  (including a 64-seed sweep), disjoint-transaction independence, trace
  reproducibility.
- `tests/accord_execute.rs` (execution + durability): conflicting transactions
  **execute** in a consistent order with a converged store (single seed + a
  48-seed sweep with a slow-path third coordinator), a replica restarted via
  `Simulator::stop` recovering its executed state from `accord.wal`, and
  execution-path trace reproducibility.

Use `run_for` (the driver has no perpetual timers today, but follow the house
convention).
