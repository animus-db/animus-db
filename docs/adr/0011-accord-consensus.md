# ADR 0011 — Accord-style leaderless transaction consensus

- **Status:** Accepted (first minimal slice; extended with execution + durability;
  then storage-backed execution + coordinator failover)
- **Date:** 2026-08-01 (execution + durability increment: 2026-08-01;
  storage-backed execution + coordinator-failover increment: 2026-08-01)

## Context

CustosDB's data plane is leaderless and AP (ADR 0001): it serves single-key
reads/writes with tunable quorums and converges via repair/anti-entropy (ADR
0010). That gives no multi-key atomicity and no strict serialization order — the
job of a *transaction* layer. The bootstrap brief earmarks **Accord** (Apache
Cassandra's leaderless consensus) for this: a coordinator gives each transaction
a unique, globally-comparable timestamp and reaches agreement on an *execution*
timestamp and a *dependency* set in a small number of message rounds, with no
leader and no Paxos-style master.

We also have a hard constraint: determinism (ADR 0003). Every distributed
behavior must be driven through the `Env` seam so a run is byte-reproducible
from a seed, and the control-plane Raft (ADR 0009) already established the
pattern that makes this work — a **synchronous, I/O-free core** that takes
time/entropy as parameters and returns outbound messages, wrapped by a thin
`Env`-driven node driver.

Accord is large (PreAccept/Accept/Commit/Apply, the fast/slow path, recovery of
a failed coordinator, the dependency wait-graph for execution, durability). The
risk in a single milestone is overreach. We want a *real* increment whose
correctness we can demonstrate, not a broad sketch.

## Decision

Implement a **first, minimal slice** of Accord in `custos-consensus`, mirroring
the control-plane Raft architecture:

- `core::AccordCore` is a **synchronous, I/O-free** state machine. It holds the
  per-node logical clock, the replica view of every transaction it has heard of
  (keys, best-known execution timestamp, dependencies, phase), and the
  coordinator view of the transactions it owns. Its entry points (`submit`,
  `handle`) return `Vec<Out>` outbound messages and never touch `Env`. Unlike
  `RaftCore` it needs no `now`/`entropy` parameters: timestamps are *logical*
  (Lamport `(logical, node)` pairs, totally ordered and unique), and there are
  no timers in this slice, so the only nondeterminism that could arise —
  iteration order — is excluded by using `BTreeMap`/`BTreeSet` throughout.
- `node::AccordNode<E>` is the thin `Env` driver: a plain `recv` loop that
  decodes messages, feeds the core, and ships the core's outbound messages.
  `submit` mints `t0` and ships the initial `PreAccept` burst.

The protocol implemented is the **happy path plus the slow path**:

- **PreAccept**: the coordinator mints `t0` and broadcasts it with the
  transaction's key set. Each replica witnesses `t0`, records the transaction's
  keys, and replies with (a) the timestamp it proposes — `t0` unless a
  conflicting transaction already sits at a higher execution timestamp, in which
  case it mints a strictly higher one — and (b) the conflicting transactions it
  has seen (the new transaction's dependencies). Conflict = intersecting key
  sets.
- **Fast path**: if a fast quorum returns `t0` unchanged *and* identical deps,
  the coordinator commits at `t0` in one round trip (PreAccept → Commit).
- **Slow path**: otherwise (a replica bumped the timestamp, or deps disagree)
  the coordinator picks the highest returned timestamp, unions the deps, runs an
  **Accept** round to install that `(execute_at, deps)` on a simple-majority
  quorum, then commits.
- **Commit**: the coordinator broadcasts the agreed `(execute_at, deps)`; each
  replica records it as the final order.

This is enough to demonstrate the property that matters: under deterministic
simulation a small replica set commits a transaction on every replica at the
same execution timestamp, and two conflicting transactions commit in a
**consistent timestamp order on all replicas** — the later one carrying the
earlier one as a dependency.

## Consequences

- We have a real, deterministic, seed-reproducible Accord increment that fits
  the established sync-core / `Env`-driver shape, so it slots into the simulator
  exactly like the control plane. Tests live in
  `custos-consensus/tests/accord_commit.rs` (single-txn fast-path commit;
  conflicting-txn consistent order, including a 64-seed sweep; disjoint-txn
  independence; trace reproducibility).
- The fast-path quorum is a **conservative `ceil(3N/4)`** placeholder, not
  Accord's exact tight bound (`f + ⌊(f+1)/2⌋` over `2f+1` replicas). For the
  tested N=3 this is all 3 replicas; the slice proves the *mechanism*, and the
  precise bound is deferred.
## Execution + durability increment (2026-08-01)

The second increment takes the slice from "commits a timestamp + deps" to
"executes durably", without reshaping the sync-core / `Env`-driver split:

- **Execution / Apply.** Once a transaction commits, the replica *executes* it,
  but only in agreed order. A committed transaction is *applicable* when every
  **conflicting** transaction this replica knows of (intersecting key set, any
  phase) that could order before it has already applied — specifically: every
  conflict not yet committed blocks (its final timestamp is still unknown and
  might land lower), and every committed conflict ordered before it
  (`(execute_at, txn)` total order) blocks until it has applied. Applicable
  transactions drain smallest-`(execute_at, txn)`-first. The effect is an opaque
  op — "write your id to each key you touch" — against a tiny in-memory
  key→last-writer store. Because the order is total and every replica converges
  to the same committed `(execute_at)` for every transaction, **all replicas
  execute conflicting transactions in the same order** and their stores
  converge. This is the execution-time wait condition; the broader transitive
  wait-graph and recovery of a dependency closure remain future work.
- **Durability / recovery.** `AccordCore` now emits `WalRecord`s
  (`PreAccepted` / `Accepted` / `Committed` / `Applied`) into a `pending` buffer
  at each phase transition, mirroring `RaftCore`. `AccordNode` drains them,
  appends + `fsync`s them to a per-node `accord.wal` on the `Env` disk **before**
  shipping the messages that depend on them, and on startup replays the WAL
  (`PersistedState`) into `AccordCore::recovered`. A stopped-and-restarted
  replica recovers its committed/executed transactions, its execution order, and
  its store. The WAL is the full per-transaction history — **snapshotting / log
  truncation is deferred** (contrast `RaftCore`, which compacts). Replay is
  order-insensitive (the per-record merge is commutative for our fields), so the
  driver may flush from either `submit` or the recv loop.

Tests added in `custos-consensus/tests/accord_execute.rs`: conflicting
transactions execute in a consistent order with a converged store (single seed +
a 48-seed sweep including a slow-path third coordinator), replica restart
recovers executed state from disk, and execution-path trace reproducibility.

- **Still deliberately deferred** (each a substantial follow-up):
  - **The full dependency wait-graph**: the execution wait condition above is
    conflict-and-timestamp based; the transitive dependency closure and its
    recovery are not implemented.
  - **WAL snapshotting / log truncation**: the WAL grows with the transaction
    count; no compaction yet.
  - **Contention / livelock handling**, **timeouts and retries** (the slice
    assumes a reliable enough network to gather a quorum; lost messages can stall
    a transaction), and **sharding/placement** (one global replica set — every
    transaction goes to every node; no tablet/partition routing yet).
  - **The Elle cycle checker** (`custos-test`) is *not* yet wired to this path;
    now that a real execution history exists, wiring it is the natural next step.
- The clean sync-core boundary held: execution and durability slotted in behind
  the existing `handle`/`submit` entry points and the `Env`-driver, exactly as
  the first slice anticipated.
- ADR 0001's two-plane split is unchanged; this layer sits above the data plane
  and does not alter the control plane.

## Storage-backed execution + coordinator failover increment (2026-08-01)

This increment swaps the stand-in execution store for a real `StorageEngine`
and adds a first slice of coordinator failover, again without reshaping the
sync-core / `Env`-driver split.

- **Storage-backed execution.** Execution still happens in the same agreed
  `(execute_at, txn)` order, but the *effect* is now applied to a real (async)
  `custos-storage::StorageEngine` instead of an opaque in-core map. The sync
  `AccordCore` keeps deciding the order: when a transaction becomes applicable it
  pushes an `ApplyEffect { txn, keys, version }` into a `pending_apply` buffer
  (`version` is the transaction's `execute_at.logical`); the `AccordNode<E, S>`
  driver drains it and `merge`s the transaction's id into each key it touches at
  that MVCC version. `merge` (per-key last-writer-wins) is the right primitive:
  the execution timestamps are not globally monotonic across keys, and `merge`
  bypasses the engine-wide monotonic floor while staying idempotent and
  commutative — so a re-apply on recovery (the driver replays the recovered
  execution order into a *fresh*, volatile engine) converges to the identical
  store. The node defaults to the in-memory `MemoryEngine` used under simulation;
  `AccordNode::start_with_storage` accepts any engine. The crate now depends on
  `custos-storage` (no cycle — storage does not depend on consensus).

- **Coordinator failover (first slice).** A coordinator that dies after
  PreAccept/Accept but before the replicas learn the `Commit` no longer strands
  its transaction. Any replica can take over as a **recovery coordinator**
  (`AccordCore::recover` / `AccordNode::recover`): it broadcasts a `Recover`
  query, replicas reply `RecoverOk` with their recorded `(phase, execute_at,
  deps, keys)` (witnessing the transaction as a fresh `PreAccepted` if they had
  never seen it), and once a simple-majority recovery quorum is in the recovery
  coordinator decides:
  - if any quorum replica reports `Committed`/`Applied`, it **adopts that
    decision verbatim** (a committed value is immutable);
  - otherwise it **re-drives the slow path**: it re-broadcasts `PreAccept`
    carrying the *union of keys* the replies reported (so a replica that missed
    the original PreAccept learns the keys and can execute the write), then —
    being a recovery coordinator — *never* takes the fast path, picks the highest
    proposed `execute_at`, unions the deps, and runs `Accept` → `Commit`.

  **Why this is safe (the simplification we accept).** A fast-path commit at
  `t0` requires a *fast quorum* (`ceil(3N/4)` here) to have agreed on `t0` and
  deps. Any recovery quorum (a simple majority) intersects that fast quorum, so
  if the original committed on the fast path at least one recovery replica
  carries `t0`/deps; forcing the slow path with the max-ts/union-deps rule can
  only reproduce or supersede that, never contradict an *already-committed*
  value (which we adopt outright). We deliberately do **not** implement Accord's
  precise recovery (the `PreAcceptOk`-witness/superseding-ballot rules, recovery
  ballots, and duelling recovery coordinators) — concurrent recovery
  coordinators for the same transaction, and a real *failure detector* to
  *trigger* recovery, are out of scope; recovery is invoked explicitly (e.g. by a
  test).

  Tests in `custos-consensus/tests/accord_recover.rs`: a stalled/partitioned
  coordinator's transaction recovered to a consistent commit + execution on the
  survivors (single seed + a 32-seed sweep), recovery adopting an
  already-committed decision verbatim (idempotent), and recovery-path trace
  reproducibility. The existing `accord_execute.rs` tests now assert against the
  real `StorageEngine` (a recovered replica re-applies its execution order into a
  fresh `MemoryEngine`).

- **Still deferred after this increment:** the full transitive dependency
  wait-graph, the precise Accord recovery ballot rules + duelling recoverers + a
  failure detector to trigger recovery, WAL snapshotting / log truncation,
  integration with the *live* data-plane replicas (`custos-data`) and read
  transactions (execution is backed by a per-node consensus store, not yet the
  shared data plane), contention/livelock handling and timeouts/retries, and
  sharding/placement (one global replica set). The Elle cycle checker is still
  unwired.
