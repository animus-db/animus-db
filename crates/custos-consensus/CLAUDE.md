# CLAUDE.md — custos-consensus

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Accord-style **leaderless transaction consensus** (ADR 0011). Each transaction
gets a unique, totally-ordered timestamp; a coordinator agrees with the replicas
on an *execution* timestamp and a *dependency* set via PreAccept → (fast path)
Commit, or PreAccept → Accept → Commit (slow path), then each replica
**executes** the transaction in agreed order against a real `StorageEngine`,
**durably** (WAL + recovery). A dead coordinator's transaction is recoverable by
another replica (**coordinator failover**, first slice). No leader. This is the
layer that will eventually give the AP data plane multi-key atomicity and a
strict serialization order.

## Entry points

- `timestamp.rs` — `Timestamp { logical, node }` (totally ordered, unique) and
  the per-node `LogicalClock` (`witness` to advance past a peer; `mint` for a
  strictly-greater fresh stamp).
- `core.rs` — `AccordCore`: a **synchronous, I/O-free** state machine mirroring
  `custos-control`'s `RaftCore`. `submit(keys)` starts a **write** transaction
  this node coordinates; `submit_read(keys)` starts a **read-only** transaction;
  `recover(txn)` takes over a stranded transaction as a *recovery coordinator*;
  `handle(from, msg)` processes an inbound message. All return `Vec<Out>` and
  never touch `Env`. Holds the replica view (`txns`), the coordinator view
  (`coordinating`), the recovery-coordinator view (`recovering`), reached
  `decisions`, `applied_order`, a `pending` buffer of `WalRecord`s, a
  `pending_apply` buffer of `ApplyEffect`s (write execution work), and a
  `pending_read` buffer of `ReadEffect`s (read execution work) — the core decides
  *order*, the driver does the *I/O*. `drain_persist`/`drain_apply`/`drain_reads`
  hand these to the driver; `recovered` rebuilds the core from a `PersistedState`
  and re-emits the apply/read effects for its recovered execution order.
- `message.rs` — `AccordMsg` (PreAccept/PreAcceptOk/Accept/AcceptOk/Commit, plus
  `Recover`/`RecoverOk` for failover), (de)serialized with `serde_json` over the
  `Network`'s `Vec<u8>` payloads. Execution/Apply is *local* — no wire message.
- `persist.rs` — `WalRecord` (PreAccepted/Accepted/Committed/Applied) and
  `PersistedState` (replay/decode/encode), mirroring `custos-control::persist`.
- `node.rs` — `AccordNode<E, S = MemoryEngine>`: the thin `Env` driver, generic
  over the `StorageEngine` backing execution (defaults to the in-memory
  `MemoryEngine`; `start_with_storage` injects another). `persist_then_ship`
  drains the core's `WalRecord`s + `ApplyEffect`s + `ReadEffect`s, appends +
  `fsync`s the records to `accord.wal`, **then** `merge`s the write effects into
  the engine (`apply_all`) and `get_at`s the read effects (`satisfy_reads`),
  then ships outbound (durable before action). `drive` recovers from the WAL on
  startup and replays the recovered execution order into the (fresh) engine. A
  plain `recv` loop — still no perpetual timers. `submit_read(keys)` runs a
  read-only transaction; `read_result(txn)` returns the per-key writer it
  observed (populated once `is_applied(txn)`). `store_writer(key)` is `async`
  (it reads the engine).

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
- **Execution effect goes to a real `StorageEngine`, not an in-core map.** The
  core emits an `ApplyEffect { txn, keys, version }` when a txn becomes
  applicable; the driver `merge`s the txn's id into each key at `version =
  execute_at.logical`. Use **`merge`, not `put`** — execution timestamps are not
  globally monotonic across keys (so `put`'s engine-wide floor would reject
  them), and `merge`'s per-key LWW is idempotent + commutative, so the
  recovery/duplicate re-apply converges. The value stored is the writer's
  `TxnId` (encoded as two big-endian u64s); `store_writer` decodes it back.
- **Read-only transactions are ordered like writes; only the effect differs.**
  `submit_read` mints a `t0`, intersects conflicting keys, and runs the same
  PreAccept/(Accept)/Commit machinery (the `read_only` flag rides on
  `PreAccept`/`Commit`/`RecoverOk` and the `PreAccepted`/`Committed` WAL
  records). At apply time the core emits a **`ReadEffect`** (not an
  `ApplyEffect`) and the driver does `StorageEngine::get_at(key, execute_at)` —
  so the read sees the writes ordered before it (lower MVCC version) and none
  after, identically on every replica. **It writes nothing.** Reads execute in
  the *same* gated `(execute_at, txn)` order as writes (`apply` branches on
  `read_only`), so a read waits for the earlier-ordered conflicting writes to
  apply before it reads — that is what makes the snapshot consistent. Driver
  ordering matters: in a single drain the writes (`apply_all`) are applied
  **before** the reads (`satisfy_reads`); across drains the read effect is only
  emitted after its earlier-ordered conflicts are `Applied`, so their write
  effects were drained no later. Don't reorder those two in the driver task.
- **The multi-thread liveness lesson is now wired here too**
  (`tests/accord_concurrent.rs`, `#[tokio::test(multi_thread)]` over `ProdEnv`,
  timeout-guarded). `SimEnv` proves order/logic, **not** real-thread liveness.
  `AccordNode` is audited clean for the deadlock class — the core lock is taken
  only to drain, then dropped; all I/O happens lock-free in a spawned task, so
  **no `std::sync::Mutex` guard is ever held across an `.await`**. Keep it that
  way: drain under the lock, drop it, then `await`. This slice has **no message
  retry** (`Network::send` is fire-and-forget), so don't pile unbounded in-flight
  work on the transport in a liveness test — that flakes on transport drops, not
  on the bug class the test targets.
- **Recovery sets phase to `Applied` when `PersistedTxn.applied`** even though
  the phase-bearing records stop at `Committed` — the separate `Applied` WAL
  record carries the executed bit. On recovery the core **re-emits the apply
  effects** for `applied_order` so the driver repopulates a *fresh, volatile*
  storage engine in the original order (the `MemoryEngine` dies on `stop`; the
  WAL is the source of truth).
- WAL replay is **order-insensitive** (per-record merge is commutative for our
  fields: `max` on timestamp/phase, union on deps, single `Committed`/`Applied`
  per txn), so the driver may flush from either `submit` or the recv loop.
- **Coordinator failover is a separate sub-protocol** (`recover` / `Recover` /
  `RecoverOk`, state in `recovering`, *not* `coordinating`). A recovery
  coordinator: (1) **adopts** any `Committed`/`Applied` decision a recovery
  quorum reports verbatim; else (2) **re-broadcasts `PreAccept` with the union
  of keys** the replies carried (so a replica that missed the original PreAccept
  learns the keys — otherwise it would execute an *empty* write) and forces the
  **slow path** (recovery never takes the fast path — see the `recovery` flag
  threaded through `advance_from_pre_accept`'s fast-path + `fast_still_possible`
  gates). Safe because any recovery (majority) quorum intersects the fast quorum
  that a fast-path commit required. **Gotcha that bit during development:** the
  recovered txn's *keys* must reach every replica, or replicas that never saw the
  original PreAccept commit with empty key sets and execute nothing — hence the
  re-broadcast. The precise Accord recovery ballot, duelling recoverers, and a
  failure detector to *trigger* recovery are out of scope (recovery is invoked
  explicitly).

## Deferred (see ADR 0011)

The full transitive dependency wait-graph (the execution wait is conflict +
timestamp based), the precise Accord recovery ballot rules + duelling recovery
coordinators + a failure detector to trigger recovery (today `recover` is called
explicitly), WAL snapshotting/log truncation (the WAL is the full per-txn
history — contrast `RaftCore`), integration with the **live** data-plane
replicas (`custos-data`) — execution (reads included) is a per-node consensus
store, not yet the shared data plane — message **retry/timeouts** (a stalled
transaction is not retried; `Network::send` is fire-and-forget), an interactive
read/write transaction API, livelock handling, sharding/placement (one global
replica set), and wiring the Elle cycle checker (`custos-test`) — now natural
since a real execution history exists. **Read-only transactions** are now
implemented (`submit_read`); see the read-transaction note above. The sync-core
boundary is where each remaining piece slots in.

## Tests

`cargo test -p custos-consensus` — unit tests on the timestamp/clock, plus three
`SimEnv` test files:

- `tests/accord_commit.rs`: single-transaction fast-path commit on all replicas,
  two conflicting transactions committing in a consistent timestamp order
  (including a 64-seed sweep), disjoint-transaction independence, trace
  reproducibility.
- `tests/accord_execute.rs` (execution + durability + storage): conflicting
  transactions **execute** into the `MemoryEngine` in a consistent order with a
  converged store (single seed + a 48-seed sweep with a slow-path third
  coordinator), a replica restarted via `Simulator::stop` recovering its executed
  state from `accord.wal` (and replaying it into a fresh engine), and
  execution-path trace reproducibility. `store_writer` is `async`; tests resolve
  it with `futures::executor::block_on` (the `MemoryEngine` awaits nothing real).
- `tests/accord_recover.rs` (coordinator failover): a coordinator stalled by a
  partition has its transaction recovered by another replica to a consistent
  commit + execution on the survivors (single seed + a 32-seed sweep), recovery
  adopting an already-committed decision verbatim (idempotent), and recovery-path
  trace reproducibility. The stall is set up by partitioning the coordinator from
  one peer so it never reaches a fast quorum while a *different* peer still
  witnessed the `PreAccept` (and its keys) — then recovery runs from the peer
  that can still reach the key-bearing replica.
- `tests/accord_read.rs` (read transactions): a read observes the write ordered
  before it and not the one ordered after; a read of an unwritten key observes
  nothing; the read snapshot is identical on every replica across a 48-seed
  sweep; a read's observation recovers from disk through a stop/restart; and the
  read path is trace-reproducible.
- `tests/accord_concurrent.rs` (**real multi-threaded**, *not* `SimEnv`):
  `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` over `ProdEnv`,
  timeout-guarded — several replicas + concurrent coordinators committing
  conflicting transactions must not deadlock/strand, and the safety property
  (consistent order + converged store) must hold under genuine parallelism. This
  is the liveness regression `SimEnv` cannot give; mirrors
  `custos-storage/tests/lsm_concurrent.rs`.

Use `run_for` for the `SimEnv` tests (the driver has no perpetual timers today,
but follow the house convention); the multi-thread test polls real time with a
timeout.
