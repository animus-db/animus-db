# CLAUDE.md — custos-consensus

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

Accord-style **leaderless transaction consensus** (ADR 0011). Each transaction
gets a unique, totally-ordered timestamp; a coordinator agrees with the replicas
on an *execution* timestamp and a *dependency* set via PreAccept → (fast path)
Commit, or PreAccept → Accept → Commit (slow path), then each replica
**executes** the transaction in agreed order against a real `StorageEngine`,
**durably** (WAL + recovery). A dead coordinator's transaction is recoverable by
another replica (**coordinator failover**, first slice). Dropped messages are
**retried** on a driver timer so a lossy network does not strand a transaction.
A committed transaction's write effect can land in the **replicated data plane**
(`custos-data` quorum) — the "frontier" path — so this is the layer that gives
the AP data plane multi-key atomicity and a strict serialization order. No
leader.

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
  `resend_pending()` recomputes the outbound messages still owed for every
  in-flight round (PreAccept/Accept/Commit/Recover) and to whom — the driver's
  retry tick calls it (message retry).
- `message.rs` — `AccordMsg` (PreAccept/PreAcceptOk/Accept/AcceptOk/Commit/
  `CommitAck`, plus `Recover`/`RecoverOk` for failover), (de)serialized with
  `serde_json` over the `Network`'s `Vec<u8>` payloads. `CommitAck` lets the
  coordinator's retry tick know a replica has the `Commit` (otherwise
  fire-and-forget) so it stops re-sending. Execution/Apply is *local* — no wire
  message.
- `persist.rs` — `WalRecord` (PreAccepted/Accepted/Committed/Applied) and
  `PersistedState` (replay/decode/encode), mirroring `custos-control::persist`.
- `node.rs` — `AccordNode<E, S = MemoryEngine>`: the thin `Env` driver, generic
  over the `StorageEngine` backing execution (defaults to the in-memory
  `MemoryEngine`; `start_with_storage` injects another). `persist_then_ship`
  drains the core's `WalRecord`s + `ApplyEffect`s + `ReadEffect`s, appends +
  `fsync`s the records to `accord.wal`, **then** `merge`s the write effects into
  the engine (`apply_all`) and `get_at`s the read effects (`satisfy_reads`),
  then ships outbound (durable before action). `drive` recovers from the WAL on
  startup and replays the recovered execution order into the (fresh) engine.
  Alongside the `recv` loop the driver now runs a **`retry_loop`** on an `Env`
  timer (`RETRY_INTERVAL`) — a **perpetual timer**, so drive tests with
  `run_for`/`run_until`, never `run()`. `submit_read(keys)` runs a read-only
  transaction; `read_result(txn)` returns the per-key writer it observed
  (populated once `is_applied(txn)`). `store_writer(key)` is `async` (it reads
  the engine). **Frontier:** `start_with_data_plane(env, all_nodes, storage,
  coordinator_env, view)` attaches a `DataSink { DataClient, TabletView }`; on
  Apply a committed *write* effect is also pushed through the data-plane quorum
  (`DataClient::write`) so it is readable via data-plane quorum reads. The
  data-plane coordinator uses a **distinct node id** (`coordinator_env`) — the
  inbox is single-consumer.

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
  way: drain under the lock, drop it, then `await`. The driver's retry tick keeps
  the same discipline: it locks only to call `resend_pending()`, drops the lock,
  then ships via `persist_then_ship` (lock-free in a spawned task).
- **Message retry lives in the driver; the core only decides what to re-send.**
  `resend_pending()` (sync, I/O-free) recomputes the still-owed outbound for each
  in-flight round to peers that have not answered: PreAccept → `replies` absent;
  Accept → `replies` absent (using the `chosen` `(execute_at, deps)`); Done →
  `Commit` to peers absent from `commit_acks`; recovering → `Recover` to
  `replies` absent. `Coordinating` now carries `keys` / `chosen` / `commit_acks`
  to rebuild those messages. Replicas reply `CommitAck` so a committed coordinator
  knows when to stop re-sending `Commit`. Re-sends are idempotent at the replica
  (handlers fold by `max`/union, de-dup), and a completed round emits nothing, so
  retries stop on their own. The retry timer is a real `Env` timer — a perpetual
  timer — so it is deterministic and the run stays seed-reproducible, but you must
  bound test time with `run_for`.
- **Frontier execution is additive, not a replacement.** With a `DataSink`, the
  apply effect still `merge`s the writer id into the **local** engine (kept as the
  recovery substrate, and what `store_writer` reads) **and** writes it through the
  data-plane quorum (`DataClient::write`) at `version = execute_at.logical`.
  Recovery re-applies into the local engine **only** (`apply_all(.., None, ..)`) —
  the data plane already holds the committed writes durably, so a restart must not
  re-storm it. The data-plane write result is not asserted in the apply path (a
  transient quorum miss converges via the data plane's own anti-entropy); the test
  verifies through a quorum read. Read-only transactions are **not** routed to the
  data plane yet (no `get_at`-by-version on the data-plane wire) — they still use
  the local engine snapshot.
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
history — contrast `RaftCore`), **data-plane reads** (a read txn still uses the
local engine's `get_at` snapshot — the data-plane wire has no
historical-by-version read), **sharded** transactions whose key set spans more
than one tablet (the frontier routes one `TabletView`), an interactive
read/write transaction API, livelock handling, an **adaptive** retry timeout /
backoff (today the retry is a fixed-interval re-send and does not itself detect a
dead coordinator — that is still the explicit `recover` path), and wiring the
Elle cycle checker (`custos-test`). **Now implemented:** read-only transactions
(`submit_read`), **message retry** (the driver's retry tick + `resend_pending`),
and the **data-plane frontier** (`start_with_data_plane` — committed writes land
in the `custos-data` quorum, readable via quorum reads). The sync-core boundary
is where each remaining piece slots in.

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
- `tests/accord_retry.rs` (**message retry**): under a **lossy** network (an
  independent per-message drop probability via `NetConfig::set_drop_prob`) a
  single transaction, two conflicting transactions, and a seed sweep all still
  commit and execute in a consistent order — the retry tick re-drives dropped
  messages. Plus retry-path trace reproducibility.
- `tests/accord_data_plane.rs` (**the frontier**): assembles Accord coordinators
  (ids 0–2) + per-node data-plane coordinators (10–12) + `serve_replica`
  data-plane replicas (3–5) + a verifier `DataClient` (20). A **multi-key**
  transaction's writes are readable via data-plane quorum reads at its id (and an
  untouched key is absent); two conflicting multi-key transactions land
  **atomically and in a consistent order** (shared key → second-ordered txn; each
  private key → its own txn). Plus the stored-value encoding guard and frontier
  trace reproducibility.
- `tests/accord_concurrent.rs` (**real multi-threaded**, *not* `SimEnv`):
  `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` over `ProdEnv`,
  timeout-guarded — several replicas + concurrent coordinators committing
  conflicting transactions must not deadlock/strand, and the safety property
  (consistent order + converged store) must hold under genuine parallelism. This
  is the liveness regression `SimEnv` cannot give; mirrors
  `custos-storage/tests/lsm_concurrent.rs`.

Use `run_for`/`run_until` for the `SimEnv` tests — the driver now has a
**perpetual retry timer**, so `run()` would never return; the multi-thread test
polls real time with a timeout.
