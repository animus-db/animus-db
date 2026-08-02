# CLAUDE.md — animus-consensus

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
(`animus-data` quorum) — the "frontier" path — and a wired read transaction
**reads from that same data plane** (quorum read at execution time), so this is
the layer that gives the AP data plane multi-key atomicity and a strict
serialization order. Transactions may be **sharded** — a single transaction's key
set can span more than one tablet, each key's effect routed to its own tablet's
quorum (`start_with_router`) while one global execution timestamp orders all
shards. An **interactive** `begin → read → decide → write → commit` handle
(`AccordNode::begin`) runs a multi-step read-modify-write under one Accord
transaction, with the **session's reads folded into the committed transaction's
conflict set** (`submit_rw`). No leader.

## Entry points

- `timestamp.rs` — `Timestamp { logical, node }` (totally ordered, unique) and
  the per-node `LogicalClock` (`witness` to advance past a peer; `mint` for a
  strictly-greater fresh stamp). Also `Ballot { round, node }` (totally ordered:
  round then node-id) — the recovery proposal number; `Ballot::ZERO` is the
  original coordinator's, `Ballot::next_above(highest, node)` mints one strictly
  above the highest seen.
- `core.rs` — `AccordCore`: a **synchronous, I/O-free** state machine mirroring
  `animus-control`'s `RaftCore`. `submit(keys)` starts a **write** transaction
  this node coordinates; `submit_read(keys)` starts a **read-only** transaction;
  `submit_rw(read_keys, write_keys)` starts a **read-modify-write** whose conflict
  set is the union (reads ∪ writes) but whose write *effect* hits only
  `write_keys` (so a read key participates in ordering but produces no write);
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
- `message.rs` — `AccordMsg` (PreAccept/PreAcceptOk/Accept/AcceptOk/`AcceptNack`/
  Commit/`CommitAck`, plus `Recover`/`RecoverOk`/`RecoverNack` for failover),
  (de)serialized with `serde_json` over the `Network`'s `Vec<u8>` payloads.
  `CommitAck` lets the coordinator's retry tick know a replica has the `Commit`
  (otherwise fire-and-forget) so it stops re-sending. `Accept`/`Recover`/
  `RecoverOk` carry a recovery `ballot` (`#[serde(default)]` → `Ballot::ZERO`);
  `AcceptNack`/`RecoverNack` report the higher ballot a replica promised so a
  superseded recoverer learns it. Execution/Apply is *local* — no wire message.
- `persist.rs` — `WalRecord` (PreAccepted/Accepted/Committed/Applied/`Promised`)
  and `PersistedState` (replay/decode/encode), mirroring `animus-control::persist`.
  `Accepted` now carries the `accepted_ballot`, and `Promised { txn, ballot }`
  records a durable recovery-ballot promise (`PersistedTxn.promised` /
  `.accepted_ballot`) so a restarted replica does not renege.
- `node.rs` — `AccordNode<E, S = MemoryEngine>`: the thin `Env` driver, generic
  over the `StorageEngine` backing execution (defaults to the in-memory
  `MemoryEngine`; `start_with_storage` injects another). `persist_then_ship`
  drains the core's `WalRecord`s + `ApplyEffect`s + `ReadEffect`s, appends +
  `fsync`s the records to `accord.wal`, **then** `merge`s the write effects into
  the engine (`apply_all`) and `get_at`s the read effects (`satisfy_reads`),
  then ships outbound (durable before action). `drive` recovers from the WAL on
  startup and replays the recovered execution order into the (fresh) engine.
  Alongside the `recv` loop the driver now runs a **`retry_loop`** on an `Env`
  timer with **exponential backoff** (`RETRY_BASE_INTERVAL` doubling to
  `RETRY_MAX_INTERVAL`, reset on progress) — a **perpetual timer**, so drive tests
  with `run_for`/`run_until`, never `run()`. `submit_read(keys)` runs a read-only
  transaction; `submit_rw(reads, writes)` a read-modify-write (txn-id effect);
  `submit_writes(map)` / `submit_writes_rw(reads, map)` carry **arbitrary write
  values** (ADR 0011). `read_result(txn)` returns the per-key *writer id* it
  observed and `read_value_result(txn)` the **raw value bytes** (populated once
  `is_applied(txn)`). `store_writer(key)` (decoded id) / `store_value(key)` (raw
  bytes) / `current_value(key)` are `async` (they read the engine / data plane). **Frontier:**
  `start_with_data_plane(env, all_nodes, storage, coordinator_env, view)` attaches
  a `DataSink { DataClient, DataRouting::Single(TabletView) }`; on Apply a
  committed *write* effect is also pushed through the data-plane quorum
  (`DataClient::write`) so it is readable via data-plane quorum reads.
  **Sharded:** `start_with_router(.., router)` attaches `DataRouting::Sharded(
  Router)` instead, routing each key's data-plane write/read to **its own**
  tablet's `TabletView` via `Router::view_for` — so one transaction's keys can
  span multiple tablets (the Accord round agrees one global execution timestamp;
  only the effect is sharded). The data-plane coordinator uses a **distinct node
  id** (`coordinator_env`) — the inbox is single-consumer. **Data-plane reads:** with a `DataSink`,
  `satisfy_reads` issues a data-plane **quorum read** (`DataClient::read`) per key
  instead of a local `get_at`, so a read observes the same replicated state writes
  land in (recovery still re-satisfies reads from the local substrate — `None`
  sink). **Interactive API:** `begin()` returns an `InteractiveTxn`
  (`read(key).await` → current committed writer id, `read_value(key).await` → raw
  bytes, through the data plane or local store; `write(key)` buffers a txn-id
  write, `write_value(key, bytes)` an arbitrary-value write; `commit()` submits the
  session as one Accord read-modify-write via `submit_rw`/`submit_writes_rw` — so
  the **session's reads fold into the committed transaction's conflict set** and
  are ordered against conflicting writes). Pure driver state — the core stays sync
  + I/O-free, reached only at commit. `current_writer`/`current_value` are the
  ad-hoc reads it uses.

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
  core emits an `ApplyEffect { txn, keys, values, version }` when a txn becomes
  applicable; the driver `merge`s each key's value at `version =
  execute_at.logical`. Use **`merge`, not `put`** — execution timestamps are not
  globally monotonic across keys (so `put`'s engine-wide floor would reject
  them), and `merge`'s per-key LWW is idempotent + commutative, so the
  recovery/duplicate re-apply converges.
- **Arbitrary write values are additive (ADR 0011).** `ApplyEffect.values` is a
  `BTreeMap<Key, Vec<u8>>` of caller-supplied bytes; a key **absent** from it
  defaults *at the driver* to the writer's `TxnId` (two big-endian u64s — the
  classic register effect, which `store_writer` decodes back). So
  `submit`/`submit_rw` (valueless) still "write my id", while
  `submit_writes(map)` / `submit_writes_rw(reads, map)` /
  `InteractiveTxn::write_value(k, v)` write the **actual bytes**. Values flow
  through the sync core purely as data (on `ReplicaTxn.write_values`, the
  `PreAccept`/`Commit`/`RecoverOk` wire, and the `PreAccepted`/`Committed` WAL —
  `#[serde(default)]`), so the core never encodes a txn id and the values survive
  recovery + failover (recovery unions them across the quorum). Reads expose raw
  bytes: `read_value_result(txn)` / `store_value(key)` / `current_value(key)`
  return them verbatim; `read_result(txn)` / `store_writer(key)` decode them as a
  writer id for the register view. **`merge`'s per-key LWW is still the substrate**
  — concurrent writers to one key lose updates by the data model, so a list-append
  workload over this needs single-writer-per-key (see `animus-test`).
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
  bound test time with `run_for`. The interval uses **exponential backoff**
  (`RETRY_BASE_INTERVAL` doubling to `RETRY_MAX_INTERVAL`), **reset to the base on
  progress** (strictly fewer messages owed than the previous tick) or completion
  (none owed) — so a stuck round retries ever less often (fewer redundant sends)
  while a transient drop is still recovered promptly. The backoff state is a plain
  local in `retry_loop`; the core's `resend_pending` is unchanged (it only decides
  *what* is owed), so determinism is untouched.
- **Frontier execution is additive, not a replacement.** With a `DataSink`, the
  apply effect still `merge`s the writer id into the **local** engine (kept as the
  recovery substrate, and what `store_writer` reads) **and** writes it through the
  data-plane quorum (`DataClient::write`) at `version = execute_at.logical`.
  Recovery re-applies into the local engine **only** (`apply_all(.., None, ..)`) —
  the data plane already holds the committed writes durably, so a restart must not
  re-storm it. The data-plane write result is not asserted in the apply path (a
  transient quorum miss converges via the data plane's own anti-entropy); the test
  verifies through a quorum read.
- **Data-plane reads ride the execution-order gate, not a versioned snapshot.**
  With a `DataSink`, `satisfy_reads` does a data-plane **quorum read**
  (`DataClient::read`) per key — *not* a `get_at`-by-version (the data-plane wire
  has none). That is sound only because the core emits a read's `ReadEffect`
  **after** every earlier-ordered conflicting write has `Applied`, and an applied
  write was already pushed through the same quorum — so a *current* quorum read
  observes exactly the writes ordered before the read and none after. Keep the
  effect-emission gate intact: routing the read to a live quorum read would be
  unsound without it. **Recovery is the exception:** a recovered read is
  re-satisfied from the *local* engine (`None` sink in `drive`), because the local
  substrate was repopulated in execution order on recovery, whereas a live quorum
  read would reflect current (possibly newer) state. A transient quorum miss reads
  as absent and converges via anti-entropy.
- **The interactive API lives entirely in the driver.** `InteractiveTxn` is pure
  driver state (`reads`/`writes` key sets + a node clone); it never touches the
  sync core except through `submit_rw` at `commit()`. So the core stays I/O-free
  and the atomicity/ordering guarantees are exactly `submit_rw`'s. The session's
  **reads are folded into the committed transaction's conflict set** (conflict set
  = reads ∪ writes), so a concurrent write to a key the session read is ordered
  relative to the commit (the read-then-write hazard). A buffered `write(key)`
  writes the committed txn id; `write_value(key, bytes)` writes arbitrary bytes
  (ADR 0011), and `read_value(key)` returns the current raw bytes (vs `read`'s
  decoded writer id) — so an interactive read-modify-write over arbitrary values
  (e.g. list-append) works. `commit()` routes to `submit_rw` (txn-id effect) when
  no values were supplied, else `submit_writes_rw` (real values); mixing valued
  and valueless writes in one session is unsupported (debug-asserted). An empty
  write set commits to `None`.
- **`write_keys` vs `keys` (read/write transactions).** A `ReplicaTxn` carries
  both its full conflict `keys` (every key read *or* written) and the `write_keys`
  subset it writes. Conflict/ordering uses `keys`; the write `ApplyEffect` uses
  only `write_keys` (the extra read-only keys order but produce no write). Both
  ride the `PreAccept`/`Commit`/`RecoverOk` wire and the `PreAccepted`/`Committed`
  WAL (`#[serde(default)]` write-key fields), so the distinction survives recovery
  + failover (recovery unions both across the quorum). `read_only` is now exactly
  `write_keys.is_empty()` — a read-modify-write (non-empty `write_keys`) is never
  treated as read-only.
- **Sharding is in the *effect*, not the consensus.** Every transaction is still
  replicated to the whole Accord replica set and agrees one global `execute_at`
  regardless of tablet — that is why Accord is naturally multi-shard. Only the
  data-plane write/read is routed per key (`DataRouting::Sharded(Router)` →
  `Router::view_for`), so a key's effect lands in its own tablet's quorum. The
  global `execute_at` is the MVCC version on every key, so per-tablet writes stay
  consistently ordered across shards.
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
  coordinator, once a majority quorum (all having **promised its ballot**) is in,
  decides in order: (1) **adopts** any `Committed`/`Applied` decision a recovery
  quorum reports verbatim; (2) else, if any reply was **`Accept`ed** under a
  ballot, re-proposes the `(execute_at, deps)` of the reply with the **highest
  `accepted_ballot`** under its own (higher) ballot; (3) otherwise **re-broadcasts
  `PreAccept` with the union of keys** the replies carried (so a replica that
  missed the original PreAccept learns the keys — otherwise it would execute an
  *empty* write) and forces the **slow path** (recovery never takes the fast path —
  see the `recovery` flag threaded through `advance_from_pre_accept`'s fast-path +
  `fast_still_possible` gates). Safe because any recovery (majority) quorum
  intersects the fast quorum that a fast-path commit required. **Gotcha that bit
  during development:** the recovered txn's *keys* must reach the recovery quorum,
  or replicas that never saw the original PreAccept commit with empty key sets and
  execute nothing — hence the re-broadcast (and why the failover tests let the
  original PreAccept reach a quorum *before* isolating the coordinator: with N=5 a
  recovery quorum can entirely miss a single key-bearing replica).
- **Recovery ballots + duelling recoverers** (ADR 0011, the precise-ballot slice).
  `timestamp::Ballot { round, node }` is the proposal number a recovery coordinator
  runs under (totally ordered: round, then node-id tiebreak). The original
  coordinator is the implicit `Ballot::ZERO`; recoverers mint `round >= 1`. A
  replica **promises** the highest ballot it has seen for a txn (`ReplicaTxn.
  promised`, durable via `WalRecord::Promised` / `PersistedTxn.promised`) and
  **rejects** a `Recover`/`Accept` below it — replying `RecoverNack`/`AcceptNack`
  with the promised ballot. An `Accept` records its `accepted_ballot` (durable on
  the `Accepted` WAL record), which `RecoverOk` reports so step (2) above can adopt
  the most-recent prior proposal. **Public signatures stayed additive**:
  `recover(txn)` is unchanged (it mints a ballot strictly above the highest it has
  promised); the new ballot wire/WAL fields are `#[serde(default)]`. **Livelock
  gotcha that bit:** "on supersession, bump my ballot and re-broadcast *now*"
  livelocks two duelling recoverers (an unbounded same-instant message storm — it
  hung the test). `handle_superseded` instead uses a deterministic **id tiebreak**:
  the superseded recoverer only retries (higher) if its node id exceeds the
  winner's; otherwise it **stands down** and adopts the winner's `Commit`. So the
  duel converges in bounded rounds. A late original coordinator (`Ballot::ZERO`) is
  simply fenced and cannot revert the recovered decision. A real **failure
  detector** to *trigger* recovery is still out of scope (recovery is invoked
  explicitly; the ballots make *concurrent* explicit recoveries safe).

## Deferred (see ADR 0011)

The full transitive dependency wait-graph (the execution wait is conflict +
timestamp based), a **failure detector** to *trigger* recovery and a retry
*escalation* (today `recover` is called explicitly; the adaptive tick backs off
but does not itself declare a coordinator dead — recovery **ballots** now make
*concurrent* explicit recoveries safe, but nothing yet auto-detects a dead
coordinator), WAL snapshotting/log truncation (the WAL is the full per-txn history
— contrast `RaftCore`), the precise fast-path quorum bound, the precise
`PreAcceptOk`-witness *fast-path*-recovery decision (we always force the slow path
on re-proposal, and the duel converges by an id tiebreak rather than Accord's full
randomized-backoff rules), and **per-shard consensus replica sets / placement of
the consensus participants** (sharding routes only the *effect* per tablet; the
Accord replica set is still one global group). **Now implemented:** read-only
transactions, **recovery ballots + duelling recovery coordinators** (a replica
promises the highest ballot seen and fences lower `Recover`/`Accept`; superseded
recoverers converge via an id tiebreak; `RecoverOk` adopts the highest-ballot
accepted proposal — `tests/accord_recover_ballots.rs`),
(`submit_read`), **message retry with adaptive (exponential) backoff** (the
driver's retry tick + `resend_pending`), the **data-plane frontier**
(`start_with_data_plane`), **data-plane reads**, an **interactive transaction
API** (`AccordNode::begin` → `InteractiveTxn`), **sharded (multi-tablet)
transactions** (`start_with_router` — each key's effect routed to its own tablet's
quorum), **folding the interactive/RMW read set into dependency tracking**
(`submit_rw` — conflict set = reads ∪ writes), **arbitrary caller-supplied write
values** (`submit_writes`/`submit_writes_rw`/`InteractiveTxn::write_value` — the
execution effect is the supplied bytes, defaulting to the txn id when absent), and
**wiring the Elle cycle checker** (`animus-test`, ADR 0014 — now genuine
black-box). The sync-core boundary is where each remaining piece slots in.

## Tests

`cargo test -p animus-consensus` — unit tests on the timestamp/clock, plus three
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
- `tests/accord_recover_ballots.rs` (**recovery ballots + duelling recoverers**,
  5-node cluster): two recoverers racing the same txn converge to one decision on
  every replica; failover under partition then heal (the healed original cannot
  revert the recovered decision); a `Recover` racing the original coordinator's
  `Commit` (recovery adopts the existing commit, never contradicts it); recovery
  surviving message loss; a superseded recoverer not stranding the txn; trace
  reproducibility. Every test asserts cross-replica agreement and that **no
  committed decision is reverted**. The failover tests let the original PreAccept
  reach a quorum *before* isolating the coordinator (with N=5 a recovery quorum can
  miss a single key-bearing replica).
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
- `tests/accord_data_plane_read.rs` (**data-plane reads + interactive API**):
  same assembly as `accord_data_plane.rs` (Accord 0–2, coordinators 10–12, data
  replicas 3–5). A read transaction observes a prior write transaction's data
  **through the data-plane quorum** (and an unwritten key reads absent),
  consistently on every replica; an **interactive** read-modify-write commits
  atomically (both keys carry the committed txn on every replica); two conflicting
  interactive transactions are ordered consistently; an empty interactive txn is a
  no-op; plus trace reproducibility.
- `tests/accord_sharded.rs` (**sharded / multi-tablet transactions**): two
  tablets split at a key boundary (replicas {3,4} and {4,5}), Accord nodes wired
  via `start_with_router`. A cross-tablet transaction commits atomically and both
  keys are readable via the data plane (each from its own tablet) at its id; two
  conflicting cross-tablet transactions order consistently on every shard (shared
  key → second-ordered txn; each private key in the *other* tablet → its own txn);
  plus trace reproducibility.
- `tests/accord_rw_conflict.rs` (**read-set folded into deps**): a
  read-then-write transaction (`submit_rw(reads, writes)`) is ordered consistently
  against a conflicting write to the key it *read* (and the conflict is recorded as
  a dependency), a control proving the same transactions are disjoint when the read
  is dropped, a seed sweep, and trace reproducibility.
- `tests/accord_values.rs` (**arbitrary write values**): a value-carrying write's
  actual bytes land on every replica's store; two conflicting values resolve in
  agreed order (shared key → the second-ordered txn's value); a read observes the
  actual value; the value survives a stop/restart (WAL replay); a value lands in
  the data plane and is readable via a quorum read; an **interactive**
  read-modify-write reads the current value, appends, and writes the modified
  value back; a **sharded** transaction routes the real value per tablet; plus
  trace reproducibility.
- `tests/accord_backoff.rs` (**adaptive retry backoff**): a fully-partitioned
  coordinator's re-send count over a long window is far sub-linear (backoff vs the
  fixed-interval count); the transaction still converges promptly after a heal; it
  still commits everywhere under lossy-but-unpartitioned operation across a seed
  sweep; plus trace reproducibility.
- `tests/accord_concurrent.rs` (**real multi-threaded**, *not* `SimEnv`):
  `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` over `ProdEnv`,
  timeout-guarded — several replicas + concurrent coordinators committing
  conflicting transactions must not deadlock/strand, and the safety property
  (consistent order + converged store) must hold under genuine parallelism. This
  is the liveness regression `SimEnv` cannot give; mirrors
  `animus-storage/tests/lsm_concurrent.rs`.

Use `run_for`/`run_until` for the `SimEnv` tests — the driver now has a
**perpetual retry timer**, so `run()` would never return; the multi-thread test
polls real time with a timeout.
