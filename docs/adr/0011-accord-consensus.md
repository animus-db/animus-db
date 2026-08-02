# ADR 0011 — Accord-style leaderless transaction consensus

- **Status:** Accepted (first minimal slice; extended with execution + durability;
  then storage-backed execution + coordinator failover; then read transactions +
  multi-thread liveness regression; then message retry + the data-plane frontier;
  then data-plane reads + an interactive transaction API; then sharded
  transactions + read-set dependency folding + adaptive retry backoff; then
  arbitrary caller-supplied write values)
- **Date:** 2026-08-01 (execution + durability increment: 2026-08-01;
  storage-backed execution + coordinator-failover increment: 2026-08-01;
  read-transactions + multi-thread-liveness increment: 2026-08-02;
  message-retry + data-plane-frontier increment: 2026-08-02;
  data-plane-reads + interactive-transaction-API increment: 2026-08-02;
  sharded-transactions + read-set-folding + adaptive-backoff increment:
  2026-08-02; arbitrary-write-values increment: 2026-08-02)

## Context

AnimusDB's data plane is leaderless and AP (ADR 0001): it serves single-key
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

Implement a **first, minimal slice** of Accord in `animus-consensus`, mirroring
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
  `animus-consensus/tests/accord_commit.rs` (single-txn fast-path commit;
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

Tests added in `animus-consensus/tests/accord_execute.rs`: conflicting
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
  - **The Elle cycle checker** (`animus-test`) is *not* yet wired to this path;
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
  `animus-storage::StorageEngine` instead of an opaque in-core map. The sync
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
  `animus-storage` (no cycle — storage does not depend on consensus).

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

  Tests in `animus-consensus/tests/accord_recover.rs`: a stalled/partitioned
  coordinator's transaction recovered to a consistent commit + execution on the
  survivors (single seed + a 32-seed sweep), recovery adopting an
  already-committed decision verbatim (idempotent), and recovery-path trace
  reproducibility. The existing `accord_execute.rs` tests now assert against the
  real `StorageEngine` (a recovered replica re-applies its execution order into a
  fresh `MemoryEngine`).

- **Still deferred after this increment:** the full transitive dependency
  wait-graph, the precise Accord recovery ballot rules + duelling recoverers + a
  failure detector to trigger recovery, WAL snapshotting / log truncation,
  integration with the *live* data-plane replicas (`animus-data`) and read
  transactions (execution is backed by a per-node consensus store, not yet the
  shared data plane), contention/livelock handling and timeouts/retries, and
  sharding/placement (one global replica set). The Elle cycle checker is still
  unwired.

## Read transactions + multi-thread liveness increment (2026-08-02)

This increment adds **read-only transactions** and hardens the driver's liveness
with a real multi-threaded regression test — again without reshaping the
sync-core / `Env`-driver split.

- **Read-only transactions.** A read transaction is **ordered exactly like a
  write**: it mints a `t0`, intersects conflicting keys (so it carries the
  conflicting writes as dependencies), and is committed at an agreed
  `execute_at` via the same PreAccept/(Accept)/Commit machinery. Only its
  *execution effect* differs. When it becomes applicable (every earlier-ordered
  conflict has applied), the sync `AccordCore` emits a `ReadEffect { txn, keys,
  version = execute_at }` into a `pending_read` buffer instead of an
  `ApplyEffect`, and **writes nothing**. The `AccordNode` driver drains it and
  performs an async `StorageEngine::get_at(key, version)` for each key, so the
  read observes the value as of its execution timestamp — i.e. the write of
  every transaction ordered **before** it (which executed at a strictly lower
  MVCC version) and **none** ordered after. Because every replica converges to
  the same committed order, the read observes the **same** snapshot on every
  replica. The read/write nature is carried on `PreAccept`/`Commit`/`RecoverOk`
  and the `PreAccepted`/`Committed` WAL records, so it survives recovery (a
  recovered read re-runs its `get_at` in the recovered execution order). The
  observed per-key writer ids are exposed via `AccordNode::read_result(txn)`.
  Tests in `animus-consensus/tests/accord_read.rs` (under `SimEnv`): a read
  observes the write ordered before it and not the one after; a read of an
  unwritten key observes nothing; the read snapshot is identical on every
  replica across a 48-seed sweep; the read result recovers from disk; and the
  read path is trace-reproducible.

- **Multi-thread liveness regression (race audit).** The deterministic
  single-threaded `SimEnv` proves ordering/logic but not real-thread liveness —
  it cannot surface a `std::sync::Mutex` guard held across an `.await` or a
  stranded waker handoff (the class that bit the storage WAL group-commit). We
  audited `AccordNode` for that class and **found no bug**: the driver takes the
  core lock only briefly to drain (`drain_persist`/`drain_apply`/`drain_reads`),
  drops it, and does all I/O lock-free inside a spawned task — no guard is held
  across any `.await`, and there is no custom future/waker. To lock that in,
  `animus-consensus/tests/accord_concurrent.rs` drives several Accord replicas
  over the **real multi-threaded `ProdEnv`** (tokio multi-thread runtime + real
  TCP + real disk) with multiple coordinators concurrently committing conflicting
  transactions, guarded by `tokio::time::timeout` so a strand fails loudly
  instead of hanging. It also re-asserts the safety property under genuine
  parallelism (consistent execution order + converged store). The test
  deliberately keeps per-round conflict depth bounded and drives each round to
  completion, because this slice has **no message retry** (`Network::send` is
  fire-and-forget) — a transport drop is a deferred limitation, not the
  mutex/waker liveness bug the test targets.

- **Still deferred** (unchanged, plus): multi-key read *snapshot atomicity* in
  the presence of concurrent commits to different keys of the read set is only
  as strong as the per-key `(execute_at, txn)` ordering gives (sufficient for
  the conflict-set the read declares); a richer interactive read/write
  transaction API, and message retry/timeouts (a stalled transaction is not yet
  retried).

## Message retry + data-plane frontier increment (2026-08-02)

This increment closes the two remaining transport/integration gaps the earlier
slices named as deferred — **message retry/timeouts** and **live data-plane
integration** — again without reshaping the sync-core / `Env`-driver split.

- **Message retry / timeouts.** `Network::send` is fire-and-forget and may drop,
  which previously could strand a transaction (a coordinator blocked on a quorum
  reply that never arrives, or a replica that never learns the `Commit`). The
  driver now runs a periodic retry tick on an `Env` timer; the synchronous core
  exposes `AccordCore::resend_pending()`, which **recomputes** the outbound
  messages still owed for every in-flight round and to whom — a coordinating txn
  in PreAccept re-sends `PreAccept` to peers absent from its reply set; in Accept,
  `Accept`; once `Done` (committed), `Commit` to peers that have not yet
  acknowledged it; and a recovering txn re-sends `Recover`. Retries are
  idempotent at the replica (every handler folds by `max`/union and de-dups), and
  a completed round emits nothing so retries stop on their own. To know when a
  `Commit` has landed (it was otherwise fire-and-forget), a replica now replies
  `CommitAck` and the coordinator tracks acked peers. The core stays I/O-free —
  it only decides *what* to re-send; the driver (no lock held across the
  `.await`) does the I/O. Determinism is preserved: the retry timer is an `Env`
  timer and the run remains byte-reproducible from its seed. Tests in
  `animus-consensus/tests/accord_retry.rs` inject a **lossy** network (an
  independent per-message drop probability) and assert a transaction — and two
  conflicting transactions, across a seed sweep — still commit and execute in a
  consistent order on every replica.

- **The data-plane frontier — Accord over the replicated data plane.** A
  committed transaction's *write effect* can now land in the leaderless AP **data
  plane** (`animus-data`), not only a per-node consensus store. An `AccordNode`
  started via `AccordNode::start_with_data_plane(env, all_nodes, storage,
  coordinator_env, view)` carries a `DataSink { DataClient, TabletView }`: on
  Apply, for each key a committed *write* transaction touches, the node writes the
  transaction's id through the data-plane **quorum** coordinator
  (`DataClient::write`) to the tablet's replica set, stamped with the
  transaction's execution timestamp as the MVCC version. Because every replica
  executes the same committed effect at the same version and the data plane
  reconciles by per-key last-writer-wins, the data plane converges to a single
  writer per key in the agreed order. Those writes are then **readable via
  ordinary data-plane quorum reads** — the transaction's atomic, ordered effect
  made durable across the replicated AP store. The local `StorageEngine` is kept
  as the per-node recovery substrate (and recovery re-applies into it only, since
  the data plane already holds the committed writes durably); the sink is purely
  additive. **No dependency cycle:** `animus-consensus` depends on `animus-data`,
  which does not depend on consensus. The node inbox is single-consumer, so the
  data-plane coordinator runs on a **distinct node id** from the Accord replica.
  Tests in `animus-consensus/tests/accord_data_plane.rs` assemble Accord
  coordinators + data-plane replicas under `SimEnv` and prove: a **multi-key**
  transaction's writes are all readable via quorum reads at its id; an untouched
  key is absent; and two conflicting multi-key transactions land **atomically and
  in a consistent order** (the shared key carries the second-ordered txn; each
  txn's private keys all carry that same txn — no torn write set).

- **Still deferred after this increment:** wiring data-plane *reads* into Accord
  (a read transaction still executes against the local engine's `get_at`
  snapshot — the data plane has no historical/`get_at`-by-version read on the
  wire yet); **sharded** transactions whose key set spans more than one tablet
  (the frontier routes through a single `TabletView`); an interactive
  begin/read/write/commit transaction API; the full transitive dependency
  wait-graph; the precise Accord recovery ballot + duelling recoverers + a
  failure detector to *trigger* both recovery and retry escalation; WAL
  snapshotting / log truncation; and the precise fast-path quorum bound. The
  retry tick is a fixed-interval re-send, not an adaptive timeout or a backoff,
  and it does not itself *detect* a dead coordinator (that remains the explicit
  `recover` path). The Elle cycle checker is still unwired.

## Data-plane reads + interactive transaction API increment (2026-08-02)

This increment closes the two data-plane/UX gaps the frontier slice named — a
read transaction reading a *private local snapshot* rather than the replicated
data plane, and the lack of an *interactive* transaction API — again without
reshaping the sync-core / `Env`-driver split.

- **Reads through the data plane.** When an `AccordNode` is wired to the data
  plane (`start_with_data_plane`), a committed read-only transaction now observes
  the **replicated data plane** at execution time — the same store committed
  *write* transactions land in — instead of the per-node local engine. The sync
  core is untouched: it still orders the read like a write and emits a
  `ReadEffect` when the read becomes applicable. Only the driver's `satisfy_reads`
  changed: with a `DataSink` it issues a data-plane **quorum read**
  (`DataClient::read`) for each key rather than a local `get_at`. This is correct
  *because* of the existing execution-order gate — the core emits the read effect
  only once every earlier-ordered conflicting write has `Applied`, and an applied
  write's effect was already pushed through the same data-plane quorum, so a
  *current* quorum read at execution time observes exactly the writes ordered
  before the read and none after. The data-plane wire still carries **no**
  historical `get_at`-by-version read; we rely on the ordering gate, not a
  versioned snapshot, which is why a *recovered* read is still re-satisfied from
  the local recovery substrate (the local engine was repopulated in execution
  order on recovery; a live quorum read would instead reflect current, possibly
  newer, state). A transient quorum miss reads as absent and converges via the
  data plane's own anti-entropy. Tests in
  `animus-consensus/tests/accord_data_plane_read.rs` assemble Accord + data-plane
  replicas and prove a read transaction observes a prior write transaction's data
  through the quorum (and an unwritten key reads as absent), consistently on every
  replica.

- **Interactive transaction API.** A `begin → read* → write* → commit` handle
  (`AccordNode::begin` → `InteractiveTxn`) lets a caller run a multi-step
  read-modify-write under **one** Accord transaction instead of submitting a
  pre-baked op set. `read(key)` returns the current committed writer of a key
  (through the data plane when wired, else the local store) so the caller can
  *decide*; `write(key)` buffers a write; `commit()` submits the buffered write
  set as a single Accord write transaction via the existing `submit` entry point —
  agreed, ordered, and applied atomically at one execution timestamp, so
  conflicting interactive transactions are ordered consistently on every replica
  and each lands all-or-nothing. The **core stays sync + I/O-free**: the handle is
  pure driver state and reaches the core only through `submit` at commit time.
  Tests prove an interactive read-modify-write commits atomically and two
  conflicting interactive transactions are ordered consistently across replicas
  (`accord_data_plane_read.rs`).

- **Deliberately scoped this slice.** The interactive session's *reads* inform
  the commit decision but are **not** yet serialized into the committed
  transaction's conflict/dependency set — full Accord read/write transactions in a
  single round (where the read set also carries dependencies, against
  read-set-vs-write-set conflicts) are the natural next step. The committed write
  effect is still "write my id" (the standard execution effect), not an arbitrary
  caller-supplied value.

- **Still deferred after this increment:** **sharded** (multi-tablet)
  transactions — the frontier still routes a single `TabletView`, so a
  transaction's key set must live in one tablet; folding the interactive read set
  into the transaction's dependency tracking (full read/write transactions in one
  round); an **adaptive** retry timeout / backoff (the retry tick is still a
  fixed-interval re-send and does not itself detect a dead coordinator — that
  remains the explicit `recover` path); the full transitive dependency wait-graph;
  the precise Accord recovery ballot + duelling recoverers + a failure detector;
  WAL snapshotting / log truncation; the precise fast-path quorum bound; and
  wiring the Elle cycle checker.

## Sharded transactions + read-set folding + adaptive backoff increment (2026-08-02)

This increment closes the three depth gaps the previous slice named — **sharded
(multi-tablet) transactions**, **folding the interactive read set into dependency
tracking**, and an **adaptive retry backoff** — again without reshaping the
sync-core / `Env`-driver split.

- **Sharded (multi-tablet) transactions.** A transaction whose key set spans more
  than one tablet/replica-set is now coordinated across the involved shards.
  Accord is naturally multi-shard, and that falls out cleanly from this slice's
  shape: the consensus round already replicates *every* transaction to the whole
  Accord replica set and agrees **one global execution timestamp** and one
  dependency set, independent of which tablets the keys live in — so the
  agreement is already correct across shards. The only place sharding shows up is
  the *execution effect*: each key must be written to (and read from) the quorum
  of **its own** tablet. The `AccordNode`'s data-plane sink therefore now routes
  per key: `start_with_router(env, all_nodes, storage, coordinator_env, router)`
  attaches a `DataRouting::Sharded(Router)` (an `animus-data::Router` over a
  multi-tablet map) instead of a single `DataRouting::Single(TabletView)`, and the
  apply/read paths resolve each key's `TabletView` via `Router::view_for` before
  issuing the quorum write/read. Because the agreed execution timestamp is the
  MVCC version on every key regardless of tablet, the per-tablet writes stay
  consistently ordered across shards; a conflicting cross-tablet transaction lands
  the shared key's winner and each private key's own transaction in the same order
  on every shard. `start_with_data_plane` (single-tablet frontier) is retained
  unchanged. The Accord protocol traffic and the data-plane coordinator's quorum
  replies still use distinct node ids (single-consumer inbox). Tests in
  `animus-consensus/tests/accord_sharded.rs` (two tablets split at a key boundary,
  replicas {3,4} and {4,5}): a 2-tablet transaction commits atomically and both
  keys are readable via the data plane in agreed order; a conflicting cross-tablet
  transaction orders consistently on all shards.

- **Folding the interactive read set into dependency tracking.** A
  read-modify-write transaction now declares a **conflict set = reads ∪ writes**.
  The sync core grows a `ReplicaTxn.write_keys` (the subset of `keys` it writes);
  `keys` is the full conflict set (every key read *or* written). `submit_rw(read_
  keys, write_keys)` mints a `t0`, conflicts on the union, and commits at an agreed
  `execute_at` exactly like a write — but at execution only the `write_keys` carry
  the write `ApplyEffect`; the extra read-only keys order the transaction (and
  carry it as a dependency to a later conflicting write) but produce no write. So
  a concurrent write to a key the transaction *read* is ordered relative to it
  (the read-then-write hazard) and recorded as a dependency, identically on every
  replica. The write set / conflict set ride the `PreAccept`/`Commit`/`RecoverOk`
  wire messages and the `PreAccepted`/`Committed` WAL records (new
  `write_keys` fields, `#[serde(default)]` for forward-compat), so the
  distinction survives recovery and coordinator failover (recovery unions both the
  conflict keys and the write keys across the recovery quorum). `read_only` is now
  exactly "writes nothing" (`write_keys.is_empty()`). `InteractiveTxn::commit`
  submits via `submit_rw(reads, writes)`, so an interactive session's reads fold
  into the committed transaction's dependency tracking (previously they merely
  informed the caller's decision). Tests in
  `animus-consensus/tests/accord_rw_conflict.rs`: a read-then-write transaction is
  ordered consistently against a conflicting write to the key it read (with the
  conflict recorded as a dependency), a control showing the same transactions are
  disjoint when the read is dropped, a seed sweep, and trace reproducibility.

- **Adaptive retry backoff.** The driver's retry tick replaces its fixed
  interval with **exponential backoff**: the wait starts at a base interval and
  doubles (capped) each round in which the same-or-more messages are still owed,
  and resets to the base the moment a round makes progress (strictly fewer
  messages owed — a reply got through) or completes (none owed). So a transaction
  that genuinely cannot gather a quorum is retried ever less often — far fewer
  redundant sends under persistent loss — while a transient drop is still
  recovered promptly at the base interval. The backoff state is a plain local in
  the driver's `retry_loop`; the timer is still a deterministic `Env` timer (the
  core's `resend_pending` is unchanged — it only decides *what* is owed), so the
  run stays byte-reproducible. Tests in
  `animus-consensus/tests/accord_backoff.rs`: a fully-partitioned coordinator's
  re-send count over a long window is far sub-linear (backoff, vs the
  fixed-interval count), the transaction still converges promptly after a heal,
  and it still commits everywhere under lossy-but-unpartitioned operation across a
  seed sweep.

- **Still deferred after this increment:** the full transitive dependency
  wait-graph; the precise Accord recovery ballot + duelling recoverers + a failure
  detector to *trigger* both recovery and a retry escalation (the adaptive tick
  backs off but does not itself declare a coordinator dead — that remains the
  explicit `recover` path); WAL snapshotting / log truncation; and the precise
  fast-path quorum bound. Sharding here routes the *execution effect* per tablet;
  the Accord replica set is still one global group (every transaction is
  replicated to every consensus node), so per-shard consensus replica sets /
  placement of the consensus participants themselves remain future work.

## Arbitrary caller-supplied write values increment (2026-08-02)

This increment closes the last execution-effect gap the earlier slices named as
deferred — **arbitrary caller-supplied write values** — again without reshaping
the sync-core / `Env`-driver split. It also unblocks a *true black-box* Elle
check over Accord (ADR 0014).

- **A transaction carries an explicit write set as `(key → value)`.** Previously
  the execution effect was hard-coded to "write my transaction id" (a register).
  A `ReplicaTxn` now carries, alongside its `write_keys`, a `write_values:
  BTreeMap<Key, Vec<u8>>` of caller-supplied bytes; the conflict set is still the
  union of read + write keys, and ordering is unchanged. On execution the
  `AccordNode` driver writes each key's **actual value** to the `StorageEngine`
  (and, on the frontier, the data-plane quorum) at the execution timestamp; a key
  *absent* from `write_values` defaults at the driver to the transaction's encoded
  id, so the classic register effect (and `store_writer`/`store_value` read-back)
  is exactly preserved for valueless callers. The `AccordCore` stays synchronous
  + I/O-free: values flow through it purely as data on the `ApplyEffect`; the
  driver does all the storage I/O.
- **The API is additive.** `submit`/`submit_rw` (txn-id effect) are unchanged;
  `submit_writes(BTreeMap<Key, Vec<u8>>)` and `submit_writes_rw(read_keys,
  BTreeMap<Key, Vec<u8>>)` carry explicit values, and `InteractiveTxn` grows
  `write_value(key, value)` / `read_value(key)` (raw bytes) so an interactive
  read-modify-write over arbitrary values (e.g. list-append) works. Reads now
  record **raw value bytes**: `read_value_result(txn)` returns them verbatim
  while `read_result(txn)` still decodes them as a writer id for the register
  view; `store_value`/`current_value` expose the raw bytes too.
- **Durability + failover replay the values.** The `write_values` ride the
  `PreAccept`/`Commit`/`RecoverOk` wire messages and the `PreAccepted`/`Committed`
  WAL records (new `write_values` fields, `#[serde(default)]` for
  forward-compat), so a replica that learns a transaction only at `Commit` writes
  the right value, recovery replays the actual bytes into a fresh engine, and a
  recovery coordinator unions the write values across the recovery quorum.
  Determinism is preserved (values are opaque `Vec<u8>` data; no new
  nondeterminism). Tests in `animus-consensus/tests/accord_values.rs`: a write's
  actual value lands on every replica; conflicting values resolve in agreed order;
  a read observes the actual value; the value survives stop/restart; an
  interactive read-modify-write carries a real value through the data plane; a
  sharded transaction routes the real value per tablet; trace reproducibility.
- **Still deferred after this increment:** the full transitive dependency
  wait-graph; the precise Accord recovery ballot + duelling recoverers + a failure
  detector to trigger recovery/retry escalation; WAL snapshotting / log
  truncation; the precise fast-path quorum bound; and per-shard consensus replica
  sets / placement (one global Accord group). The Elle cycle checker is now wired
  (ADR 0014) — and, with real write values, it is a *genuine black-box* check
  (reads observe stored state, not a reconstruction from `applied_order`).

## Recovery ballots + duelling recoverers increment (2026-08-02)

This increment closes the **precise recovery ballot + duelling recoverers**
deferral as far as is bounded and well-tested, again without reshaping the
sync-core / `Env`-driver split. The earlier failover slice was safe only because
recovery was invoked *once* per transaction (no two recoverers could contend);
this slice makes **concurrent recovery coordinators converge deterministically**.

- **Recovery ballots.** A new `timestamp::Ballot { round, node }` (totally
  ordered: round, then node-id tiebreak) is the proposal number a recovery
  coordinator runs under. The original coordinator runs at the implicit
  [`Ballot::ZERO`] (`round = 0`); every recoverer mints `round >= 1`, so a
  recoverer always outranks the original coordinator's steady-state `Accept`. A
  replica **promises** the highest ballot it has seen for a transaction
  (`ReplicaTxn.promised`) and **rejects** any `Recover`/`Accept` carrying a lower
  one, reporting the promised ballot so the sender learns it was superseded
  (`RecoverNack`/`AcceptNack` carry that ballot). The promise is **durable** (a new
  `WalRecord::Promised`, and `PersistedTxn.promised`/`.accepted_ballot`), so a
  restarted replica does not renege and let a superseded recoverer win. An
  `Accept` also records the ballot it was accepted under (`accepted_ballot`), which
  `RecoverOk` reports.

- **`RecoverOk` aggregation.** Once a simple-majority recovery quorum (all having
  *promised this round's ballot*) is in, the recoverer decides, in order: (1) if
  any reply is `Committed`/`Applied`, adopt that decision verbatim; (2) else if any
  reply was `Accept`ed under a ballot (`accepted_ballot > ZERO`), adopt the
  `(execute_at, deps)` of the reply with the **highest `accepted_ballot`** (the
  most recent prior proposal, which may already have been committed by that
  recoverer) and re-`Accept` it under our (higher) ballot; (3) otherwise force the
  slow path over the recovery replies (max-ts/union-deps). With ballots totally
  ordered, every recoverer that reaches step (2) adopts the *same* value, so
  duelling recoverers cannot diverge.

- **Duelling convergence (livelock avoidance).** The naïve "on supersession, bump
  my ballot and re-broadcast now" rule reproduces the classic duelling-proposers
  **livelock** (two recoverers ratchet each other's ballot forever within one
  instant — an unbounded message storm; this bit during development and hung the
  test). We instead use a deterministic **id tiebreak**: a superseded recoverer
  (`AcceptNack`/`RecoverNack`) abandons its attempt and only retries (above the
  ballot that fenced it) if its node id is **higher** than the winner's; otherwise
  it **stands down** and lets the winner finish — the winner's `Commit` (re-driven
  by its retry tick) then reaches it. So the duel converges in a bounded number of
  rounds. A late/superseded *original* coordinator (running at `Ballot::ZERO`) is
  simply fenced and stalls; its decision cannot overturn the recovered one.

- **All public signatures stayed additive.** `AccordNode::recover(txn)` /
  `AccordCore::recover(txn)` are unchanged (a recoverer mints a ballot strictly
  above the highest it has promised — `round = 1` initially, higher on retry); the
  ballot fields on `Accept`/`Recover`/`RecoverOk` and the WAL `Accepted` record are
  `#[serde(default)]` (an absent ballot decodes to `Ballot::ZERO`), so older wire/
  WAL bytes remain readable. `AccordCore` stays synchronous + I/O-free.

- **Tests** in `animus-consensus/tests/accord_recover_ballots.rs` (5-node cluster,
  `SimEnv`): two recoverers racing the same transaction converge to one decision
  on every replica; coordinator failover under partition then heal (the healed
  original cannot revert the recovered decision); a `Recover` racing the original
  coordinator's `Commit` (recovery adopts the existing commit, never contradicts
  it); recovery surviving message loss; a superseded recoverer not stranding the
  transaction; and trace reproducibility. Every test asserts cross-replica
  agreement and that **no committed decision is reverted**.

- **Still deferred after this increment:** the full transitive dependency
  wait-graph; a real **failure detector** to *trigger* recovery (it is still
  invoked explicitly — the ballots make *concurrent* explicit recoveries safe, but
  nothing yet *declares* a coordinator dead); WAL snapshotting / log truncation;
  the precise fast-path quorum bound; and per-shard consensus replica sets /
  placement (one global Accord group). The duel tiebreak guarantees convergence by
  *yielding* the lower-id recoverer rather than Accord's full randomized-backoff
  fast-path-recovery rules; the precise `PreAcceptOk`-witness fast-path-recovery
  decision procedure remains a simplification (we always force the slow path on
  re-proposal).
