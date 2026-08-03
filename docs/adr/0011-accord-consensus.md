# ADR 0011 — Accord-style leaderless transaction consensus

- **Status:** Accepted (first minimal slice; extended with execution + durability;
  then storage-backed execution + coordinator failover; then read transactions +
  multi-thread liveness regression; then message retry + the data-plane frontier;
  then data-plane reads + an interactive transaction API; then sharded
  transactions + read-set dependency folding + adaptive retry backoff; then
  arbitrary caller-supplied write values; then recovery ballots + duelling
  recoverers; then a **failure detector** that auto-triggers recovery + commit-ballot
  fencing; then **per-shard consensus** — one Accord group per tablet; then the
  **transitive dependency wait-graph** + the **precise fast-path quorum bound** +
  **WAL snapshotting / log truncation**)
- **Amended for v1 by [ADR 0019](0019-cp-only-v1-defer-ap.md):** the **AP
  data-plane frontier** paths (`start_with_data_plane`/`start_with_router`,
  `DataSink`/`DataRouting`, data-plane reads — landing committed writes in the
  leaderless `animus-data` quorum and reading them back) are **removed** with the AP
  plane. Pure Accord (local execution + versioned-snapshot reads, the serialization
  authority), per-shard consensus, recovery, retry, and the interactive API are
  unchanged. "Effect-sharding" (one global Accord round, AP-routed effect) is gone;
  "per-shard consensus" (one group per tablet) stays.
- **Date:** 2026-08-01 (execution + durability increment: 2026-08-01;
  storage-backed execution + coordinator-failover increment: 2026-08-01;
  read-transactions + multi-thread-liveness increment: 2026-08-02;
  message-retry + data-plane-frontier increment: 2026-08-02;
  data-plane-reads + interactive-transaction-API increment: 2026-08-02;
  sharded-transactions + read-set-folding + adaptive-backoff increment:
  2026-08-02; arbitrary-write-values increment: 2026-08-02;
  recovery-ballots + duelling-recoverers increment: 2026-08-02;
  failure-detector-triggered-recovery + commit-ballot-fencing increment: 2026-08-02;
  per-shard-consensus increment: 2026-08-02;
  transitive-wait-graph + precise-fast-path-quorum + WAL-snapshotting increment:
  2026-08-02)

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

## Failure-detector-triggered recovery + commit-ballot fencing (2026-08-02)

This increment closes the **failure detector to *trigger* recovery** deferral the
previous slices left open: recovery ballots made *concurrent* recoveries safe, but
recovery was still invoked **explicitly** — nothing declared a coordinator dead.
The `AccordNode` driver now does, again without reshaping the sync-core /
`Env`-driver split.

- **A driver-side failure detector.** Alongside the recv loop and the retry tick,
  `AccordNode` runs a periodic **liveness tick** on an `Env` timer. Each tick it
  asks the sync core for the transactions this replica holds **un-committed**
  (`AccordCore::uncommitted_txns`, phase `< Committed`) and re-samples a monotone
  per-transaction **progress fingerprint** (`AccordCore::progress_fingerprint` —
  phase + execute_at + dep/promised-ballot summary, strictly increasing on any
  advance). A transaction whose fingerprint *changes* between ticks is making
  progress, so the stall counter resets — a **slow-but-live coordinator is never
  spuriously recovered**. Only a transaction stuck at the same fingerprint for a
  whole bound (`LIVENESS_INTERVAL × LIVENESS_STALL_TICKS`, ≈5s) is suspected
  stranded. The core stays synchronous + I/O-free and time-free: it only reports
  *what* is un-committed and its progress summary; the driver owns the clock and
  the bound (no wall clock — `env.now`/`env.sleep`/`env.spawn_task`).

- **Deterministic nominee to keep duels rare.** When the bound trips, the driver
  does not blindly self-recover: it asks `AccordCore::is_recovery_nominee(txn,
  tier)` whether *this* node is the designated recoverer. The candidates are the
  replica set with the transaction's **original coordinator** (`txn.node`, the
  minting node, presumed dead) removed, ascending by id; the tier-0 nominee is the
  lowest-id survivor, so in the common case exactly **one** node recovers each
  stranded transaction — no duel. If that nominee is itself dead/partitioned, the
  next full stall window promotes the next **tier** (the next-lowest survivor),
  until recovery lands. When duels *do* still occur, the **ballot** machinery makes
  them safe and convergent; the nominee only reduces their frequency.

- **Commit-ballot fencing (a safety fix the detector exposed).** A `Commit` now
  carries the **ballot** it was decided under (`Ballot::ZERO` for the original
  coordinator, the recovery ballot otherwise). A replica records the highest
  commit-ballot it has seen and **ignores a `Commit` whose ballot is below it**
  (`AccordCore::replica_commit`, durable via `WalRecord::Committed.commit_ballot` /
  `PersistedTxn.commit_ballot`). This closes the **heal race** the detector can
  provoke: if a coordinator is only transiently partitioned and a survivor recovers
  its transaction at a higher ballot, the original's late `Ballot::ZERO` `Commit`
  (re-driven by its own retry tick after a heal) can no longer **revert** the
  recovered decision. (`Accept` was already ballot-fenced; `Commit` was not.)

- **The bound is set by a real workload interaction, not just latency.** A
  *replica* can only watch its own view of a transaction it does not coordinate,
  which advances solely on a phase change — it cannot see the coordinator slowly
  gathering a same-timestamp `PreAcceptOk` quorum, so *slow-but-live* and *dead*
  are indistinguishable except by elapsed time. The bound must therefore exceed a
  realistic slow-commit / partition-and-heal window: recovering a transaction that
  *would* have committed at its original `t0` re-orders it **after** every
  conflicting transaction committed in the meantime (`replica_pre_accept` bumps the
  recovered timestamp past them), which for a **single-writer list-append**
  workload (ADR 0014 / `animus-test`) lets a stale earlier write land last and lose
  later appends. A genuinely dead coordinator (crash/stop) never heals, so it is
  recovered after the bound; ~5s of recovery latency for a dead coordinator is
  acceptable, and correctness rests on the ballot fence + the bound exceeding the
  live-but-slow window, not on the exact value. (This interaction was caught by the
  frozen corpus — `wide_write`/`isolate_one` scenarios — when an over-aggressive
  600ms bound recovered slow-but-live coordinators.)

- **All public signatures stayed additive.** `AccordNode::recover` /
  `AccordCore::recover` are unchanged; the new core methods (`uncommitted_txns`,
  `progress_fingerprint`, `is_recovery_nominee`, `is_driving`, `is_uncommitted`)
  are additive; the `Commit.ballot` wire field and `WalRecord::Committed.commit_
  ballot` are `#[serde(default)]` (absent ⇒ `Ballot::ZERO`), so older wire/WAL
  bytes remain readable. `AccordCore` stays synchronous + I/O-free.

- **Tests** in `animus-consensus/tests/accord_auto_recover.rs` (5-node cluster,
  `SimEnv`, every long run bounded by `run_for`): a coordinator that dies after
  `PreAccept` is **auto-recovered within the bound** (no explicit `recover`) and
  commits + executes on every survivor; escalating auto-recoverers **converge** to
  one decision (ballots); a **slow-but-progressing** coordinator is **not**
  spuriously recovered (the nominee records no decision) and a healthy cluster
  never auto-recovers; auto-recovery **preserves arbitrary write values**; and the
  run is reproducible from its seed. The whole existing suite stays green,
  including the frozen Elle corpus (`animus-test`).

- **Still deferred after this increment:** the full transitive dependency
  wait-graph; WAL snapshotting / log truncation; the precise fast-path quorum
  bound; the precise `PreAcceptOk`-witness fast-path-recovery decision (we still
  force the slow path on re-proposal); per-shard consensus replica sets / placement
  (one global Accord group); and an **adaptive / membership-aware** failure
  detector (the bound is a fixed virtual-time threshold and the nominee assumes the
  whole replica set is alive — a real detector would use heartbeats / a liveness
  oracle rather than a per-txn stall timer, and would not need a bound large enough
  to absorb a partition-and-heal window).

## Per-shard consensus increment (2026-08-02)

This increment closes the long-standing **per-shard consensus replica sets /
placement** deferral that every prior slice named: until now there was **one
global Accord replica set** over the whole key space, and "sharding" routed only
the *execution effect* per tablet (`start_with_router`). This increment runs **one
Accord consensus group per tablet** — a tablet's replica set *is* its own Accord
group — again without reshaping (indeed without touching) the sync `AccordCore`.

- **A tablet is a consensus group.** Accord conflicts on intersecting key sets,
  and the keyspace already partitions into disjoint tablets (ADR 0002), each with
  its own replica set in the control plane's tablet map (ADR 0001). So per-shard
  consensus falls out by *composition*: for each tablet `T`, an `AccordNode` whose
  `all_nodes` is `T`'s replica set, keyed only on `T`'s keys, **is** the consensus
  group for that shard. Two transactions touching disjoint tablets never conflict,
  so they never share a group; a transaction touching only `T` is agreed entirely
  within `T`'s replicas. This adds **no** new control-plane state — the groups are
  *derived from the existing tablet map* (`ShardRouter` over the same `Vec<Tablet>`
  that `animus-data::Router` already routes data-plane I/O with).

- **`ShardRouter` + `ShardedOwner` (driver-level, the sync core untouched).**
  `ShardRouter` maps an Accord `Key` → owning `Tablet` (id + replica set) and
  splits a transaction's key set into one per-tablet **slice**. `ShardedOwner` is
  what a *physical* node runs: it hosts **one `AccordNode` per local shard** — one
  per tablet whose replica set includes this node — each on its **own** `Env`
  node-id (a distinct inbox *and* a distinct `accord.wal`, because a node's inbox
  is single-consumer, ADR 0001). The owner is the routing front-end. The
  `AccordCore` and `AccordNode` are entirely unchanged: per-shard consensus is a
  composition of existing per-group nodes, so every group keeps the full Accord
  machinery (fast/slow path, durability, recovery ballots, the failure-detector
  tick) independently.

- **Single-shard transactions (the common case).** When every key falls in one
  tablet, the transaction is submitted to **that group only** via
  `ShardedOwner::submit`; every other group is untouched. A fault on an unrelated
  shard therefore cannot stall it — the groups are independent consensus instances
  on disjoint env-ids. (Tested: a partition that strands one shard's coordinator
  leaves an unrelated shard committing + executing within the window.)

- **Cross-shard transactions.** When the key set spans tablets, the transaction is
  split into one per-tablet slice and **each slice is submitted to its own group**
  as a sub-transaction (`ShardedTxn` names the per-group ids). What this guarantees
  today:
  - **Atomic visibility:** the coordinator (`ShardedOwner::is_applied`) treats the
    transaction as done only when **all** slices have applied — all-or-nothing at
    the read point.
  - **Consistent ordering of conflicting cross-shard transactions:** any two
    cross-shard transactions that conflict must share at least one key, hence one
    common tablet/group, and **that shared group serializes them** (the Accord
    order on the shared tablet decides the winner); every key's data is owned by
    exactly one group, so there is no torn write set across shards once all slices
    commit.
  - The coordinator must replicate every tablet the transaction touches (so it can
    drive each slice locally); `submit` returns `ShardError::NotLocal` rather than
    silently dropping a slice otherwise.

- **What is deliberately *not* yet done (the precise scope of this slice).** This
  is **independent per-shard agreement under a shared logical-clock domain**, not a
  single unified cross-shard Accord round. Specifically deferred: a **single global
  execution timestamp** computed as the max of the per-shard `PreAccept` replies
  (each shard agrees its slice's own `execute_at`), and a **2PC-style atomic-commit
  protocol** that makes a cross-shard transaction commit-or-abort as one unit even
  under a coordinator crash mid-commit (today a stranded slice is finished by *that
  shard's* own recovery nominee, so the cross-shard transaction completes shard by
  shard rather than via a unified abort/commit decision). Also deferred: a
  cross-node slice dispatch so a coordinator need not replicate every involved
  tablet; folding a cross-shard transaction's deps across shards into one global
  dependency set; and dynamic tablet split/merge re-sharding a live consensus
  group. These are the remaining steps toward full multi-shard Accord.

- **Tests** in `animus-consensus/tests/accord_per_shard.rs` (`SimEnv`, two tablets
  on overlapping replica sets so one node coordinates cross-shard; every run
  seed-reproducible): a single-shard transaction executes on its owning group only
  and the other group is wholly untouched; single-shard transactions on distinct
  tablets are independent; a non-local key is rejected; a cross-shard transaction
  commits atomically on both groups (each key carries its own slice on every
  replica of its shard); two conflicting cross-shard transactions serialize via the
  shared group (and neither private key is torn); arbitrary write values route per
  shard; and a fault confined to one shard does not stall an unrelated shard. The
  whole existing suite stays green, including `accord_sharded.rs` (effect-sharding,
  retained unchanged) and the frozen Elle corpus (`animus-test`).

- **Still deferred after this increment** (unchanged from above, minus per-shard
  consensus which this closes): the unified global cross-shard timestamp + 2PC
  atomic commit described above; the full transitive dependency wait-graph; WAL
  snapshotting / log truncation; the precise fast-path quorum bound; the precise
  `PreAcceptOk`-witness fast-path-recovery decision; and an adaptive /
  membership-aware failure detector. Placement of the consensus participants is now
  exactly tablet placement (ADR 0005) — `ShardRouter` derives the groups from the
  same tablet map the control plane already maintains.

## Transitive wait-graph + precise fast-path quorum + WAL snapshotting (2026-08-02)

This increment closes three long-standing core-correctness / housekeeping
deferrals at once — the **full transitive dependency wait-graph**, the **precise
fast-path quorum bound**, and **WAL snapshotting / log truncation** — again
without reshaping the sync-core / `Env`-driver split.

- **Transitive dependency wait-graph.** Execution previously gated only on
  *direct key conflicts* (`conflicts_clear_for`): a committed transaction applied
  once every key-intersecting transaction this replica knew (any phase) that
  orders before it had applied. That under-waits for a dependency the replica
  knows **only as an id** — learnt via a peer's `Commit`/`Accept` dependency set,
  never the dependency's own `PreAccept`, so there is no local key to detect it —
  or a **transitive** dependency (a conflict-of-a-conflict that shares no key with
  the transaction). Such a predecessor could execute *after* the transaction,
  violating the agreed serialization order. `next_applicable` now additionally
  requires `deps_clear_for`: the **transitive closure** of the transaction's
  recorded `deps` that orders before it must be committed *and* applied. The walk
  is **cycle-aware** — Accord dependencies can be mutual, and a dependency ordering
  *after* the transaction (`(execute_at, dep) > (execute_at, txn)`) neither blocks
  nor is recursed through, so the total `(execute_at, txn)` order breaks every
  cycle and the closure is finite. A dependency this replica has never heard of at
  all blocks like an un-committed one (its position is unknown and might precede);
  the failure detector / retry guarantees it eventually arrives. The core stays
  synchronous + I/O-free. White-box unit tests in `core.rs` prove a transitive
  dependency on a disjoint key set (and a deeper `d → m → t` chain) blocks
  execution until the predecessor applies — and **fail without the new gate** — and
  that a mutual-dependency cycle drains in timestamp order without deadlocking.

- **Precise fast-path quorum bound.** The fast quorum was a conservative
  placeholder `⌈3N/4⌉`. The precise bound for the *simplified* recovery this core
  uses (recovery always forces the slow path) is **all-but-the-failure-tolerance**
  replicas — `F = N − 1` for the common `N = 2f+1` (e.g. **2** for N=3, vs the old
  3) — sized so it **intersects every recovery (simple-majority) quorum in ≥ 1
  replica** (`F + slow − N = ⌊N/2⌋ ≥ 1`) and **every other fast quorum in ≥ 1**
  (`2F − N = N − 2 ≥ 1`). The first intersection is exactly the recoverability
  condition: any recovery quorum contains a fast-path witness, so the
  max-ts/union-deps recovery over it reproduces (never contradicts) a fast value.
  The *optimized* bound `f + ⌊(f+1)/2⌋` is smaller but needs Accord's full
  `PreAcceptOk`-witness recovery (still deferred), so pairing it with our slow-path
  recovery would be unsafe — we take the simplified bound. **A latent bug the
  tighter bound exposed and this increment fixes:** two *conflicting* transactions
  can now legitimately fast-commit at the **same logical timestamp** (the later one
  carries the earlier as a dependency and is ordered after it purely by the `node`
  tiebreak). The data-plane / storage MVCC version was `execute_at.logical` alone,
  dropping that tiebreak, so the per-key LWW kept whichever *applied first* instead
  of the agreed `(execute_at, txn)` winner. The driver now stamps writes (and
  reads) with `mvcc_version` = `(logical << 16) | node`, preserving the full order,
  so conflicting same-logical writes converge to the agreed winner on every
  replica. White-box unit tests assert the exact bound and the recoverability
  intersection arithmetic over many `N`; a 5-node `SimEnv` test
  (`accord_fast_path.rs`) takes a genuine fast-path commit, kills the coordinator,
  and proves a recovery quorum that **excludes** it reconstructs the identical
  decision.

- **WAL snapshotting / log truncation.** The per-node WAL grew with every phase
  transition of every transaction (`PreAccepted` + `Accepted`/`Promised` +
  `Committed` + `Applied`). Mirroring the control-plane Raft's compaction, the core
  now produces a compact `wal_image` — a **single** `WalRecord::Snapshot` carrying
  the `PersistedState` image (one `PersistedTxn` per tracked transaction + the
  recovered execution order) — and the driver **atomically replaces**
  (`env.replace`) the WAL with it once `applied_since_snapshot()` crosses a
  threshold (64). The truncation is a full rewrite, never incremental, so a crash
  sees the whole old or whole new WAL; replay folds the `Snapshot` first, then the
  live tail on top (additive — a WAL with no `Snapshot` replays exactly as before).
  The per-transaction facts ride the `Snapshot` as a `Vec<(TxnId, PersistedTxn)>`
  (a JSON object cannot key on a `Timestamp` struct). The new core methods
  (`persisted_state`, `wal_image`, `snapshot`, `take_snapshot_dirty`,
  `applied_since_snapshot`) are additive and the core stays I/O-free. Tests: a core
  unit test proves `wal_image` replays to an **identical** core (same execution
  order, decisions, in-flight phase, and a byte-identical re-snapshot); a `SimEnv`
  test (`accord_snapshot.rs`) drives past the threshold and asserts the on-disk WAL
  collapsed to a `Snapshot`-led image with a record count far below the applied
  count (it **fails** when compaction is disabled), and that a node restarted on
  the truncated WAL recovers identical executed state + store.

- **Public signatures stayed additive.** No existing entry point changed; the new
  `AccordCore` methods, the `WalRecord::Snapshot` variant (`#[serde]`-additive),
  and `PersistedState`/`PersistedTxn` serde derives are all additive. The whole
  existing suite stays green, including the frozen Elle corpus (`animus-test`).

- **Still deferred after this increment:** the optimized fast-path quorum
  (`f + ⌊(f+1)/2⌋`) and the precise `PreAcceptOk`-witness fast-path-recovery
  decision it requires (we still force the slow path on re-proposal); the unified
  global cross-shard timestamp + 2PC atomic commit; and an adaptive /
  membership-aware failure detector. The snapshot collapses the per-phase *history*
  into one record per live transaction but does **not** garbage-collect terminal
  (applied) transactions — they still gate successors via the dependency closure,
  and dropping them safely would need a dependency low-water-mark; so the WAL is
  bounded by the *live transaction set*, not by a fixed window.
