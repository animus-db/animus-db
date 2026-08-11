# CLAUDE.md — animus-consensus

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

**Testbed-only (ADR 0018/0019).** This crate has **no production consumer**:
`animusd` does not depend on it, and ADR 0018 chose 2PC-over-Raft for CP
transactions, so Accord is off the roadmap (a long-shot future improvement,
ADR 0019). Its sole workspace consumer is `animus-test`'s Elle consistency
corpus (ADR 0014), which uses it as the **known-serializable testbed** the
checkers are proven against — via `AccordNode::start` (pure in-memory Accord
over `SimEnv`) plus `submit_writes`, `submit_read`, `read_value_result`,
`is_applied`, `store_value`. Everything the corpus exercises (sync core,
timestamps/ballots, recovery + failure detector, retry, WAL +
snapshotting/recovery) must stay green.

What it implements: Accord-style **leaderless transaction consensus**
(ADR 0011 — read that for the protocol rationale; this file only records
mechanism and gotchas). A coordinator agrees an *execution* timestamp and a
*dependency* set via PreAccept → (fast path) Commit or PreAccept → Accept →
Commit (slow path); each replica then executes in agreed order against a local
`MemoryEngine`, durably (WAL + recovery). Coordinator failover, message retry
with backoff, read and interactive read-modify-write transactions all exist.
Trimmed (retrievable from git history): the per-shard driver (`shard.rs`:
`ShardedOwner`/`ShardRouter`), the pluggable-engine `start_with_storage`, and
the AP data-plane "frontier" constructors (`start_with_data_plane`/
`start_with_router` + `DataSink`/`DataRouting`) removed with `animus-data`
(ADR 0019).

## Entry points

- `timestamp.rs` — `Timestamp { logical, node }` (totally ordered, unique) and
  the per-node `LogicalClock` (`witness`/`mint`); `Ballot { round, node }` (the
  recovery proposal number; `Ballot::ZERO` = original coordinator,
  `Ballot::next_above` mints strictly above the highest seen).
- `core.rs` — `AccordCore`: a **synchronous, I/O-free** state machine mirroring
  `animus-control`'s `RaftCore`. `submit(keys)` / `submit_read(keys)` /
  `submit_rw(read_keys, write_keys)` start transactions this node coordinates;
  `recover(txn)` takes over a stranded one; `handle(from, msg)` processes an
  inbound message. All return `Vec<Out>` and never touch `Env`. The core decides
  *order*; the driver does the *I/O*: `drain_persist`/`drain_apply`/`drain_reads`
  hand out buffered `WalRecord`s / `ApplyEffect`s / `ReadEffect`s, `recovered`
  rebuilds from a `PersistedState`, `resend_pending()` recomputes the outbound
  still owed for every in-flight round (the driver's retry tick calls it).
- `message.rs` — `AccordMsg` (PreAccept/PreAcceptOk/Accept/AcceptOk/AcceptNack/
  Commit/CommitAck + Recover/RecoverOk/RecoverNack), serde_json over the
  `Network`'s `Vec<u8>` payloads. `Accept`/`Recover`/`RecoverOk`/`Commit` carry a
  `ballot` (`#[serde(default)]`); `Commit`'s ballot fences a stale lower-ballot
  commit. Execution is local — no wire message.
- `persist.rs` — `WalRecord` (PreAccepted/Accepted/Committed/Applied/Promised/
  `Snapshot`) and `PersistedState`, mirroring `animus-control::persist`.
  `Snapshot` is the compact log-truncation image the driver atomically
  `replace`s the WAL with; replay folds it first, and a WAL without one replays
  as before (additive). Its txns ride as `Vec<(TxnId, PersistedTxn)>`, not a
  `BTreeMap` — **serde_json cannot key a map on a struct** (runtime failure).
- `node.rs` — `AccordNode<E>`: the thin `Env` driver over an in-memory
  `MemoryEngine`. `persist_then_ship` drains records + effects, appends +
  `fsync`s `accord.wal`, **then** applies writes (`apply_all`) and satisfies
  reads (`satisfy_reads`), then ships outbound — durable before action. `drive`
  recovers from the WAL on startup. Runs a `retry_loop` (exponential backoff,
  reset on progress) and a `liveness_loop` (the failure detector) — both
  **perpetual timers**, so drive tests with `run_for`/`run_until`, never
  `run()`. Write APIs: `submit_writes(map)` / `submit_writes_rw(reads, map)`
  carry arbitrary value bytes. Read APIs: `read_result` (writer id) /
  `read_value_result` (raw bytes, valid once `is_applied`); `store_writer` /
  `store_value` / `current_value` are `async` (they read the engine).
  Interactive API: `begin()` → `InteractiveTxn` (`read`/`read_value`,
  `write`/`write_value`, `commit()` → one Accord RMW via
  `submit_rw`/`submit_writes_rw`).

## Key invariants & gotchas

Protocol rationale lives in ADR 0011; each bullet here is a rule to preserve,
with the bug it guards against where one bit during development.

- **All protocol logic stays in the sync `AccordCore`** — never reach for `Env`
  inside the core. Unlike `RaftCore` it takes **no `now`/`entropy`**:
  determinism rests purely on logical clocks + `BTreeMap` ordering (no
  `HashMap`, lint-enforced; no time source).
- A node is **both coordinator and replica**: `submit` seeds the coordinator's
  reply set with its own PreAcceptOk (it counts itself toward the quorum).
- **Fast-path gating** (`advance_from_pre_accept`): don't escalate to the slow
  path while the fast path is still reachable — but DO escalate once every
  replica answered and it didn't fire (all-agree-on-`t0`-but-deps-differ), or
  the txn stalls forever. Bit during development; the multi-seed sweep guards it.
- **`fast_quorum() = max(N-1, slow_quorum)`** — the precise *simplified*-recovery
  bound, sized so any majority quorum intersects every fast quorum. Don't drop
  to the optimized `f+⌊(f+1)/2⌋` without implementing Accord's full
  PreAcceptOk-witness recovery. The tighter bound lets two conflicting txns
  fast-commit at the same `logical` (ordered by node tiebreak), so storage
  versions are **`mvcc_version(ts) = (logical<<16)|node_index`**, never
  `logical` alone — and `mvcc_version` hard-`assert!`s its encoding contract
  (`node_index < 2^16`, `logical < 2^48`); a violation would silently
  collapse two timestamps into one version and LWW would keep an arbitrary
  winner. **ADR 0040 PR3**: `NodeId` is a validated string now (no longer a
  small dense `u64`), so it can't be bit-packed directly — `node_index`
  folds in the node's position in the **sorted, closed, static** replica set
  instead (every replica derives the same index for the same id since the
  set is closed for this testbed crate). This is deliberately narrower than
  introducing an opaque `NodeIdx` type for `Timestamp`/`Ballot.node`
  generally: `core.rs` reads `txn.node`/`ballot.node` *semantically*
  (`is_recovery_nominee`/`handle_superseded` compare it against real
  survivor `NodeId`s for actual coordination decisions, not just as an
  ordering tiebreak), so an index would need constant reverse-translation
  there for no simplification — the index trick is scoped to this one
  purely-numeric MVCC-storage-encoding boundary in `node.rs`
  (`node_index`/`mvcc_version`), not threaded through the protocol core.
- **Two execution gates, both required** (`next_applicable`):
  `conflicts_clear_for` (direct key conflicts — judged against every conflicting
  txn known in *any* phase, not just committed ones; gating only on committed
  txns let same-timestamp txns execute in arrival order and diverge) and
  `deps_clear_for` (the cycle-aware transitive dependency closure — catches
  id-only and conflict-of-a-conflict deps the direct gate is blind to; an
  unknown dep blocks, a dep ordering after the txn is skipped). Removing either
  fails `core::tests`.
- **Execution uses `merge`, not `put`**: execution timestamps aren't globally
  monotonic across keys (`put`'s engine-wide floor would reject them), and
  `merge`'s per-key LWW is idempotent + commutative so recovery re-apply
  converges. Consequence: LWW is the substrate — concurrent writers to one key
  lose updates *by the data model*, so list-append workloads over this need
  single-writer-per-key (see `animus-test`).
- **Arbitrary write values are additive** (ADR 0011): `ApplyEffect.values`
  carries caller bytes; a key absent from it defaults *at the driver* to the
  writer's `TxnId` (the classic register effect). Values flow through the core
  purely as data (wire + WAL fields all `#[serde(default)]`) and survive
  recovery/failover (recovery unions them across the quorum).
- **Read-only transactions are ordered like writes; only the effect differs**
  (a `ReadEffect` → versioned `get_at(key, execute_at)`). Driver ordering
  matters: within one drain, writes (`apply_all`) apply **before** reads
  (`satisfy_reads`) — don't reorder them.
- **`write_keys` vs `keys`**: a `ReplicaTxn` carries the full conflict set
  (`keys` = reads ∪ writes) and the `write_keys` subset it writes; ordering uses
  `keys`, the write effect only `write_keys`. `read_only` is exactly
  `write_keys.is_empty()`. Both survive the wire + WAL (`#[serde(default)]`).
- **The interactive API lives entirely in the driver** (`InteractiveTxn` is
  driver state; it reaches the core only through `submit_rw` at `commit()`), so
  the session's reads fold into the committed txn's conflict set. Mixing valued
  and valueless writes in one session is unsupported (debug-asserted).
- **No `std::sync::Mutex` guard across an `.await`** — the core lock is taken
  only to drain, then dropped; all I/O is lock-free in a spawned task. The
  multi-thread liveness regression is `tests/accord_concurrent.rs` (`SimEnv`
  proves order, not real-thread liveness).
- **Retry lives in the driver; the core only decides what to re-send**
  (`resend_pending`, sync). Re-sends are idempotent at the replica (handlers
  fold by max/union); `CommitAck` tells a committed coordinator to stop
  re-sending. Backoff doubles `RETRY_BASE_INTERVAL` → `RETRY_MAX_INTERVAL`,
  resetting on progress; the backoff state is a local in `retry_loop`, so core
  determinism is untouched.
- **WAL replay is order-insensitive** (per-record merge: max on
  timestamp/phase, union on deps, single Committed/Applied per txn), so the
  driver may flush from either `submit` or the recv loop. On recovery the core
  re-emits apply effects for `applied_order` so the driver repopulates the
  fresh, volatile engine (`Applied` phase comes from the separate `Applied` WAL
  record).
- **Coordinator failover** (`recover`/`Recover`/`RecoverOk`, state in
  `recovering`): once a majority has promised its ballot it (1) adopts any
  reported Committed/Applied decision verbatim, (2) else re-proposes the
  highest-`accepted_ballot` `(execute_at, deps)` under its own ballot, (3) else
  **re-broadcasts PreAccept with the union of keys the replies carried** and
  forces the slow path (recovery never takes the fast path). The key
  re-broadcast is load-bearing: without it a replica that missed the original
  PreAccept commits with an empty key set and executes nothing — which is also
  why the failover tests let the original PreAccept reach a quorum before
  isolating the coordinator.
- **Recovery ballots + duelling recoverers**: replicas promise the highest
  ballot seen (durable via `WalRecord::Promised`) and Nack anything below it.
  **Livelock gotcha:** "on supersession, bump my ballot and re-broadcast now"
  livelocks two duelling recoverers (an unbounded same-instant storm — it hung
  the sim test); `handle_superseded` instead breaks ties deterministically —
  only the higher-id recoverer retries, the other stands down and adopts the
  winner's commit.
- **Failure-detector-triggered recovery** (`liveness_loop`): a txn is suspected
  dead only after its monotone `progress_fingerprint` sat unchanged for the
  whole bound (`LIVENESS_INTERVAL × LIVENESS_STALL_TICKS` ≈ 5s), and recovery
  fires only from the deterministic nominee (`is_recovery_nominee` — lowest-id
  survivor ≠ the dead coordinator), so the common case has one recoverer and no
  duel. **The bound is load-bearing, not just latency**: a replica can't tell
  dead from slow except by time, and recovering a live txn re-orders it after
  every conflict committed meanwhile — an over-aggressive 600ms bound passed
  every targeted test and failed the Elle corpus; ≈5s (above a
  partition-and-heal window) is the floor. Safety additionally rests on
  **commit-ballot fencing**: `replica_commit` ignores a `Commit` below the
  highest commit-ballot recorded (durable), so a healed original coordinator's
  late `Ballot::ZERO` commit can't revert a recovered decision.
- Conflict = intersecting key sets; `Key` is a bare `u64` (the real system keys
  by partition/range).

## Deferred (see ADR 0011)

Accord as a whole is off the roadmap — nothing here is planned work; it marks
where the testbed's fidelity to real Accord stops:

- A richer (heartbeat/liveness-oracle) failure detector — the per-txn stall
  timer works but needs a bound big enough to absorb a partition-and-heal
  window and assumes the whole replica set is alive.
- The optimized fast-path quorum `f+⌊(f+1)/2⌋` and the PreAcceptOk-witness
  fast-path recovery it requires (we use the simplified `N-1` bound and always
  force the slow path on re-proposal; duels converge by id tiebreak, not
  Accord's randomized backoff).
- Sharding / cross-shard atomic commit (the per-shard driver was trimmed; the
  sync core never learned about tablets).

Everything else described above is implemented and tested.

## Tests

`cargo test -p animus-consensus` — unit tests on the timestamp/clock and the
white-box core gates, plus twelve `SimEnv` test files and one real
multi-threaded `ProdEnv` test. Use `run_for`/`run_until` for the `SimEnv` tests
(perpetual retry/liveness timers mean `run()` never returns).

- `tests/accord_commit.rs` — fast-path commit; conflicting txns commit in a
  consistent order (64-seed sweep); disjoint independence; trace reproducibility.
- `tests/accord_execute.rs` — execution + durability: converged stores under
  contention (48-seed sweep), stop/restart recovery from `accord.wal` into a
  fresh engine. (`store_writer` is async; tests use
  `futures::executor::block_on`.)
- `tests/accord_recover.rs` — coordinator failover: a partitioned coordinator's
  txn is recovered to a consistent commit + execution (32-seed sweep); recovery
  adopts an existing decision idempotently.
- `tests/accord_recover_ballots.rs` — duelling recoverers converge (5 nodes); a
  healed original can't revert; recovery racing a `Commit` adopts it; survives
  message loss; no committed decision is ever reverted.
- `tests/accord_auto_recover.rs` — failure-detector recovery: a dead
  coordinator is auto-recovered within the bound with no explicit `recover`; a
  slow-but-progressing coordinator is NOT spuriously recovered; write values
  survive auto-recovery.
- `tests/accord_read.rs` — read snapshots: sees writes ordered before, not
  after; identical on every replica (48-seed sweep); survives stop/restart.
- `tests/accord_retry.rs` — commits + executes consistently under a lossy
  network (per-message drop) via the retry tick.
- `tests/accord_rw_conflict.rs` — the read set folds into deps: a
  read-then-write orders against a conflicting write to the key it read, with a
  disjoint control.
- `tests/accord_values.rs` — arbitrary values: bytes land on every replica,
  conflicting values resolve in agreed order, survive restart; an interactive
  RMW appends correctly on every replica.
- `tests/accord_backoff.rs` — backoff: a fully-partitioned coordinator's
  re-send count is far sub-linear; still converges promptly after heal.
- `tests/accord_fast_path.rs` — the `N-1` bound: uncontended fast-path commit,
  and a fast-path commit recoverable by a quorum excluding the dead coordinator.
- `tests/accord_snapshot.rs` — WAL truncation: past the threshold the WAL is
  rewritten `Snapshot`-led and far shorter; restart on it recovers identical
  state.
- `tests/accord_concurrent.rs` — `#[tokio::test(multi_thread)]` over `ProdEnv`,
  timeout-guarded: concurrent coordinators must not deadlock/strand and safety
  must hold under genuine parallelism (mirrors
  `animus-storage/tests/lsm_concurrent.rs`).
- `src/core.rs` `#[cfg(test)] mod tests` — white-box gates: the transitive
  wait-graph (blocking + cycle drain), fast-path quorum arithmetic over many
  `N`, snapshot round-trip.
