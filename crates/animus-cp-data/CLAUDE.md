# CLAUDE.md — animus-cp-data

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The **leaderful, strongly-consistent (CP) data plane** (ADR 0016, ADR 0017): each
tablet is its own Raft group with a single leader serving **linearizable**
single-tablet reads/writes, durable on a real `StorageEngine`. It is the CP
counterpart to the leaderless AP `animus-data` plane, built **additively** — AP is
untouched, no dual-mode seam yet. The control plane (`animus-control`) remains the
metadata authority (tablet map, placement, failure detection) in both modes.

## How it reuses the control-plane Raft

It instantiates `animus-control`'s generic, sync, I/O-free `RaftCore<C, S>`
(ADR 0009) with `C = KvCommand` and a **`DRIVER_APPLIED` state machine**
(`KvState`, a unit placeholder): the core agrees the order of commands but does
**not** apply them in-core (a `StorageEngine` apply is async I/O the sync core
can't do). Instead the core buffers committed-and-durable commands as effects;
this crate's **async driver** drains them (`RaftCore::drain_apply`) and applies to
the engine — the `AccordCore` sync-core/async-driver split.

## Entry points

- `KvCommand` (`Put`/`Batch`/`Delete`/`Cas`/`NoOp`), `KvState` (the `DRIVER_APPLIED` SM).
- **`StorageScope`** (ADR 0026/0028): confines a `RaftKvNode`'s physical key
  access within a possibly node-shared `StorageEngine` — a `prefix` (the
  owning table's identity, `escape(table_name)`) plus a live-narrowable
  `range` (this tablet's own sub-portion of that table's keyspace, an
  `Arc<Mutex<KeyRange>>` so a split can narrow it without restarting the
  group). `physical(key)` maps a logical key to its on-engine key;
  `strip_in_range`/`strip_prefix_only` are the inverse for scans;
  `has_data(&storage)` is the async presence check `animusd` uses to tell "am
  I re-forming after a restart" (full voter config) from "am I a brand-new
  spare joining" (non-voter) without a durable per-tablet marker. `start`
  defaults to `StorageScope::whole()` (no prefix, unbounded range — the
  identity, so an unscoped caller is byte-for-byte the pre-scoping behavior);
  `start_scoped`/`start_hosted` take an explicit scope.
- **Fenced commands** (ADR 0026): `put_fenced`/`delete_fenced`/`cas_fenced`/
  `put_batch_fenced` (and their unfenced siblings, which use
  `KeyRange::whole()`) carry a `fence: KeyRange` *inside the proposed command
  itself* — stamped by the leader at propose time from its own current
  `StorageScope.range` — so every replica's apply checks the key against the
  fence embedded in the log entry, never a locally-polled value. This is what
  keeps a crossover window (a stale client addressing the old, wider range
  right after a split narrows it) deterministic: two replicas at different
  points in observing the split's `Metadata` still make the identical
  accept/reject decision for the same log entry, because the decision travels
  *with* the entry.
- **Stream addressing** (ADR 0026 Stage B): `start_hosted(env, all_nodes,
  storage, scope, stream)` addresses a tablet's Raft traffic by `(node,
  stream)` (`env.send_stream`/`recv_stream`, `stream` = the tablet id) instead
  of a distinct `NodeId`/env per tablet — so every tablet a node hosts shares
  one env/port. Replaces the retired `Coresident` sibling-minting approach for
  this crate (`animus-env`'s `Coresident` trait itself is unused here now).
- **Batch put** — `KvCommand::Batch(Vec<(k, v)>)` + `RaftKvNode::put_batch` commit
  **N keys as one Raft log entry** (one propose → one commit round → one apply), the
  bulk-write throughput primitive. Applied as one arm in `apply_and_compact`: every
  key merges at the entry's shared Raft `index` (the MVCC version — keys are
  distinct, so per-key LWW is well-defined; `engine_applied` advances once past the
  whole batch at end of the loop iteration). Re-applies idempotently on recovery
  exactly as a single `Put`. Uses the normal per-key `merge` path, so it composes
  with a coalesced-fsync `merge_batch` optimization. `tests/batch.rs`.
- `RaftKvNode<E, S>` — a running tablet-group node: `start(env, all_nodes,
  storage)`, `put`/`put_batch`/`delete` (proposed via Raft, honored on the leader), `is_leader`,
  `linearizable_get` (ReadIndex), `local_get` (a replica's raw engine read — *not*
  linearizable; a test/observability aid).
- **Linearizable CAS** — `cas(key, expected, value) -> ProposeResult` proposes a
  `KvCommand::Cas` (set iff the committed value `== expected`; `expected: None` ==
  "only if absent"); `cas_result(index) -> Option<bool>` reads the outcome recorded
  at the `Accepted { index }` once that entry applies; `compare_and_swap(key,
  expected, value) -> Option<bool>` is the all-in-one (propose on the leader, wait
  for the entry to apply, return the recorded outcome — `None` if not leader / times
  out). All public-additively; existing signatures unchanged.
- **Admin/debug accessors** (ADR 0020, consumed by `animusd`): read-only
  `role`/`term`/`commit_index`/`last_applied`/`durable_index`/`snapshot_index`/
  `log_len` (thin locks over `RaftCore`), and `storage()` (a `&S` borrow so the
  assembly layer can surface the engine's SSTable/WAL debug views without engine
  state leaking into the consensus core). **`engine_applied_index()`** is the
  confirm-by-index primitive (audit A4): the engine-merge watermark linearizable
  reads gate on, so a proposer confirms a specific `Accepted { index }` applied
  (`engine_applied_index() >= index` while still leader in the proposal's term)
  instead of polling value equality, which false-negatives under a concurrent
  same-key overwrite. `linearizable_scan` pushes a bounded range into
  `storage.scan(start, end)` (key-ordered by contract — no re-sort, no
  whole-tablet materialization); only the unbounded-above case still reads
  `entries()` (the trait's `scan` has no open upper bound).
- `KvWire` — the data-plane wire enum wrapping `RaftMsg` plus the ReadIndex
  read-barrier probes (`ReadProbe`/`ReadProbeAck`). The probes are driver-only, so
  ReadIndex lives entirely in this crate and the shared `RaftCore`/`RaftMsg` are
  untouched.

## What's non-obvious

- **The driver is split into two tasks: a consensus loop + an apply task.** This is
  the **driver-liveness fix** (ADR 0017): engine apply and compaction are slow
  (~180–300ms for a batch of LSM merges + a compaction rewrite on a real disk) and
  used to run *inline* on the same loop that services Raft messages, so under write
  load the driver blocked past the 150ms election timeout → followers campaigned → a
  **leader-election storm** (term climbed continuously) that truncated in-flight
  writes and collapsed throughput to ~15/s. Now:
  - **Consensus loop** (`drive`): recover from WAL, spawn the apply task, then loop
    `persist_wal` (drain WAL records → append + `fsync` → `mark_durable_through`,
    under `wal_lock`) → `select(recv, timer)` → step the core → `persist_wal` again
    (durability before shipping) → send. It does **no** engine apply, so it always
    heartbeats/acks within the election timeout. Heartbeat ticks re-replicate.
  - **Apply task** (`apply_loop` → `apply_and_compact`): install received snapshots,
    `drain_apply` → `merge`/`merge_tombstone` into the engine in commit order, and
    compact — all off the consensus loop. Backs off (`APPLY_IDLE_POLL`) only when
    idle; under load it stays in lockstep behind commit.
  - **Two invariants the split introduces:** (1) the core's `last_applied` (a buffer
    cursor advanced by the consensus loop) now *leads* the engine, so linearizable
    reads gate on a separate **`engine_applied`** atomic the apply task advances
    after each merge — never `last_applied` (else a read could observe past the
    engine). (2) The WAL file is written by both tasks (append vs. compaction
    rewrite), serialized by an async `wal_lock`; compaction snapshots only up to
    `engine_applied` via `RaftCore::snapshot_upto` (not `last_applied`, which the
    engine hasn't merged yet) and **discards the consensus loop's pending records**
    in the same locked block (`replay` is push-based → re-appending would duplicate).
    Compaction is skipped while `halted` (it's a WAL-bounding optimization; a rewrite
    racing teardown can fail the `replace`). `is_stopped()` requires *both* tasks
    stopped (`stopped` && `apply_stopped`) before the GC deletes artifacts.
- **Wake-on-propose cuts single-write latency.** `put`/`delete`/`cas`/
  `change_membership` route through `propose_and_wake`: after the
  core appends the entry, the proposer raises a `ProposeSignal` (`AtomicBool` +
  `futures::task::AtomicWaker`) that the consensus loop races as a third arm of its
  `select(recv, timer)`. On that wake the loop calls `RaftCore::replicate_now`
  (broadcast `AppendEntries` immediately, resetting the heartbeat deadline) instead
  of leaving the entry parked until the next ~50ms heartbeat tick. **`AtomicWaker` is
  deliberately executor-agnostic** — it works under both `SimEnv`'s `ArcWake`
  executor (the wake runs synchronously on the single thread, marking the driver task
  ready for the next run-loop poll — fully deterministic, no wall clock) and tokio's
  multi-threaded `ProdEnv` (it resolves the register/wake race); **no tokio-only
  primitive** is used, so determinism holds. The `ProposePending` future *registers
  the waker before checking the flag* (the AtomicWaker discipline against a lost
  wakeup) and consumes the flag (`swap(false)`) on resolve, so it never busy-spins.
  A `NotLeader` propose appends nothing, so it doesn't wake. Latency-verified over
  `ProdEnv` in `animusd/tests/cp_plane.rs::single_write_latency_is_low` (median
  ~52ms → ~11ms).
- **The Raft log index is the MVCC version.** Apply uses `index` as the engine
  `version`, so per-key LWW reproduces the agreed Raft total order, and re-applying
  on recovery is idempotent.
- **CAS is decided at *apply* time, not propose time — that is what makes it
  linearizable + contention-correct.** The `RaftCore` agrees only the command
  *order*; `Cas` rides through it as opaque data (sync core untouched, like every
  other effect). `flush_and_apply` evaluates it in commit order: it `storage.get`s
  the key's *current committed* value (every earlier entry in the same batch has
  already merged, so this is the post-predecessor state) and compares to `expected`;
  equal → `merge` at `index` (same write path as `Put`), else no-op. Because every
  replica applies the same order against the same committed state with no clock/RNG,
  every replica makes the **identical** accept/reject decision. Two CAS racing from
  the same `expected` therefore have **exactly one winner** — whichever Raft ordered
  first: its swap moves the committed value, so the second's compare then fails.
- **CAS outcome plumbing: keyed by the Raft log index.** `propose` returns
  `Accepted { index }`; apply records `CasResults.outcomes[index] = swapped` (a
  `BTreeMap<u64,bool>` in driver state, mirroring `animus-consensus`'s `ReadResults`
  stash). The proposer waits until the entry applies (`last_applied >= index`, or
  just polls `cas_result`) and reads it. No wall clock in the wait — `compare_and_swap`
  uses only `env.now()`/`env.sleep()`, so it stays a pure function of the seed.
- **Durable-before-visible holds** (ADR 0009): effects are only drained for fsynced
  entries, and the engine write follows the WAL `fsync`.
- Distinct WAL file (`raftkv.wal`) from the control plane's `raft.wal`, so a node
  can host both planes. The name is exported (`animus_cp_data::WAL`) so the
  drop-table GC (ADR 0024) can delete a stopped group's WAL.
- **`shutdown()` is a graceful driver halt, not a kill** (ADR 0024): it latches a
  flag the driver observes at the top of its loop — i.e. *between* full
  persist+apply passes and within one wake (message or pending timer), so the WAL
  and engine are never left mid-write. Poll `is_stopped()` before touching the
  group's files. A halted node's accessors still answer from the **frozen** core
  (a halted leader keeps reporting `is_leader() == true`), so never route to a
  handle after unregistering it; a halted node must not be reused — restarting
  the tablet means a fresh `start` (the sim tests: `tests/shutdown.rs`).

## Stage status (ADR 0017)

- **B.1 (done)** — single-group driver + write path; `tests/single_tablet.rs`
  (writes replicate + apply on every replica; survive a leader kill + rejoin
  catch-up; trace reproducibility).
- **B.2 (done)** — linearizable **ReadIndex** reads (`linearizable_get`): the
  barrier first waits for the leader to have **committed an entry of its own
  term** (Raft §6.4's mandatory half — `commit_index >=
  RaftCore::first_term_index()`, the election no-op; a fresh leader's log holds
  every acked entry but its `commit_index` may not cover one the old leader
  committed, and the term-only probe can't see that), then a read-barrier quorum
  probe (`KvWire::ReadProbe`/`Ack`, driver-only) confirms the leader still leads
  its term, then it serves locally once applied (gated on `engine_applied`). No
  log entry, no wall clock; the whole barrier shares one `READ_TIMEOUT`.
  `tests/read_index.rs` (reads reflect committed writes + RYW; a
  deposed/partitioned leader returns `None`, never a stale value) and
  `tests/read_index_fresh_leader.rs` (drives *into* the fresh-leader window at
  1ms sim granularity — uncommitted acked entry on the heir + a ~40-entries-
  behind third replica so the no-op commit needs ~40 backtrack round-trips while
  the probe needs one; the read must wait and serve the acked value, never the
  stale one).
- **CAS (done)** — linearizable **compare-and-swap** (`cas`/`cas_result`/
  `compare_and_swap`, `KvCommand::Cas`). Decided at apply time in commit order
  against the committed engine state (deterministic; sync `RaftCore` untouched —
  CAS is opaque command data); outcome stashed in driver `CasResults` keyed by the
  log index. `tests/cas.rs` (concurrent same-`expected` race → exactly one winner,
  agreed on every replica; CAS-if-absent; a successful CAS survives `stop`+restart
  via WAL replay re-apply; seed sweep + trace reproducibility).
- **A.2 (done)** — compaction + streaming `InstallSnapshot`, with **lazy
  on-demand engine images** (audit P1/P5). The driver compacts once
  `COMPACT_THRESHOLD` entries apply: the core truncates the log prefix and the
  WAL is rewritten to its bounded image — **no whole-tablet scan/serialize on
  the threshold path**, and no image retained in the core at rest. Only when a
  replication attempt actually needs to ship a snapshot does the core raise
  `RaftCore::take_snapshot_needed`; the apply task then scans the engine
  (`engine_image`), re-bases (`snapshot_upto(engine_applied)` *before*
  `set_snapshot_blob`, so base and image agree), and the next heartbeat ships
  the chunks. The receiving follower writes the bytes into its engine
  (`drain_pending_install` → `merge`) and replays the log tail on top; it does
  *not* retain the bytes — a later re-ship regenerates from its engine (the
  second-hop invariant, and it now also covers a **recovered** leader, which
  used to ship 0 bytes until its next compaction). Wire + image ride the
  crate's compact **binary codec** (`codec.rs`, audit P2 — length-prefixed
  framing like the storage manifest codec; serde_json's decimal-array `Vec<u8>`
  rendering cost ~3–4x; decode failures are loud: magic/version-checked `Err`s
  logged before the message is dropped; the Raft WAL keeps the shared
  control-plane serde_json `PersistedState` format).
  `tests/snapshot_catchup.rs` (crash a follower, write past the
  threshold so the leader compacts, restart → it catches up via snapshot).
- **C (done)** — single-server Raft **membership change** (`change_membership`):
  config lives in the log (`RaftCore`, branched so the control plane is unchanged);
  a node uses the latest log config for quorum/election, the config rides snapshots
  + `InstallSnapshot`, a removed node stops campaigning, and changes are restricted
  to a single-server delta + one-in-flight + no leader self-removal.
  `tests/membership.rs` (remove a follower, add + catch up a node, reconfigure off
  a crashed node, reject multi-server/self-removal, reproducibility). The
  **automatic trigger is now wired** (SimEnv): `reconfigure_step` takes one
  single-server step toward a desired voter set (remove an extra non-leader voter
  before adding a missing one), and `spawn_reconfigure_loop` drives it from an
  **epoch-driven pull** — each group leader polls the control plane's replicated
  `Metadata.tablets[t].replicas` and reconfigures itself (no new control→data
  command; mirrors the control plane's `reconcile_loop` — decision in
  `reconfigure_step`, timing in the loop). `tests/reconfigure_trigger.rs` proves
  the end-to-end cascade (crash → detector `Down` → reconciler `CasTabletReplicas`
  → group leader swaps the dead node for a same-zone spare, which catches up and
  the group keeps serving). The `ProdEnv`/`animusd` production assembly (hosting
  groups + leader-reporting for routing) remains.
  - **Test gotcha (membership):** pre-start a to-be-added node knowing only the
    *current* voters, NOT itself — a node started inside its own initial config is
    a voter that can campaign, win, and inject itself into the group before the
    real add (`RaftCore::start_election` gates on `is_voter`). A `RaftKvNode::start`
    whose `all_nodes` excludes its own id is a quiet non-voter until the leader adds
    it. (Caught by the `reconfigure_trigger` seed sweep — a single seed hid it.)
- **D (superseded by ADR 0028) — tablet split is no longer a data-plane
  concern at all.** The original design (`KvCommand::Split`, `propose_split`,
  `current_split_bound`, `Coresident`-minted sibling groups, a split hook fired
  on apply) is **deleted**. Since ADR 0026 Stage B gave every tablet a node
  hosts one shared env (stream-addressed) and ADR 0028 gave every tablet on a
  node one shared `StorageEngine` (confined by its own `StorageScope`), a split
  needs no data-plane command at all: the control plane's `MetaCommand::
  SplitTablet` (`animus-control`) narrows the source tablet's `StorageScope`
  range and the new sibling's range starts already covering live data on the
  *same* engine — no handoff, no new-group bootstrap message, nothing for this
  crate to agree on. `animusd`'s per-node join-host loop then simply starts the
  new tablet's `RaftKvNode` the same way it starts any fresh tablet. See
  ADR 0028 and `animusd/CLAUDE.md` for the full mechanism and the calling side.
  This history (in-band `Coresident` sibling minting, the split-hook
  recovery-idempotency story, the "a group can be split more than once"
  CAS-against-a-moving-boundary design, `SPLIT_BOUND_KEY`'s in-engine
  durability) is preserved in the root `CLAUDE.md` Engineering Practices
  section and ADR 0017's original text for archaeology — it no longer
  describes any code in this crate.

## Tests

`cargo test -p animus-cp-data` — `tests/single_tablet.rs` (SimEnv; drive with
`run_for`, never `run()` — the driver has perpetual heartbeat/election timers).
