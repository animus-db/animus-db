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
  *with* the entry. **`RaftKvNode::scope_range()`** (additive accessor,
  2026-08-07) is the read side: a point-in-time snapshot of the group's own
  live `StorageScope` range, meant to be both (1) checked against a proposed
  key **before** proposing (a pre-propose reject, not just relying on the
  embedded fence — see `animusd/CLAUDE.md`'s CP-routing section for why the
  pre-check is load-bearing, not redundant, given how `animusd` confirms a
  write) and (2) stamped as that same proposal's `fence`. It was this
  accessor's *absence* that had left the fences unwired in `animusd` for as
  long as they existed — see the root `CLAUDE.md`'s entry on a safety
  mechanism with zero production callers.
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
- **C (done, extended by ADR 0029)** — single-server Raft **membership change**
  (`change_membership`): config lives in the log (`RaftCore`, branched so the
  control plane is unchanged); a node uses the latest log config for
  quorum/election, the config rides snapshots + `InstallSnapshot`, a removed
  node stops campaigning, and changes are restricted to a single-server delta +
  one-in-flight + no leader self-removal. `tests/membership.rs` (remove a
  follower, add + catch up a node, reconfigure off a crashed node, reject
  multi-server/self-removal, reproducibility). The **automatic trigger is
  wired** (SimEnv): `RaftKvNode::reconfigure_step(desired, down)` takes one
  single-server step toward a desired voter set, and `spawn_reconfigure_loop`
  drives it from an **epoch-driven pull** — each group leader polls the control
  plane's replicated `Metadata.tablets[t].replicas` (+ `Down`-status members)
  and reconfigures itself (no new control→data command; mirrors the control
  plane's `reconcile_loop` — decision in `reconfigure_step`, timing in the
  loop). `tests/reconfigure_trigger.rs` proves the end-to-end cascade (crash →
  detector `Down` → reconciler `CasTabletReplicas` → group leader swaps the
  dead node for a same-zone spare, which catches up and the group keeps
  serving).
  - **ADR 0029 gave `reconfigure_step` a priority-ordered, down-aware sequence**
    (see its doc comment): remove an extra `Down` voter first (unchanged repair
    order); add a missing voter before removing a *healthy* extra one, gated on
    every `desired` member having caught up to `commit_index` (a healthy move
    must never drop quorum margin under a still-catching-up newcomer); and if
    the only remaining delta is removing the **leader's own** replica —
    previously a permanently-stuck case, `change_membership` always rejects
    self-removal — transfer leadership (`RaftCore::transfer_leadership` /
    `RaftMsg::TimeoutNow`, `animus-control`) to a caught-up member of `desired`
    first, so the new leader performs the removal itself as an ordinary step.
  - **A follow-up fix hardened three related defects in this sequence** (see the
    root `CLAUDE.md` engineering-practices entry for the full writeup):
    (A) step 4's selection (`peer_match(n) >= commit_index()`) and
    `transfer_leadership`'s arm gate (`peer_match(target) == last_log_index()`)
    used *different* thresholds — under sustained writes on a write-hot tablet
    (`propose` is fire-and-forget, so `last_log_index` moves before any
    replication round trip) the target was essentially always one entry short
    of `last_log_index` at the sampling instant, so the arm silently failed
    *forever*, and the discarded `bool` meant nothing surfaced it. Fixed by
    relaxing the arm gate to `>= commit_index` (now consistent with the
    selector) and having `propose`/`change_membership` **freeze** (`NotLeader`,
    hinting the target) while a transfer is armed — the freeze is what lets a
    target that is merely "caught up to commit" actually close the gap to
    `last_log_index`, since new writes stop landing once armed;
    `broadcast_append` now sends `TimeoutNow` only once the target *reaches*
    `last_log_index`, and an unresolved transfer **aborts** (clears the arm,
    resumes proposing) if a one-election-timeout deadline passes with no
    step-down. (B) was the missing proposal-freeze itself (folded into the same
    fix — see `animus-control/CLAUDE.md`'s "Leadership transfer" entry for the
    core-level mechanics). (C) step 1's down-extra search reused the generic
    "lowest-id extra" helper and only *then* filtered it on down-ness, so a
    `Down` extra sorting after a healthy one was invisible to the ungated
    removal — the step fell through to step 3's catch-up-gated healthy removal
    instead, which could then stall the *whole* step behind an unrelated
    `desired` survivor's catch-up state. Fixed by searching directly for an
    extra that *is* down, independent of id order. `reconfigure_step` now also
    traces (`tracing`) both a successful step-4 arm and an arming failure, so a
    stalled transfer is no longer silent. Regressions:
    `tests/leader_transfer_reconfigure.rs` (a sim-level reproduction — the
    hand-driven variant demonstrably fails against the pre-fix source) and
    `tests/reconfigure_down_extra_priority.rs` (defect C in isolation:
    extras `{healthy lower-id, down higher-id}` with the `desired` survivor
    deliberately lagging — the down extra must still be removed in one step,
    with no catch-up gate).
  - **ADR 0029 also fixed a latent bug `reconfigure_step`'s original design
    never triggered: the read barrier's quorum (`majority()` + which peers get
    probed) was keyed on `all_nodes` (this node's hosting-time peer snapshot),
    never the live `config()`.** Every membership change before ADR 0029 was a
    same-size, pre-known swap — a failure-repair spare was already listed in
    every replica's `all_nodes` from the start (see the join-host gotcha
    below), so `all_nodes` never actually diverged from the live config. A
    healthy rebalance move can rotate a majority of a group's replicas onto
    nodes no surviving replica's `all_nodes` ever included, at which point a
    stale-`all_nodes` read barrier can only ever self-ack and every
    `linearizable_get`/`scan` on that tablet times out — reporting the key
    **absent** — forever after. Fixed by deriving both from `self.config()`;
    `all_nodes` is no longer a stored field (only a one-time bootstrap value
    for `RaftCore::new`/`recovered`'s *initial* config). Regression:
    `tests/read_index.rs::linearizable_read_succeeds_after_a_full_membership_rotation`
    (rotates `{0,1,2}` → `{2,3,4}`, two of three members replaced — the exact
    production shape — and stops the departed nodes outright, since a
    still-live departed peer can accidentally still ack on term match alone
    and mask the bug). See the root `CLAUDE.md` "a cached per-node handle
    needs an explicit re-sync step" entry — this is that pattern's data-plane
    read-barrier instance.
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
  SplitTablet` (`animus-control`) narrows the source tablet's range in
  replicated `Metadata`, and the new sibling's range starts already covering
  live data on the *same* engine — no handoff, no new-group bootstrap message,
  nothing for this crate to agree on. `animusd`'s per-node join-host loop then
  simply starts the new tablet's `RaftKvNode` the same way it starts any fresh
  tablet — **and, separately, must also call `RaftKvNode::narrow_scope` on the
  source tablet's already-hosted `RaftKvNode`** (its `StorageScope` predates
  the split and is otherwise never touched again; this was initially missed —
  see the root `CLAUDE.md` Engineering Practices "cached per-node handle"
  entry). See ADR 0028 and `animusd/CLAUDE.md` for the full mechanism and the
  calling side.
  This history (in-band `Coresident` sibling minting, the split-hook
  recovery-idempotency story, the "a group can be split more than once"
  CAS-against-a-moving-boundary design, `SPLIT_BOUND_KEY`'s in-engine
  durability) is preserved in the root `CLAUDE.md` Engineering Practices
  section and ADR 0017's original text for archaeology — it no longer
  describes any code in this crate.

## Per-node tablet-host reconciler (ADR 0031, `host` module)

**Wired into production as of PR4.** `animusd` used to scatter "which tablets
does this node host, and what should it do about each one" across four
independent `ProdEnv` loops (`cp_join_host_loop`/`cp_join_host`,
`cp_gc_loop`/`cp_gc_release_phase`, `cp_reconfigure_loop`), each re-deriving
its own slice of replicated `Metadata` and its own per-node bookkeeping
(`minted`, `pending_release`). `host::plan` (PR3) unifies the *decision* those
four loops used to make into one pure, synchronous function — mirroring this
crate's own sync-core/async-driver split (`RaftCore` decides, the driver does
I/O): **the decision lives here and is unit-tested directly.**
`host::Reconciler<E: Env, S: StorageEngine>` (PR4) is the **execute** half,
also living in this crate (not `animusd`) so the crate owns the whole
lifecycle's invariants and is directly `SimEnv`-testable:

- **It owns the hosted `RaftKvNode` map** — `hosted: BTreeMap<TabletId,
  RaftKvNode<E, S>>` — making it the **single writer** of "does this node
  host tablet T." `Reconciler::new(env, storage, base_id, prefix_for,
  on_host, on_teardown)` takes this node's `raftkv` env + shared storage
  engine + base id, a `prefix_for: Fn(&str) -> Vec<u8>` hook (the caller's
  own table→`StorageScope`-prefix convention — `animusd`'s
  `escape(table)` — this crate never duplicates it), and two hooks,
  `on_host: Fn(TabletId, &RaftKvNode<E, S>)` / `on_teardown: Fn(TabletId)`,
  that let a caller (`animusd`) mirror every hosting change into its own
  routing registry (`ClusterEdgeState`) as a **read-only reaction** — never a
  second writer.
- **`Reconciler::tick(&mut self, view: &MetadataView)` is the whole per-tick
  contract**: gather `TabletFacts` from its *own* hosted nodes
  (`is_leader()`, `config()`, `scope_range()`) plus an async
  `StorageScope::has_data` check for any not-yet-hosted join candidate, call
  `plan` exactly once, then execute the returned actions **in the order
  `plan` emits them** (`NarrowScope` → `Host` → `Reconfigure` →
  `Release`/`Reclaim`). `Host` constructs the `StorageScope` from
  `prefix_for` + the action's range and calls `RaftKvNode::start_hosted`
  with the full or others-only config (`animusd::cp_join_host`'s exact
  decision, now ported here); `Release`/`Reclaim` mirror
  `animusd::cp_gc_tablet`'s teardown exactly: call `on_teardown` (unregister
  from the caller's routing *before* touching the driver), `shutdown()`,
  poll `is_stopped()` bounded by `RECLAIM_STOP_TIMEOUT` (10s, via
  `env.sleep` — no tokio-only primitive, so this stays `SimEnv`-testable),
  re-register via `on_host` and leave `LocalState` untouched on a timeout (so
  `plan` re-emits the identical action next tick), else narrow to
  `erase_bound` (Release only — **the sibling-sparing invariant this whole
  redesign exists to make structural**) then `erase_scope()`, delete the
  tablet's WAL file, and only then `LocalState::confirm_torn_down`.
- **The caller still owns the trigger and the pre-recovery guard.**
  `Reconciler::tick` takes no clock/RNG of its own beyond `env.sleep`/
  `env.now()` inside the Release/Reclaim teardown wait; deciding *when* to
  call `tick` (an event-driven `metadata_watch` wake + a periodic fallback,
  ADR 0031 §trigger) and the `last_applied() == 0` pre-recovery guard (a live
  control-plane `RaftNode` read this crate has no business taking) both stay
  in `animusd::tablet_host_reconciler_loop`.
- `tests/reconciler.rs::reconciler_hosts_narrows_releases_and_confirms_sparing_a_sibling`
  drives a `Reconciler<SimEnv, MemoryEngine>` through a full host → narrow →
  (a **real** 2-voter Raft membership change excluding this node) → release
  → confirm sequence and asserts the sibling-sparing erase bound end to end
  through the reconciler's own `tick`, mirroring
  `narrow_scope.rs::narrow_then_erase_scope_spares_a_co_hosted_siblings_data`'s
  invariant at the execute-loop level instead of the bare-primitive level. A
  dedicated `SimEnv` fault-injection corpus for the full lifecycle across more
  scenarios remains PR5.

- **Contract**: `plan(view: &MetadataView, facts: &BTreeMap<TabletId,
  TabletFacts>, state: &LocalState, base_id: NodeId) -> (Vec<HostAction>,
  LocalState)`. Pure and synchronous — no `Env`, clock, RNG, or I/O.
  `MetadataView` is a small owned projection (`tablets: BTreeMap<TabletId,
  Tablet>`, `down: BTreeSet<NodeId>`) — deliberately *not* the whole
  `animus_control::Metadata`, keeping this crate decoupled from the control
  plane's full state shape. `TabletFacts` bundles the impure per-tablet inputs
  the caller must gather before calling (`hosted`, `is_leader`,
  `config_excludes_me`, `scope_range`, `has_data` — see each field's doc for
  exactly which live read backs it). `LocalState` is the pure-state mirror of
  `animusd`'s `minted` claim set + `pending_release` epoch-stability
  dampener, threaded from one `plan` call to the next.
- **Actions, in a fixed emission order** (`NarrowScope` → `Host` →
  `Reconfigure` → `Release`/`Reclaim`): `HostAction::NarrowScope` (narrow an
  already-hosted tablet's scope to its current metadata range — provably
  narrow-only, `is_subrange`), `Host` (stand up a fresh/joining/restarting
  tablet), `Reconfigure` (one `reconfigure_step` toward the desired replica
  set for every tablet this node leads, carrying the down-set), `Release`
  (tear down a tablet moved off this node, gated by
  `RELEASE_CONFIRM_TICKS` consecutive confirming calls at an unchanged
  epoch — the ADR 0029 dampener, ported verbatim) and `Reclaim` (tear down a
  tablet whose whole table was dropped). `Release`'s `erase_bound` is always
  the tablet's **current** metadata range, never a `TabletFacts::scope_range`
  fact — the sibling-corruption regression (root `CLAUDE.md`) is now provable
  directly in a unit test
  (`release_erase_bound_is_always_the_current_metadata_range_never_the_stale_scope_fact`)
  instead of only via a timing-dependent end-to-end reproduction.
- **`plan` never removes a tablet from `LocalState::hosted` on its own** when
  emitting `Reclaim`/`Release` — real teardown is async and can time out
  (mirroring the pre-PR4 `animusd::cp_gc_tablet`'s conditional
  `minted.remove`, which only fired once shutdown + erase + WAL removal
  actually succeeded — that function is deleted now that `Reconciler::
  teardown` does the same job directly). The caller (`Reconciler::tick`)
  calls `LocalState::confirm_torn_down` once its own teardown has actually
  completed; until then, the next `plan` call keeps re-planning the same
  action, exactly like the pre-PR4 loop retrying on a later tick
  (`a_pending_reclaim_is_replanned_until_confirmed_torn_down`).
- `plan_join_host`, `tablets_to_reclaim`, `tablets_to_release` are also
  exported standalone (not just as `plan` internals) — direct semantic ports
  of `animusd::topology`'s functions of the same name **before ADR 0031**
  (that module now holds only the routing decision, `tablet_for_key`/
  `decide_cp_route` — the hosting/GC predicates moved here for good),
  parity-tested against the same cases. `plan_join_host`'s `initial_formation`
  decision (fresh formation vs. non-voter join, keyed on `epoch <=
  Epoch::INITIAL`) is unchanged from the original; the async
  `StorageScope::has_data` restart-upgrade this pure function can't do stays a
  caller-gathered fact (`TabletFacts::has_data`), gathered by
  `Reconciler::tick` itself now (`gather_facts`).
- 29 unit tests in `src/host.rs` (`cargo test -p animus-cp-data --lib host::`):
  parity ports of every pre-ADR-0031 `animusd::topology` hosting/GC test case,
  idempotence on a converged state, reclaim/release mutual exclusion on
  arbitrary input, the release dampener's exact-N-ticks/epoch-reset/
  re-add-cancels semantics, the narrow-only invariant (never widens),
  reconfigure's leader-gating, the has-data restart upgrade, and the
  reclaim/release replan-until-confirmed behavior.

## Tests

`cargo test -p animus-cp-data` — `tests/single_tablet.rs` (SimEnv; drive with
`run_for`, never `run()` — the driver has perpetual heartbeat/election timers).
`tests/leader_transfer_reconfigure.rs` (ADR 0029 follow-up fix — a 3-voter
group under sustained writes converges when `reconfigure_step` must relocate
the leader itself; the hand-driven variant that proposes immediately before
every `reconfigure_step` call is the one proven to fail against the pre-fix
source) and `tests/reconfigure_down_extra_priority.rs` (defect C in
isolation: a `Down` extra sorting after a healthy one is still removed first,
with no catch-up gate) round out the reconfigure/transfer coverage alongside
`tests/reconfigure_trigger.rs` and `tests/membership.rs`.
`tests/reconciler.rs` (ADR 0031 PR4) drives the `host::Reconciler` executor
end to end under `SimEnv` — see the host-module section above. Note its
documented `SimEnv` gotcha: a `tick()` whose planned action tears a group
down internally polls `env.sleep()` (waiting out `is_stopped()`), so the
whole scenario runs as a spawned task driven by `Simulator::run_for`, never a
bare `block_on`.
