# CLAUDE.md — animus-cp-data

This file provides guidance to Claude Code (claude.ai/code) when working in this
crate.

## Purpose

The **leaderful, strongly-consistent (CP) data plane** (ADR 0016, ADR 0017):
each tablet is its own Raft group with a single leader serving **linearizable**
single-tablet reads/writes, durable on a real `StorageEngine`. For v1 (ADR
0019) this is the *only* data plane — the original leaderless AP plane
(`animus-data`) is deferred and its crate deleted. The control plane
(`animus-control`) remains the metadata authority (tablet map, placement,
failure detection).

It instantiates `animus-control`'s generic, sync, I/O-free `RaftCore<C, S>`
(ADR 0009) with `C = KvCommand` and a **`DRIVER_APPLIED` state machine**
(`KvState`, a unit placeholder): the core agrees the *order* of commands but
does **not** apply them in-core (a `StorageEngine` apply is async I/O the sync
core can't do). The core buffers committed-and-durable commands as effects;
this crate's **async driver** drains them (`RaftCore::drain_apply`) and applies
to the engine — the same sync-core/async-driver split as `animus-consensus`'s
`AccordCore`.

## Entry points

Three modules:

- **`lib.rs`** — `RaftKvNode<E, S>` (the running tablet-group node) and its
  command/state types (`KvCommand`, `KvState`), `StorageScope`, the fenced
  commands, ReadIndex + CAS, the consensus-loop/apply-task split, and
  `ProposeSignal`. See the API bullets below.
- **`host.rs`** — the per-node tablet-host reconciler (ADR 0031): the pure
  `plan()` decision, `Reconciler` executor, `MetadataView`/`TabletFacts`/
  `LocalState`, and the `HostAction` set (incl. `Absorb`/`WidenScope`). 34
  unit tests. See "The host module".
- **`codec.rs`** — the crate's compact binary wire/image codec (ADR 0017 A.2):
  length-prefixed framing (like the storage manifest codec), magic/version
  checked. Carries `KvWire` messages and engine images; `serde_json`'s
  decimal-array `Vec<u8>` rendering cost ~3–4x. Decode failures are loud (a
  logged `Err` before the message is dropped). The Raft WAL keeps the shared
  control-plane serde_json `PersistedState` format.
- **`hlc.rs`** (ADR 0018 §2, PR1) — a pure, I/O-free Hybrid Logical Clock:
  `HlcTimestamp { wall_ms, logical }` and the per-node `Hlc` (`mint`/`witness`,
  both take the caller-sampled `Nanos` — `Hlc` never touches an `Env` or the
  wall clock itself). `pack`/`unpack` encode a timestamp as the storage-engine
  `u64` MVCC version directly (`(wall_ms << 20) | logical`, no node-id bits —
  settled over `animus-consensus`'s `(logical, node)` scheme because a string
  `NodeId`, ADR 0040, can't bit-pack); the 20-bit `LOGICAL_BITS` budget is
  hard-`assert!`-checked in `pack` (never `debug_assert!` — a silent overflow
  would silently collapse two distinct timestamps to one version). Not yet
  wired into `RaftKvNode`'s apply path — that lands in PR2, which replaces the
  current floor-scaled-Raft-index `mvcc_version` (see the Key invariants
  section above) with this packed HLC.

### lib.rs API

- **`RaftKvNode<E, S>`** — `start(env, all_nodes, storage)` (defaults scope to
  `StorageScope::whole()`, the identity — byte-for-byte the pre-scoping
  behavior), `start_scoped`/`start_hosted` for an explicit scope.
  `put`/`put_batch`/`delete` propose via Raft (honored on the leader);
  `is_leader`; `linearizable_get` (ReadIndex); `local_get` (a replica's raw,
  *non*-linearizable engine read — a test/observability aid).
- **`StorageScope`** (ADR 0026/0028) — confines a node's physical key access
  within a possibly node-shared `StorageEngine`: a `prefix` (the owning
  table's identity, `escape(table_name)`) plus a live-narrowable `range` (this
  tablet's sub-portion of that keyspace, an `Arc<Mutex<KeyRange>>` so a split
  narrows it without restarting the group). `physical(key)` maps a logical key
  to its on-engine key; `strip_in_range`/`strip_prefix_only` invert it for
  scans. `has_data(&storage)` is the async presence check `animusd` uses to
  tell "re-forming after a restart" (full voter config) from "brand-new spare
  joining" (non-voter) without a durable per-tablet marker.
  `physical_bounds()` (ADR 0034) computes a genuinely bounded physical upper
  bound via the **prefix-upper-bound** trick (increment the prefix's last
  non-`0xFF` byte) — `None` only for `whole()` or an all-`0xFF` prefix — so a
  periodic byte-estimate over an unbounded-above logical range never degrades
  into a whole-engine scan.
- **Fenced commands** (ADR 0026) — `put_fenced`/`delete_fenced`/`cas_fenced`/
  `put_batch_fenced` (and unfenced siblings using `KeyRange::whole()`) carry a
  `fence: KeyRange` *inside the proposed command*, stamped by the leader at
  propose time from its own `StorageScope.range`. See "Key invariants" for why
  this is load-bearing. **`scope_range()`** (additive accessor) is the read
  side: a point-in-time snapshot of the group's live scope range, used both to
  reject a key **before** proposing (a pre-propose check) and to stamp that
  proposal's `fence`.
- **`approx_bytes()`** (ADR 0034) — the per-tablet cheap byte estimate
  `animusd::auto_split_loop` gates on, delegating to
  `StorageEngine::approx_bytes_in_range` over this group's live
  `physical_bounds()`.
- **Batch put** — `KvCommand::Batch(Vec<(k, v)>)` + `put_batch` commit **N
  keys as one Raft log entry** (one propose → one commit round → one apply),
  the bulk-write throughput primitive. Every key merges at the entry's shared
  Raft `index` (per-key LWW is well-defined since keys are distinct); composes
  with a coalesced-fsync `merge_batch`. Re-applies idempotently on recovery
  like a single `Put`.
- **Linearizable CAS** — `cas(key, expected, value) -> ProposeResult` proposes
  `KvCommand::Cas` (set iff committed value `== expected`; `expected: None` ==
  "only if absent"); `cas_result(index) -> Option<bool>` reads the outcome
  recorded at the `Accepted { index }`; `compare_and_swap` is the all-in-one
  (propose on leader, wait for apply, return the outcome — `None` if not
  leader / times out). All additive; existing signatures unchanged.
- **Admin/debug accessors** (ADR 0020, consumed by `animusd`) — read-only
  `role`/`term`/`commit_index`/`last_applied`/`durable_index`/
  `snapshot_index`/`log_len` (thin locks over `RaftCore`), and `storage()` (a
  `&S` borrow so the assembly layer surfaces SSTable/WAL debug views without
  engine state leaking into the core). **`engine_applied_index()`** is the
  confirm-by-index primitive: the engine-merge watermark linearizable reads
  gate on, so a proposer confirms a specific `Accepted { index }` applied
  (`engine_applied_index() >= index` while still leader in that term) instead
  of polling value equality (which false-negatives under a concurrent same-key
  overwrite). `linearizable_scan`/`local_scan` push a bounded range into
  `storage.scan(start, end)` (key-ordered by contract — no re-sort, no
  whole-tablet materialization); the unbounded-above case derives its bound
  from `physical_bounds` rather than an `entries()` whole-engine scan (see
  "What's non-obvious").
- **`KvWire`** — the data-plane wire enum wrapping `RaftMsg` plus the ReadIndex
  read-barrier probes (`ReadProbe`/`ReadProbeAck`). The probes are driver-only,
  so ReadIndex lives entirely in this crate and the shared `RaftCore`/`RaftMsg`
  are untouched.
- **Stream addressing** (ADR 0026 Stage B) — `start_hosted(.., stream)`
  addresses a tablet's Raft traffic by `(node, stream)`
  (`env.send_stream`/`recv_stream`, `stream` = the tablet id) instead of a
  distinct `NodeId`/env per tablet, so every tablet a node hosts shares one
  env/port. Replaces the retired `Coresident` sibling-minting approach.

## Key invariants

State once here; cross-referenced from the sections below.

- **The Raft log index is the MVCC version — scaled by this group's own
  `version_floor` (cross-group LWW fix, confirmed real; full writeup in
  `docs/engineering-lessons.md`).** Apply computes
  `effective_version(floor, index) = floor * VERSION_FLOOR_SCALE + index`
  (`VERSION_FLOOR_SCALE = 2^40`) and stamps *that* as the engine `version` at
  all four apply sites (`Put`/`Batch`/`Delete`/`Cas`) — never the raw `index`
  directly, so per-key LWW reproduces the agreed Raft total order and
  re-applying on recovery stays idempotent (the same command always computes
  the same `effective_version`). `floor` (`0` for a tablet that has never
  been split/merged — byte-identical to using the raw index) closes a real
  hazard: every tablet on a node shares one physical `StorageEngine`
  (ADR 0026/0028), so a **fresh** group's own log index restarting low could
  carry a version no higher than what a *different* group (the split source,
  or an absorbed merge sibling) already stamped for the same key — and
  per-key LWW (`StorageEngine::merge`) would silently drop the write.
  `RaftKvNode::start_hosted_with_floor` seeds a fresh group's floor at
  construction (`start_hosted`/`start`/`start_scoped`/`start_with_metrics`
  all default to `0`, unchanged); `bump_version_floor` (a `fetch_max`,
  mirroring `narrow_scope`/`widen_scope`'s one-directional shape) raises an
  **already-running** group's floor for the merge-survivor case, called by
  the reconciler alongside `widen_scope` for the same `WidenScope` action.
  Both read `animus_tablet::Tablet::version_floor` — computed once by the
  control plane's `SplitTablet`/`MergeTablets` apply, so every replica
  converges on the identical value with no live per-replica computation.
  `engine_applied` and the group's own Raft log-matching are untouched — only
  the *storage-layer version number* a command stamps changes. Regression:
  `tests/cross_group_lww.rs` (reproduces the hazard with an un-seeded
  fresh/widened group, then proves the seeded/bumped variant keeps the write,
  for both the split and merge shape).
- **CAS is decided at *apply* time, not propose time** — this is what makes it
  linearizable and contention-correct. `RaftCore` agrees only the order; `Cas`
  rides through as opaque data. Apply evaluates it in commit order against the
  key's *current committed* value (every earlier entry in the batch has
  already merged) and compares to `expected`; equal → merge at `index`, else
  no-op. Every replica applies the same order against the same state with no
  clock/RNG, so every replica makes the **identical** decision — two CAS
  racing from the same `expected` have exactly one winner (whichever Raft
  ordered first). Outcome is stashed in driver `CasResults` keyed by the log
  index (a `BTreeMap<u64,bool>`).
- **`engine_applied` vs `last_applied`.** The two-task split (below) means the
  core's `last_applied` (a buffer cursor the consensus loop advances) *leads*
  the engine. Linearizable reads therefore gate on the separate
  **`engine_applied`** atomic the apply task advances after each merge —
  **never** `last_applied` (else a read could observe past the engine).
- **Durable-before-visible** (ADR 0009): effects are only drained for fsynced
  entries, and the engine write follows the WAL `fsync`.
- **Fences are per-entry, decided at apply, and backed by a pre-propose
  check.** Every replica's apply checks a command's key(s) against the fence
  **embedded in the log entry**, never a locally-polled value — so two
  replicas at different points in observing a split's `Metadata` make the
  identical accept/reject decision for the same entry. The embedded fence only
  has to cover the residual race between a caller's pre-propose `scope_range()`
  check and the entry's actual apply; the pre-propose reject is load-bearing,
  not redundant, given how `animusd` confirms a write (see `animusd/CLAUDE.md`
  and the root `CLAUDE.md` entry on a safety mechanism with zero production
  callers).
- **An `Absorb` teardown DRAINS the committed log into the engine BEFORE
  halting, and the survivor's `WidenScope` is deferred until the absorb
  confirms.** The apply task exits on `shutdown()` at its next loop-top check
  **without** draining committed-but-unapplied entries, and teardown then
  deletes the group's Raft WAL — the only local copy. That is harmless for
  `Release`/`Reclaim` (they erase the data anyway) but fatal for `Absorb`: the
  absorbed range is about to be *served* from this same engine through the
  survivor's widened scope, so an acked write still in the commit pipeline
  (commit-index propagation lags the leader by up to a heartbeat; the
  reconciler's watch fires on the merge commit within ms) would silently never
  reach the engine. The drain (`ABSORB_DRAIN_TIMEOUT`) waits — while the driver
  is live — for commit to cover the full local log and engine-applied to cover
  that commit; on timeout with engine ≥ local-commit it proceeds with a loud
  warning (documented residual), else retries next tick. `plan`'s `absorbing`
  gate (any `state.hosted ∩ view.merged` tablet defers every `WidenScope`)
  sequences drain-before-widen across the two otherwise-independent actions.
  This is ADR 0033 post-merge hardening — the 1-in-5 `ProdEnv` flake in
  `animusd`'s `tablet_merge.rs` was a real, permanent false-"absent". The
  read-side halves (`linearizable_get_served`'s served/absent disambiguation;
  `animusd`'s `cp_get_local`/`cp_scan_local` scope pre-checks) live in this
  crate's `RaftKvNode` + `animusd` — see the root `CLAUDE.md` and ADR 0033.

## The host module

**Wired into production as of ADR 0031 PR4.** `animusd` used to scatter "which
tablets does this node host, and what should it do about each" across four
independent `ProdEnv` loops (join-host, GC release/reclaim, reconfigure), each
re-deriving its own slice of `Metadata` and its own bookkeeping. `host::plan`
unifies the **decision** into one pure, synchronous function (mirroring this
crate's own sync-core/async-driver split: the core decides, the driver does
I/O — so the decision is unit-tested directly). `host::Reconciler<E, S>` is the
**execute** half, also in this crate so the crate owns the whole lifecycle's
invariants and is directly `SimEnv`-testable.

- **`plan` contract**: `plan(view: &MetadataView, facts: &BTreeMap<TabletId,
  TabletFacts>, state: &LocalState, base_id: NodeId) -> (Vec<HostAction>,
  LocalState)`. Pure and synchronous — no `Env`, clock, RNG, or I/O.
  `MetadataView` is a small owned projection (`tablets`, `down`, **`merged`** —
  ADR 0033, mirroring `Metadata::merged_tablets`), deliberately *not* the whole
  `animus_control::Metadata`, keeping the crate decoupled from the control
  plane's state shape. `TabletFacts` bundles the impure per-tablet inputs the
  caller gathers first (`hosted`, `is_leader`, `config_excludes_me`,
  `scope_range`, `has_data`). `LocalState` is the pure mirror of `animusd`'s
  `minted` claim set + `pending_release` epoch dampener, threaded call to call.
- **`plan` never removes a tablet from `LocalState::hosted` on its own** when
  emitting a fallible teardown (`Reclaim`/`Release`) — real teardown is async
  and can time out. The caller calls `LocalState::confirm_torn_down` once its
  own teardown actually completes; until then the next `plan` re-plans the same
  action (`a_pending_reclaim_is_replanned_until_confirmed_torn_down`). Compare
  the pre-PR4 `animusd::cp_gc_tablet`'s conditional `minted.remove`, which only
  fired once shutdown + erase + WAL removal succeeded.
- **`Reconciler::tick(&mut self, view: &MetadataView)` is the whole per-tick
  contract**: gather `TabletFacts` from its *own* hosted nodes (`gather_facts`
  — `is_leader()`, `config()`, `scope_range()`, plus an async
  `StorageScope::has_data` for a not-yet-hosted join candidate), call `plan`
  exactly once, then execute the returned actions **in the order `plan`
  emits**. The reconciler **owns the hosted map** (`hosted: BTreeMap<TabletId,
  RaftKvNode<E, S>>`), making it the single writer of "does this node host
  tablet T". `Reconciler::new(env, storage, base_id, prefix_for, on_host,
  on_teardown)` takes a `prefix_for: Fn(&str) -> Vec<u8>` hook (the caller's
  table→scope-prefix convention — `animusd`'s `escape(table)`, never
  duplicated here) and `on_host`/`on_teardown` hooks that let `animusd` mirror
  hosting changes into its `ClusterEdgeState` routing registry as a **read-only
  reaction**, never a second writer.
- **The caller still owns the trigger and the pre-recovery guard.** Deciding
  *when* to call `tick` (an event-driven `metadata_watch` wake + a periodic
  fallback, ADR 0031 §trigger) and the `last_applied() == 0` pre-recovery guard
  (a live control-plane `RaftNode` read this crate has no business taking) both
  stay in `animusd::tablet_host_reconciler_loop`.
- `plan_join_host`, `tablets_to_reclaim`, `tablets_to_release` are also
  exported standalone — direct semantic ports of `animusd::topology`'s
  functions of the same name *before ADR 0031* (that module now holds only the
  routing decision, `tablet_for_key`/`decide_cp_route`), parity-tested against
  the same cases. `plan_join_host`'s `initial_formation` decision (fresh
  formation vs. non-voter join, keyed on `epoch <= Epoch::INITIAL`) is
  unchanged; the async `has_data` restart-upgrade this pure function can't do
  stays a caller-gathered fact.

### HostAction

Emitted in this fixed order: `NarrowScope`/`WidenScope` → `Host` →
`Reconfigure` → `Release`/`Reclaim`/`Absorb`.

| Action | What it does |
|--------|--------------|
| `NarrowScope` | Narrow an already-hosted tablet's scope to its current metadata range — provably narrow-only (`is_subrange`). |
| `WidenScope` | ADR 0033 dual: widen when the metadata range *grew* (the surviving `left` of a `MergeTablets`) — provably widen-only. A range neither subset nor superset of the live scope is a defensive no-op, never guessed. Deferred while an absorb is pending (see Key invariants). Also carries `version_floor` (read off `Tablet::version_floor`) so the executor bumps the survivor's cross-group LWW floor in the same pass (see the MVCC-version invariant above). |
| `Host` | Stand up a fresh/joining/restarting tablet via `start_hosted`, with full or others-only config (`animusd::cp_join_host`'s exact decision). Carries `version_floor` (read off `Tablet::version_floor`) to seed the group's cross-group LWW floor (see the MVCC-version invariant above). |
| `Reconfigure` | One `reconfigure_step` toward the desired replica set, for every tablet this node leads, carrying the down-set. Replanned every tick a node leads a group (a no-op once matched). |
| `Release` | Tear down a tablet moved off this node, gated by `RELEASE_CONFIRM_TICKS` consecutive confirming calls at an unchanged epoch (the ADR 0029 dampener). Erases up to `erase_bound`, which is **always the tablet's current metadata range**, never a `TabletFacts::scope_range` fact — the sibling-sparing invariant this redesign makes structural. |
| `Reclaim` | Tear down a tablet whose whole table was dropped; erases. |
| `Absorb` | ADR 0033: tear down a tablet that vanished because it was **merged into a sibling** (`tablet ∈ view.merged`). Unlike `Reclaim`, **never erases** — the survivor now owns the range on the same node-shared engine. Drains before halting (see Key invariants). |

- **`Reclaim` vs `Absorb` cannot be told apart from `tablets` alone.** A hosted
  tablet vanishing looks identical whether its table was dropped or it was
  merged; inferring "merge" from "some other tablet's range now covers mine" is
  unsound (two tables' still-unsplit tablets can have byte-identical
  `KeyRange::whole()` ranges, with no table identity in scope). `merged` is the
  explicit signal (a tiny, never-pruned marker — tablet ids are never reused).
- **`Reconciler` teardown** (`Release`/`Reclaim`): call `on_teardown`
  (unregister from routing *before* touching the driver), `shutdown()`, poll
  `is_stopped()` bounded by `RECLAIM_STOP_TIMEOUT` (10s, via `env.sleep` — no
  tokio-only primitive), re-register via `on_host` and leave `LocalState`
  untouched on timeout (so `plan` re-emits the same action next tick), else
  narrow to `erase_bound` (Release only), `erase_scope()`, delete the WAL, and
  only then `confirm_torn_down`. `Absorb`'s teardown (`TeardownKind::Absorb`)
  skips both the narrow and the `erase_scope()` — only the driver stops and its
  WAL is removed. Timeouts: `ABSORB_DRAIN_TIMEOUT`, `RELEASE_CONFIRM_TICKS`,
  `RECLAIM_STOP_TIMEOUT`.

## What's non-obvious

- **The driver is split into a consensus loop + an apply task** — the
  driver-liveness fix (ADR 0017). Engine apply + compaction are slow
  (~180–300ms for a batch of LSM merges + a compaction rewrite on real disk)
  and used to run *inline* on the loop servicing Raft messages, so under write
  load the driver blocked past the 150ms election timeout → followers
  campaigned → a **leader-election storm** that truncated in-flight writes and
  collapsed throughput to ~15/s. Now:
  - **Consensus loop** (`drive`): recover from WAL, spawn the apply task, then
    loop `persist_wal` (drain records → append + `fsync` → `mark_durable_
    through`, under `wal_lock`) → `select(recv, timer, propose-wake)` → step
    the core → `persist_wal` again → send. It does **no** engine apply, so it
    always heartbeats/acks within the election timeout.
  - **Apply task** (`apply_loop` → `apply_and_compact`): install received
    snapshots, `drain_apply` → `merge`/`merge_tombstone` in commit order, and
    compact — all off the consensus loop. Backs off (`APPLY_IDLE_POLL`) only
    when idle.
  - The WAL is written by both tasks (append vs. compaction rewrite),
    serialized by the async `wal_lock`; compaction snapshots only up to
    `engine_applied` via `snapshot_upto` (not `last_applied`, which the engine
    hasn't merged) and **discards the consensus loop's pending records** in the
    same locked block (`replay` is push-based → re-appending would duplicate).
    Compaction is skipped while `halted`. `is_stopped()` requires *both* tasks
    stopped (`stopped && apply_stopped`) before the GC deletes artifacts.
  - This is also where `engine_applied` vs `last_applied` (Key invariants)
    comes from.
- **Wake-on-propose cuts single-write latency.** `put`/`delete`/`cas`/
  `change_membership` route through `propose_and_wake`: after the core appends,
  the proposer raises a `ProposeSignal` (`AtomicBool` +
  `futures::task::AtomicWaker`) that the consensus loop races as a third
  `select` arm, then calls `RaftCore::replicate_now` (broadcast immediately,
  resetting the heartbeat deadline) instead of leaving the entry parked until
  the next ~50ms heartbeat. `AtomicWaker` is deliberately **executor-agnostic**
  — synchronous `wake()` under `SimEnv`'s `ArcWake` executor (deterministic, no
  wall clock), resolves the register/wake race under tokio's `ProdEnv`; no
  tokio-only primitive, so determinism holds. The `ProposePending` future
  registers the waker *before* checking the flag (against a lost wakeup) and
  consumes it (`swap(false)`) on resolve, so it never busy-spins. A `NotLeader`
  propose appends nothing, so it doesn't wake. Verified over `ProdEnv` in
  `animusd/tests/cp_plane.rs::single_write_latency_is_low` (median ~52ms →
  ~11ms).
- **Unbounded scans must not fall through to `entries()`.** `local_scan`'s
  `end: None` branch (used by `/admin/raftkv`'s `raft_view`, by `erase_scope()`
  teardown, and transparently by `linearizable_scan` — the real DynamoDB
  `Scan`/CQL full-table path) derives a bounded upper bound from
  `physical_bounds` instead. `entries()` scans the **whole shared engine** (ADR
  0028), so on a node hosting several tablets every unbounded scan cost
  O(hosted tablets × whole node engine) — live-observed as `/admin/raftkv`
  hanging for 20s+ on a grown, auto-split cluster. `entries()` remains the
  fallback only for `StorageScope::whole()` (no finite bound).
- Distinct WAL file (`raftkv.wal`) from the control plane's `raft.wal`, so a
  node can host both planes. The name is exported (`animus_cp_data::WAL`) so
  the drop-table GC (ADR 0024) can delete a stopped group's WAL.
- **`shutdown()` is a graceful driver halt, not a kill** (ADR 0024): it latches
  a flag the driver observes at the top of its loop — *between* full
  persist+apply passes and within one wake — so WAL and engine are never left
  mid-write. Poll `is_stopped()` before touching the group's files. A halted
  node's accessors still answer from the **frozen** core (a halted leader keeps
  reporting `is_leader() == true`), so never route to a handle after
  unregistering it; a halted node must not be reused — restarting the tablet
  means a fresh `start`.
- **Test gotcha (membership):** pre-start a to-be-added node knowing only the
  *current* voters, NOT itself — a node started inside its own initial config
  is a voter that can campaign, win, and inject itself before the real add
  (`start_election` gates on `is_voter`). A `start` whose `all_nodes` excludes
  its own id is a quiet non-voter until the leader adds it. (Caught by the
  `reconfigure_trigger` seed sweep — a single seed hid it.)
- The ADR 0029 reconfigure/leadership-transfer follow-up fix (the two-layer
  transfer-gate threshold mismatch, the proposal-freeze while a transfer is
  armed, and the down-extra search fix) is a cross-cutting lesson — see the
  root `CLAUDE.md` engineering-practices log and `animus-control/CLAUDE.md`'s
  "Leadership transfer" entry for the core mechanics; the regressions are
  `tests/leader_transfer_reconfigure.rs` and
  `tests/reconfigure_down_extra_priority.rs`.

## Tests

`cargo test -p animus-cp-data`. All 19 test binaries drive `SimEnv` — use
`run_for`/`run_until`, never `run()` (the driver has perpetual heartbeat/
election timers). Linearizable reads are async (a read-barrier probe round), so
drive them as spawned tasks + `run_for`, and never `block_on` a `tick()` whose
planned action tears a group down (`Reconciler::teardown` polls `env.sleep()`
internally).

- `single_tablet.rs` (B.1) — a group elects, replicates writes, applies on
  every replica across a leader kill; `engine_applied_index` confirms a
  specific proposal; trace reproducibility.
- `read_index.rs` (B.2) — linearizable ReadIndex reflects committed writes +
  RYW; a deposed/partitioned leader returns `None`, never a stale value;
  `linearizable_scan` returns the sorted live range;
  `linearizable_read_succeeds_after_a_full_membership_rotation` (rotates
  `{0,1,2}` → `{2,3,4}` and stops the departed nodes — the regression for the
  read barrier keying on the live `config()`, not the hosting-time `all_nodes`;
  see the root `CLAUDE.md`).
- `read_index_fresh_leader.rs` — drives *into* the fresh-leader window at 1ms
  sim granularity (Raft §6.4: a fresh leader must commit its own-term no-op
  before serving); the read must wait and serve the acked value, never the
  stale one.
- `cas.rs` — concurrent same-`expected` race → exactly one winner agreed on
  every replica; CAS-if-absent; a successful CAS survives restart via WAL
  replay; seed sweep + reproducibility.
- `batch.rs` — a `KvCommand::Batch` applies all keys on every replica, survives
  a leader kill, and re-applies idempotently on restart.
- `membership.rs` (C) — single-server change: remove a follower, add + catch up
  a node, reconfigure off a crashed node, reject multi-server/self-removal.
- `reconfigure_trigger.rs` — the end-to-end automatic cascade with **no
  test-side membership call**: control-plane failure detection → `Down` →
  reconciler `CasTabletReplicas` → the group leader's epoch-driven-pull
  reconfigure loop swaps the dead node for a same-zone spare, which catches up.
- `reconfigure_down_extra_priority.rs` — defect C in isolation: a `Down` extra
  sorting *after* a healthy one is still removed first, with no catch-up gate.
- `leader_transfer_reconfigure.rs` — the ADR 0029 follow-up: a write-hot
  3-voter group converges when `reconfigure_step` must relocate the leader
  itself; the hand-driven variant that proposes before every step is the one
  proven to fail against the pre-fix source.
- `snapshot_catchup.rs` (A.2) — crash a follower, write past the compaction
  threshold, restart → it catches up via a streaming `InstallSnapshot`
  carrying the leader's engine image.
- `narrow_scope.rs` — `narrow_scope` makes a group's `StorageScope` range
  live-narrowable (the split-source shape, so `engine_image` stops shipping the
  handed-off portion); `narrow_then_erase_scope_spares_a_co_hosted_siblings_
  data` is the primitive-level proof of the sibling-sparing release-GC
  invariant.
- `fenced_commands.rs` — the crossover-window range fence directly: a mutating
  command whose key is outside its embedded fence is a no-op on every replica;
  the decision is per-entry, not retroactively reconsidered; `scope_range`
  reflects narrowing and a fence stamped from it gates apply.
- `has_data.rs` — `StorageScope::has_data` distinguishes reforming-after-restart
  from joining-fresh (bounded and unbounded ranges) and never sees a sibling
  scope's data.
- `shared_engine.rs` (ADR 0028) — two independent scoped groups share **one
  physical `StorageEngine`**: writes to the same logical key don't collide,
  scans (bounded + unbounded) never return a sibling scope's keys, and snapshot
  catch-up doesn't leak the sibling's data.
- `stream_addressing.rs` (ADR 0026 Stage B) — two groups on identical node ids
  don't cross-talk when addressed by distinct streams; sustained interleaved
  writes stay isolated and reproducible.
- `hlc_skew.rs` (ADR 0018 §2, PR1) — the HLC/sim-clock-skew integration test:
  lives here (not in `animus-sim`, which can't depend on this crate) because
  it needs both `hlc::Hlc` and `animus_sim::Simulator::set_clock_skew_for`. A
  node whose clock reads ahead mints, a node whose clock reads behind
  witnesses it, and the behind node's next mint still strictly exceeds the
  ahead node's — causality survives clock skew.
- `metrics.rs` (ADR 0015) — CP-plane observability counters move under a known
  workload, threading a recording `MetricsHandle` via `start_with_metrics`:
  the *real outcome* moves each counter (accepted vs. not-leader-rejected
  proposals, commits, batched applies, read barriers served) — never the mere
  attempt.
- `shutdown.rs` (ADR 0024) — a halted follower stops applying; survivors
  re-elect after a leader shutdown and keep serving.
- `reconciler.rs` (ADR 0031 PR4) — the `host::Reconciler` executor end to end:
  host → narrow → release, asserting the sibling-sparing erase bound through
  the reconciler's own `plan`-then-execute `tick`, including a **real** Raft
  membership change so `config_excludes_me` comes from durable Raft config.
- `reconciler_corpus.rs` (ADR 0031 PR5) — see below.

### Reconciler lifecycle corpus (`tests/reconciler_corpus.rs`)

The 34 `host.rs` unit tests prove `plan` correct as a pure function; the entry
above proves ONE end-to-end sequence; this corpus is the first
**seed-reproducible fault-injection** suite for the whole tablet lifecycle. It
follows the house corpus doctrine (ADR 0014): a frozen, name-seeded scenario
list (`scenario_cells()`), a depth knob, and coverage/seed-expansion guards.

- **Harness**: each scenario builds a small `Cluster` (`BTreeMap<NodeId,
  ClusterNode>`, each owning a real `Reconciler<SimEnv, MemoryEngine>` + its own
  `MemoryEngine`) and drives it via `tick(node, &view)` with hand-scripted
  `MetadataView`s standing in for the control plane (no live control-plane
  `RaftNode` needed). Real `RaftKvNode` groups form/elect/replicate underneath.
  Every scenario runs as a **spawned task** driven by `Simulator::run_for`.
- **`Simulator` gained `#[derive(Clone)]`** (`animus-sim`) so a scenario's own
  spawned driver task can call `&self` fault methods (`stop`/`crash`/
  `partition_pair`/`heal`/`env`) while the outer test drives `run_for`/
  `run_until` (the `&mut self` methods) — cloning just hands out another
  reference to the same `Arc`-backed world, like `SimEnv`'s own `Clone`.
- **The 18 frozen scenarios** (`ANIMUS_RECONCILER_SEEDS`, default 1 =
  byte-identical; variant 0 keeps each cell's canonical name-derived seed):
  fresh-host (whole-keyspace + two-replica), split-narrows-source,
  rebalance-off-releases (sparing a sibling), drop-table-reclaims,
  spare-join-then-promoted, late-first-view-still-converges,
  reconfigure-removes-a-down-replica-first,
  `merge_widens_survivor_and_absorbs_sibling_unerased` (ADR 0033: the
  deterministic absorb-drain regression — a write proposed through the absorbed
  group with **zero** sim time before the merge view ticks, so the apply task
  provably hasn't merged it; pre-drain-fix the acked write was permanently
  lost), idempotent-tick-on-converged-multi-tablet,
  reconfigure-transfers-leadership-before-removing-the-leader,
  crash-restart-single-replica-upgrades-via-has-data,
  crash-restart-follower-rejoins-no-loss, replay-epoch-flicker-mid-release,
  replay-absent-then-present (scenario 15, a deliberate **contract-boundary**
  test — bypassing the caller's `last_applied == 0` guard genuinely
  erases-then-rehosts empty, proving *why* the guard is load-bearing, not a
  crate defect), partition-during-removal-blocks-release-until-healed,
  split-then-immediate-release-zero-ticks (the deterministic version of the
  real split-then-immediate-release sibling-corruption race — the `ProdEnv`
  version only reproduced ~3/5 runs), re-add-after-exclusion-cancels-release.
- **Invariant checks, generic across scenarios**: (a) hosting convergence
  (`assert_hosted_converged`); (b) data safety (`assert_present`/
  `assert_absent`, raw physical-key reads — survivors readable, released/
  reclaimed erased, a co-hosted sibling never touched); (c) no zombie groups
  (`assert_all_stopped`); (d) idempotence (`assert_idempotent`) — meaning the
  observable *state* doesn't drift (hosted set, hook call counts, live scope
  ranges, Raft configs), **not** "the second tick emits zero actions"
  (`Reconfigure` is replanned every tick a node leads a group).
- **Depth found a test-robustness gap, not a reconciler bug**: at
  `ANIMUS_RECONCILER_SEEDS=60`/`150`, two hand-rolled "force a real membership
  removal" helpers hit a `NotLeader` from `change_membership`/
  `transfer_leadership` right after confirming `is_leader()` — the documented
  proposal-freeze-while-transfer-armed behavior a single-shot assert can't tell
  from a real failure. Fixed by retrying the whole arm/transfer/propose
  sequence each poll tick. Held green at `=300` (5,400 runs).
- **To add a scenario**: write `fn scenario_my_thing(seed: u64)` in the
  existing shape (`run(seed, |sim| async move { .. })`), add a
  `scenario!("my_thing_name", scenario_my_thing)` to `scenario_cells()`, and
  run it under `ANIMUS_RECONCILER_SEEDS=100` (or higher) with a `timeout` the
  first time (a hang means a same-instant unbounded-work loop, not slowness —
  see the root `CLAUDE.md`).
- **Run at depth**: `ANIMUS_RECONCILER_SEEDS=K cargo test -p animus-cp-data
  --test reconciler_corpus reconciler_corpus_runs_every_scenario` (default
  `K=1`; held green through `K=300` in ~52s).
