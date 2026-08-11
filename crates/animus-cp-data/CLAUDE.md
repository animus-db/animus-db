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
  would silently collapse two distinct timestamps to one version). **Wired
  into `RaftKvNode`'s apply path since PR2** (see the Key invariants section
  above): every mutating propose method mints `ts` from the group's own
  `Hlc` (constructed with `HLC_MAX_OFFSET = 500ms`); `apply_and_compact`
  stamps `hlc::pack(ts)` as the engine version. Witnessing happens at four
  points — WAL recovery (`drive`, each recovered entry), every received
  `AppendEntries` (`witness_append_entries`, before `RaftCore::handle`),
  snapshot install, and group start (off the shared engine's
  `latest_version()`, in `start_inner`) — the chain that keeps a group's
  applied `ts` strictly increasing across restarts and leader changes,
  asserted (hard, not `debug_assert!`) by `assert_ts_monotonic` at apply.
- **`seal.rs`** (ADR 0018 §2 amendment, PR2) — the **range seal**: the
  structural replacement for the retired `version_floor` cross-group-LWW
  fix. `KvCommand::Seal { range, ts }` is proposed by a range-handoff
  source (a split's `NarrowScope`, or a merge's `Absorb`, both wired in
  `host.rs`'s `Reconciler`) through its own Raft log; every replica applies
  its log in the same order, so a later-ordered mutating entry for a key
  inside a sealed range is rejected at apply (`is_sealed`, checked against
  a per-group in-memory `sealed: Vec<(KeyRange, HlcTimestamp)>` rebuilt at
  group start from a durable **engine marker key** — deliberately from the
  engine, not log replay, since compaction can truncate a `Seal` entry out
  of the log long before its rejection duty is done). The marker's
  physical key (`seal_marker_key`) lives under
  `animus_control::syskv::RESERVED_NAMESPACE` — engine-global, outside
  every `StorageScope` — keyed by `(source tablet id, sealed range)` so a
  tablet can seal more than once over its lifetime without one seal
  overwriting another's stored range. See the module's own doc for the
  full key-disjointness proof (including why a naive `[0x00, 0x00]` lead
  pair does **not** work: it collides with the legacy whole-keyspace
  tablet's own scope prefix).
- **`ts_cache.rs`** (ADR 0018 §2/PR2b) — the per-tablet **read-timestamp
  cache** (`TsCache`): leader-local, in-memory, best-effort
  write-conflict push. A two-generation rotating `BTreeMap<(start, end),
  HlcTimestamp>` (`bump`/`max_overlapping`/`raise_low_water`); every served
  read bumps the span it read at its serve ts, and a propose-time write
  (`RaftKvNode::mint_pushed`) is pushed above any overlapping bump (and the
  committed ceiling, folded in via `raise_low_water`) before it is
  embedded in a command. Losing this cache (crash/restart) is always
  **safe** — over-conservative, never incorrect; see the module doc and
  the Key invariants entry below for why the real safety net is the
  logged read ceiling, not this cache.
- **`ceiling.rs`** (ADR 0018 §2/PR2b) — the **logged read ceiling**'s
  durable marker: a single per-tablet engine-global key (`ceiling_marker_
  key`, same `RESERVED_NAMESPACE`-under-`escape` disjointness proof as
  `seal.rs`'s marker, distinct tag) always overwritten with the newest
  `KvCommand::ReadCeiling` value, so `storage.latest_version()` durably
  reflects it — the *existing* group-start witness (`start_inner`'s
  `hlc.witness(unpack(storage.latest_version()), ..)`) then re-derives a
  floor covering it on any future restart with no further changes, even
  after the `ReadCeiling` log entry itself has been compacted away. See
  the Key invariants entry below for the full mechanism and its safety
  argument.

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
  `ts` (per-key LWW is well-defined since keys are distinct); composes
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
- **MVCC snapshot reads — `read_at`/`scan_at`** (ADR 0018 §2/PR2b) — the
  same ReadIndex barrier as `linearizable_get`/`_scan`, then a read at MVCC
  version `≤ hlc::pack(ts)` (`storage.get_at`/`scan_at`) instead of the
  latest: every write with commit `ts' ≤ ts` already committed *and
  applied* before the barrier confirmed, **not** one still in flight — the
  single-tablet snapshot-read building block a cross-tablet transaction's
  read will sit on (PR3+), not a transaction's read itself. Refuses (outer
  `None`, same `Option<Option<_>>` shape as `linearizable_get_served`) a
  `ts` not yet strictly below `committed_ceiling()` — see the write-push/
  ceiling invariant below.
- **`committed_ceiling()`** (ADR 0018 §2/PR2b, admin/debug accessor
  alongside `term`/`commit_index`/etc.) — this group's highest *applied*
  `KvCommand::ReadCeiling`, the floor `read_at`/`scan_at` check against and
  `ensure_ceiling_above` (internal, called from every read-serving method)
  drives forward by proposing a fresh one when needed.
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

- **The packed HLC commit timestamp is the MVCC version (ADR 0018 §2
  amendment, PR2 — replaces the retired `version_floor`-scaled Raft-index
  invariant).** Every mutating `KvCommand` variant carries a `ts:
  HlcTimestamp`, minted from the proposing leader's own per-group `Hlc` at
  propose time; apply stamps `hlc::pack(ts)` as the engine `version` at all
  four apply sites (`Put`/`Batch`/`Delete`/`Cas`) — never a Raft index — so
  per-key LWW reproduces cross-group HLC order and re-applying on recovery
  stays idempotent (the same command always computes the same `ts`, hence
  the same version). This closes the same cross-group-shared-engine hazard
  `version_floor` did (a fresh/widened group's own version sequence must
  never undercut a different group's), but by **witnessing** instead of a
  structural version-space separation:
  - **Witnessing** (`Hlc::witness`) folds a just-observed timestamp into a
    group's clock so its own future mints are guaranteed to exceed it. Four
    points do this: WAL recovery (`drive`, every recovered log entry, before
    `RaftCore::recovered` consumes the replay output); every received
    `AppendEntries` (`witness_append_entries`, before `RaftCore::handle` —
    every entry in the message, whether or not the core ultimately accepts
    it, since a redundant witness is always safe); snapshot install
    (`apply_and_compact`, after `install_engine_image`); and **group start**
    (`start_inner`, off the shared engine's own `latest_version()` — this
    alone subsumes the old floor-seeding for the steady-state case: a
    restart, or a co-hosted sibling's data already present).
  - **The range seal** (`seal.rs`) closes the one residual witnessing alone
    cannot: an in-flight write from a source-group leader that hasn't yet
    observed a split/merge, still in its own commit pipeline when the
    handoff happens. A source proposes `KvCommand::Seal { range, ts }`
    through its own log at handoff time (`host.rs`'s `Reconciler`, on a
    split's `NarrowScope` and a merge's `Absorb`); apply rejects any
    later-ordered mutating entry whose key falls in an already-sealed range,
    regardless of that entry's own `ts` — because within one group, log
    order and HLC order coincide (a leader's own `Hlc::mint` is monotonic;
    a leader change is covered by witnessing), so "later-ordered" and
    "higher-timestamped" are the same test, but it is the **log position**
    that is authoritative. The reconciler gates a split child's `Host` /
    a merge survivor's `WidenScope` on observing the relevant seal marker
    locally first (`TabletFacts::parent_seal_observed`/`widen_seal_observed`,
    `Metadata::split_parents`/`absorbed_by` provenance) — see `host.rs`'s own
    entry below and `seal.rs`'s module doc for the full design, including
    the key-disjointness proof.
  - **A hard, non-`debug` assert** (`assert_ts_monotonic`, at apply) checks
    every applied entry's `ts` strictly exceeds the previous one this group
    applied — the load-bearing invariant the whole witnessing chain exists
    to guarantee; a failure means the chain itself is broken (a missed
    witness point), not a recoverable condition.
  - `engine_applied` and the group's own Raft log-matching are untouched —
    only the *storage-layer version number* a command stamps changes.
    Regression: `tests/cross_group_lww.rs` (split/merge/seal-rejection/
    in-flight-race/clock-skew shapes) and `tests/witnessing.rs`
    (leader-change and restart-recovery monotonicity).
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
- **Write-conflict push + the logged read ceiling (ADR 0018 §2/PR2b) —
  the serializability half of the MVCC design.** A write must never commit
  at a `ts ≤` a `ts` at which its keys were already served to a reader.
  Two layers, deliberately separate:
  - **`ts_cache.rs`'s `TsCache`** is leader-local, in-memory,
    best-effort — every served read bumps the span it read at its serve
    `ts`; every mutating propose (`RaftKvNode::mint_pushed`) checks its
    minted `ts` against the highest overlapping bump (plus the committed
    ceiling, folded in via `raise_low_water`) and, if not strictly above,
    witnesses that floor and re-mints (one retry always suffices —
    asserted, not assumed). Losing this cache (a crash/restart) is always
    **safe**, never wrong: over-conservative pushes are still correct
    writes, just marginally later-timestamped ones.
  - **The logged read ceiling** (`ceiling.rs`, `KvCommand::ReadCeiling`) is
    the actual safety net a leader-local cache alone can't be, across a
    leader change: a leader may only serve a read at a `ts` strictly below
    the highest `ReadCeiling` its group has **committed and applied**
    (`RaftKvNode::committed_ceiling`), and proposes a fresh one
    (`Hlc::uncertainty_upper(serve_ts)`, a comfortable margin so proposals
    amortize to roughly one per `HLC_MAX_OFFSET` of wall time, not one per
    read) when it wants to serve above the current one. Safety: every
    served read had `ts` below some committed ceiling; a live leader
    change's new leader already witnessed that ceiling's `ts` via ordinary
    `AppendEntries` receipt (`command_ts` covers `ReadCeiling` like every
    other variant) **before it could ever campaign** (Raft leader
    completeness), so its own future mints — and hence every write it
    proposes — strictly exceed it. A durable **engine marker**
    (`ceiling.rs`, mirroring `seal.rs`'s marker shape) closes the residual
    a purely in-memory design would leave: a read-only workload can
    compact a `ReadCeiling` entry out of the log with no interleaved write
    to otherwise raise `storage.latest_version()`, so the marker's own
    merge does that job directly, letting the *existing* group-start
    witness (`start_inner`) re-derive the floor on any future restart.
    **Never disambiguate a ceiling candidate via `Hlc::witness`** — it
    would drag the *proposing leader's own* clock forward to match a
    value that's deliberately `HLC_MAX_OFFSET` in the future, poisoning
    every ordinary mint right after and turning the intended O(1)
    amortized proposal rate into O(N) (a real regression a seed-driven
    test caught); `RaftKvNode::next_ceiling_candidate` is a **separate**
    CAS ratchet for exactly this reason. See ADR 0018's PR2b amendment for
    the full account, including the safety argument and the two
    regressions this design's own gate run found. Regression:
    `tests/ts_cache.rs`, `tests/snapshot_reads.rs`.
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
  ADR 0033, mirroring `Metadata::merged_tablets` — and, since ADR 0018 §2's
  PR2 amendment, **`split_parent`**/**`absorbed_by`**, mirroring
  `Metadata::split_parents`/`absorbed_by`), deliberately *not* the whole
  `animus_control::Metadata`, keeping the crate decoupled from the control
  plane's state shape. `TabletFacts` bundles the impure per-tablet inputs the
  caller gathers first (`hosted`, `is_leader`, `config_excludes_me`,
  `scope_range`, `has_data`, and since PR2 `parent_seal_observed`/
  `widen_seal_observed` — each an async, tablet-scoped engine scan for the
  relevant seal marker, gathered only when actually needed). `LocalState` is
  the pure mirror of `animusd`'s `minted` claim set + `pending_release` epoch
  dampener, threaded call to call.
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
| `WidenScope` | ADR 0033 dual: widen when the metadata range *grew* (the surviving `left` of a `MergeTablets`) — provably widen-only. A range neither subset nor superset of the live scope is a defensive no-op, never guessed. Deferred while an absorb is pending, **and** (ADR 0018 §2 amendment) until this node's own engine contains the absorbed tablet's range-seal marker covering the widened portion (`TabletFacts::widen_seal_observed` — see the MVCC-version invariant above; replaces the retired `version_floor` bump). |
| `Host` | Stand up a fresh/joining/restarting tablet via `start_hosted`, with full or others-only config (`animusd::cp_join_host`'s exact decision). A split child (named in `Metadata::split_parents`) is deferred until this node's own engine contains its parent's range-seal marker covering its own range (`TabletFacts::parent_seal_observed` — see the MVCC-version invariant above; replaces the retired `version_floor` seeding). A tablet with no parent (a bootstrapped fresh table, or a merge survivor) hosts immediately. |
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

`cargo test -p animus-cp-data`. All 24 test binaries drive `SimEnv` — use
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
- `cross_group_lww.rs` (ADR 0018 §2 amendment, PR2) — cross-group MVCC
  ordering under the range-seal design (retired the `version_floor` fix this
  file used to prove): a split source seals its handed-off range and only
  then does a fresh sibling host and win a subsequent overwrite; a merge's
  absorbed group seals before teardown and the survivor's overwrite wins; a
  write proposed through the same group after its own range is sealed is
  rejected outright (the "wide fence, un-ticked leader" case); the **in-flight
  race** the seal specifically exists for (an entry proposed with zero
  intervening sim time before the split lands, mirroring
  `reconciler_corpus.rs`'s zero-tick technique, driven through the real
  `host::Reconciler` so the `parent_seal_observed` gate genuinely defers
  hosting rather than the test having to reason about timing by hand) —
  the successor's write must win regardless of whether the racing write ends
  up applied-then-overridden or seal-rejected; and a clock-skew composition
  test (the successor's node reads *behind* the source's the whole time and
  still wins, proving the design depends on witnessing via the shared
  engine, never wall-clock synchronization).
- `witnessing.rs` (ADR 0018 §2 amendment, PR2) — the witnessing chain
  directly: a leader change mid-writes keeps a group's applied `ts` strictly
  increasing (a fresh `Hlc` on the new leader, causality carried across by
  witnessing the old leader's entries as a follower); a genuine process
  restart (`Simulator::stop`, not the network-only `crash`/`restart` pair)
  re-witnesses its own recovered WAL, and the first write proposed after
  recovery is timestamped strictly past everything recovered.
- `snapshot_reads.rs` (ADR 0018 §2/PR2b) — `read_at`/`scan_at` directly:
  each sees exactly the version committed at or before `ts` (including a
  value strictly between two writes' timestamps, and `scan_at` across
  several keys); refused above the group's committed ceiling, then served
  once a `linearizable_get` has driven the ceiling past it; a deposed/
  partitioned leader's `read_at` returns not-served (outer `None`), never a
  stale value — mirrors `read_index.rs`'s shape.
- `ts_cache.rs` (ADR 0018 §2/PR2b) — the read-timestamp cache + logged read
  ceiling integration properties (the rotation math itself is unit-tested
  directly against `TsCache` in `src/ts_cache.rs`): a served read pushes
  the next write's ts strictly above it; the load-bearing leader-change
  test — leader A serves reads with its clock skewed 60s ahead, is
  partitioned away, and leader B's subsequent write still lands strictly
  above A's served-read ceiling, with a **negative control** proving a
  bare, un-witnessed `Hlc` on B's own clock *would* have minted below it
  (so the ceiling mechanism, not coincidental clock ordering, is what
  saves it); ceiling proposals amortize (a handful, not one per read, over
  hundreds of sequential reads, via a recording `MetricsHandle` and
  `CpReadCeilingProposals`); a real node stays correct after thousands of
  distinct-key reads via a single-voter group (majority = 1, so each read
  barrier resolves at ~zero simulated cost). **Gotcha this file's own
  history is the regression for**: never drive a linearizable read with a
  per-call `run_for` budget when timing across *several* reads matters
  (`run_for` always advances to its full deadline once idle), and never
  `block_on` a linearizable read at all (see the root `docs/engineering-
  lessons.md` Testing section for both).
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
