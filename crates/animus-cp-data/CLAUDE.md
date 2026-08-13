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
- **`txn.rs`** (ADR 0018 §2/PR3) — the **single-participant transaction**
  machinery: the 1-byte-tagged value **envelope** (`Envelope::Committed`/
  `Intent`) every apply-path write now wraps its value in, and the
  transaction **record** (`TxnId`, `TxnStatus`, `TxnRecord`) that is the
  atomic commit point. Unlike `seal.rs`/`ceiling.rs`'s engine-global
  markers, a txn record has to be an ordinary **in-scope logical key** of
  the anchor tablet (so it replicates/snapshots/splits like real data);
  `record_key` derives it from the anchor write's own 8-byte partition
  token plus a lead-byte pair (`[0x00, 0x02]`) proved disjoint from every
  real key sharing that token via a structural argument about
  `animus_tablet::escape`'s own encoding (never `escape(pk)`'s first two
  bytes, for any `pk`) — see the module's own doc for the full proof
  (and `docs/engineering-lessons.md`'s Code-patterns entry for the
  general technique). `is_record_key` is what `lib.rs`'s scan paths and
  `has_data` filter on. See the Key invariants entry below and ADR 0018's
  PR3 amendment for the full design. **Since ADR 0018 §2/PR4**:
  `Envelope::Intent` gained a `record_table: String` field (the anchor's
  own table name, stamped into every intent) — a record key alone doesn't
  identify which table's tablet ring owns it (tables' rings are
  independent, ADR 0022/0023, so two tables can assign the same token to
  different rows), and a non-anchor participant's own reader needs it to
  route a cross-tablet `TxnStatus` query. Also new: `TxnOutcome` (`pub`,
  re-exported) — the `Committed{commit_ts}`/`Aborted` decision, carried
  explicitly by `KvCommand::TxnResolve` rather than re-derived from a local
  record (a non-anchor participant's tablet never has one); and
  `TxnDecisionStatus` (`pub`, re-exported) — the public mirror of
  `TxnStatus` a cross-tablet status query reads back. **Since ADR 0018
  §2/PR5**: `TxnRecord::intent_spans` changed from `Vec<KeyRange>` to
  `Vec<(String, KeyRange)>` — every key any participant ever staged, table
  name attached, not just the anchor's own writes (the gap this closes and
  the structural fix's full argument are in the module doc above and the
  ADR's PR5 amendment §2); `TxnRecordView` (`pub`, in `lib.rs`) is the
  public recovery-view mirror a cross-tablet caller reads back via
  `RaftKvNode::txn_record_view`.

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
  leader / times out). All additive; existing signatures unchanged. **Since
  PR3**: a current value covered by a `Pending`/unresolved intent
  deterministically fails the swap (never guesses a match or an absence) —
  see the Key invariants entry below.
- **Single-participant transactions — `txn_stage`/`txn_decide`/`txn_write`**
  (ADR 0018 §2/PR3) — the degenerate single-Raft-group 2PC. `txn_stage(table,
  writes, conditions) -> Option<(TxnId, Vec<u8>, StageOutcome)>` proposes
  `KvCommand::TxnStage` (the first write's key is the *anchor*, whose
  partition token anchors the record — see `txn.rs`; `table` is embedded
  into every intent as `record_table`, ADR 0018 §2/PR4; `conditions`/
  `StageOutcome` are the apply-time write-key conditions amendment, see the
  bullet below) and returns the minted `TxnId` + record key + outcome once
  the entry applies (`Option` still means only "not leader / never
  applied" — whether the stage's *content* actually landed is now
  `StageOutcome`, not the `Option` itself). `txn_decide(txn_id, record_key,
  keys, commit) -> Option<HlcTimestamp>` commits or aborts, then resolves
  every key in `keys` — three log entries, fully synchronous; this remains
  the single-participant/anchor-local convenience (`keys` must all be
  local to the anchor). `txn_write(table, writes) -> Option<HlcTimestamp>`
  is the one-shot commit-only convenience (`txn_stage` + `txn_decide(..,
  commit: true)`, `conditions: Vec::new()`) — since the amendment below, it
  also checks `txn_stage`'s returned `StageOutcome` and returns `None`
  rather than proceeding to decide if the stage didn't genuinely land (a
  latent gap this convenience had before: `Option::is_some()` alone used
  to mean only "the entry applied," not "my writes actually staged"). All
  leader-only (`None` if not leader / a phase times out).
- **Multi-participant transactions** (ADR 0018 §2/PR4) — the primitives a
  cross-tablet coordinator (`animusd::ClientCtx::cp_txn`) composes; see the
  ADR's PR4 amendment for the full protocol.
  `txn_stage_participant(txn_id, record_key, record_table, writes,
  conditions) -> Option<(HlcTimestamp, StageOutcome)>` stages a
  **non-anchor** participant's writes as intents referencing an
  already-known anchor record — no record is created or touched on this
  group; `conditions`/`StageOutcome` are the apply-time write-key
  conditions amendment, see the bullet below. `txn_commit_at_least(txn_id,
  record_key, min_ts) -> Option<HlcTimestamp>` commits the anchor's record
  at a ts that strictly exceeds both `min_ts` (the coordinator's
  candidate — the max of every participant's acked stage ts) and this
  group's own log floor (`mint_at_least`, mirroring `mint_pushed`'s
  witness-and-floor shape), returning the **actual** ts used — the
  transaction's canonical `commit_ts`, since this may exceed `min_ts` if
  this group's own floor already had. `txn_resolve(txn_id, record_key,
  keys, outcome: TxnOutcome) -> Option<HlcTimestamp>` is the one low-level
  resolve primitive used identically by the anchor's own keys and every
  other participant's (and internally by `txn_decide`). `txn_status_local(
  record_key) -> Option<TxnDecisionStatus>` is a ReadIndex-consistent
  status read for a caller that already knows it's talking to the
  record's own tablet. `linearizable_get_served_fast(key) ->
  Option<FastRead>` is a non-blocking, single-attempt linearizable read —
  `FastRead::Foreign(IntentInfo)` is the new outcome an intent whose
  record isn't found in this tablet's own scope produces (carrying
  `txn_id`/`record_key`/`record_table`/`staged_value`), alongside the
  existing `Value`/`Pending`. `resolve_intent_given_status(key, read_ts,
  txn_id, status) -> Option<Option<Vec<u8>>>` finishes a read given an
  externally-obtained status (from a cross-tablet query), re-checking the
  key still holds that exact intent first.
- **In-doubt recovery** (ADR 0018 §2/PR5) — `txn_stage_anchor(table,
  writes, participant_spans, conditions) -> Option<(TxnId, Vec<u8>,
  StageOutcome)>` is the general anchor-stage entry point (`txn_stage` is
  now a thin wrapper passing an empty `participant_spans`/`conditions`):
  `participant_spans` names every *other* participant's `(table, span)`
  pairs, merged into the freshly-created record's `intent_spans` alongside
  this stage's own — the structural fix that gives recovery something to
  verify participants against at all (see `txn::TxnRecord::intent_spans`'s
  doc). `txn_abort(txn_id, record_key) ->
  Option<HlcTimestamp>` is the abort-only dual of `txn_commit_at_least` (no
  inline resolve). `txn_record_view(record_key) -> Option<TxnRecordView>`
  is the recovery-view dual of `txn_status_local` (also returns
  `intent_spans`/`created_ts`). `txn_verify_staged(span, txn_id) ->
  Option<bool>` answers "does this tablet still hold a live intent for
  `txn_id` over `span`" via a small bounded scoped scan of the raw
  envelope. `txn_abort_orphan(txn_id, record_key, created_ts) ->
  Option<HlcTimestamp>` is the orphan-record dual of `txn_abort`:
  synthesizes a fresh `Aborted` tombstone if (and only if) no record
  exists yet for `record_key` — used when a pusher's `txn_record_view`
  query finds nothing at all (a real possibility: the anchor's own stage
  can silently no-op on a fence/seal miss just like a participant's
  already could). `pending_txns()`/`unresolved_decided()` expose this group's
  `TxnTracker` snapshot (cheap lock-and-clone) — see the Key invariants
  entry below for the tracker's insert/remove rules and rebuild-at-start
  source, and `animusd::txn_recover`/`txn_resolver_loop` for how these
  compose into the actual push protocol.
- **Apply-time write-key conditions** (ADR 0018 §2 follow-up amendment) —
  `KvCommand::TxnStage` gained `conditions: Vec<(Vec<u8>, Option<Vec<u8>>)>`:
  own-key byte-level OCC (`Cas`-shaped — `Some(bytes)` must equal exactly,
  `None` must be absent), checked at apply against the key's pre-intent
  committed value, whole-or-nothing across the stage like every other
  `TxnStage` rejection. `StageOutcome` (`pub`, re-exported) is the
  per-stage introspection this feeds — `Staged`/`ConditionFailed { key
  }`/`IntentBlocked { key, txn_id }`/`Fenced` — recorded at apply time keyed
  by Raft log index exactly like `Cas`'s own `CasResults`, read back via
  `stage_outcome(index)`. `ConditionFailed` is final (retrying an identical
  stage changes nothing); `IntentBlocked` is the pre-existing PR6
  foreign-intent no-op, now named instead of only inferred after the fact;
  `Fenced` covers a fence/seal miss or a late anchor stage racing an
  already-decided record. Layering: this crate speaks bytes only — a
  richer caller (the Dynamo edge, `animusd::dynamo::run_transact`)
  evaluates its own expression against a pre-read and compiles a true
  result down to this exact byte-equality shape. See the ADR's
  2026-08-12 follow-up amendment for the full design, including a
  corpus-found gotcha in the introspection primitive itself (below).
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
  read will sit on (PR4+), not a transaction's read itself. Refuses (outer
  `None`, same `Option<Option<_>>` shape as `linearizable_get_served`) a
  `ts` not yet strictly below `committed_ceiling()` — see the write-push/
  ceiling invariant below. **Since PR3**: a stored value may itself be an
  intent, resolved against `ts` per the Key invariants entry below
  (`RaftKvNode::read_resolved`) before this method ever returns.
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
  - **Proposing (and, on the absorbed side, waiting out) the seal is one
    atomic critical section spanning both mint and log-append — and it is a
    persistent condition, re-derived every tick, never a one-shot side
    effect of the tick that performs the local irreversible action it
    precedes.** As shipped, both halves were one-shot: `NarrowScope`
    proposed the seal inline, only if this replica happened to be leader at
    the exact tick it first narrowed its own scope (a mismatch condition
    that vanishes, on that replica, the instant the narrow runs — so a
    replica that narrows while a *follower*, then is later promoted to
    leader, never gets a second chance); `Absorb`'s teardown proposed the
    seal once (leader-gated) then waited only for "nothing pending
    locally" — satisfied trivially by a quiescent replica that hasn't even
    received the proposal yet, so a fast follower could tear its own copy
    down (deleting the only local WAL) *before* the leader ever proposed,
    stranding the seal below quorum forever. A genuine multi-process split
    deployment (independent per-node reconcile timers, real network
    latency — `animusd/tests/split_cluster.rs`) exposed both
    deterministically. Fixed: `host.rs`'s `gather_facts` computes
    `TabletFacts::pending_seals` fresh every tick — every range this
    tablet's own hosted group still owes a *committed* seal for, checked
    via the same `seal_covers` engine scan regardless of local scope/
    teardown state — and `plan` turns each into `HostAction::ProposeSeal`
    (leader-gated at execution, harmless no-op otherwise, replanned every
    tick until observed); `Reconciler::teardown`'s Absorb drain additionally
    requires `seal_covers` locally before proceeding, never "nothing
    pending" alone. This gate is self-supporting, not a deadlock: requiring
    every absorbed replica to observe the seal before tearing down is
    exactly what keeps every replica — hence the quorum needed to commit
    the seal — alive for as long as it takes to commit; a genuinely
    quorum-dead group (an unrelated double failure) correctly stalls loudly
    instead of tearing down early. See ADR 0018's PR2 amendment corrective
    note #2 and `docs/engineering-lessons.md` for the full story.
    Regression: `tests/reconciler_corpus.rs`'s
    `absorb_follower_waits_for_committed_seal_before_tearing_down`/
    `narrow_seal_survives_a_late_promotion_after_narrowing_as_a_follower`
    (both proven to fail against the pre-fix code); the originating
    `animusd/tests/split_cluster.rs` pair is the end-to-end acceptance.
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
  - **`propose_ordered` (found via `animusd`'s `self_heal.rs` panicking under
    real concurrent client load): minting a proposal's `ts` and appending it
    to the Raft log must be one atomic step, not two.** Every mutating
    propose method (`put_fenced`/`put_batch_fenced`/`delete_fenced`/
    `cas_fenced`/`propose_seal`, plus `ensure_ceiling_above`'s `ReadCeiling`)
    now computes its `ts` **while holding the group's own `core` lock**
    (`propose_ordered`, `lib.rs`), immediately followed by `core.propose(..)`
    in the same critical section — not two separate, unsynchronized calls.
    Two proposers could otherwise mint ts=A then ts=B (A < B, correctly
    monotonic as *mints* — `Hlc`'s own mutex guarantees that much) but race
    to actually append to the log in the *opposite* order, so apply would
    see ts=B then ts=A, a real decrease. **This is a `ProdEnv`-only bug —
    provably unreachable under `SimEnv`**: the original code had no
    `.await` point between minting and proposing, so two tasks could never
    interleave there under `SimEnv`'s single-threaded cooperative scheduler;
    only genuine OS-thread parallelism can. `propose_ordered` also floors
    every ts-producing path on a new `last_proposed_ts` (this leader's own
    last-*logged*, not just last-*applied*, ts) — `committed_ceiling`/
    `ts_cache` only reflect *applied* state, which the apply task can lag
    the consensus loop on by design (the driver-liveness split above), so a
    write proposed right after an as-yet-unapplied `ReadCeiling` this same
    leader just logged must still check against it. **A second, narrower bug
    surfaced once the first was fixed**: `next_ceiling_candidate`'s ratchet
    must never hand back `last_proposed_ts` (or its own history)
    *unmodified* as a candidate — only ever as a floor to strictly exceed —
    or a `ReadCeiling` proposed right after a write can tie that write's
    exact ts. See `next_ceiling_candidate`'s doc and `docs/engineering-
    lessons.md` for both the full mechanism and the diagnostic story.
    Regression: `tests/prod_concurrent_ts_monotonic.rs` — deliberately the
    one real-thread `ProdEnv` test in a crate whose other 24 binaries are
    all `SimEnv`, since this specific race needs genuine thread parallelism
    to express at all.
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
- **`TxnStage`'s own-key conditions are decided at *apply* time too (ADR
  0018 §2 follow-up amendment), the identical CAS-style discipline** —
  `conditions: Vec<(key, expected)>` evaluated against each key's current
  committed value inside the same apply arm that decides the pre-existing
  fence/seal/foreign-intent gates, recording a `StageOutcome` per Raft log
  index in a driver `StageOutcomes` (`BTreeMap<u64, StageOutcome>`)
  mirroring `CasResults` exactly. **`wait_applied(index).await == true`
  does NOT imply `stage_outcome(index)` is `Some` — found by the ADR 0018
  §4 corpus at `ANIMUS_TXN_SEEDS=5`**: a snapshot install (a replica
  catching up after losing leadership) can advance `engine_applied` past
  `index` without this replica ever individually applying — hence
  recording an outcome for — the entry at that exact index (the install
  globs many commands together). `txn_stage_anchor`/`_participant` poll
  `stage_outcome` directly instead (`wait_stage_outcome`, mirroring
  `compare_and_swap`'s own outcome-polling loop, which never had this bug
  since it was never a separate wait-then-fetch step) — `None` on timeout,
  the same "give up, caller retries" contract every other propose-and-wait
  method here already has, never a hard-`expect`ed fact that turns out not
  to be guaranteed. See the ADR's 2026-08-12 amendment and
  `docs/engineering-lessons.md` for the general lesson.
- **The value envelope + single-participant transactions (ADR 0018 §2/PR3,
  `txn.rs`).** Every value the apply path merges into the engine
  (`Put`/`Batch`/`Cas`, and a `TxnResolve`'s final rewrite) is 1-byte-tagged:
  `0` = committed (the rest is the raw value), `1` = an intent naming the
  staging `TxnId`, its record's logical key, and the staged value (`None` =
  a staged delete). Every read path unwraps it before a value reaches a
  caller: `local_get`/`linearizable_get`/`_served`/`read_at` resolve via
  `RaftKvNode::read_resolved` (a bounded retry while the covering
  transaction is `Pending` — `INTENT_WAIT_TIMEOUT`/`_POLL` — full push/wait
  scheduling is PR4); `local_scan`/`scan_at`/`linearizable_scan` resolve via
  `resolve_scan_rows`, **non-blocking** — a still-`Pending` row is silently
  omitted rather than retried. Resolution: `Committed{commit_ts}` at or
  before the read's own timestamp serves the staged value; `Aborted` (or a
  `Committed` strictly *after* the read's timestamp, equally invisible to
  that snapshot) serves the value the key held immediately before the
  intent, restored by rewinding to `get_at(key, intent_version - 1)` —
  **never** a tombstone, which would incorrectly shadow that older,
  still-live committed value.
  - **The txn record lives *inside* the anchor tablet's own `StorageScope`**
    — an ordinary in-scope logical key, not an engine-global marker like
    `seal.rs`/`ceiling.rs` — so it replicates/snapshots/splits exactly like
    real data. Its key (`txn::record_key`) is `token(8 bytes) || [0x00,
    0x02] || encode(txn_id)`, `token` being the anchor write's own
    partition token; disjointness from every real key sharing that token
    is proved structurally from `animus_tablet::escape`'s own encoding rule
    (never emits `[0x00, 0x02, ..]` as a real key's post-token lead), not a
    reserved-name convention — see `txn.rs`'s module doc for the full
    proof. `txn::is_record_key` is what every scan path (and `has_data`)
    filters on so this internal key never leaks to a client.
  - **`erase_scope` deliberately does NOT go through `local_scan`** (which
    filters record keys and resolves values) — it uses its own
    `raw_scoped_keys`, since drop-table GC must physically erase everything
    this scope ever wrote (ordinary values, still-pending intents, and txn
    records alike), not just what a read would ever serve.
  - **`TxnCommit`/`TxnAbort` carry no `fence`**, like `Seal`/`ReadCeiling`: a
    2PC decision must be durable and final regardless of any later range
    change, and neither ever touches user data — only the record key. A
    *conflicting* second decision on an already-decided record (a different
    `commit_ts`, or committing an already-aborted one) is a protocol-bug
    hard assert, mirroring `assert_ts_monotonic`'s doctrine — not a
    silently-tolerated case. `TxnStage`'s fence/seal check is whole-or-
    nothing across every write key *and* the record key, matching `Batch`.
  - **Writers push intents, never overwrite one (ADR 0018 §2/PR6, task
    #16)**: `TxnStage`'s apply also rejects (whole-or-nothing, same shape
    as the fence/seal check) any target key whose *current* value is an
    unresolved `Envelope::Intent` naming a **different** `txn_id` (same-txn
    re-staging — idempotent WAL replay — is unaffected). This closes a real
    durability hole a corpus depth run found: overwriting a still-
    unresolved intent doesn't erase it (MVCC keeps every version), so if
    the *overwriting* transaction later aborts, its restore's one-hop-back
    `get_at` (see the value-envelope entry above) can land on that stale
    intent instead of a genuinely committed value or true absence — a
    chain a later correct resolve can never repair (its own lower
    `commit_ts` always loses that race via ordinary LWW), permanently
    hiding an already-committed value. Chasing the version chain back
    multiple hops on the *read* side was considered and rejected as
    unsound (an intermediate hop skipped over could belong to a
    transaction that *later* commits, moving the same corruption onto it);
    rejecting the overwrite at apply time instead makes the corrupt chain
    structurally unrepresentable — see `KvCommand::TxnStage`'s own doc for
    the full argument, including why a plain `Put`/`Batch`/`Cas` over a
    foreign intent stays legal (analyzed safe: a genuine overwrite
    serialized strictly after the intent's own transaction, so that
    transaction's eventual resolve loses to it via ordinary LWW — no chain
    results). **The proposer-side half matters just as much**: a stage
    call returning `Some(ts)` only ever meant "this entry applied," never
    "my content landed" (the same footgun the PR6 duelling-decider fix
    above already corrected for `txn_commit_at_least`/`txn_abort`) — so
    `animusd::ClientCtx::txn_prepare_pushing` and the multi-tablet corpus's
    own `stage_anchor_pushing`/`stage_participant_pushing` now verify every
    staged key via `txn_verify_staged` after each attempt, retrying
    (bounded, short backoff) before reporting a client-facing conflict;
    without this, a blocked stage would look identical to a genuine one,
    and a transaction could commit without one of its own writes ever
    having happened. Regression:
    `tests/txn_recovery.rs`'s
    `stage_over_a_foreign_pending_intent_no_ops_then_a_pushed_retry_succeeds`
    and `abort_restore_never_meets_another_transactions_intent`; see ADR
    0018's PR5 amendment §1b for the full account and the corpus depth
    seed that found it.
  - **A residual, documented gap**: a tablet split's `split_key` is an
    arbitrary existing row's key (`animusd::auto_split_loop`'s
    byte-weighted median), not token-aligned, so a split racing an
    in-flight transaction could in principle separate a token's rows (and
    its txn record) across two sibling tablets. Deferred to PR4+, per
    `txn.rs`'s module doc and ADR 0018's PR3 amendment §2.
  Regression: `tests/txn_single.rs` (commit/abort paths, a committed
  delete's real tombstone, a pending read blocking then serving once
  committed, intent/record markers never leaking into a scan, crash/restart
  WAL-replay idempotency, a stage into an already-sealed range rejected
  wholesale); `tests/snapshot_catchup.rs`'s
  `snapshot_catchup_carries_txn_records_and_intents` (records/intents ship
  through `engine_image` like ordinary data); `tests/
  prod_concurrent_ts_monotonic.rs`'s `concurrent_txn_writes_and_reads_never_
  violate_ts_monotonicity` (the PR2 mint/propose-ordering regression's
  coverage extended to the new commands).
  - **Multi-participant 2PC (ADR 0018 §2/PR4)**: `KvCommand::TxnStage`
    gained `record_table`/`is_anchor`; a non-anchor participant's own
    stage merges intents only (`record_key`/`record_table` name the
    **anchor's** record — a different tablet's, possibly a different
    table's, keyspace entirely — so it is never checked against or
    written into this group's own fence/engine). `KvCommand::TxnResolve`
    gained an explicit `outcome: TxnOutcome` field — the decision travels
    with the command rather than being re-derived by reading `record_key`
    locally, since a non-anchor participant's tablet never holds a local
    copy to read at all; this is uniformly true for the anchor's own
    resolve too (same primitive, `txn_resolve`). A reader that can't
    resolve an intent locally (`ResolveStep::Foreign`, carrying an
    `IntentInfo`) is a **new**, distinct outcome from `Pending` — a
    caller with no cross-tablet resolver (the internal `read_resolved`
    retry loop) treats them identically; `linearizable_get_served_fast`
    is the one caller that acts on `Foreign` differently, handing the
    routing info to `animusd`'s cross-tablet `TxnStatus` query. See the
    ADR's PR4 amendment for the full protocol, the record-key
    cross-tablet-routing answer, and the deliberate deviations from the
    spec. Regression: `tests/txn_multi.rs`.
  - **In-doubt recovery + decision semantics (ADR 0018 §2/PR5).** Recovery
    makes a **second, independent decider** on an already-decided record
    legal — a still-live coordinator's commit can race a recovery pusher's
    abort, or vice versa. `apply_and_compact`'s `TxnCommit`/`TxnAbort` arms
    changed from "any conflicting second decision is an assert" to "the
    **first**-applied decision wins (log position is the ballot — every
    replica applies its one Raft log in the same total order), and any
    later conflicting proposal is a logged no-op (`tracing::warn!`, both
    outcomes named), never a panic." **Since ADR 0018 §2/PR6**: this now
    also covers two `Committed` flips at **two different** `commit_ts`
    values — PR5 shipped this as a hard assert ("impossible by
    construction"), but it isn't: `txn_commit_at_least`'s own
    `mint_at_least` mints a fresh ts every call, so a still-live
    coordinator's own commit attempt and the recovery resolver's
    independent post-grace push can each legitimately conclude "commit"
    with *different* timestamps — found live, deterministically, by the
    multi-tablet transaction corpus's `participant_leader_kill_early`
    scenario (`animusd`'s own `CLIENT_TIMEOUT`, 10s, is longer than
    `RECOVERY_GRACE`, 5s, so this is reachable under nothing more exotic
    than an ordinary leader election). Same-outcome-different-ts is now
    exactly as legal a duelling-decider shape as different-outcome
    duelling; the *only* remaining hard assert is two genuinely
    **conflicting** decisions racing the same log position, which one
    sequential log still rules out. See ADR 0018's PR5 amendment §1's
    corrective note for the full account. A new abort-only primitive,
    `RaftKvNode::txn_abort` (the dual of `txn_commit_at_least`, no inline
    resolve), lets a caller decide without also resolving — every decider
    (`animusd`'s ordinary coordinator path and its recovery pusher alike)
    must re-read the record's actual status afterward
    (`txn_status_local`/`txn_record_view`) and act on *that*, never assume
    its own proposal won.
  - **`TxnRecord::intent_spans`/`KvCommand::TxnStage.spans` changed from
    `Vec<KeyRange>` to `Vec<(String, KeyRange)>`** — a real gap PR3/PR4 left
    open (a non-anchor stage never populated it at all, so a record had zero
    visibility into other participants) closed exactly like PR4 closed the
    analogous `record_table` gap: `animusd`'s coordinator now hands the
    anchor's stage the complete cross-participant `(table, span)` list up
    front (`RaftKvNode::txn_stage_anchor`, the new general entry point;
    `txn_stage` is now a thin single-participant wrapper). `codec.rs`
    `VERSION` bumped 8 → 9 (internal wire/record format only, no
    back-compat concern).
  - **Orphan records + the resurrection guard** (a corner the
    `intent_spans` fix's own review caught): PR4's prepare phase stages
    every participant concurrently, so a participant's own stage can
    succeed while the *anchor's* own `TxnStage` — which would create the
    record — never lands (the same fence/seal-miss gap PR4 already
    documented for a participant's stage, now recognized to apply to the
    anchor's own stage too: `wait_applied` only confirms the entry
    *applied*, never that its content check succeeded). Two fixes:
    `KvCommand::TxnAbort` gained `orphan_created_ts: Option<HlcTimestamp>`
    (`codec.rs` `VERSION` bumped 9 → 10) — `Some` means "no record exists
    at all; synthesize a fresh `Aborted` tombstone directly" (a new
    primitive, `RaftKvNode::txn_abort_orphan`) instead of the ordinary
    "missing record is a fence-miss no-op." An absent record can only ever
    decide abort (committing needs a participant list to verify against,
    which only the record itself would have provided). Second,
    `apply_and_compact`'s `TxnStage` arm now checks — before merging
    anything, only for `is_anchor: true` — whether a **decided** record
    for this exact `txn_id` already exists; if so the whole entry no-ops
    (logged), never resurrecting it to `Pending`. `IntentInfo` gained a
    `version: HlcTimestamp` field (the intent's own applied timestamp) as
    the grace-clock substitute a pusher uses when no record exists to read
    `created_ts` from. Regression: the in-crate
    `pr5_orphan_and_resurrection_tests` module (`lib.rs`) — not an
    external `tests/` file, since reproducing "a late `TxnStage` for an
    already-known `txn_id`" needs `pub(crate)` access (`txn::record_key`,
    a direct `KvCommand::TxnStage` construction, `propose_ordered_aux`/
    `mint_pushed`) the public `txn_stage_anchor` (always mints a *fresh*
    id) cannot express.
  - **New recovery primitives**: `RaftKvNode::txn_record_view` (the
    recovery-view dual of `txn_status_local`, also returning
    `intent_spans`/`created_ts`) and `txn_verify_staged` (does this tablet
    still hold a live intent for `txn_id` over a given span — a small
    bounded scoped scan of the raw envelope, since every span this crate
    builds is an exact single-key point-span). `RECOVERY_GRACE` (5s,
    `pub`) is the liveness-only knob gating when a push may act — grace
    never affects *what* it decides, only *when*, per the argument above.
  - **`TxnTracker`** (per-group, `Arc<Mutex<_>>`): `pending: BTreeMap<TxnId,
    (record_key, created_ts)>` (records this group anchors, still
    `Pending`) and `unresolved_decided: BTreeMap<TxnId, (record_key,
    TxnOutcome)>` (decided but not yet locally resolved — a deliberately
    approximate, still-safe signal: a group only ever observes a
    `TxnResolve` landing on *itself*, so this really tracks "the anchor's
    own local resolve happened," not "every participant resolved"; a
    resolver that stops tracking slightly early never loses correctness — a
    straggling remote intent is still resolved on demand the moment any
    reader hits it). Rebuilt at group start (`rebuild_txn_tracker`) via one
    bounded scope scan for `txn::is_record_key` markers — deliberately not
    log replay, mirroring `sealed`/`committed_ceiling`'s own
    engine-marker-survives-compaction reasoning — the same accepted cost
    `has_data`/`engine_image` already pay. A documented residual: since a
    decided record is never pruned (no record/intent GC yet), a restart's
    rebuild re-adds every historical decided record to
    `unresolved_decided`, not just genuinely-unresolved ones; the resolver
    loop's re-attempts on these are harmless (idempotent) but real
    background cost at scale — accepted, out of PR5's scope. Exposed via
    `pending_txns`/`unresolved_decided` (cheap lock-and-clone, no barrier).
  - **Read-path push, scoped to the foreign-intent path** — `animusd`'s
    `cp_get_local_resolving` now calls `ClientCtx::txn_recover` on a
    still-`Pending`/failed `TxnStatus` query instead of immediately
    reporting "retry" (lifting the PR4 amendment's own flagged deferral).
    The **locally**-`Pending` case (this crate's own bounded
    `read_resolved` retry, no network layer to push with) and scans stay
    unchanged — the resolver loop is what eventually pushes those.
    See the ADR's PR5 amendment for the full protocol, safety argument, and
    the async-post-ack resolve change on `cp_txn`'s commit path.
    Regression: `tests/txn_recovery.rs`; `animusd/tests/cp_txn.rs`'s
    coordinator-crash pair.
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
- **Uncertainty-interval read restarts (ADR 0018 §2/PR4).** `RaftKvNode::
  read_at` restarts **once** at `Hlc::uncertainty_upper(ts)` when it
  observes no value at `ts` but a version exists in `(ts,
  uncertainty_upper(ts)]` — a bounded *liveness* cost (counted via
  `Metric::CpUncertaintyRestarts`), never a correctness one: the restart
  only ever moves the serve timestamp later, so it can only pick up more
  committed data, never lose any. Not wired into `linearizable_get_served`
  (serves at "latest", where the question doesn't apply) or scans.
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
  that commit **and** (ADR 0018 §2 amendment fix, below) for this replica's
  own engine to locally observe a *committed* range-seal covering this
  tablet's scope — never proceeding on "nothing pending locally" alone, which
  a quiescent replica satisfies trivially before the seal has even been
  proposed. On timeout: the original stuck-apply escape hatch (proceed with a
  loud warning) fires only when the seal is already locally observed and it's
  purely the engine-merge watermark lagging a local commit; a stuck seal
  *commit* never takes that escape hatch — it retries next tick instead,
  logging loudly on every retry past the timeout so a genuinely quorum-dead
  absorbed group is visible to operators rather than silently torn down.
  `plan`'s `absorbing` gate (any `state.hosted ∩ view.merged` tablet defers
  every `WidenScope`) sequences drain-before-widen across the two otherwise-
  independent actions. This is ADR 0033 post-merge hardening — the 1-in-5
  `ProdEnv` flake in `animusd`'s `tablet_merge.rs` was a real, permanent
  false-"absent"; the seal-commit gate above is a *second*, later hardening
  pass over the same drain (see the range-seal bullet above and ADR 0018's
  PR2 amendment corrective note #2) — a fast replica racing ahead of the
  seal's own commit is a distinct hazard from racing ahead of an ordinary
  write's engine-apply, caught only once a genuine multi-process split
  deployment exposed it. The read-side halves (`linearizable_get_served`'s
  served/absent disambiguation; `animusd`'s `cp_get_local`/`cp_scan_local`
  scope pre-checks) live in this crate's `RaftKvNode` + `animusd` — see the
  root `CLAUDE.md` and ADR 0033.

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

Emitted in this fixed order: `ProposeSeal` → `NarrowScope`/`WidenScope` →
`Host` → `Reconfigure` → `Release`/`Reclaim`/`Absorb`.

| Action | What it does |
|--------|--------------|
| `ProposeSeal` | ADR 0018 §2 amendment fix: (re-)propose this already-hosted tablet's own range-seal for a range named in `TabletFacts::pending_seals` — a split handoff (a child of this tablet still lacking a locally-observed covering seal) or an absorb handoff (this tablet's own scope, once `view.merged` names it, still lacking one). A no-op at execution unless this node currently leads the tablet. **Persistent, re-derived every tick from `gather_facts`'s own `seal_covers` scan — never a one-shot side effect of the `NarrowScope`/`Absorb` tick that used to bundle it**, so whichever replica eventually holds leadership gets its chance regardless of when leadership shuffles relative to the local scope mutation or teardown. |
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

`cargo test -p animus-cp-data`. All but one of the 25 test binaries drive
`SimEnv` — use `run_for`/`run_until`, never `run()` (the driver has perpetual
heartbeat/election timers). Linearizable reads are async (a read-barrier probe
round), so drive them as spawned tasks + `run_for`, and never `block_on` a
`tick()` whose planned action tears a group down (`Reconciler::teardown` polls
`env.sleep()` internally). The one exception is `prod_concurrent_ts_monotonic.rs`
(below) — a real-thread `ProdEnv` test, deliberately, because the race it
guards is provably unreachable under `SimEnv`'s single-threaded scheduler.
There is also one **in-crate** `#[cfg(test)] mod` at the bottom of `lib.rs`
itself (`pr5_orphan_and_resurrection_tests`, ADR 0018 §2/PR5's §2b) —
`cargo test -p animus-cp-data --lib` runs it; it needs `pub(crate)` access
(`txn::record_key`, a direct `KvCommand::TxnStage` construction,
`propose_ordered_aux`/`mint_pushed`) no external `tests/` file can reach, to
build a "late `TxnStage` for an already-known `txn_id`" scenario the public
API (which always mints a *fresh* id) cannot express.

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
  carrying the leader's engine image. `snapshot_catchup_carries_txn_records_
  and_intents` (ADR 0018 §2/PR3) extends this: stages (never decides) a
  transaction before the compacting write burst, confirms the
  snapshot-caught-up follower's raw engine holds the identical still-
  `Pending` intent envelope, then resolves and confirms convergence.
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
- `prod_concurrent_ts_monotonic.rs` — the **real-thread `ProdEnv`** regression
  for `propose_ordered`'s fix (`lib.rs`, this file's `CLAUDE.md` entry above):
  a real 3-node group under many concurrent put+linearizable-get client tasks
  (real OS-thread parallelism, `#[tokio::test(flavor = "multi_thread")]`),
  asserting no `assert_ts_monotonic` panic and every write reads back
  correctly. The one test in this crate that can't be `SimEnv` — the mint/
  propose-ordering race it guards has no `.await` point for two tasks to
  interleave at under `SimEnv`'s single-threaded cooperative scheduler.
  `concurrent_txn_writes_and_reads_never_violate_ts_monotonicity` (ADR 0018
  §2/PR3) extends the identical hammer to `txn_write` (stage + commit +
  resolve, three proposals per call through the same critical section) —
  the regression that the new commands didn't reopen the race.
- `txn_single.rs` (ADR 0018 §2/PR3) — the single-participant transaction
  suite: the commit path visible via `read_at`/`local_get`/a scan; the
  abort path restoring the prior committed value (never a tombstone or the
  staged one); a committed staged-delete producing a real tombstone; a
  linearizable read of a `Pending`-intent key blocking (confirmed via a
  short `run_for` budget under `INTENT_WAIT_TIMEOUT`) then serving once
  committed; a scan over a mix of committed rows and a still-staged intent
  returning exactly the committed rows (no record-marker/intent bytes
  leak, no early/garbled staged value); crash/restart WAL-replay
  idempotency (`Simulator::stop` + a fresh `RaftKvNode::start` on the same
  engine, mirroring `witnessing.rs`'s idiom) re-deriving the identical
  committed value and a post-restart commit ts still strictly exceeding
  the recovered one; a stage proposed into an already-sealed range
  committing/applying as a whole-or-nothing no-op (`propose_seal` directly,
  mirroring `cross_group_lww.rs`'s shape); and seed reproducibility.
- `txn_multi.rs` (ADR 0018 §2/PR4) — the multi-participant suite: two- and
  three-scoped-group atomic commits, visible on every replica of every
  participant group (`shared_engine.rs`'s harness style — a minimal
  in-test coordinator over the raw `RaftKvNode` handles, mirroring what
  `animusd::ClientCtx::cp_txn` does over real forwarding); abort cleanup
  (every staged participant's key reverts to its pre-transaction value);
  foreign-intent resolution end to end
  (`linearizable_get_served_fast`'s `FastRead::Foreign` →
  `txn_status_local` on the anchor → `resolve_intent_given_status` on the
  participant, with no local record ever existing on the participant's own
  tablet); a participant's stage into an already-sealed range as a true
  engine-level no-op (confirmed directly via `local_get`, since the
  propose outcome alone can't distinguish it from a genuine stage); a
  participant leader-kill during prepare converging to a clean abort with
  no half-staged intent surviving re-election; and a five-seed
  reproducibility sweep. See the ADR's PR4 amendment for the full design
  (including the `record_table` routing-info answer and the deliberate
  deviations from the spec).
- `txn_recovery.rs` (ADR 0018 §2/PR5) — in-doubt recovery + the
  decision-semantics fix, with `push`/`recovery_resolve` test helpers
  mirroring `animusd::ClientCtx::txn_recover`'s protocol directly over raw
  `RaftKvNode` handles (this crate has no wire layer of its own): a push
  commits when every participant genuinely staged past `RECOVERY_GRACE`
  (both keys visible on every replica of both groups); a push aborts when a
  participant never staged (every value restored); a recovery abort beating
  a late coordinator commit with no assert (driving both proposals
  explicitly, confirming the actual status is the abort — the
  decision-semantics fix's core regression); two duelling recoverers'
  conflicting proposals converging on one identical status with no assert
  (zero intervening sim time, mirroring `cross_group_lww.rs`'s
  in-flight-race technique); a push declining before grace elapses; an
  orphan intent with no record anywhere (`push_aborts_an_orphan_intent_
  with_no_record_anywhere`, the record-absent branch of §2b's fix) — the
  anchor's whole range sealed first (`propose_seal`, `txn_single.rs`'s
  already-sealed-range technique) so its own stage silently no-ops, still
  handing back a genuine, minted `(txn_id, record_key)` with no record ever
  written on the anchor and a real participant intent referencing it —
  decided abort past grace via `txn_abort_orphan` (`push`'s own
  record-absent branch, added alongside this test), the synthesized
  tombstone confirmed to carry empty `intent_spans`, and the triggering
  intent resolved away directly by the caller (never via the tombstone's
  own spans — it has none); `pending_txns` surviving a genuine process
  restart via the rebuild scan (a single-voter group, mirroring
  `witnessing.rs`'s own restart idiom, to sidestep
  which-of-three-replicas-becomes-leader-again nondeterminism a
  multi-voter restart would add for no benefit to what this test proves);
  a five-seed reproducibility sweep; and (ADR 0018 §2/PR6)
  `duelling_commits_at_different_timestamps_the_second_is_a_no_op_never_a_panic`
  + its own five-seed sweep — two independent `txn_commit_at_least` calls
  for the same `txn_id` at the same floor, proven to mint genuinely
  different timestamps, the second confirmed a no-op (never a panic) and
  the record correctly reflecting the first's ts; regresses the
  corpus-found bug directly. **Gotcha this file's own test
  authoring is the regression for**: its `drive` helper's `sim.run_for(budget)`
  always advances the full `budget` regardless of when the future actually
  completes (the same gotcha `ts_cache.rs`'s history warns about) — this
  file's `SETTLE` is deliberately 300ms, not `txn_multi.rs`'s 2s, because
  the grace-boundary tests need to reason precisely about how much sim time
  has elapsed relative to `RECOVERY_GRACE`. Also (ADR 0018 §2/PR6):
  `duelling_commits_resolve_every_participant_consistently_never_torn` —
  two independent `txn_commit_at_least` calls across a two-group
  transaction, both resolved from a re-read (never a losing decider's own
  candidate), asserting every replica of both groups converges on the
  identical value and that a `read_at` snapshot straddling the two decide
  attempts' timestamps sees both participants' committed values together
  (never one without the other); and (ADR 0018 §2/PR6, task #16)
  `stage_over_a_foreign_pending_intent_no_ops_then_a_pushed_retry_succeeds`
  (a second transaction's stage over a still-`Pending` key is a true
  no-op — confirmed via `txn_verify_staged` and that no record gets
  created — until the blocking transaction is pushed to a decision, after
  which a retried stage succeeds) and
  `abort_restore_never_meets_another_transactions_intent` (reconstructs
  the exact three-transaction sequence that used to corrupt the MVCC
  version chain — a committed value, a second transaction overwriting it
  and abandoned before resolving, a third transaction's own stage attempt
  over that still-unresolved intent — and proves the third transaction's
  stage is now rejected outright, so the second transaction's own later
  abort-restore correctly finds the first transaction's real committed
  value, never a stale intent).
- `txn_conditions.rs` (ADR 0018 §2 follow-up amendment) — the apply-time
  write-key conditions suite, single-Raft-group harness style like
  `txn_single.rs` (conditions are entirely a `TxnStage` apply-arm concern,
  no multi-participant coordinator needed): a matching condition (present
  value, and separately "must be absent" on a genuinely absent key) stages
  and commits; a mismatched value and a violated "must be absent" both
  reject the whole stage as `ConditionFailed { key }`, the absence case
  proven with a *second*, unconditioned key in the same multi-key stage to
  show whole-or-nothing (neither key staged, not just the conditioned
  one); a key already holding a *different* transaction's unresolved
  intent reports `IntentBlocked`, not `ConditionFailed`, even when the
  condition would (irrelevantly) have evaluated true against the
  blocker's own staged value — proving the foreign-intent gate is checked
  before a condition is ever evaluated, per the priority order the ADR
  amendment settles; crash/restart WAL-replay idempotency for a
  condition-gated commit; a five-seed determinism sweep.
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
- **The 19 frozen scenarios** (`ANIMUS_RECONCILER_SEEDS`, default 1 =
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
  version only reproduced ~3/5 runs), re-add-after-exclusion-cancels-release,
  and (ADR 0018 §2 amendment fix, the `split_cluster.rs` livelock — see the
  Key invariants entry above) two scenarios that make the fixed race
  deterministic under `SimEnv` by controlling per-node tick order by hand:
  `absorb_follower_waits_for_committed_seal_before_tearing_down` (tick only
  the follower of a to-be-absorbed tablet, with the leader never having been
  ticked with the merge view at all — proven to tear down prematurely against
  the pre-fix code, converges once the leader gets its own tick post-fix) and
  `narrow_seal_survives_a_late_promotion_after_narrowing_as_a_follower` (tick
  only a follower with the split view — it narrows locally without proposing,
  as expected — then force a *real* Raft membership removal promoting that
  same replica to sole leader, and prove it still eventually proposes the
  seal, which the pre-fix one-shot-per-tick design structurally cannot do
  once that replica's own local scope already matches the target).
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
