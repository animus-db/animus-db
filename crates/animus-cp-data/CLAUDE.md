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

- **`lib.rs`** — `RaftKvNode<E, S>` (the running tablet-group node), its
  command/state types, `StorageScope`, the fenced commands, ReadIndex + CAS,
  and the consensus-loop/apply-task split. See the API bullets below.
- **`host.rs`** — the per-node tablet-host reconciler (ADR 0031): `plan()`,
  `Reconciler`, and the `HostAction` set (`ProposeSeal`/`NarrowScope`/
  `Host`/`Reconfigure`/`Release`/`Reclaim` — tablets are split-only, ADR
  0044; the merge-dual `Absorb`/`WidenScope` actions were removed). 31 unit
  tests. See "The host module".
- **`cluster_segment_store.rs`** (ADR 0043 §A7b) — `ClusterSegmentStore<E,
  S>`: the **default** `SegmentStore` for the stream-shard subsystem, K-way
  replication of an immutable segment over `E`'s `Network` seam. The
  module's own 69-line `//!` doc has the full design (replica selection,
  the request/reply correlation, `repair`); wired into `animusd`
  (`animusd::build_segment_store`, `SegmentStoreHandle` — see that crate's
  `CLAUDE.md`).
- **`codec.rs`** — the crate's compact binary wire/image codec (ADR 0017
  A.2): length-prefixed, magic/version-checked framing for `KvWire`
  messages and engine images (`serde_json`'s decimal-array `Vec<u8>`
  rendering cost ~3–4x). Decode failures are loud. The Raft WAL keeps the
  shared control-plane serde_json `PersistedState` format.
- **`hlc.rs`** (ADR 0018 §2) — a pure, I/O-free Hybrid Logical Clock:
  `HlcTimestamp { wall_ms, logical }` and the per-node `Hlc` (`mint`/
  `witness`, both take the caller-sampled `Nanos` — `Hlc` never touches an
  `Env` or the wall clock itself). `pack`/`unpack` encode a timestamp as the
  storage-engine `u64` MVCC version directly (`(wall_ms << 20) | logical`,
  no node-id bits — settled over `animus-consensus`'s `(logical, node)`
  scheme because a string `NodeId` can't bit-pack); the 20-bit
  `LOGICAL_BITS` budget is hard-`assert!`-checked in `pack`, never
  `debug_assert!` (a silent overflow would silently collapse two distinct
  timestamps to one version). See the Key invariants section for how this
  is wired into the apply path and the witnessing chain. `bump_strictly_
  above(ts)` (ADR 0018 §2 amendment, the `mint_pushed`
  clock-witnessing-runaway fix) is the pure "next value that strictly
  exceeds `ts`" step shared by `next_ceiling_candidate`'s CAS-ratchet bump
  and `mint_pushed`'s no-witness write-push — the safe alternative to
  `Hlc::witness` at both call sites, neither of which may drag `self.hlc`'s
  own persistent state toward a deliberately future-shifted value.
- **`cursor.rs`** (ADR 0042/0043, `KIND_CURSOR = 0x04`) — consumer cursor
  rows: the per-tablet, per-consumer HLC watermark the DynamoDB Streams
  change-log lifecycle rework rests on. The module's own 79-line `//!` doc
  has the key layout, the escape-disjointness proof, and a documented
  residual gap; `RaftKvNode::cursor_watermark`/`cursor_rows`/
  `cursor_min_watermark` (`lib.rs`) are the read-side accessors, called in
  production only by `animusd`'s GSI drain (`index_drain.rs`).
  **`cursor_rows_with_token`/`token_of` have no production caller today** —
  their original caller, the trim janitor's merge-residue cleanup, was
  deleted along with `MergeTablets` (ADR 0044, tablets are now split-only);
  kept in case a future consumer needs the same token-vs-physical-presence
  disambiguation.
- **`seal.rs`** (ADR 0018 §2 amendment) — the **range seal**: the structural
  replacement for the retired `version_floor` cross-group-LWW fix.
  `KvCommand::Seal { range, ts }` is proposed by a range-handoff source (a
  split's `NarrowScope`) through its own Raft log; a
  later-ordered mutating entry for a key inside a sealed range is rejected
  at apply, checked against a per-group in-memory set rebuilt at group
  start from a durable **engine marker key** (deliberately from the
  engine, not log replay, since compaction can truncate a `Seal` entry out
  of the log long before its rejection duty is done). The marker's key
  lives under `animus_control::syskv::RESERVED_NAMESPACE` — engine-global,
  outside every `StorageScope` — see the module's own doc for the
  key-disjointness proof.
- **`segment.rs`** (ADR 0042/0043) — the stream-shard **segment codec**: a
  sealed shard's `SegmentStore` object format, pure and I/O-free. The
  module's own 50-line `//!` doc has the codec/validation list
  (`encode`/`decode`, `shard_id`/`segment_id` formats). **The superset-slice
  rule (ADR 0042 §10)** is the one contract worth naming here:
  `slice_to_hlc_range(records, (start_exclusive, end_inclusive))` keeps
  exactly the records inside the catalog row's own committed range,
  dropping a deposed leader's late-`put` superset's extra tail;
  `decode_and_slice` composes decode-then-slice in one call so a reader
  (the `GetRecords` sealed-shard path) can't decode a segment and forget to
  slice it. `change_record` bytes are opaque to this crate throughout (ADR
  0043's own layering rule) — only ever moved, never interpreted.
- **`ts_cache.rs`** (ADR 0018 §2) — the per-tablet **read-timestamp cache**
  (`TsCache`): leader-local, in-memory, best-effort write-conflict push. A
  two-generation rotating map; every served read bumps the span it read at
  its serve ts, and a propose-time write is pushed above any overlapping
  bump before it is embedded in a command. Losing this cache is always
  **safe** — see the Key invariants entry for why the real safety net is
  the logged read ceiling, not this cache.
- **`ceiling.rs`** (ADR 0018 §2) — the **logged read ceiling**'s durable
  marker: a single per-tablet engine-global key, same disjointness proof
  as `seal.rs`'s marker, always overwritten with the newest
  `KvCommand::ReadCeiling` value so `storage.latest_version()` durably
  reflects it — the group-start witness then re-derives a floor covering
  it on any future restart even after the log entry is compacted away. See
  the Key invariants entry for the full mechanism.
- **`txn.rs`** (ADR 0018 §2) — the transaction machinery: the 1-byte-tagged
  value **envelope** (`Envelope::Committed`/`Intent`) every apply-path
  write wraps its value in, and the transaction **record** (`TxnId`,
  `TxnStatus`, `TxnRecord`) that is the atomic commit point. A txn record
  is an ordinary **in-scope logical key** of the anchor tablet (unlike
  `seal.rs`/`ceiling.rs`'s engine-global markers, so it replicates/
  snapshots/splits like real data); `record_key` derives it from the
  anchor write's own partition token plus a lead-byte pair proved disjoint
  from every real key sharing that token — see the module's own doc for
  the full proof (and `docs/engineering-lessons.md`'s Code-patterns entry
  for the general technique). `Envelope::Intent::record_table` names the
  anchor's table (a record key alone doesn't identify which table's ring
  owns it, since tables' rings are independent). `TxnRecord::intent_spans:
  Vec<(String, KeyRange)>` names every key any participant ever staged,
  table name attached. See the Key invariants section for the full design.
  **`TxnWrite` (ADR 0046 A1, `TxnStage` kind-writes stack, codec version
  16)**: `KvCommand::TxnStage.writes`' element, a named struct (`key`,
  `value`, plus an optional derived `kind_writes`/`change_log` payload for
  a write against an indexed/streamed table) — carried inside the write's
  own `Envelope::Intent`, opaque until `TxnResolve`'s commit branch
  materializes it. See the Key invariants section's `materialize_derived`
  entry and `docs/adr/0018-cross-tablet-transactions.md`'s 2026-08-16
  amendment for the full mechanism.

### lib.rs API

`RaftKvNode<E, S>` is the running tablet-group node (start/propose/read
methods); `StorageScope` (ADR 0026/0028) confines a node's physical key
access within a possibly node-shared `StorageEngine` (a `prefix` plus a
live-narrowable `range`); `KvCommand::KindBatch` (ADR 0041 §3) is the
multi-kind atomic batch backing materialized secondary indexes and the
change log; **transactions** (ADR 0018 §2) are covered in Key invariants
below. See the crate's rustdoc for the full method/accessor inventory.
Four rules that aren't derivable from a doc comment:

- **A group owns a scope *set*, not one scope.** `with_kind(kind)` derives a
  sibling scope per row kind (`KIND_BASE`/`KIND_LSI`/`KIND_CHANGE`/
  `KIND_FOOTPRINT`/`KIND_CURSOR`) over the *same* `Arc<Mutex<KeyRange>>`, so
  one `narrow`/`widen` moves every kind at once. **`StorageScope::whole()`
  is no longer an identity transform** — its base-kind scope prefixes one
  `KIND_BASE` byte, so *any* group's physical key is `prefix || kind ||
  logical`. **Anything reading a group's bytes straight off the engine must
  go through `RaftKvNode::physical_key(kind, key)` rather than assembling
  `prefix || key` itself** — hard-coding the layout was correct only while
  a group had exactly one scope, and four tests broke on exactly that
  assumption.
- **`local_get_kind`/`local_scan_kind`'s `end: Option<&[u8]>` mirrors
  `local_scan`'s unbounded-above handling for the base scope — when `end`
  is `None`, the bound is derived from this kind scope's own
  `physical_bounds()`, never the caller's**, because no finite byte string
  can bound an LSI row's keyspace in general; the bound still comes from
  the kind scope's own prefix, never `entries()`, so it can only ever read
  this one scope on this one tablet.
- **`approx_bytes()` is deliberately pinned to the base kind scope**
  (measures only base data, the ADR 0034 fix that stops auto-split
  reacting to change-log churn). `approx_bytes_kind(kind)` is its
  kind-scoped sibling — the Streams sealer's size trigger needs
  `KIND_CHANGE`'s own bytes specifically, and reusing `approx_bytes` for
  that is exactly the trap this sibling exists to avoid (see
  `docs/engineering-lessons.md`'s Code-patterns entry).
- **`engine_applied_index()`** is the confirm-by-index primitive
  linearizable reads gate on, so a proposer confirms a specific
  `Accepted { index }` applied instead of polling value equality.

## Key invariants

State once here; cross-referenced from the sections below.

- **The packed HLC commit timestamp is the MVCC version (ADR 0018 §2
  amendment — replaces the retired `version_floor`-scaled Raft-index
  invariant).** Every mutating `KvCommand` variant carries a `ts:
  HlcTimestamp`, minted from the proposing leader's own per-group `Hlc` at
  propose time; apply stamps `hlc::pack(ts)` as the engine `version` at all
  four apply sites (`Put`/`Batch`/`Delete`/`Cas`) — never a Raft index — so
  per-key LWW reproduces cross-group HLC order and re-applying on recovery
  stays idempotent. This closes the cross-group-shared-engine hazard
  `version_floor` did (a fresh/widened group's own version sequence must
  never undercut a different group's), but by **witnessing** instead of a
  structural version-space separation:
  - **Witnessing** (`Hlc::witness`) folds a just-observed timestamp into a
    group's clock so its own future mints are guaranteed to exceed it, at
    four points: WAL recovery, every received `AppendEntries`
    (`witness_append_entries`, before `RaftCore::handle` — every entry,
    accepted or not, since a redundant witness is always safe), snapshot
    install, and group start (off the shared engine's own
    `latest_version()`, which alone covers a restart or a co-hosted
    sibling's already-present data).
  - **The range seal** (`seal.rs`) closes the one residual witnessing alone
    cannot: an in-flight write from a source-group leader that hasn't yet
    observed a split, still in its own commit pipeline when the handoff
    happens. A source proposes `KvCommand::Seal { range, ts }` through its
    own log at handoff time (`host.rs`'s `Reconciler`, on a split's
    `NarrowScope`); apply rejects any later-ordered mutating entry whose key
    falls in an already-sealed range, regardless of that entry's own `ts`,
    because within one group log order and HLC order coincide, so it is the
    **log position** that is authoritative. The reconciler gates a split
    child's `Host` on observing the relevant seal marker locally first
    (`TabletFacts::parent_seal_observed`, `Metadata::split_parents`
    provenance) — see `host.rs`'s entry below and `seal.rs`'s module doc for
    the key-disjointness proof. (Tablet merge had a mirror-image gate here —
    a survivor's `WidenScope` on `widen_seal_observed`/`Metadata::
    absorbed_by` — until ADR 0044 removed merge and this half of the
    mechanism along with it.)
  - **Proposing the seal is a persistent condition, re-derived every tick —
    never a one-shot side effect of the tick that performs the local
    irreversible action it precedes.** A replica that narrows its scope
    while a follower must still propose the seal once later promoted to
    leader. `host.rs`'s `gather_facts` computes `TabletFacts::pending_seals`
    fresh every tick (via `seal_covers`), and `plan` turns each into
    `HostAction::ProposeSeal`, replanned until observed. This gate is
    self-supporting, not a deadlock: it keeps the quorum needed to commit
    the seal alive for as long as it takes; a genuinely quorum-dead group
    correctly stalls loudly instead of proceeding early. See ADR 0018's §2
    amendment and `docs/engineering-lessons.md` for the full story.
    Regression: `tests/reconciler_corpus.rs`'s
    `narrow_seal_survives_a_late_promotion_after_narrowing_as_a_follower`.
    (Tablet merge had a mirror-image waiting-side gate here — an absorbed
    replica's `Reconciler::teardown` requiring a locally-*committed* seal,
    never "nothing pending locally" alone, before tearing down — until ADR
    0044 removed merge and this half of the mechanism along with it; its
    own regression, `absorb_follower_waits_for_committed_seal_before_
    tearing_down`, went with it.)
  - **A hard, non-`debug` assert** (`assert_ts_monotonic`, at apply) checks
    every applied entry's `ts` strictly exceeds the previous one this group
    applied — the load-bearing invariant the whole witnessing chain exists
    to guarantee; a failure means the chain itself is broken, not a
    recoverable condition. Regression: `tests/cross_group_lww.rs`
    (split/seal-rejection/in-flight-race/clock-skew shapes) and
    `tests/witnessing.rs` (leader-change and restart-recovery
    monotonicity).
  - **`propose_ordered`: minting a proposal's `ts` and appending it to the
    Raft log must be one atomic step, not two** — found via `animusd`'s
    `self_heal.rs` panicking under real concurrent client load. Every
    mutating propose method now computes its `ts` **while holding the
    group's own `core` lock**, immediately followed by `core.propose(..)`
    in the same critical section; two proposers could otherwise mint
    monotonically (ts=A then ts=B) but race to append to the log in the
    *opposite* order, so apply would see a real decrease. **This is a
    `ProdEnv`-only bug, provably unreachable under `SimEnv`**: the original
    code had no `.await` point between minting and proposing, so only
    genuine OS-thread parallelism can interleave there. `propose_ordered`
    also floors every ts-producing path on `last_proposed_ts` (this
    leader's own last-*logged*, not just last-*applied*, ts, since the
    apply task can lag the consensus loop by design). A second, narrower
    bug: `next_ceiling_candidate`'s ratchet must never hand back
    `last_proposed_ts` *unmodified* as a candidate, only as a floor to
    strictly exceed, or a `ReadCeiling` can tie a write's exact ts. See
    `docs/engineering-lessons.md` for the diagnostic story. Regression:
    `tests/prod_concurrent_ts_monotonic.rs` — deliberately the one
    real-thread `ProdEnv` test in a crate whose other binaries are all
    `SimEnv`, since this race needs genuine thread parallelism to express.
- **CAS is decided at *apply* time, not propose time** — this is what makes it
  linearizable and contention-correct. `RaftCore` agrees only the order; `Cas`
  rides through as opaque data. Apply evaluates it in commit order against the
  key's *current committed* value and compares to `expected`; equal → merge
  at `index`, else no-op. Every replica applies the same order against the
  same state with no clock/RNG, so every replica makes the **identical**
  decision. Outcome is stashed in driver `CasResults` keyed by the log index.
- **`TxnStage`'s own-key conditions are decided at *apply* time too, the
  identical CAS-style discipline** — evaluated against each key's current
  committed value inside the same apply arm that decides the pre-existing
  fence/seal/foreign-intent gates, recording a `StageOutcome` per Raft log
  index (`StageOutcomes`, mirroring `CasResults`). **`wait_applied(index)
  .await == true` does NOT imply `stage_outcome(index)` is `Some`** — a
  snapshot install can advance `engine_applied` past `index` without this
  replica individually applying (hence recording an outcome for) that exact
  entry, since an install globs many commands together. `txn_stage_anchor`/
  `_participant` poll `stage_outcome` directly instead (`wait_stage_outcome`)
  — `None` on timeout, never a hard-`expect`ed fact that turns out not to be
  guaranteed. See `docs/engineering-lessons.md` for the general lesson.
- **`KindBatch` gained the identical own-key `conditions` field (ADR 0046
  "evaluate at leader" seatbelt, codec version 15)** — modeled directly on
  `TxnStage.conditions`, `(key, expected)` byte-level OCC pairs checked
  against the KIND_BASE scope. Two deliberate differences from `TxnStage`'s
  own handling: (1) **no `StageOutcome` analogue** — a condition-failed
  `KindBatch` no-ops silently, indistinguishable from a fence/seal miss (the
  existing generic-error/probe-poll-timeout contract every
  `put_kind_batch_fenced` caller already handles), so there is nothing to
  disambiguate; and (2) **checked BEFORE the fence/seal gate**, not behind
  it — `TxnStage`'s `condition_failure` only evaluates once already known
  in-fence so `StageOutcome` can report the fence/seal reason ahead of a
  condition one, but with no outcome channel to prioritize for `KindBatch`
  there is no reason to gate the read behind the fence check. The two gates
  still simply AND together either way. Production caller:
  `dynamo::kind_write_item_at_leader`'s leader-side evaluate-then-propose
  write path (ADR 0046 U3) passes `seatbelt: vec![(base_key, raw_old)]` —
  the guard against a concurrent `TxnStage`/`TxnResolve` commit landing
  between that evaluator's own-key read and its own propose call. Tests:
  `tests/kind_batch_conditions.rs`, mirroring `tests/txn_conditions.rs`
  scenario-for-scenario.
- **`KindBatch.change_log` is a `Vec<(prefix, record)>` (codec v17, ADR
  0049 Train A rung-1 fixup)** — one entry can carry a whole marker-table
  batch's records (one per item, all completed at the entry's own apply
  `ts`; per-item prefixes keep keys distinct). `TxnWrite.change_log` stays
  an `Option` (a transactional write stages at most its own record). The
  entry-granularity contract this preserves: a batch to one tablet is ONE
  Raft entry — never one per item, which is ~N× the WAL/apply work and
  regressed `animusd`'s populate-then-backfill path when briefly shipped.
- **`materialize_derived` — the ONE shared "materialize derived writes at
  this ts" helper (ADR 0046 binding decision, `TxnStage` kind-writes stack
  PR1)**: both `KvCommand::KindBatch`'s apply arm and `KvCommand::
  TxnResolve`'s commit branch call this and only this — never two
  independently-maintained copies (principle 5 of ADR 0046, replay/
  snapshot-stability: two copies would start identical and diverge the
  first time either is touched alone). Queues every `(kind, key, value)`
  write at `hlc::pack(ts)` and, if present, completes the change-log key as
  `prefix || hlc::pack(ts)` — `ts` is always the caller's OWN entry's
  commit timestamp (`KindBatch`'s own entry for that arm; the *resolve*
  entry's own ts for `TxnResolve`'s, never the transaction's `commit_ts`
  and never the stage's own ts — ADR 0018 §2 B1). Regression:
  `tests/txn_kind_writes.rs::kind_batch_and_txn_resolve_materialize_byte_
  identical_rows_for_identical_payloads`.
- **The value envelope + transactions (`txn.rs`).** Every value the apply
  path merges into the engine is 1-byte-tagged: `0` = committed (raw value
  follows), `1` = an intent naming the staging `TxnId`, its record's
  logical key, and the staged value (`None` = a staged delete). Every read
  path unwraps it before a value reaches a caller: point reads resolve via
  `RaftKvNode::read_resolved` (bounded retry while `Pending`); scans resolve
  via `resolve_scan_rows`, **non-blocking** — a still-`Pending` row is
  silently omitted. See ADR 0018 §2 for the full 2PC protocol, and
  `txn.rs`'s own doc for the txn-record-key structural disjointness proof
  (`record_key` lives *inside* the anchor tablet's own `StorageScope`, an
  ordinary in-scope logical key, not an engine-global marker like
  `seal.rs`/`ceiling.rs`). The prohibitions below are load-bearing
  regardless of caller:

  - `Aborted` (or a later `Committed`) resolution serves the value the key
    held immediately before the intent, restored by rewinding to
    `get_at(key, intent_version - 1)` — **never** a tombstone, which would
    incorrectly shadow that older, still-live committed value.
  - **`erase_scope` deliberately does NOT go through `local_scan`** (which
    filters record keys and resolves values) — it uses `raw_scoped_keys`,
    since drop-table GC must physically erase everything this scope ever
    wrote (ordinary values, pending intents, and txn records alike).
  - **`TxnCommit`/`TxnAbort` carry no `fence`**, like `Seal`/`ReadCeiling`: a
    2PC decision must be durable and final regardless of any later range
    change, and never touches user data. A *conflicting* second decision
    on an already-decided record is a protocol-bug hard assert, mirroring
    `assert_ts_monotonic`'s doctrine.
  - **Writers push intents, never overwrite one**: `TxnStage`'s apply
    rejects (whole-or-nothing) any target key whose *current* value is an
    unresolved `Envelope::Intent` naming a **different** `txn_id`
    (same-txn re-staging is unaffected). This closes a durability hole: an
    overwritten-but-unresolved intent isn't erased (MVCC keeps every
    version), so if the overwriting transaction later aborts, its
    one-hop-back restore could land on the stale intent instead of a
    genuinely committed value — a chain a later correct resolve can never
    repair. Rejecting the overwrite at apply time makes the corrupt chain
    structurally unrepresentable. **The proposer side matters just as
    much**: a stage call returning `Some(ts)` only ever means "this entry
    applied," never "my content landed" — so `animusd::ClientCtx::
    txn_prepare_pushing` verifies every staged key via `txn_verify_staged`
    after each attempt. Regression: `tests/txn_recovery.rs`'s
    `stage_over_a_foreign_pending_intent_no_ops_then_a_pushed_retry_succeeds`
    and `abort_restore_never_meets_another_transactions_intent`.

  Other invariants, one line each: a tablet split's `split_key` is not
  token-aligned, so a split racing an in-flight transaction could in
  principle separate a token's rows across siblings (deferred, per
  `txn.rs`'s doc); a non-anchor participant's stage merges intents only,
  never touching this group's own fence/engine (`tests/txn_multi.rs`);
  in-doubt recovery lets a **first-applied** decision win on an
  already-decided record, with a hard assert only on two genuinely
  **conflicting** decisions racing the same log position; an orphan record
  (anchor `TxnStage` never landed) can only ever decide abort, via
  `KvCommand::TxnAbort`'s `orphan_created_ts`; `TxnTracker`'s
  `unresolved_decided` is deliberately approximate but safe (a straggling
  remote intent resolves on demand the moment any reader hits it).
  Regression (whole txn suite): `tests/txn_single.rs`,
  `tests/snapshot_catchup.rs`, `tests/prod_concurrent_ts_monotonic.rs`, the
  in-crate `pr5_orphan_and_resurrection_tests` module.
- **`engine_applied` vs `last_applied`.** The two-task split (below) means the
  core's `last_applied` (a buffer cursor the consensus loop advances) *leads*
  the engine. Linearizable reads therefore gate on the separate
  **`engine_applied`** atomic the apply task advances after each merge —
  **never** `last_applied` (else a read could observe past the engine).
- **Durable-before-visible** (ADR 0009): effects are only drained for fsynced
  entries, and the engine write follows the WAL `fsync`.
- **Write-conflict push + the logged read ceiling — the serializability half
  of the MVCC design.** A write must never commit at a `ts ≤` a `ts` at
  which its keys were already served to a reader. Two layers, deliberately
  separate:
  - **`ts_cache.rs`'s `TsCache`** is leader-local, in-memory, best-effort —
    every served read bumps the span it read at its serve `ts`; every
    mutating propose (`mint_pushed`) checks its minted `ts` against the
    highest overlapping bump (plus the committed ceiling, folded in — see
    the per-term note below) and, if not strictly above, bumps past it as
    pure arithmetic and re-mints (one retry always suffices). Losing this
    cache is always **safe**: over-conservative pushes are still correct
    writes, just marginally later-timestamped.
  - **The logged read ceiling** (`ceiling.rs`, `KvCommand::ReadCeiling`) is
    the actual safety net a leader-local cache alone can't be, across a
    leader change: a leader may only serve a read at a `ts` strictly below
    the highest `ReadCeiling` its group has **committed and applied**, and
    proposes a fresh one (`Hlc::uncertainty_upper(serve_ts)`, amortizing to
    roughly one per `HLC_MAX_OFFSET`) when serving above the current one.
    Safety: a live leader change's new leader already witnessed the prior
    ceiling's `ts` via ordinary `AppendEntries` receipt **before it could
    ever campaign** (Raft leader completeness), so its own future mints —
    and every write it proposes — strictly exceed it. A durable **engine
    marker** closes the residual a purely in-memory design would leave: a
    read-only workload can compact a `ReadCeiling` entry out of the log
    with no interleaved write to raise `storage.latest_version()`, so the
    marker's own merge does that job directly. **Never disambiguate a
    ceiling candidate via `Hlc::witness`** — it would drag the proposing
    leader's own clock forward to a value deliberately `HLC_MAX_OFFSET` in
    the future, poisoning every ordinary mint right after and turning the
    intended O(1) amortized proposal rate into O(N) (a real regression a
    seed-driven test caught); `next_ceiling_candidate` is a **separate**
    CAS ratchet for exactly this reason. Regression: `tests/ts_cache.rs`,
    `tests/snapshot_reads.rs`.
  - **`mint_pushed` folds the committed ceiling in at most once per Raft
    term, never on every mint (ADR 0018 §2 amendment, the
    `mint_pushed` clock-witnessing-runaway fix).** The ceiling's write-floor
    role only exists to cover a *predecessor* leader's reads — reads
    *this* leader itself served are already covered by `ts_cache`'s
    per-span entries, bumped at their real serve `ts`; a predecessor's
    ceiling is fixed as of this leader's own takeover (it already
    witnessed it via `AppendEntries` before it could campaign, and a
    deposed leader cannot commit a fresher one). Absorbing it again on
    every later mint in the same term fed a real, self-sustaining feedback
    loop instead: since the ceiling is deliberately `HLC_MAX_OFFSET` ahead
    of real time, an ordinary mint almost always fell short of it,
    triggering a push on *every* write that — via the old
    `Hlc::witness`-based push — dragged this leader's clock toward that
    future value, which made the *next* read approach and exceed the
    ceiling almost immediately, forcing a fresh `ReadCeiling` proposal
    almost every round: a k×`HLC_MAX_OFFSET` runaway lattice, independent
    of real elapsed time, that also starved genuine log entries behind the
    manufactured ceiling churn. `RaftKvNode::last_absorbed_term` (an
    `AtomicU64`, sentinel `u64::MAX`) tracks the last term absorbed;
    `mint_pushed` cannot read `term()` itself (it always runs inside
    `propose_ordered`/`propose_ordered_aux`'s already-held `core` lock, so
    a second `lock()` would deadlock) — those two methods read
    `core.term()` once and hand it to their `build` closure instead. **The
    push itself is also no longer a `Hlc::witness` call** — `mint_pushed`
    computes the pushed replacement as pure arithmetic
    (`hlc::bump_strictly_above`, the same bump rule
    `next_ceiling_candidate`'s own CAS ratchet uses, factored out so both
    stay identical by construction), leaving `self.hlc`'s persistent state
    untouched; monotonicity across a leader's own proposes still holds via
    the pre-existing `last_proposed_ts` floor. Regression:
    `tests/ts_cache.rs::interleaved_reads_and_writes_never_let_minted_
    timestamps_outrun_real_time` (interleaved reads-and-writes on a tight
    loop, asserting the group's clock never diverges from real elapsed
    time by more than a small bounded multiple of `HLC_MAX_OFFSET` —
    proven to fail pre-fix); the pre-existing leader-change safety test
    (`leader_change_never_lets_a_write_undercut_a_served_read_even_
    under_extreme_clock_skew`) stays green, since it is exactly the
    property the once-per-term absorption preserves. See the ADR 0018 §2
    amendment and `docs/engineering-lessons.md`'s Code-patterns entry
    ("a fix must cover every path to a dangerous primitive's sink") for
    the full incident.
- **Uncertainty-interval read restarts.** `RaftKvNode::read_at` restarts
  **once** at `Hlc::uncertainty_upper(ts)` when it observes no value at
  `ts` but a version exists in `(ts, uncertainty_upper(ts)]` — a bounded
  *liveness* cost (`Metric::CpUncertaintyRestarts`), never a correctness
  one: the restart only ever moves the serve timestamp later, so it can
  only pick up more committed data, never lose any. Not wired into
  `linearizable_get_served` (serves at "latest") or scans.
- **Fences are per-entry, decided at apply, and backed by a pre-propose
  check.** Every replica's apply checks a command's key(s) against the fence
  **embedded in the log entry**, never a locally-polled value — so two
  replicas at different points in observing a split's `Metadata` make the
  identical accept/reject decision. The embedded fence only covers the
  residual race between a caller's pre-propose `scope_range()` check and
  the entry's actual apply; the pre-propose reject is load-bearing, not
  redundant (see `animusd/CLAUDE.md` and the root `CLAUDE.md` entry on a
  safety mechanism with zero production callers). **Every key-writing
  `KvCommand` variant carries one** (`Put`/`Batch`/`Delete`/`Cas`/
  `TxnStage`/`TxnResolve` — `TxnCommit`/`TxnAbort`/`Seal`/`ReadCeiling`
  deliberately don't, since they never touch user data). `TxnResolve`
  gained its `fence` last (ADR 0018 §2 write-loss amendment, Bug 3): it
  was originally reasoned to need none ("every key here was already
  fence-checked at `TxnStage` time"), which held for every in-crate caller
  but not for `animusd`'s own coordinator, whose pre-fix `recovery_resolve`
  could misroute a resolve to the wrong tablet of a split table — with no
  fence, that landed directly on the wrong tablet's shared physical key
  (ADR 0028), permanently breaking the owning tablet's future LWW. See
  the amendment and `docs/engineering-lessons.md`'s "every key-writing
  command variant must carry AND enforce the apply-time fence" entry for
  the general lesson.
- **Superseded by ADR 0044**: an `Absorb` teardown's drain-before-halt
  mechanism (a merge survivor's `WidenScope` deferred on the absorbed
  group's own committed-log drain, closing a data-loss window
  `shutdown()`'s non-draining halt otherwise left open) no longer exists —
  tablet merge, `HostAction::Absorb`/`WidenScope`, and `TeardownKind::Absorb`
  were all removed (tablets are split-only). `Release`/`Reclaim`'s teardown
  remains non-draining, safely, since both erase the data anyway. The full
  original postmortem (the `ProdEnv` flake that found the gap, the
  three-part fix) is archived verbatim in
  `docs/engineering-lessons-archive.md`'s "Superseded by ADR 0044" section —
  the still-general lesson: a teardown that deletes local state must drain
  first if that state is about to be served elsewhere. See ADR 0033/0044.

## The host module

**Wired into production (ADR 0031).** `host::plan` is the pure, synchronous
per-tick **decision** function (no `Env`/clock/RNG/I/O — see `host.rs`'s
own doc for its signature and field shapes); `host::Reconciler<E, S>` is
the **execute** half, in this crate so it owns the lifecycle's invariants
and is directly `SimEnv`-testable.

- **`plan` never removes a tablet from `LocalState::hosted` on its own**
  when emitting a fallible teardown (`Reclaim`/`Release`) — real teardown
  is async and can time out. The caller calls
  `LocalState::confirm_torn_down` once its own teardown actually
  completes; until then the next `plan` re-plans the same action.
- **`Reconciler::tick(&mut self, view: &MetadataView)` is the whole
  per-tick contract**: gather `TabletFacts` from its own hosted nodes
  (`gather_facts`), call `plan` exactly once, then execute the returned
  actions **in the order `plan` emits**. The reconciler owns the hosted
  map, making it the single writer of "does this node host tablet T."
  `on_host`/`on_teardown` hooks let `animusd` mirror hosting changes into
  its `ClusterEdgeState` routing registry as a **read-only reaction**,
  never a second writer.
- **The caller still owns the trigger and the pre-recovery guard.**
  Deciding *when* to call `tick` (an event-driven `metadata_watch` wake +
  a periodic fallback) and the `last_applied() == 0` pre-recovery guard (a
  live control-plane `RaftNode` read this crate has no business taking)
  both stay in `animusd::tablet_host_reconciler_loop`.

### HostAction

**Emitted in this fixed order: `ProposeSeal` → `NarrowScope` → `Host` →
`Reconfigure` → `Release`/`Reclaim`.** `ProposeSeal` (re-)proposes a
still-owed range-seal (persistent, no-op unless leading); `Host` is
deferred for a split child until its parent's range-seal is locally
observed; `Release`/`Reclaim` tear down a tablet moved off or dropped,
respectively. Tablets are split-only (ADR 0044): merge's dual actions,
`WidenScope` and `Absorb`, no longer exist — a hosted-but-now-absent
tablet is unconditionally `Reclaim`ed, with no second case to
disambiguate (the `Reclaim`-vs-`Absorb` ambiguity this section used to
resolve is now moot).

- **`Reconciler` teardown** (`Release`/`Reclaim`): unregister from routing
  *before* touching the driver, `shutdown()`, poll `is_stopped()` bounded
  by `RECLAIM_STOP_TIMEOUT` (10s), re-register and leave `LocalState`
  untouched on timeout (so `plan` re-emits the same action next tick), else
  narrow to `erase_bound` (Release only), `erase_scope()`, delete the WAL,
  and only then `confirm_torn_down`. (Merge's `Absorb` teardown — which
  skipped the narrow/`erase_scope()` and drained the committed log before
  halting, since the absorbed data was about to be served elsewhere — was
  removed along with `TeardownKind::Absorb`; see the Key invariants entry
  above for what remains of that mechanism's lesson.)

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
    compact — all off the consensus loop. When idle it races a new
    `ApplySignal` (ADR 0044 phase-1 PR1, same shape as `ProposeSignal` below)
    against a long `APPLY_SAFETY_POLL` (250ms) rather than spinning on the old
    unconditional 5ms `APPLY_IDLE_POLL` — the consensus loop raises it at
    every point that can create apply work (a `mark_durable_through` call in
    `persist_wal`, a commit-index advance observed after stepping the core —
    covering both a follower's in-line apply on `AppendEntries` and a
    completed snapshot install's `commit_index` jump — and a single-node
    group's own commit-advancing propose), and `shutdown()` also raises it so
    a parked apply task notices a halt within one wake instead of waiting out
    the now much longer safety poll. A signal-less transition (the lazy
    on-demand snapshot-image build `RaftCore::take_snapshot_needed` sets,
    purely off the leader's own heartbeat/replicate cycle with no commit
    advance) still converges off the safety poll alone — see
    `tests/apply_signal.rs`.
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
- **Quiescence (ADR 0044 phase 1 / ADR 0048), data-plane groups only.** An
  idle group opted in via `RaftKvNode::enable_quiescence(after)` stops
  ticking entirely once its leader has had no local activity for `after`
  and every other clause of `RaftCore::quiesce_entry_ok` holds (nothing left
  to replicate, every voter caught up, no transfer/config-change/snapshot in
  flight, the async apply task caught up, no veto held) — `next_deadline()`
  returns `None`, so both drivers drop the timer arm from their `select`
  and a quiesced group posts zero `SimEnv` timeline events. The leader
  **stays** leader (fork A: every background sweeper gates on `is_leader()`).
  `RaftKvNode::wake()` (idempotent, safe on every state) is the one
  external hook every wake path funnels through: `animusd`'s
  `resolve_cp_route` calls it before routing, and the tablet-host
  reconciler (`host::Reconciler::tick`) calls it on any hosted group whose
  replica set intersects `MetadataView::down` (fork H — closes the
  TiKV-hibernate-regions hazard: without this, a quiesced follower whose
  leader died while both were dormant has no timer at all and nothing else
  will ever wake it). A locally-woken **follower** sends `RaftMsg::
  WakeRequest` to its recorded leader and re-arms a fresh election timeout,
  campaigning only if unanswered (fork B) — never a bare stale-timeout
  campaign, which would depose a healthy quiesced leader on every cold
  tablet's first touch.
  - **Vetoes (fork D)**: `RaftKvNode::set_quiesce_veto(bool)` lets an
    external subsystem (`animusd`'s `change_consumer_loop`, for a led
    tablet whose change log was non-empty on its last sweep) hold the group
    awake; ORed, once per consensus-loop iteration, with this crate's own
    in-memory check that `TxnTracker` (`pending`/`unresolved_decided`) is
    empty — a group with a live 2PC intent or an undelivered resolve can
    never quiesce out from under `txn_resolver_loop`. Both together make
    "quiesced ⇒ nothing new for the sweeper" a sound invariant for
    `animusd`'s own sweeper-skip (below).
  - `RaftKvNode::is_quiesced()` is a pure frozen-accessor read — never
    itself a wake (fork F: an admin/dashboard poll must not un-quiesce a
    fleet). `host::Reconciler::enable_quiescence(after)` is the production
    hook that opts every group this reconciler hosts *from now on* into
    quiescence (`animusd`'s `--quiesce-after` CLI flag calls it once at
    node start).
  - See `tests/quiescence.rs` (the end-to-end `SimEnv` corpus, depth knob
    `ANIMUS_QUIESCE_SEEDS`) and `tests/reconciler_corpus.rs`'s
    `quiesced_group_wakes_when_a_replica_goes_down`/`quiesce_races_a_split_
    seal_handoff` scenarios for the regressions; ADR 0048 for the full
    design (including why the control plane's own `RaftNode` never calls
    the equivalent, fork G) and the phase-2 handoff constraints this
    mechanism was built to satisfy.
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
- **`ClusterSegmentStore`'s request/reply correlation (ADR 0043 §A7b) is a
  shared `Mutex<BTreeMap<req_id, Option<Reply>>>` polled via `env.sleep`,
  never a `tokio::sync::oneshot`** — the identical shape `RaftKvNode`'s own
  `ReadProbe`/`ReadProbeAck` read-barrier confirmation already uses, for the
  same reason: a `SimEnv` caller has no tokio runtime present to drive a
  oneshot's waker. See `docs/engineering-lessons.md`'s Testing section for
  the general rule (does the primitive come from `std`/`futures`, or a
  specific async runtime crate). A request and its reply are two variants of
  one `serde_json`'d wire enum sharing **one** stream/inbox
  ([`SEGMENT_STREAM`] `= u64::MAX`, deliberately outside any `TabletId`'s
  realistic range) — the same "one dedicated stream, one single-consumer
  serving task" shape the per-tablet driver loop uses on its own `stream`,
  generalized to a cluster-wide (not per-tablet) responsibility.
- **A `put_replicated`/`delete_from` failure can leave harmless orphans on
  whichever targets *did* succeed** — never cataloged (the segment janitor
  only commits `SealStreamShard` after `put_replicated` itself returns `Ok`).
  **As-built amendment**: this used to say a retry "converges" onto the same
  deterministic id because `SegmentStore::put`/`delete` were idempotent
  overwrite/delete by contract — that contract caused a real data-loss bug
  (two independently-computed seal attempts for the same `(tablet, epoch)`
  raced their `put`s at the identical id; see `segment.rs`'s own module doc
  for the full incident) and no longer holds. `put` is now **write-once**:
  identical-content re-puts (a genuine same-attempt retry) still converge
  safely, but every real attempt writes at its own unique id
  (`segment::segment_object_id`), so a *different* attempt's partial-K
  copies at the *old* id are permanent orphans, not something a later retry
  ever revisits — reclaimed by the segment janitor's own orphan sweep
  (`animusd::segment_janitor::reap_orphans`), not by overwrite. Don't "fix"
  a partial failure by trying to roll back the targets that already
  succeeded — that would add a second distributed failure mode (the rollback
  itself can partially fail) to clean up a case that is already safe to leave
  alone.

## Tests

`cargo test -p animus-cp-data`. All but one of the 26 test binaries drive
`SimEnv` — use `run_for`/`run_until`, never `run()` (the driver has perpetual
heartbeat/election timers). Linearizable reads are async (a read-barrier probe
round), so drive them as spawned tasks + `run_for`, and never `block_on` a
`tick()` whose planned action tears a group down (`Reconciler::teardown` polls
`env.sleep()` internally). The one exception is `prod_concurrent_ts_monotonic.rs`
(below) — a real-thread `ProdEnv` test, deliberately, because the race it
guards is provably unreachable under `SimEnv`'s single-threaded scheduler.
There is also one **in-crate** `#[cfg(test)] mod` at the bottom of `lib.rs`
(`pr5_orphan_and_resurrection_tests`, ADR 0018 §2) — `cargo test
-p animus-cp-data --lib` runs it; it needs `pub(crate)` access
(`txn::record_key`, a direct `KvCommand::TxnStage` construction) no
external `tests/` file can reach, to build a "late `TxnStage` for an
already-known `txn_id`" scenario the public API (which always mints a
*fresh* id) cannot express.

One binary per behavior; the file names describe them (`ls
crates/animus-cp-data/tests/`) — covering single-tablet Raft mechanics,
automatic reconfiguration/leadership-transfer, the ADR 0026/0041/0042/0043
stream-addressing/`KindBatch`/`KIND_CURSOR`/`ClusterSegmentStore` suites,
the ADR 0018 HLC/MVCC/range-seal/transaction suites, the `host.rs`
reconciler end to end, and the real-thread `ProdEnv` regression noted
above.

### Reconciler lifecycle corpus (`tests/reconciler_corpus.rs`)

The 31 `host.rs` unit tests prove `plan` correct as a pure function; this
corpus is the **seed-reproducible fault-injection** suite for the whole
tablet lifecycle, following the house corpus doctrine (ADR 0014): a frozen,
name-seeded scenario list, a depth knob, and coverage/seed-expansion
guards. See the test file for the 18 frozen scenarios and the generic
invariant checks (hosting convergence, data safety, no zombie groups,
idempotence) — two merge-lifecycle scenarios (the absorb-drain regression
and its livelock-fix twin) were removed along with the reconciler actions
they exercised (ADR 0044, tablets are split-only).

- **Idempotence (`assert_idempotent`) means the observable *state* doesn't
  drift** (hosted set, hook call counts, live scope ranges, Raft configs)
  — **not** "the second tick emits zero actions" (`Reconfigure` is
  replanned every tick a node leads a group).
- **To add a scenario**: write `fn scenario_my_thing(seed: u64)` in the
  existing shape (`run(seed, |sim| async move { .. })`), add a
  `scenario!("my_thing_name", scenario_my_thing)` to `scenario_cells()`, and
  run it under `ANIMUS_RECONCILER_SEEDS=100` (or higher) with a `timeout` the
  first time (a hang means a same-instant unbounded-work loop, not slowness —
  see the root `CLAUDE.md`).
- **Run at depth**: `ANIMUS_RECONCILER_SEEDS=K cargo test -p animus-cp-data
  --test reconciler_corpus reconciler_corpus_runs_every_scenario` (default
  `K=1`; held green through `K=300` in ~52s).
