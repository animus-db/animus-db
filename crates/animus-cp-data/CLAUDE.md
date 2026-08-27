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
to the engine. (Historical note: this sync-core/async-driver split was shared
with the Accord slice in `animus-consensus`, deleted by ADR 0019's 2026-08-23
amendment — the shape predates and outlives it.)

## Entry points

- **`lib.rs`** — `RaftKvNode<E, S>` (the running tablet-group node), its
  command/state types, `StorageScope` (a thin kind-byte prepend/strip since
  F2b), ReadIndex + CAS, and the consensus-loop/apply-task split. The
  per-entry write fences were deleted in the ADR 0050 Train B rung-7 sweep
  (immutable ranges made them inert); the seal set survives as `Freeze`'s
  apply-time backstop. See the API bullets below.
- **`host.rs`** — the per-node tablet-host reconciler (ADR 0031): `plan()`,
  `Reconciler`, and the `HostAction` set (`Host`/`Reconfigure`/`Release`/
  `Reclaim` — tablets are split-only, ADR 0044; the zero-copy split's
  `NarrowScope`/`ProposeSeal` actions were deleted in the ADR 0050 rung-7
  sweep, as the merge-dual `Absorb`/`WidenScope` were by ADR 0044). See
  "The host module".
- **`backup.rs`** (ADR 0059 §2/§4, Train 1 PR②) — the on-demand-backup
  **object naming + codec**: `backup_manifest_object_id`/
  `backup_data_object_id` (`backup/{backup_id}/manifest` and
  `backup/{backup_id}/{tablet}/{chunk}`, a fixed namespace the stream
  sealer's own `{table}/{label}/{tablet}/{epoch}` shape never produces
  except for a table literally named `backup` — an accepted, documented
  edge case; the real collision-freedom guarantee is `animusd`'s separate
  `--backup-store` handle/instance, never the streams one, per the ADR);
  `encode_data_chunk`/`decode_data_chunk` (a magic+version-headed binary
  codec over `SeedRow` — the identical `(kind, logical_key,
  value-or-tombstone, version)` tuple `engine_image`/`install_engine_image`
  already use for split-build snapshot transfer, ADR 0050, reused rather
  than a second tuple codec); `BackupManifestObject`/`encode_manifest_
  object`/`decode_manifest_object` (a magic+version envelope, `segment.rs`'s
  own discipline, wrapping a plain `serde_json` payload of PR①'s
  `animus_control::BackupManifest` stub plus the per-tablet
  `BackupTabletProgress` completion records — JSON rather than a hand-rolled
  binary encoder because `BackupManifest` nests the multi-field, evolving
  `TableSchema` shape and this object is written/read once per backup, never
  a hot path). **Consumed since Train 1 PR③** by `animusd`'s capture driver
  (`backup_capture.rs`, writing chunked data objects) and completion
  aggregator (`backup_completion.rs`, assembling + writing the manifest
  object) — see `animusd`'s `CLAUDE.md` for both, and its
  `BackupStoreConfig`/`BackupStoreHandle` for the store-handle half of PR②.
  **Train 1 PR④** adds the wire surface (`CreateBackup`/`DescribeBackup`/
  `ListBackups`/`DeleteBackup`, `animusd::dynamo`) and the backup janitor
  (`animusd::backup_janitor`) — the janitor's own reclaim sweep reuses
  [`backup_prefix`] to scope a local `SegmentStore::list()`/`delete()` sweep
  per backup id (this module contributes only the naming convention; no
  code here changed for PR④). Restore consumed it in Train 2 (`animusd::
  backup_restore`, `encode_restored_value`); **Train 3 (ADR 0059 §9)** adds
  `pitr_prefix`/`pitr_segment_object_id` — the PITR sealing consumer's own
  object namespace (`backup/pitr/...`), sharing `segment.rs`'s codec
  (`segment::new_header`/`encode`/`decode_and_slice`) rather than this
  module's own data-chunk codec, since a PITR segment IS a sealed-shard-
  shaped object over the change log, just written to the backup store
  instead of the streams `SegmentStoreHandle`.
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
  shared control-plane serde_json `PersistedState` format. **A field added
  to the shared `LogEntry`/`RaftMsg` types (`animus-control::raft`) needs an
  explicit encode/decode arm here too** — `#[serde(default)]` only protects
  the `serde_json` WAL path, not this hand-rolled one (version `22`, ADR
  0058 Train 1's `learners: Option<BTreeSet<NodeId>>` field, is the
  regression: see `docs/engineering-lessons.md`'s Code-patterns entry for
  the general lesson).
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
- **`seal.rs`** (ADR 0050 rung 5/7) — the **freeze marker**: the durable
  half of `KvCommand::Freeze` (the split-cutover terminal whole-range
  close; the zero-copy range seal this module used to serve was deleted
  with its proposer in the rung-7 sweep). A later-ordered mutating entry
  after the freeze is rejected at apply, checked against a per-group
  in-memory set rebuilt at group start from a durable **engine marker
  key** (deliberately from the engine, not log replay, since compaction
  can truncate a `Freeze` entry out of the log long before its rejection
  duty is done). The marker's key
  lives under `animus_control::syskv::RESERVED_NAMESPACE` — engine-global,
  outside every `StorageScope` — see the module's own doc for the
  key-disjointness proof.
- **`split.rs`** (ADR 0058 Train 2 rung 3) — the **in-place split fork
  marker**: the durable half of `KvCommand::SplitTablet`, mirroring
  `seal.rs`'s discipline exactly (engine-global key, survives compaction)
  but keyed by `tablet` alone (a tablet forks AT MOST ONCE, unlike a seal's
  per-range keying) and carrying a real payload — the split key, both
  children's `(id, replicas)` pairs, and the `bootstrap_voters` set
  captured once at apply from the parent's own `RaftCore::config() ∪
  RaftCore::learners()` (see the module's own doc for why this read is
  guaranteed identical across replicas). `RaftKvNode::pending_split()` is
  the one accessor the host reconciler polls every tick.
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
  amendment for the full mechanism. **`TxnWrite.stage_marker` (ADR 0049 §3,
  codec version 18)**: an image-less, consumer-hidden `(prefix, record)`
  pair `TxnStage`'s own apply arm materializes into `KIND_CHANGE` at the
  *stage* entry's own `ts` (via the same shared `materialize_derived`) —
  the dirty-key signal that lets a change-log consumer observe a freshly
  staged intent envelope. Deliberately a separate record from `change_log`
  (writing that one early would surface a pre-commit full-image event);
  never carried in the intent envelope (consumed entirely at stage);
  prefix token-validated at apply like `kind_writes`' keys
  (`stage_marker_token_valid`); an aborted transaction's marker remains as
  a harmless dirty hint. Tests: `txn_kind_writes.rs`'s `stage_marker_*`/
  `stage_writes_*` group. **`TxnWrite.change_log`'s prefix is validated
  the same way** (`change_log_token_valid`, Train A rung 4 — it was the
  one of the three wire-reachable stage payload prefixes that went
  unvalidated; `TxnResolve` completes-and-writes it wherever it points, so
  a mis-tokened prefix rejects the whole stage as `Fenced` at the stage,
  never at resolve). Test: `txn_kind_writes.rs::
  a_change_log_prefix_off_its_own_token_is_rejected_at_apply`.

### lib.rs API

`RaftKvNode<E, S>` is the running tablet-group node (start/propose/read
methods); `StorageScope` (ADR 0050 rung 2, F2b) is a thin **kind-scoping**
helper over a tablet's own private engine (a kind-byte `prefix` plus the
tablet's **immutable** declared `range` — physical keys are
`[kind] || logical`, no table or tablet bytes); `KvCommand::KindBatch`
(ADR 0041 §3) is the
multi-kind atomic batch backing materialized secondary indexes and the
change log; `KvCommand::SeedBatch` (ADR 0050 Train B rung 4, codec v19) is
the split-build driver's history-transfer command — `SeedRow`s
(`(kind, logical, value-or-tombstone, version)`) merge-applied at their
**carried** versions with the stored bytes verbatim (intent envelopes
included), emitting nothing into the child's change log and witnessing the
batch's max version into the group's HLC (`propose_seed_batch` /
`seed_rows_kind` are the driver's propose/read pair; `tests/seed_batch.rs`
+ `ANIMUS_SPLIT_SEEDS` is its corpus); `KvCommand::Freeze` (ADR 0050 rung
5, codec v20) is the split-cutover freeze — a whole-range entry of the
existing sealed-set discipline whose durable seal marker re-latches
`is_frozen()` at group start, refusing every later-ordered USER mutation
(base/LSI — a consumer-bookkeeping `KindBatch` still applies) while reads
keep serving; `propose_freeze` is idempotent and `tests/freeze.rs` is its
suite; **`KvCommand::SplitTablet`** (ADR 0058 Train 2 rung 3, codec v23) is
the **in-place split's** single-entry atomic fork — reuses `Freeze`'s exact
whole-range seal/`frozen` discipline for the ordering fence (the two
workflows share the flag-selected `frozen` latch, mutually exclusive per
tablet in production) and additionally writes the durable fork payload
(`split.rs`) every fork participant's `pending_split()` reads back;
`propose_split_tablet` is idempotent and `tests/split_tablet.rs` is its
suite (fence + idempotency + restart survival; a plain `Freeze` is proven
to carry no fork payload, so the shared latch never confuses the two).
**This crate's own apply does NOT bootstrap the two children** — that is
the host reconciler's job (below), discovering the fork via
`pending_split()` the same way it discovers every other per-tablet fact;
**`add_learner`/`promote_learner`/`remove_learner`/`learners`/
`learner_caught_up`** (ADR 0058 Train 1) are thin wrappers over the shared
`RaftCore`'s identically-named methods, mirroring `change_membership`'s own
lock/record-metrics/wake-on-propose shape exactly — see
`animus-control/CLAUDE.md`'s "Learner (non-voting) membership class" entry
for the full mechanism (shared by both planes; the wrapper methods
themselves add no learner-specific logic beyond that mirroring —
**`reconfigure_step` is the one place in this crate that does**, see the
next paragraph). `tests/
learner_membership.rs` in this crate is the integration-level half of the
"Stage C audit note" discipline (a shared primitive exercised at both the
`animus-control` core level and here); the fault-injection corpus
(`ANIMUS_LEARNER_SEEDS`) lives in `animus-control/tests/learner_corpus.rs`
since the property under test (quorum math, election gating) is
plane-agnostic; **transactions** (ADR 0018 §2) are covered in Key invariants
below. See the crate's rustdoc for the full method/accessor inventory.

**`reconfigure_step`'s learner-phased replica-move sequencing (ADR 0058
Train 1's reconciler adoption)**: adding a replica no longer proposes it
straight into the voter set. `reconfigure_step` sequences an add as
**add-learner → (wait for `learner_caught_up` against the fixed
`RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` = 4 log entries) → promote →
remove-the-old-replica**, still exactly **one single-server step per call**
(ADR 0031 discipline unchanged — no new `HostAction`, `host::plan` is
untouched; only what `reconfigure_step` proposes on a given call changed).
Full priority order, most urgent first: (1) remove a `Down` extra **voter**
(unchanged, failure repair); (2) drop a current **learner** no longer in
`desired` — regardless of its liveness or catch-up progress, since it is
stale by construction the moment placement retargets away from it (the
fix for "a learner mid-catch-up that dies or is decommissioned must not
wedge every later step" — the reconciler's job is only to not block on a
target nobody wants any more; *re*-targeting `desired` is placement's job,
untouched); (3) promote a learner that is both still desired and caught up
(finish an in-flight move before starting a new one); (4) add a `desired`
member missing from both `config` and `learners`, as a **learner**, never
straight to voter; (5)/(6) — once every `desired` member is already a
voter — the pre-Train-1 remove-healthy-extra/leader-self-removal-via-
transfer steps, unchanged. A remove-only delta and a brand-new group's
initial bootstrap (`host::plan_join_host`) are both untouched — this only
changes the sequencing of an *add*. **Gotcha this shipped with**: the early
"already converged" return must check `current == desired &&
learners.is_empty()`, not `current == desired` alone — a stray learner at
that point is stale by construction (see step 2), and an early return
before it fires would wedge the exact case step 2 exists to unwedge.
**Test-authoring gotcha** (found writing this rung's own corpus): under
`SimEnv`'s near-zero message latency, a learner on a genuinely short log
can satisfy `learner_caught_up`'s threshold and get promoted within the
very next `reconfigure_step` call even with a real network partition or
zero real replication — the absolute-gap threshold has no way to
distinguish "caught up" from "the log itself is short." A test meaning to
catch a newcomer "still mid-catch-up" must either grow the log well past
the threshold first (so a genuinely-unreplicated learner's gap stays
provably large — `tests/learner_reconfigure.rs`'s and
`tests/reconciler_corpus.rs`'s learner scenarios do this), or check
immediately after the single tick that adds the learner rather than after
several ticks (promotion cannot happen in the same call as the add). No
`Metadata`/tablet-map representation change was needed for any of this:
`Tablet::replicas` stays the *target* voter set placement wants, unchanged
in shape — the learner bookkeeping already lives entirely in each tablet's
own `RaftCore` state, replicated to every replica (voter and learner alike)
via the group's own log since PR #383, which is exactly the state
`reconfigure_step` already had local access to. `admin::CpRaftView`
(`animusd`) gained a `learners` field purely for `/admin/raftkv`
observability of this — read-only, drives nothing. Tests: `tests/
learner_reconfigure.rs` (unit-level, including the structural regression
this rung exists to close —
`old_quorum_survives_an_old_voter_loss_while_the_new_replica_is_still_a_learner`)
and `tests/reconciler_corpus.rs`'s `learner_move_survives_partition_
during_catchup`/`learner_move_survives_leader_change_mid_move`/
`learner_crash_is_replaced_by_a_new_target` scenarios (the full
`Reconciler`/`MetadataView`-driven path); `animusd/tests/
learner_reconfigure.rs` is the real multi-process `ProdEnv` exercise.
**Eventually-consistent reads (ADR 0055)** are the second read path this
crate serves, and the one whose budget is easiest to destroy by accident:
`stale_read_ready()` (the gate), `stale_get_served()` (outer `None` =
"not served", never absence — the `linearizable_get_served` discipline),
`stale_scan`/`stale_scan_rev`, and — for a non-base kind scope, which only
ever holds committed values — plain `local_scan_kind`/`local_scan_kind_rev`.
Three things about them that a doc comment cannot enforce:

- **Nothing on this path may block, propose, round-trip, or `wake()`.** No
  read barrier, no `ensure_ceiling_above`, no `ts_cache` bump, no anchor
  query for an intent, no quiescence wake. Adding any of those would make
  the cheap read silently cost what a strong one costs, and **no test would
  fail** — which is why `tests/stale_read.rs` deliberately drives these with
  `block_on` rather than the `drive` helper: a stale read that grew an
  internal `env.sleep` hangs that file instead. The quiescence half of that
  claim (no wake, checked against a real quiesced 3-node group's own
  timeline/metrics rather than just structurally) is `tests/
  quiesced_eventual_read.rs`'s regression.
- **An unresolved intent reads back one MVCC version**
  (`stale_value` → `prior_committed`), never as absent. `local_get`'s raw
  peek reports it as absent — correct for its admin/debug callers, a
  fabricated deletion for a client-visible read. `stale_scan_rows` applies
  the same rule row-by-row, where `resolve_scan_rows` drops the row.
- **The gate is not a staleness bound.** It only excludes a replica whose
  engine is not yet *any* state of this tablet (no leader known yet, or a
  committed tail / snapshot image not merged). A partitioned replica passes
  it and answers arbitrarily stale data — which is the DynamoDB contract,
  not an oversight. See ADR 0055 §2.

Four rules that aren't derivable from a doc comment:

- **A group owns a scope *set*, not one scope.** `with_kind(kind)` derives a
  sibling scope per row kind (`KIND_BASE`/`KIND_LSI`/`KIND_CHANGE`/
  `KIND_FOOTPRINT`/`KIND_CURSOR`), each carrying the same immutable
  declared range (`narrow`/`widen` died with the zero-copy split, ADR
  0050). **`StorageScope::whole()` is not an identity transform** — its
  base-kind scope prefixes one `KIND_BASE` byte, so *any* group's physical
  key is `[kind] || logical` (engine-global reserved-namespace markers lead
  `0x5F` and match no kind). **Anything reading a group's bytes straight off
  the engine must
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
- **`engine_latest_version()`/`local_scan_kind_snapshot(kind, start,
  version_ceiling, limit)`** (ADR 0059 §4/§5, Train 1 PR③) are the on-demand
  backup capture driver's own read primitives (`animusd::backup_capture`, a
  later PR consumes them; `tests/backup_capture_scan.rs` proves them in
  isolation). The first is a synchronous, purely local
  `StorageEngine::latest_version()` read — the watermark a capture pins
  **once**, at a tablet's own capture start, and replays on every later
  tick (never re-derived — a wider re-pinned watermark after a leader
  change would change content at an already-`put` chunk index, breaking
  `SegmentStore::put`'s write-once contract). The second is
  `local_scan_kind`'s snapshot-pinned, resumable-cursor sibling: unlike
  `local_scan_kind` (always "latest"), every row is read **as of
  `version_ceiling`** (`StorageEngine::scan_at`, the same primitive
  `scan_at` reads a live transaction against) and resolved through the
  identical intent-resolution discipline `resolve_scan_rows` already gives
  every ordinary scan (a still-`Pending` intent silently omitted, never its
  raw envelope — including dropping `txn::is_record_key` marker rows, the
  one thing this primitive's own first draft missed, see
  `docs/engineering-lessons.md`'s entry on it) — so a capture spanning many
  ticks, and across a leader change many different replicas, always
  resolves the identical row set. Cost model matches `local_scan_kind`/
  `animusd`'s TTL-reaper `local_scan_kind_capped`: `limit` bounds returned
  rows, not engine I/O (a documented follow-up, not a correctness gap).

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
    install, and group start (off the tablet's own engine's
    `latest_version()`, which alone covers a restart's already-present
    data).
  - **The freeze** (`seal.rs`, `KvCommand::Freeze`) closes the one residual
    witnessing alone cannot: an in-flight write from the parent's own
    leader, still in its commit pipeline when the split cutover happens. The
    split-build driver proposes `Freeze` through the parent's own log; apply
    rejects any later-ordered mutating entry, regardless of that entry's own
    `ts`, because within one group log order and HLC order coincide — the
    **log position** is authoritative. (The zero-copy split's range-scoped
    seal, its `ProposeSeal` reconciler action, and the `parent_seal_observed`
    host gate were deleted in the rung-7 sweep — a copy-based child never
    shares rows with its parent, so there is no handoff to seal.)
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
  against the KIND_BASE scope, **checked BEFORE the seal gate**, not behind
  it — `TxnStage`'s `condition_failure` only evaluates once already known
  unsealed so `StageOutcome` can report the seal reason ahead of a
  condition one; `KindBatch` had no outcome channel of its own at
  introduction to prioritize this way, so this ordering difference predates
  (and survives) the outcome channel described next. The two gates still
  simply AND together either way. Production caller:
  `dynamo::kind_write_item_at_leader`'s leader-side evaluate-then-propose
  write path (ADR 0046 U3) passes `seatbelt: vec![(base_key, raw_old)]` —
  the guard against a concurrent `TxnStage`/`TxnResolve` commit landing
  between that evaluator's own-key read and its own propose call. Tests:
  `tests/kind_batch_conditions.rs`, mirroring `tests/txn_conditions.rs`
  scenario-for-scenario.
- **`KindBatch` later gained its own `StageOutcome` analogue —
  `KindBatchOutcome`/`KindBatchOutcomes`, recorded per apply and keyed by
  Raft log index (plus, since a PR #334 review fix, the entry's own
  **term** — see below).** A proposer can now tell "my entry no-op'd"
  (`ConditionFailed`/`Sealed`) from "applied" without falling back to a bare
  value-equality read, mirroring `Cas`'s `CasResults`/`TxnStage`'s
  `StageOutcomes`. **Bounded, unlike those two** (a `KindBatch` proposes for
  *every* indexed or streamed write, not just a CAS or a transaction, so an
  unpruned map would grow without limit) — an aged-out entry falls back to
  the value probe, the pre-existing behavior before this channel existed.
  **`Applied` alone is not a confirm of success — the identical
  "index means no-op-vs-failure, never success" discipline `StageOutcome`'s
  own doc states, and the coordinator-side `txn_verify_staged` enforces for
  `TxnStage`.** `KindBatchOutcome` reused `StageOutcome`'s index-keyed shape
  without initially reusing that verification discipline: `animusd::
  poll_probe` used to treat `Some(KindBatchOutcome::Applied)` alone as a
  confirm, which is unsound the instant the proposer's own entry was
  *accepted* (appended locally — `ProposeResult::Accepted`) but never
  *committed* — a leadership change can truncate it and let a different
  command's apply record `Applied` at the identical index. The closed fix
  pairs the outcome with the entry's own Raft term (`ProposeResult::
  Accepted{index, term}`, `KindBatchOutcomes` storing `(term, outcome)`) and
  requires `term == accepted_term` before ever trusting `Applied` — sound by
  Raft's log-matching property (index **and** term together imply an
  identical entry, cluster-wide). `animusd::classify_kind_batch_outcome` is
  the confirm-side predicate that enforces this; see `docs/engineering-
  lessons.md` for the full incident and
  `tests/kind_batch_outcome_identity.rs` for the seed-reproducible
  truncation regression.
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
    **Gap found, not yet fixed (issue #298, 2026-08-26)**: this guard checks
    only for a *different* txn's unresolved `Intent` — it never checks
    whether the current value is already `Envelope::Committed`. A stale or
    duplicate `TxnStage` propose landing after its own transaction has
    already fully resolved is therefore never rejected: it silently
    resurrects the key from `Committed` back into `Intent`, and a later
    resolve can then re-materialize its derived change-log record a second
    time at a fresh HLC. Identified by reading, not yet caught firing live
    in a captured repro — see `docs/engineering-lessons.md`'s matching entry
    and ADR 0058's G5 row.

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

**ADR 0050 Train B rung 1 — per-tablet engines.** The reconciler no longer
receives one shared engine: it opens **one private engine per hosted
tablet** through the `host::EngineFactory<S>` seam (`open`/`probe`/
`destroy`; `animusd` maps a tablet id to an LSM filename prefix
`db-t{tablet}-`, sim/tests use `host::MemoryTabletEngines`' registry).
Consequences to keep in mind here: `Release`/`Reclaim` teardown both
**delete the tablet's engine files whole** (a private engine holds no
sibling's rows to spare, so no erase bound exists); `has_data` is a
two-step `probe`-then-scan against the tablet's own engine. The zero-copy
split lifecycle (`NarrowScope`/`ProposeSeal`/`parent_seal_observed`, the
`erase_bound` field, and their corpus scenarios) was **deleted** in the
ADR 0050 Train B rung-7 sweep.

- **`plan` never removes a tablet from `LocalState::hosted` on its own**
  when emitting a fallible teardown (`Reclaim`/`Release`) — real teardown
  is async and can time out. The caller calls
  `LocalState::confirm_torn_down` once its own teardown actually
  completes; until then the next `plan` re-plans the same action. The
  identical discipline runs in reverse for `Host`: `plan` inserts the
  claim into `LocalState::hosted` optimistically, before any live handle
  exists, so `Reconciler::host` must call `LocalState::
  release_unconfirmed_host` when it skips the action (an
  `EngineFactory::open` I/O failure, or the tablet vanishing from
  `Metadata` before execution) — otherwise the claim is permanent and
  `plan` never re-emits `Host` for a tablet this node in fact never
  hosted (a silent, permanent RF degradation with no operator signal;
  fixed as part of the same change that closed `teardown`'s mirror hole,
  below). Regression: `tests/reconciler.rs::
  reconciler_recovers_a_tablet_after_a_transient_engine_open_failure`.
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

### In-place split (ADR 0058 Train 2 rung 3)

`plan`'s phase 1.5, between `Host` and `Reconfigure`: a tablet whose
`Metadata` row carries `Tablet::inplace_split` (the control plane's
`MetaCommand::BeginSplitInPlace`) takes this branch INSTEAD of the
ordinary `Reconfigure` action for as long as the intent exists — the two
must never both fire for the same tablet in the same tick (an ordinary
reconfigure would see the split's added learner(s) as stale — not in its
own `desired` — and try to remove them mid-catch-up).

- **Not yet forked here** (`RaftKvNode::pending_split()` answers `None`):
  the leader adds the next missing member of the union of both children's
  `replicas` as a learner (`HostAction::AddSplitLearner`, one per tick,
  mirroring `reconfigure_step`'s own one-step-per-call discipline but as
  its own action — this does NOT reuse `reconfigure_step` itself, since
  that function's own stale-learner-removal step would fight a split's
  learners the moment they're not in the PARENT's own `desired`). Once
  every union member is present and caught up (a voter is trivially
  ready), the leader proposes the fork (`HostAction::ProposeSplitFork` →
  `RaftKvNode::propose_split_tablet`).
- **A node named only in a CHILD's `replicas`, never the parent's own**,
  still needs to host the PARENT (as a quiet non-voter) before it can ever
  be added as a learner to it — phase 1 gained a second host-candidate
  test for exactly this (a node recruited via either child's `replicas` of
  an in-place split intent), and phase 3's release check is correspondingly
  taught to never fire on the SAME recruited set (it is never in the
  parent's own `replicas`, and — a learner is never in `RaftCore::config()`
  by construction — `config_excludes_me` reads trivially true for it too).
- **Already forked here** (`pending_split()` answers `Some`):
  `HostAction::MaterializeSplitChild` fires for BOTH children on EVERY
  fork participant — not filtered by either child's own final `replicas` —
  since `pending_split().bootstrap_voters` (the parent's full
  voter-plus-learner set at the fork, captured once in the data-plane's
  own apply) is what both children actually bootstrap with, a deliberate
  superset Stage 5's ordinary `Reconfigure` trims down afterward. Claimed
  into `LocalState::hosted` optimistically (the same discipline `Host`
  uses) AND into `LocalState::split_forming`, which exempts a pre-cutover
  child from phase 3's reclaim check (it is `hosted` but, by design,
  absent from `Metadata` until `CutoverSplit` runs) — pruned the instant
  the child appears in `Metadata` as a real `Active` entry.
- **`Reconciler::materialize_split_child`** implements the G4
  crash-idempotency contract (see the ADR's own "Open forks" table,
  decided as of this rung): `EngineFactory::probe(child)` before ever
  cloning (skip re-clone if an earlier attempt already committed the
  engine but crashed before the group started), `EngineFactory::
  clone_engine` (over the caller's OWN already-open parent handle — never
  a fresh re-open of the same on-disk prefix, which for `LsmEngine` would
  be a real corruption hazard: two independent in-process engines
  contending over one WAL/manifest with no coordination), then
  `trim_split_child` (delete the SIBLING's own range from
  BASE/LSI/FOOTPRINT, and the WHOLE `KIND_CHANGE`/`KIND_CURSOR` scopes
  unconditionally — ADR 0050's copy-kinds rule, reused verbatim), then
  either `RaftKvNode::start_hosted` or `start_hosted_campaigning` with
  `bootstrap_voters`, selected by `HostAction::MaterializeSplitChild.
  campaign` — see "Deterministic first leader" below.
- **`EngineFactory::clone_engine(&self, source: &S, target: TabletId)`**
  takes the source's own already-open handle, not a bare `TabletId` — see
  its own doc for why re-opening would be unsafe.

#### Deterministic first leader at the fork (ADR 0058 Train 2 rung 4)

The rung-3 bench found a real regression the ADR's own "near-zero, roughly
one routing refresh" framing didn't anticipate: a freshly-forked child
group has no leader until *some* replica's cold, randomized election
timeout fires, and `animusd::cp_route`'s election-wait branch parks a
write meanwhile — measured at ~726ms median (vs. the copy-based path's
~300ms), with **zero** retries needed (a single slow request, not a
refuse-and-retry blip). The fix: the replica that was the parent's own
Raft leader **at the moment it materializes a child** campaigns for that
child's leadership immediately, instead of waiting out the timeout.

- **The decision is made once, in `plan`, purely from already-gathered
  local facts** — `HostAction::MaterializeSplitChild` gained a `campaign:
  bool` field, set to that tick's `TabletFacts::is_leader` **for the
  PARENT** (the same fact phase 1.5 already reads to gate
  `AddSplitLearner`/`ProposeSplitFork`). No new coordination, no new
  replicated state: every replica decides "was I the parent's leader just
  now" independently, and in the common case exactly one replica per
  child answers `true`.
- **`RaftKvNode::start_hosted_campaigning`** (`lib.rs`) is
  `start_hosted`'s sibling: identical bootstrap, except the driver
  (`drive`, on a genuine first formation only — `state.is_empty()`, never
  a restart) calls the new `RaftCore::campaign_now(now, entropy)` once,
  before ever entering its `select` loop, instead of waiting for `tick`'s
  own `election_deadline` to pass. `campaign_now` is a thin, safety-net-
  guarded wrapper: a no-op unless the core is a voting `Follower` (never
  demotes a sitting leader, never fires twice), and otherwise runs
  **exactly the pre-vote round `tick` would run on timeout** — no raw,
  term-incrementing `start_election`. This is what makes the mechanism
  safe with zero new machinery:
  - **Pre-vote's own lease check is the entire "don't disrupt a peer that
    hasn't started yet" story.** A peer whose own `RaftKvNode::
    start_hosted[_campaigning]` for this child hasn't run yet has no
    listener on the child's stream at all — its `PreVote` sits queued in
    the `Env`'s per-`(node, stream)` inbox (ADR 0026 queues by
    destination regardless of whether a consumer is polling — true of
    both `ProdEnv`'s per-stream `Demux` and `SimEnv`'s inbox map) until
    that peer's own bootstrap reaches its first `recv_stream` call, at
    which point the queued `PreVote` is simply its very first message.
    Since the peer starts as a genuine `Follower` with no leader belief,
    it grants — no different from a real timeout's own first round.
  - **A round that gets no majority in time re-arms the ordinary election
    timer and falls back to the untouched randomized-timeout path** —
    `start_pre_vote` (which `campaign_now` calls) always calls
    `reset_election_timer` regardless of outcome. Two replicas racing to
    self-nominate the same child (a leadership change mid-fork), or the
    parent's leader crashing exactly at the fork so nobody's tick
    observes `is_leader: true` for any child at all, both degrade to
    exactly the pre-existing cold-start election — no special-cased
    recovery path exists or is needed.
  - **A learner never campaigns, and the safety belt is structural, not
    conventional.** The self-nominating replica is a voter of the PARENT
    by construction (`start_election`/`become_leader` gate on
    `is_voter()`), and every child's `bootstrap_voters` is the parent's
    own full voter-**and**-learner union at the fork — a strict superset
    — so it is always a voter of the child too; there are, in fact, no
    learners at all on a freshly-bootstrapped child (every
    `bootstrap_voters` member starts as a voter). `drive` additionally
    `assert!`s `core.config().contains(&self_id)` immediately before
    calling `campaign_now`, as a second, structural line of defense
    against the upstream wiring ever computing `campaign` for the wrong
    replica — proven to actually fire via `RaftKvNode::
    start_hosted_campaigning_panics_if_the_caller_is_not_a_voter_of_the_
    group` (`lib.rs`'s own `campaign_now_tests`), and
    `RaftCore::campaign_now`'s own no-op-on-a-non-voter/learner behavior
    is unit-tested directly in `animus-control/tests/
    learner_membership.rs`.
- **Quorum/term math is completely unaffected** — this changes only *who
  starts the first election, and when*, never what it takes to win one.

Tests: `tests/split_tablet.rs` (the data-plane mint's own fence/idempotency/
restart suite) and `tests/inplace_split_reconciler.rs` (a self-contained
`SimEnv` corpus, depth knob `ANIMUS_INPLACE_SPLIT_SEEDS`, mirroring
`reconciler_corpus.rs`'s own harness shape — held green through
`ANIMUS_INPLACE_SPLIT_SEEDS=200`): the full happy path (real learner
catch-up, over-replication on every fork participant, exact per-child data
partitioning, empty change/cursor scopes at birth, the post-cutover trim to
final placement, parent reclaim), a leader crash mid-catch-up, the G4
crash window itself, a concurrent unrelated rebalance racing the split's
own learner-add, the immediate-campaign **fast path**
(`campaigning_replica_wins_leadership_almost_immediately` — a leader
within a handful of virtual ms of materializing, far short of a fresh
group's own 150ms election-timeout base) and its **fallback**
(`parent_leader_crash_at_fork_falls_back_to_ordinary_election` — the
parent's leader crashes at the exact instant of the fork, before any
replica's own materialize action ever campaigns, and both children still
elect via the untouched randomized-timeout path). **Residue, explicitly
not part of rung 3** (see the ADR's own as-built note): the `animusd`-
level driver that watches a forked-locally parent, runs the (unmodified)
GSI-drain/backfill vetoes against it, and proposes `CutoverSplit`; the
`--split-mode` operator flag; a real multi-node `ProdEnv` end-to-end
regression (both landed in a later rung, see `animusd/CLAUDE.md`).

#### Eager child materialization at the fork (ADR 0058 Train 2 rung 4 layer 1)

The rung-4 measurement addendum found a SECOND residual on top of the
deterministic-first-leader fix immediately above: the campaigning replica
supplies only ONE vote instantly — a fresh 3-node child still needs a
SECOND voter to grant a pre-vote before it can elect, and that voter's own
`materialize_split_child` used to run only on its next *scheduled*
tablet-host-reconciler tick (even at rung 3's fast-polled
`INPLACE_SPLIT_RECONCILE_INTERVAL`, 50ms). The fix: **every replica
triggers its own materialization the instant it applies `SplitTablet`
locally**, on every hosted tablet, not only the campaigning one.

- **The trigger moved; the mechanism did not** (the same discipline PR
  #394's own campaign fix followed, and the general lesson
  `docs/engineering-lessons.md` already names): `Reconciler::
  materialize_split_child`'s clone/trim/host logic and its G4
  crash-idempotency contract (above) are byte-for-byte unchanged. What's
  new is purely a WAKE that makes the reconciler's own tick fire sooner.
- **`ForkSignal`** (`lib.rs`, private): the same executor-agnostic
  `AtomicBool` + `AtomicWaker` shape as this crate's existing
  `ProposeSignal`/`ApplySignal`/`WakeSignal` — one per `RaftKvNode`,
  raised exactly once by the **async apply task** (`apply_and_compact`'s
  `KvCommand::SplitTablet` arm), immediately after the durable split
  marker (`split::split_marker_key`) commits. **Never raised from the sync,
  I/O-free `RaftCore`** (ADR 0003/0038 discipline: apply is sync and
  I/O-free; this notify is a plain in-memory flag+wake, no I/O of its own,
  called from the async driver-side apply exactly like every other signal
  in this file) — this is a wake, not an inline call into the
  materialization path itself, which stays fully async and reachable only
  through the ordinary reconciler tick.
- **`RaftKvNode::fork_wake(&self) -> ForkPending<'_>`** (`pub(crate)`) and
  **`host::Reconciler::fork_wake(&self)`** (`pub`, the fan-in used outside
  this crate): the latter resolves as soon as ANY currently-hosted
  tablet's own signal fires (`futures::future::select_all` over each
  hosted node's `fork_wake()`, rebuilt fresh every call — cheap, since
  each is a plain `Arc`-backed atomic + waker — so it automatically tracks
  this node's *current* hosted set) and never resolves on its own when
  `hosted` is empty (`std::future::pending`), leaving a caller's other
  `select!` arms to cover that case. `animusd::
  tablet_host_reconciler_loop` races this as a third arm alongside
  `metadata_watch`/the periodic fallback — see that function's own doc.
- **Deliberately NOT durable, and recovery does not depend on it.** A
  crash between the apply task raising the signal and any tick consuming
  it simply loses it — on restart there is no signal left at all (a fresh
  `RaftKvNode` starts with a fresh, unraised `ForkSignal`), and WAL replay
  of the already-applied `SplitTablet` entry never re-raises it either
  (the `if !frozen` idempotency guard around the whole apply arm skips the
  block on replay, exactly like `Freeze`'s own). This is safe **by
  construction**, not by luck: the signal only ever shortcuts discovery of
  a fact (`pending_split()`) that is independently durable and that the
  reconciler's ordinary periodic tick already re-derives on every pass
  regardless of whether any wake ever fired — proven directly by `tests/
  inplace_split_reconciler.rs`'s `crash_after_apply_loses_the_eager_wake_
  but_reconciler_fallback_recovers` scenario.
- **The eager attempt and a later reconciler tick may race benignly.**
  Nothing prevents `fork_wake()` firing a tick that materializes both
  children, immediately followed by the reconciler's own next periodic
  tick re-observing the identical already-forked state — this is exactly
  the existing G4 double-attempt discipline (`EngineFactory::probe` skips
  a re-clone; the optimistic `LocalState::hosted` claim skips a re-host),
  now exercised by a second, genuinely independent caller instead of only
  by a crash-retry. Proven directly by `tests/inplace_split_reconciler.rs`'s
  `eager_wake_and_reconciler_tick_race_benignly` scenario: `fork_wake()`
  resolves with zero prior ticks, the first tick after it materializes
  both children, and a second, back-to-back tick changes nothing — same
  hosted set, same two engines, byte-for-byte.

**Measured effect** (ADR 0058's own rung-4-layer-1 measurement addendum):
median write blip drops from 508.0ms (rung 4, campaign only) to 355.7ms —
landing at or below a same-session copy-based reference run (447.9ms) for
the first time, though with real run-to-run variance the addendum reports
honestly rather than smoothing over. See the ADR for the full before/after
table.

### HostAction

**Emitted in this fixed order: `Host` → (`AddSplitLearner`/
`ProposeSplitFork`/`MaterializeSplitChild`) → `Reconfigure` →
`Release`/`Reclaim`.** The parenthesized trio (ADR 0058 Train 2 rung 3) is
mutually exclusive with `Reconfigure` per tablet — see "In-place split"
above. `Release`/`Reclaim` tear down a tablet moved off or
dropped/retired, respectively. Tablets are split-only (ADR 0044) and
ranges immutable (ADR 0050): the zero-copy `ProposeSeal`/`NarrowScope`
actions were deleted in the rung-7 sweep, as merge's `WidenScope`/`Absorb`
were by ADR 0044 — a hosted-but-now-absent tablet is unconditionally
`Reclaim`ed; its two causes (dropped table, cutover-retired split parent)
demand the identical action, so no disambiguation is needed.

- **`Reconciler` teardown** (`Release`/`Reclaim`): unregister from routing
  *before* touching the driver, `shutdown()`, poll `is_stopped()` bounded
  by `RECLAIM_STOP_TIMEOUT` (10s), re-register and leave `LocalState`
  untouched on timeout (so `plan` re-emits the same action next tick), else
  delete the tablet's engine files + WAL, and only then
  `confirm_torn_down`. (Merge's `Absorb` teardown — which
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
    loop: start a persist round if the core owes the WAL anything and none is in
    flight → `select(persist-round, propose-wake, driver-wake, recv, timer)` →
    step the core → send. It does **no** engine apply, so it always
    heartbeats/acks within the election timeout.

    **The `fsync` is raced inside that `select`, not awaited before it (issue
    #279, ADR 0017's 2026-08-18 amendment).** It used to be `persist_wal` →
    `select` → step → `persist_wal` → send, with both persists awaited inline —
    which livelocked a group whenever an `fsync` outlasted the 150 ms
    `election_base` (blocked loop → no heartbeats, no election-deadline re-arm →
    followers campaign → each leadership change's no-op commit makes more
    persist work → repeat). Now only the messages that make a **durability
    claim** wait: `RequestVoteResp{granted}`, `AppendEntriesResp{success}`,
    `RequestVote` (a candidate counts its own vote) and `InstallSnapshotResp` are
    buffered against their persist round; `AppendEntries`/heartbeats, pre-vote
    traffic, `InstallSnapshot` chunks, `TimeoutNow`/`Quiesce`/`WakeRequest` and
    `ReadProbe`(`Ack`) ship at once. **`animus_control::persist_round` owns the
    accounting, shared with the control plane's own driver** (this crate keeps
    only a three-line `ships_before_durable` wrapper for `KvWire`'s
    non-consensus variants) — read its module doc before touching any of this,
    especially the "Two layers" section: the WAL has **two** drainers (this loop and the apply task's
    compaction rewrite), the interleaving that bit the two reverted fix attempts
    is a microsecond window no wall-clock test can hit, and the defect is closed
    structurally instead (one shared `drain_for_round`, plus a `fully_durable`
    release that needs no round number to be right). **`persist_wal` is
    halted-gated** (issue #278 item 1, mirroring the apply task's `env.replace`
    compaction-error handling immediately below): an `env.append`/`env.sync`
    error is tolerated — no `mark_durable_through`, no `apply_signal` notify,
    the driver's own top-of-loop `halted` check (woken by `shutdown()`'s
    `wake_signal.notify()`) exits the loop on its next pass — **iff** `halted`
    is already set (a `shutdown()` racing a still-pending append/sync, or a
    test's `TempDir` deleting the WAL out from under a still-running loop);
    while running, the identical error stays a hard panic (a live leader's WAL
    fault is a genuine durability fault — crash-stop-before-ack). Regression:
    `tests/shutdown.rs::a_halted_nodes_pending_write_tolerates_a_wal_fault_
    with_no_panic` (a `DiskConfig` fault + a `put`-then-`shutdown()` synchronous
    beat, deterministically racing the two).
  - **Apply task** (`apply_loop` → `apply_and_compact`): install received
    snapshots, `drain_apply` → `merge`/`merge_tombstone` in commit order, and
    compact — all off the consensus loop. **`flush_pending`'s `merge_batch`
    call is halted-gated too** (issue #278 item 1 follow-up, the identical
    idiom): `apply_and_compact`'s effects loop calls it up to ten times per
    pass (the `Cas`/`Freeze`/`ReadCeiling`/conditioned-`KindBatch`
    ordering-hygiene drains, plus the trailing flush) with no re-check of
    `halted` between them, so a `shutdown()` racing an in-flight `merge_batch`
    mid-pass is the same class of teardown-artifact error as `persist_wal`'s
    — tolerated iff `halted`, a hard panic otherwise (a live apply failure can
    silently leave the engine short a committed write, so this stays loud).
    No dedicated regression: unlike `persist_wal`'s pending-write queue (a
    bare synchronous `core` write bypassing the driver loop's own check
    entirely, so a `put`-then-`shutdown()` beat reaches it deterministically),
    `apply_and_compact`'s work source (`drain_apply`) only becomes non-empty
    through the *apply task's own prior progress*, and its effects loop —
    once entered, after that same iteration's own `halted` check already
    passed — runs uninterrupted to completion under `SimEnv` (disk ops
    resolve without yielding), so there is no reachable window for an
    external test driver to inject `halted` between the check and this
    call the way `persist_wal`'s regression does. Covered structurally by
    the identical, already-proven idiom instead. When idle it races a new
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
    That discard is also a **persist round** now (issue #279): it goes through
    `persist_round::drain_for_round` like the loop's own drain and completes the
    round once its `env.replace` lands, or the acks the loop buffered against
    those records would wait on a round with no drainer.
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
  `end: None` branch (used by `/admin/raftkv`'s `raft_view`, by teardown, and
  transparently by `linearizable_scan` — the real DynamoDB `Scan`
  full-table path) derives a bounded upper bound from `physical_bounds`
  instead — post-F2b always finite for a kind scope (`[kind] .. [kind+1]`).
  The engine is the tablet's own now (ADR 0050), so the historical
  O(hosted tablets × node engine) blow-up can't recur, but the bounded
  idiom stays: `entries()` would still walk sibling kinds and the
  reserved-namespace markers. `entries()` remains the fallback only for the
  un-prefixed parent scope (no finite bound).
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
  - **Vetoes (fork D)**: `RaftKvNode::set_quiesce_veto(held, fresh_through)`
    lets an external subsystem (`animusd`'s `change_consumer_loop`, for a led
    tablet whose change log was non-empty on its last sweep) hold the group
    awake. **`fresh_through` is not optional bookkeeping** (issue #302): a
    bare boolean is only as fresh as the sweeper's own 200ms tick, so a write
    landing between one sweep and the next left a stale `false` behind and a
    group could quiesce still owing stream work. The caller passes the
    `engine_applied_index()` it read **before** the scan that decided `held`,
    and `quiesce_entry_ok` additionally requires `fresh_through >=
    commit_index` — so a group cannot quiesce until a sweep has actually
    observed it since the last commit. Reading the index *after* the scan
    would be symmetrically unsound (a write committing in between would be
    absent from the scan yet counted as observed), and a wall-clock stamp
    compared against `last_activity` would be unsound too, since that marker
    is bumped at *propose* time while the sweep observes *applied* content.
    The default is `u64::MAX` — a true "never engaged, no constraint"
    sentinel, so tablets the sweeper structurally never visits (`Building`
    split children, hidden GSI-table tablets) keep quiescing exactly as
    before. ORed, once per consensus-loop iteration, with this crate's own
    in-memory check that `TxnTracker` (`pending`/`unresolved_decided`) is
    empty — and (issue #279) with the loop's own in-flight persist round or
    undelivered gated acks, since quiescence drops the timer arm entirely and
    would otherwise leave a round completion as the only wake source for a
    message a peer is waiting on — a group with a live 2PC intent or an undelivered resolve can
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

`cargo test -p animus-cp-data`. All but two of the 28 test binaries drive
`SimEnv` — use `run_for`/`run_until`, never `run()` (the driver has perpetual
heartbeat/election timers). Linearizable reads are async (a read-barrier probe
round), so drive them as spawned tasks + `run_for`, and never `block_on` a
`tick()` whose planned action tears a group down (`Reconciler::teardown` polls
`env.sleep()` internally). Two exceptions are real-thread `ProdEnv` tests, deliberately, because what they
cover is unreachable under `SimEnv`'s single-threaded scheduler:
`prod_concurrent_ts_monotonic.rs` (below) and `prod_compaction_persist_round.rs`
(issue #279 — the consensus loop's buffered acks while compaction competes for
the WAL; its module doc is explicit about the one thing it cannot force).
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
guards. See the test file for the 19 frozen scenarios and the generic
invariant checks (hosting convergence, data safety, no zombie groups,
idempotence) — two merge-lifecycle scenarios (the absorb-drain regression
and its livelock-fix twin) were removed along with the reconciler actions
they exercised (ADR 0044, tablets are split-only); three ADR 0058 Train 1
scenarios (`learner_move_survives_partition_during_catchup`/
`learner_move_survives_leader_change_mid_move`/
`learner_crash_is_replaced_by_a_new_target`) were added for the
reconciler-adoption rung's own fault-injection coverage — see this file's
`reconfigure_step` entry above.

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
