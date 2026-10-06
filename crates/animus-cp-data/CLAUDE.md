# CLAUDE.md — animus-cp-data

This file provides guidance to Claude Code (claude.ai/code) when working in this
crate.

> **Test layout (2026-10-02).** All SimEnv/pure integration tests are modules of one binary, `tests/it/main.rs` (`cargo test -p animus-cp-data --test it <file>::`); only the real-thread `prod_*` tests (`prod-heavy`) and three tests that read `SimEnv` counters stay separate `tests/*.rs` targets (`departing_removal_notice`, `reconciler`, `reconciler_stop_timing`: `SimEnv::metrics()` is the **process-wide shared no-op sink**, so a before/after counter delta races with any other test in the same process incrementing that metric — it failed `departing_removal_notice` when merged; give a test its own `MetricsHandle::recording()` via `start_with_metrics` instead of reading `env.metrics()`). Older sections below that cite `tests/<name>.rs` mean `tests/it/<name>.rs`.

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
  a hot path). **ADR 0073 Phase 0 (E, layer 3):** the `BKMF`/`BKDT` + `u8`
  envelopes are the formats' version tags (both baseline `1`, the JSON body
  carries no `"v"`); `BackupCodecError` is the shared
  `animus_control::format::FormatError` (no magic → `PreBaselineFormat`,
  version 0/future → `UnsupportedFormatVersion`, bad body/framing →
  `Malformed`); golden fixtures `tests/fixtures/formats/backup-{manifest,
  data}/v1.bin` are pinned by `tests/backup_format_fixtures.rs` — never edit
  one, add a new version + fixture. **Consumed since Train 1 PR③** by `animusd`'s capture driver
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
  `CLAUDE.md`). **ADR 0073 section 8: class G, every `SegmentWire` variant is
  `Gate::Base`** (`SegmentWire::required_gate`, exhaustive, no `_` arm; `encode`
  debug-asserts it and `gate_tests` pins each variant's JSON). A new variant or
  field is a wedge for an older replica: it must name a non-`Base` gate, which
  then needs a gated send path (this store has no `ClusterFeatures` yet) before
  the assertion is relaxed.
- **`codec.rs`** — the crate's compact binary wire/image codec (ADR 0017
  A.2): length-prefixed, magic/version-checked framing for `KvWire`
  messages and engine images (`serde_json`'s decimal-array `Vec<u8>`
  rendering cost ~3–4x). Decode failures are loud, named `FormatError`s
  (see "Versioned formats" below; the codec is `VERSION = 1` since ADR 0073
  Phase 0). The Raft WAL keeps the
  shared control-plane serde_json `PersistedState` format — which **used to
  have no per-record checksum** (`WalRecord::decode`, `animus-control::
  persist`, tolerated only a torn *trailing* line, never a mid-line
  bit-flip that kept the JSON syntactically valid). Confirmed reproducible
  (issue #495): composing `animus-sim`'s `DiskConfig::torn_tail_on_crash`
  with `corrupt_on_crash` across a genuine crash+restart could flip a byte
  inside a still-decodable `WalRecord::Append`'s packed `HlcTimestamp`,
  producing a **wrong but successfully decoded** value that later
  hard-panicked `assert_ts_monotonic` once applied — see
  `tests/quiescence.rs`'s `a_lying_fsync_revealed_by_a_
  crash_recovers_correctly_on_restart` (which now arms `corrupt_on_crash`
  precisely because this is fixed) and `docs/engineering-lessons.md`'s
  matching entry for the full account. **Fixed** by a per-record CRC32
  checksum on every WAL line (`animus-control::persist`): a checksum
  mismatch is dropped like a torn trailing line — along with
  everything physically after it in the file — never decoded into a value,
  never a panic, **unless a later v2 sync marker proves the line was already
  durable, in which case it is the loud `FormatError::MidFileCorruption`**
  (issue #1132, `CWL1` v2: the per-group WAL's own writer piggybacks the marker
  on the next round's append, `PersistedState::recover` repairs a torn tail on open —
  see `animus-control/CLAUDE.md`'s persist.rs entry). This was the WAL-side sibling of this same crate's own
  `codec.rs` wire decoder's separately-known untrusted-length-prefix
  allocator-abort gap (`Vec::with_capacity(n as usize)` on an unvalidated
  `u32` — see the sibling `raftkv`/`txn` corpora's own `Nemesis::Chaos` doc
  for that finding's own account); both shared the same root cause, "no
  checksum/bound on untrusted framing," just on opposite sides of this
  crate (WAL read-back vs wire receive).

  **A field added
  to the shared `LogEntry`/`RaftMsg` types (`animus-control::raft`) needs an
  explicit encode/decode arm here too** — `#[serde(default)]` only protects
  the `serde_json` WAL path, not this hand-rolled one (version `22`, ADR
  0058 Train 1's `learners: Option<BTreeSet<NodeId>>` field, is the
  regression: see `docs/engineering-lessons.md`'s Code-patterns entry for
  the general lesson). **Every untrusted wire-count read (`c.u32()?`/
  `c.u64()?`) that pre-sizes a `Vec` is capped (`.min(1 << 20)`)** — a
  bounds-checked-per-element read alone doesn't stop a corrupted/hostile
  count from driving an oversized `Vec::with_capacity` before any element
  is validated, which Rust's allocator handles by aborting the whole
  process (`handle_alloc_error`), not a catchable panic; reproduced live via
  a corrupted `AppendEntries` entry count. `txn.rs`'s/`split.rs`'s own
  engine-marker decoders (reachable via a corrupted/adversarial
  `InstallSnapshot` image) got the identical cap. See `docs/engineering-
  lessons.md`'s "untrusted length-prefix pre-sizing a `Vec`" entry for the
  full account and the general rule for any future hand-rolled decoder.
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
- **`heartbeat_batch.rs`** (ADR 0044 phase 2 — mechanism landed C-02 PR 2,
  on by default since PR 3's cutover, `pub mod heartbeat_batch`) — the
  per-node **`HeartbeatBatcher`**: coalesces every co-hosted group's own
  bare (no-entries) Raft heartbeat toward the same destination node into
  one physical `KvWire::HeartbeatBatch` frame per destination per
  `RaftCore::heartbeat_interval` tick, on the reserved
  `HEARTBEAT_BATCH_STREAM = u64::MAX - 2` (this crate's fourth reserved
  stream constant, alongside `cluster_segment_store::SEGMENT_STREAM` and
  `backup::BACKUP_SEGMENT_STREAM`; the authoritative table of all reserved
  streams, with a `const` distinctness assertion, is
  `animus_node::reserved_streams`) — instead of one `env.send_stream` call
  per group per tick. **The mechanism itself is unchanged by the cutover
  — only which production caller reaches for `None` vs. `Some` flipped**,
  entirely in `animusd` (`main::DEFAULT_HEARTBEAT_BATCH = true`; see that
  crate's own CLAUDE.md). A `RaftKvNode` with no batcher attached (`None`,
  now reached only via `--no-heartbeat-batch`/`cluster_settings.
  heartbeat_batch: false`) still behaves byte-for-byte as before this
  module existed — the additive-default *mechanism* contract PR 2 built
  never changed, only PR 3's own caller-side default. `RaftKvNode::start_hosted_with_batcher`/
  `start_hosted_campaigning_with_batcher` are the batching-aware siblings
  of `start_hosted`/`start_hosted_campaigning`; `host::Reconciler::
  enable_heartbeat_batching()` is the production opt-in, mirroring
  `enable_quiescence`'s own "opt in once, applies to every group hosted
  from then on" shape exactly (`animusd`'s `--heartbeat-batch` CLI/
  `cluster_settings.heartbeat_batch` config flag calls it once at node
  start). Only `RaftCore::tick`'s own heartbeat branch is filtered this
  way — a bare `AppendEntries` from `replicate_now`'s wake-on-propose path
  (ADR 0017's single-write-latency path) always ships immediately,
  unbatched; a non-empty `AppendEntries` (real replication), `TimeoutNow`,
  and `Quiesce` are all untouched. **Responses are not batched** — a
  demuxed heartbeat's own `AppendEntriesResp` ships back on the responding
  group's own stream individually, exactly like today (see the module's
  own doc for why). **Receiver-side demux is owned entirely inside this
  crate** (`HeartbeatBatcher`'s own `stream -> HeartbeatInbox` map,
  populated by each `RaftKvNode`'s own `drive` loop at start/teardown) —
  not `host.rs`'s `Reconciler` state or anything in `animusd`, so the demux
  works under a bare `SimEnv` test with no `animusd` in the loop. A
  demuxed heartbeat is fed through **exactly** the same
  `witness_append_entries` → `core.handle` → durability-gate/send path a
  wire-arrived message already takes — `from` is read from the message's
  own embedded `leader: NodeId` field, never the physical batch-frame
  envelope, since `RaftCore::handle`'s `AppendEntries` dispatch only ever
  reads `msg.leader`. `Metric::CpAppendEntriesSent` keeps its pre-existing
  meaning ("one per logical per-group heartbeat") unchanged — a batched
  heartbeat is counted at `HeartbeatBatcher::register` instead of the
  ordinary outbound-send accounting, since it never reaches that code
  path; `Metric::CpHeartbeatFramesSent`/`CpHeartbeatDemuxDropped` are the
  batcher's own new physical-frame/demux-drop counters. Fault-injection
  corpus: `tests/heartbeat_batch_corpus.rs`, depth knob
  `ANIMUS_HEARTBEAT_SEEDS` — frame-vs-logical scaling (leader forced to
  one physical node so leadership doesn't spread as group count grows;
  measured 1-vs-5-group ratio: frames 1.00, logical 5.00), every
  per-group invariant holding under batching, a genuine partition losing a
  whole batched frame at once (elections still occur) vs. a
  lossy-but-connected link at a rate the unbatched path already tolerates
  (no spurious elections), an unknown group in a received frame (dropped
  and counted), and leader/follower kill converging with batching on. See
  ADR 0044's 2026-09-06 phase-2 amendment for the full design record and
  `docs/design/heartbeat-send-sites.md` for the original cost-model
  investigation this implements.
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
- **`split.rs`** (ADR 0058 Train 2 rung 3; ADR 0062 rung 4) — the
  **in-place split fork marker**: the durable half of `KvCommand::
  SplitTablet`, mirroring `seal.rs`'s discipline exactly (engine-global
  key, survives compaction) but keyed by `tablet` alone (a tablet forks AT
  MOST ONCE, unlike a seal's per-range keying) and carrying a real payload
  — the split key, both children's `(id, replicas)` pairs (since ADR 0062
  rung 4: the parent's own current replicas, identical for both children,
  never placement-chosen — see `animus-tablet`'s `SplitChild::replicas`
  doc), and the `bootstrap_voters` set captured once at apply from the
  parent's own `RaftCore::config()` **only** — no more `.extend(
  RaftCore::learners())` (rung 4 dropped the learner union: both children
  now bootstrap directly on the parent's own current voter set, no
  over-replication, no Stage-5 trim step needed at fork time; see the
  module's own doc for the accepted fork-D residual this leaves — an
  unrelated in-flight rebalance's own learner on the parent is no longer
  inherited by either child, self-healed by an ordinary post-cutover
  reconfigure). This read is still guaranteed identical across replicas
  for the same reason as before (Raft log order). `RaftKvNode::
  pending_split()` is the one accessor the host reconciler polls every
  tick.
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
  **`TxnWrite` (ADR 0046 A1, `TxnStage` kind-writes stack)**: `KvCommand::TxnStage.writes`' element, a named struct (`key`,
  `value`, plus an optional derived `kind_writes`/`change_log` payload for
  a write against an indexed/streamed table) — carried inside the write's
  own `Envelope::Intent`, opaque until `TxnResolve`'s commit branch
  materializes it. See the Key invariants section's `materialize_derived`
  entry and `docs/adr/0018-cross-tablet-transactions.md`'s 2026-08-16
  amendment for the full mechanism. **`TxnWrite.stage_marker` (ADR 0049 §3,
  )**: an image-less, consumer-hidden `(prefix, record)`
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
  **`TxnWrite.pending` (ADR 0054 step 4a)**: an optional
  `PendingTxnWrite` — the `TxnStage` sibling of `KvCommand::KindEval`'s own
  self-contained payload (`schema`/`pk`/`sk`/`op`/`condition`/
  `ttl_expired`, no `ts`). When `Some`, `value`/`kind_writes`/`change_log`
  above are ignored at propose time and computed by `TxnStage`'s own apply
  arm instead, reusing `evaluate_kind_eval` verbatim — see the Key
  invariants section's `KvCommand::KindEval` entry for that evaluator and
  this field's own doc for the same-txn-replay discipline a non-idempotent
  update needs. `stage_marker` is unaffected (a pure function of `pk`/`sk`,
  built at propose time regardless).

### lib.rs API

`RaftKvNode<E, S>` is the running tablet-group node (start/propose/read
methods); `StorageScope` (ADR 0050 rung 2, F2b) is a thin **kind-scoping**
helper over a tablet's own private engine (a kind-byte `prefix` plus the
tablet's **immutable** declared `range` — physical keys are
`[kind] || logical`, no table or tablet bytes); `KvCommand::KindBatch`
(ADR 0041 §3) is the
multi-kind atomic batch backing materialized secondary indexes and the
change log; `KvCommand::SeedBatch` (ADR 0050 Train B rung 4, codec v19) is
a version-carrying row-merge command — `SeedRow`s
(`(kind, logical, value-or-tombstone, version)`) merge-applied at their
**carried** versions with the stored bytes verbatim (intent envelopes
included), emitting nothing into the child's change log and witnessing the
batch's max version into the group's HLC (`propose_seed_batch` /
`seed_rows_kind` are its propose/read pair; `tests/seed_batch.rs` +
`ANIMUS_SPLIT_SEEDS` is its corpus). **Originally built for the now-deleted
copy-based split-build driver, its sole surviving consumer is the restore
driver** (`animusd::backup_restore`, ADR 0059 §7) seeding a restore's
destination tablet from a backup's captured chunks; `KvCommand::Freeze`
(ADR 0050 rung 5, codec v20) is the split-cutover freeze — a whole-range
entry of the existing sealed-set discipline whose durable seal marker
re-latches `is_frozen()` at group start, refusing every later-ordered USER
mutation (base/LSI — a consumer-bookkeeping `KindBatch` still applies)
while reads keep serving; `propose_freeze` is idempotent and
`tests/freeze.rs` is its suite. **`KvCommand::Freeze` itself is
production-dead as of the copy-split-deletion stack (2026-09-01)** — the
in-place split never needed it (see this file's own "The freeze" entry
below); **`KvCommand::SplitTablet`** (ADR 0058 Train 2 rung 3, codec v23) is
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
Full priority order, most urgent first: (1) drop a current **learner** no
longer in `desired` — regardless of its liveness or catch-up progress,
since it is stale by construction the moment placement retargets away
from it (the fix for "a learner mid-catch-up that dies or is
decommissioned must not wedge every later step" — the reconciler's job is
only to not block on a target nobody wants any more; *re*-targeting
`desired` is placement's job, untouched); (2) promote a learner that is
both still desired and caught up (finish an in-flight move before
starting a new one); (3) add a `desired` member missing from both
`config` and `learners`, as a **learner**, never straight to voter; (4)
remove a `Down` extra **voter** (failure repair) — **since issue #920's
fix (2026-09-16), ordered AFTER steps 1–3, not before**: removing a down
voter fires immediately only once no `desired` member is still missing or
mid-catch-up as a learner, i.e. only once any replacement is already
safely a voter. Before the fix, this fired *first*, ahead of adding the
replacement — sound only if "marked `Down`" means "permanently gone," but
ADR 0012's failure detector (`DETECT_TIMEOUT`, 500ms) trips just as
readily on a transient absence (a pod recreation with durable storage,
issue #920's own production shape) as on a real failure, and removing the
old voter early shrinks the live quorum requirement for the whole
in-flight window with no way back if a *second* voter is then also lost
mid-rolling-restart — see `reconfigure_step`'s own doc for the full
before/after account and ADR 0048's 2026-09-16 amendment for the incident;
(5)/(6) — once every `desired` member is already a voter — the pre-Train-1
remove-healthy-extra/leader-self-removal-via-transfer steps, unchanged. A
remove-only delta (no missing/mid-catch-up member) still removes a down
extra on the very first tick that reaches step 4, exactly as before the
reordering — steps 1–3 are no-ops when nothing is stale/promotable/missing,
so the fix costs nothing in that case. A brand-new group's initial
bootstrap (`host::plan_join_host`) is untouched — this only changes the
sequencing of an *add-with-a-down-extra-to-remove*. **Gotcha this shipped with**: the early
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
**A desired target differing from `current` by MORE than one replica**
(e.g. two of three) was suspected (issue #513, filed during ADR 0062 rung
6) of making this sequencing oscillate indefinitely instead of converging
— investigated and closed as not reproducible; `tests/
reconfigure_multi_replica_diff.rs` is the dedicated regression (60 seeds,
several harness shapes) and `animusd/tests/
split_placing_two_replica_diff_e2e.rs` its real `ProdEnv` sibling. See
`docs/engineering-lessons.md` for the investigation writeup.

**Issue #1064 (2026-09-28): `RaftCore::learner_caught_up`'s promotion
predicate was baselined against the wrong metric.** It compared a learner's
tracked `match_index` to the LEADER'S OWN `last_log_index()` — under a
CONTINUOUS writer (a directed-Placing 2-of-3 diff driven every tick
alongside a never-stopping client, the exact production shape:
`animusd --cluster-control 3 --cluster-data 5 --auto-split-bytes 1000000`
under sustained load), `last_log_index()` always includes whatever the
leader just appended for ITSELF in the current tick, before it has been
sent to (let alone acked by) anyone — voter or learner alike. Once a single
write burst between reconciler ticks exceeds
`RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (4), every sample lands mid-burst
and the learner can never be seen as caught up, no matter how genuinely
caught up it is with everything the group has actually committed —
`reconfigure_step` then never adds the second desired replica or drops
either stale voter, wedging the group at its old voter set forever. Fixed
by baselining against `commit_index()` instead: it only advances once a
majority of CURRENT VOTERS (never the learner itself) have themselves
acked, so it can never be further ahead of what the established quorum has
actually achieved than one ordinary replication round costs, and it keeps
the ADR 0058 Train 1 safety intent intact (a peer promoted at `match_index
>= commit_index - threshold` can immediately help commit anything the group
has already committed). See `RaftCore::learner_caught_up`'s own doc
(`animus-control/src/raft.rs`) for the full before/after account. Every
existing caller/test of `learner_caught_up` (`animus-control`'s
`learner_corpus.rs`/`learner_promotion_leader_crash.rs`, this crate's
`learner_membership.rs`/`learner_catchup_under_load.rs`/
`snapshot_transfer_survives_compaction.rs`) was audited and is unaffected —
each already polls to convergence with the writer stopped (or genuinely
idle), where `commit_index()` and `last_log_index()` coincide. Regression:
`tests/directed_placing_under_sustained_load.rs`
(`ANIMUS_DIRECTED_PLACING_LOAD_SEEDS`, root `CLAUDE.md`'s knob table) —
deliberately built with a generous `BURST_GAP` (300ms against a 10ms
learner disk delay) so it isolates this exact baseline-metric defect from
the separate, already-understood write-rate/capacity and
compaction/snapshot-transfer-restart concerns `learner_catchup_under_load.rs`
covers (see this file's own module doc for why conflating the two produces
a much harder-to-diagnose, sawtooth-shaped false negative — a genuinely
sustained-load run at the field report's own write rate DOES still hit
repeated `InstallSnapshot` transfer restarts under heavy compaction churn,
a real, separate, PRE-EXISTING defect suspected to be issue #1061's own
mechanism; not fixed here — see `docs/lessons/testing/` for the
investigation notes and the compaction-floor/`COMPACT_DEFER_*` invariants
this fix deliberately left untouched).

**Part 2 (2026-09-28, closes issue #1061): the heavier-load defect above was
confirmed and fixed.** `apply_and_compact`'s `COMPACT_DEFER_EMERGENCY_
CEILING` — a last-resort WAL-retention safety valve sized in raw log-entry
count (`behind`) — used to override the idle-progress defer
(`COMPACT_DEFER_IDLE_CEILING`) and force a still-genuinely-progressing
transfer out **regardless of whether it was actually stalled**, the instant
`behind` crossed a fixed threshold. Under a continuous writer `behind` grows
at the WRITE rate, which has no relationship to how close a transfer is to
landing — sized in entries, this "safety valve" is not a progress
guarantee at all (see `docs/lessons/testing/`'s matching 2026-09-28 entry).
Fixed narrowly: `emergency_ceiling_hit` (`apply_and_compact`'s own local)
no longer forces a restart of a transfer in flight to a **learner**
specifically — a learner's own catch-up contract is already "one
`InstallSnapshot`, then promote" (this file's own entries above), so the
only question worth asking about its transfer is whether it's genuinely
stalled, which `idle_ceiling_hit` already answers correctly with no change.
A **voter's** in-flight transfer is completely unaffected — both ceilings
still apply to it exactly as PR #1047 tuned them, since a voter needing a
snapshot at all is still the flood scenario this whole mechanism exists to
bound. See `emergency_ceiling_hit`'s own computation and
`COMPACT_DEFER_EMERGENCY_CEILING`'s doc comment (`lib.rs`) for the full
before/after account. `RaftKvNode::set_compact_tuning_for_test` (a
test-only, additive-default seam mirroring `RaftCore::enable_quiescence`'s
own `Option`-field shape) lets a test override `COMPACT_THRESHOLD`/
`COMPACT_DEFER_EMERGENCY_CEILING` down from their production sizes, since
reaching the real 4096-entry ceiling by an unambiguous margin needs a
real-time-unaffordable step count otherwise. Regression: `tests/
learner_snapshot_livelock_under_continuous_writer.rs` — an
AppendEntries-only control (a learner joining before any compaction, at the
same sustained write rate, proving that rate is not itself a capacity
mismatch) paired with a late-joining learner needing a real, multi-chunk
`InstallSnapshot` under the SAME continuous writer, asserting — all while
the writer keeps running — that at least one transfer actually completes
(`Metric::CpSnapshotInstalls`), that `Metric::CpSnapshotTransferRestarts`
stays bounded, and that the learner's own applied index makes substantial,
ongoing progress against the leader's commit index. Confirmed live: the
pre-fix code never completes a single install (restarts=75, applied index
stuck at 0 for the whole run) against the fix's restarts=6, two completed
installs, and the learner reaching 68% of the leader's commit index — a
livelock, not a mere slowdown. See `docs/lessons/testing/2026-09-28-reach-
the-real-trigger-dont-shrink-the-test-around-a-fixed-constant.md` for the
tuning history (a write-rate-vs-peer-throughput lesson this test's own
control caught) and this file's own investigation notes on a further,
separate, not-fixed-here defect the same investigation surfaced
(`handle_append_resp`'s non-monotonic `next_index` update on an ordinary
AppendEntries ack, tracked as issue #1070).

**`RaftKvNode::voter_history()` (issue #596)** records every distinct voter
configuration a group has adopted, in adoption order, in a small bounded
in-process ring (`VoterHistory`, capacity 64, oldest dropped; never rebuilt
at recovery — a pure observability record of THIS uptime). Recorded once per
consensus-loop iteration, the same "recompute live, same lock acquisition"
cadence `state_machine_behind`/the quiesce veto already use — see the
`drive()` loop's own comment at the call site. Exists because the transient
over-replicated intermediate `reconfigure_step`'s learner-phased sequencing
guarantees is real but its *duration* is an implementation timing artifact:
`split_placing_two_replica_diff_e2e.rs` used to prove the 5-voter
intermediate occurred by sampling `/admin/raftkv` externally every 200ms,
which a fast enough pair of consecutive reconciler ticks can race shut
before the sample lands — see `docs/engineering-lessons.md`'s matching
entry. `tests/voter_history_reconfigure_diff.rs` is the dedicated `SimEnv`
regression (asserts the exact 3→4→5→4→3 sequence on the one replica present
in both the initial and the desired set); `admin::CpRaftView`'s
`voter_history` field (`animusd`) surfaces it read-only on `/admin/raftkv`,
mirroring `learners`' own "purely observational" contract above — reading it
never blocks, proposes, or wakes a quiesced group.

**`RaftKvNode::membership_history()` (issue #944)** is the LEARNER-set
sibling of `voter_history()` above, but recorded in a different place for a
reason worth being explicit about: `voter_history`'s once-per-consensus-
loop-iteration sampling cadence is fine-grained enough for a voter-set
change (which only ever advances via this group's own network-replicated
log, so this loop's own per-message processing bounds how much can happen
between two samples) but is **not** fine-grained enough for the learner
set, because `reconfigure_step`'s add-learner-then-promote pair is proposed
by the host reconciler's own task, synchronously mutating the shared
`RaftCore` from OUTSIDE this drive loop entirely — and a follower's own
`log_append` can likewise adopt several batched config-changing entries
before this loop next gets scheduled to sample. Sampling from `drive()` can
therefore coalesce the whole add-then-promote sequence into one record,
silently skipping the transient learner state — `crates/animusd/tests/
learner_reconfigure.rs`'s `spare_replacement_passes_through_an_observable_
learner_state_and_keeps_serving` used to prove the learner phase occurred
by polling `/admin/raftkv` externally every 100ms, which this exact
coalescing can race shut just like the `voter_history` incident above (same
class, `docs/engineering-lessons.md`'s matching entry). The fix records the
joint `(voters, learners)` pair **inside `RaftCore::apply_config`** itself
(`animus-control`, `config_history`, capacity 64) — the one call every real
transition funnels through regardless of which task or how much batching
triggered it — rather than sampling it from any layer above. `RaftKvNode::
membership_history()` is a thin passthrough to `RaftCore::config_history()`;
`admin::CpRaftView`'s `membership_history` field surfaces it the same way
`voter_history` does.
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
- **An unresolved intent reads back the key's last committed value**
  (`stale_value` → `prior_committed`, which takes it from the v2 intent's own
  carried `prior` — never from MVCC history, see the envelope bullet in Key
  invariants), never as absent. `local_get`'s raw
  peek reports it as absent — correct for its admin/debug callers, a
  fabricated deletion for a client-visible read. `stale_scan_rows` applies
  the same rule row-by-row, where `resolve_scan_rows` drops the row.
- **The gate is not a staleness bound.** It only excludes a replica whose
  engine is not yet *any* state of this tablet (no leader known yet, or a
  committed tail / snapshot image not merged). A partitioned replica passes
  it and answers arbitrarily stale data — which is the DynamoDB contract,
  not an oversight. See ADR 0055 §2.

  `stale_read_ready()`'s own truth table (has-a-leader × caught-up-to-commit)
  is directly unit-tested via `RaftKvNode::stale_read_ready_decision` — a
  pure associated function over just the three fields the gate reads,
  extracted so the table is provable without a running `RaftKvNode`/
  `RaftCore` (`lib.rs::stale_read_ready_tests`, ADR 0061 rung A4). The full
  integration tests above still own the "nothing blocks/proposes/wakes"
  properties, which aren't expressible at the pure-function level.

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
    data). **Issue #804 amendment (2026-09-09):** the two engine-state
    reads (`latest_version()` at snapshot install and at group start) are
    structurally weaker than the other two, which scan committed *log*
    entries directly — they read back what the engine's apply *wrote*,
    which a committed-and-applied entry whose outcome writes no row (a
    `Cas` whose `expected` never matched, a condition-failed `KindBatch`/
    `KindEval`, an aborted txn, ...) never advances, even though such an
    entry still carries a real `ts` (`assert_ts_monotonic` runs on it).
    Once compaction truncates such an entry out of the WAL, neither engine
    read can see its `ts` any more. Closed two ways, one per read site:
    (1) **snapshot install** — `engine_image`/`install_engine_image`
    (`lib.rs`) carry the sender's own running
    `max_applied_ts` in the image's header (not as a row: `engine_image`'s
    per-kind scan deliberately excludes every `RESERVED_NAMESPACE` marker,
    seal/ceiling/split's own included, so a marker row alone could never
    have crossed this way), and the install site folds it into `hlc`
    alongside the pre-existing `latest_version()` witness; (2) **group
    start** — `apply_and_compact`'s compaction path now durably `merge`s a
    dedicated per-tablet marker (`hwm.rs`, mirroring `ceiling.rs`'s own
    marker but generalized to every ts-bearing entry) at
    `hlc::pack(max_applied_ts)` whenever it truncates the WAL, which
    unconditionally raises the engine's own global MVCC high-water mark —
    so the existing `latest_version()` read at group start needs no change
    at all, it just stops undercounting. See `hwm.rs`'s and `lib.rs`'s
    `engine_image`/`install_engine_image` module docs, and
    `tests/hlc_differential_skew.rs`, for the full account and the
    differential-per-replica-clock-skew mechanism needed to reproduce the
    bug at all (a clock-skew fault applied *uniformly* across a replica
    group cannot expose it). **Same-day correction**: the fold above
    covers the sender's own header only when its `max_applied_ts` is
    populated — that resets to `None` on every restart of the sending
    apply task, so it is additionally `max`ed against `hlc::unpack(storage.
    latest_version())` at image-build time; and the receiving replica now
    ALSO durably `merge`s its own `hwm.rs` marker at install (same
    `merge_batch` as the rows), not just an in-memory `hlc.witness` of the
    header — a receiver that itself restarts before its own next
    compaction had nothing else to re-derive the mark from. See `hwm.rs`'s
    module doc ("Why the `InstallSnapshot`... half needs its own path") and
    ADR 0018's matching 2026-09-09 correction for the full account.
  - **The freeze** (`seal.rs`, `KvCommand::Freeze`) closes the one residual
    witnessing alone cannot: an in-flight write from the parent's own
    leader, still in its commit pipeline when the split cutover happens. The
    now-deleted copy-based split-build driver used to propose `Freeze`
    through the parent's own log; apply rejects any later-ordered mutating
    entry, regardless of that entry's own `ts`, because within one group log
    order and HLC order coincide — the **log position** is authoritative.
    (The zero-copy split's range-scoped seal, its `ProposeSeal` reconciler
    action, and the `parent_seal_observed` host gate were deleted in the
    rung-7 sweep — a copy-based child never shares rows with its parent, so
    there is no handoff to seal.) **`KvCommand::Freeze` itself is
    production-dead as of the copy-split-deletion stack (2026-09-01)** — no
    remaining caller proposes it (the in-place split, ADR 0058, never needed
    a freeze at all: `CutoverSplit` fires only after both children are
    already fully formed) — kept, not deleted, since nothing in that stack's
    scope touched `animus-cp-data`'s own `KvCommand` enum.
- **CAS is decided at *apply* time, not propose time** — this is what makes it
  linearizable and contention-correct. `RaftCore` agrees only the order; `Cas`
  rides through as opaque data. Apply evaluates it in commit order against the
  key's *current committed* value and compares to `expected`; equal → merge
  at `index`, else no-op. Every replica applies the same order against the
  same state with no clock/RNG, so every replica makes the **identical**
  decision. Outcome is stashed in driver `CasResults`, keyed by the log index
  **and paired with the entry's own Raft term** (fixed alongside
  `StageOutcomes` below — see that bullet's term-identity paragraph, which
  applies here identically): `cas_result(index, term)`/`compare_and_swap`
  require the caller's own `ProposeResult::Accepted`'s `term` to match before
  ever trusting a recorded outcome as this proposer's own, and
  `compare_and_swap`'s own poll loop also checks `is_leader()` every
  iteration (previously it did not, despite a comment implying it did — see
  `wait_applied`/`wait_stage_outcome`'s identical guard). Regression:
  `tests/cas_outcome_identity.rs`.
- **`TxnStage`'s own-key conditions are decided at *apply* time too, the
  identical CAS-style discipline** — evaluated against each key's current
  committed value inside the same apply arm that decides the pre-existing
  fence/seal/foreign-intent gates, recording a `StageOutcome` per Raft log
  index (`StageOutcomes`, mirroring `CasResults`). **`wait_applied(index)
  .await == true` does NOT imply `stage_outcome(index, term)` is `Some`** — a
  snapshot install can advance `engine_applied` past `index` without this
  replica individually applying (hence recording an outcome for) that exact
  entry, since an install globs many commands together. `txn_stage_anchor`/
  `_participant` poll `stage_outcome` directly instead (`wait_stage_outcome`)
  — `None` on timeout, never a hard-`expect`ed fact that turns out not to be
  guaranteed. **`StageOutcomes` (like `CasResults`) is keyed by index and
  paired with the entry's own term** (closed 2026-08-29, mirroring the fix
  `KindBatchOutcomes` got first — see `docs/engineering-lessons.md`'s
  amendment to the `KindBatchOutcome` entry): an uncommitted `TxnStage`
  entry's index can be reoccupied by a different command after a leadership
  change, so `stage_outcome`/`wait_stage_outcome` require the caller's own
  accepted term to match, propagating `None` ("not confirmed as mine, retry")
  rather than ever returning a stale entry's outcome as this proposer's own.
  `txn_stage_anchor`/`txn_stage_participant` thread the accepted `term`
  through unchanged in their own public shape (still `Option<(..,
  StageOutcome)>`, `None` on ambiguity same as before). See
  `docs/engineering-lessons.md` for the general lesson.
- **`KindBatch` briefly gained the identical own-key `conditions` field
  (ADR 0046 "evaluate at leader" seatbelt) — deleted
  outright by ADR 0054 step 4b.** Modeled directly on
  `TxnStage.conditions`, `(key, expected)` byte-level OCC pairs checked
  against the KIND_BASE scope, **checked BEFORE the seal gate**, not behind
  it (`TxnStage`'s `condition_failure` only evaluates once already known
  unsealed so `StageOutcome` can report the seal reason ahead of a
  condition one; `KindBatch` had no outcome channel of its own at
  introduction to prioritize this way). Its one production caller,
  `dynamo::kind_write_item_at_leader`'s leader-side evaluate-then-propose
  write path (ADR 0046 U3), passed `seatbelt: vec![(base_key, raw_old)]` —
  the guard against a concurrent `TxnStage`/`TxnResolve` commit landing
  between that evaluator's own-key read and its own propose call. Every
  producer moved onto apply-time evaluation (steps 3/4a), which made
  every real call site pass an empty `Vec` — confirmed before deletion —
  so the field, its apply-time check, `put_kind_batch_conditioned`
  (merged back into a single 2-argument `put_kind_batch`), and its own
  test file (`tests/kind_batch_conditions.rs`, which mirrored `tests/
  txn_conditions.rs` scenario-for-scenario) are all gone. `TxnStage`'s
  own, separate `conditions` field (added alongside it, backing a *plain-table* write action's own condition inside
  `TransactWriteItems`) is untouched — a different mechanism on a
  different variant that happened to share a name and a byte-level-OCC
  shape.
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
- **`KvCommand::Batch` and `TxnResolve`'s commit/abort-restore writes are
  coalesced-fsync too (issue #834)** — they now push onto the same
  loop-local `pending: Vec<MergeOp>` `flush_pending` already drains for
  `Put`/`Delete`/`SeedBatch`/`KindBatch`/`materialize_derived`, instead of
  calling `storage.merge`/`merge_tombstone` directly once per key. Closes
  two defects together: N un-amortized `fsync`s per `Batch`/`TxnResolve`
  entry on a durable engine collapse to one (`flush_pending`'s single
  `merge_batch` call — see `animus-storage/CLAUDE.md`'s ~9.7x batch-vs-
  per-key figure; invisible under `SimEnv`/`MemoryEngine`, where `fsync` is
  free), and both arms inherit `flush_pending`'s halted-gated error
  tolerance for free (they used to hard-panic via a bare `.expect(..)`
  even during a graceful shutdown racing an in-flight write — see the
  "Apply task" bullet above). `TxnResolve`'s commit/abort-restore writes
  used to check the direct `merge`/`merge_tombstone`'s returned
  `took_effect` against `surface_suspicious_merge_noop`'s soft seatbelt;
  that diagnostic is dropped on this now-batched path (it never covered
  `Batch` either) — see ADR 0018's 2026-09-16 amendment for why that's
  safe (a documented metric+log-only diagnostic with no correctness role;
  the real safety net, `TxnResolve`'s own per-entry `fence`, is untouched).
  **The one thing this conversion required elsewhere**: `KvCommand::
  TxnStage`'s own `already_decided`/`blocked_by` reads now need a leading
  `flush_pending` call too (mirroring `Cas`/`KindEval`'s own — see their
  doc above) — before this fix, `TxnResolve`'s commit write was direct and
  immediate, so a same-pass `TxnStage` reading `storage.get` right after it
  always saw the just-landed `Committed` envelope; deferred through
  `pending`, it could see the stale, still-`Intent` state instead and
  spuriously treat it as a foreign-transaction block. Caught by
  `animus-test`'s `txn_serializable.rs` corpus (`ANIMUS_TXN_SEEDS=5`,
  `participant_leader_kill_early`) as a genuine cross-replica divergence,
  not a flake — see ADR 0018's amendment for the full trace and
  `docs/lessons/code-patterns/` for the generalized rule (every
  `storage.get`/`get_at`/`scan` inside the effects loop needs a preceding
  `flush_pending` if what it reads could be produced by a `pending`-queued
  write earlier in the same pass). Tests: `tests/batch.rs::
  batch_rejected_wholesale_by_a_frozen_range` (the sealed-key branch, not
  previously covered by `tests/freeze.rs`'s own sweep), `tests/
  txn_single.rs::commit_with_a_mixed_put_and_a_staged_delete_lands_both_
  from_one_resolve_entry`, and `tests/batch_txn_resolve_apply_fault.rs`
  (halted-gate regression via a fault-injecting `StorageEngine` test
  double, and an `LsmEngine`-backed `Disk::sync`-counting fsync-count
  proof — both `MemoryEngine`/`SimEnv` alone can't observe).
- **`KvCommand::KindEval` — the self-contained evaluated write (ADR 0054
  step 2), live since step 3.** This crate now depends on `animus-item` (`WriteSchema`/
  `derive_kind_writes`/`AttributeValue`/`Item`/`ConditionExpression`/
  `UpdateAction` — no `animus-env`/wire-crate transitively pulled in).
  Unlike `KindBatch` (leader-evaluated bytes + an apply-time OCC seatbelt
  against staleness), this variant carries the *operation* — `schema:
  WriteSchema` (frozen at propose time; apply has no `Metadata` access at
  all, a structural boundary, not an omission — two replicas of one entry
  reading different catalog versions would derive different index rows and
  diverge), `pk`/`sk`, `op: KindEvalOp` (`Put`/`Delete`/`Update{key_item,
  actions}`), `condition: Option<ConditionExpression>` (the client's own
  rich expression — no seatbelt needed, since apply's own read already IS
  current state), `ttl_expired`, `ts`. No `base_key`/token field: apply
  derives both from `pk`/`sk` via `animus_tablet::partition_token` (this
  crate already depends on `animus-tablet`; carrying a leader-computed
  token would just be a value that could disagree with `pk` in principle).
  Apply (`evaluate_kind_eval`, a **pure** function factored out of the arm
  so the decision is unit-testable without an engine): drain the pending
  run (mirrors `KindBatch`'s own apply-time drain); check the seal/freeze
  gate; read the current value, unwrap its envelope — an unresolved intent
  from a concurrent transaction is `ConditionFailed` (ambiguous, never
  guessed at, the identical foreign-intent discipline `Cas` already has);
  evaluate `condition`; compute `new` via
  `op`; derive `writes`/`change_log` via `animus_item::derive_kind_writes`;
  materialize via the same shared `materialize_derived` helper above — no
  third copy. Codec tag `16`: the four rich, evolving nested
  field types each ride as one `serde_json` blob inside the binary
  envelope (`put_json`/`Cursor::json`) — the same convention `backup.rs`'s
  `BackupManifestObject` already uses for `TableSchema`'s own evolving
  shape, since a hand-encoded field-by-field layout for four
  still-growing types would need a codec change on every one of their own
  future field additions.

  **Outcome mapping**: reuses the existing, bounded `KindBatchOutcomes` map
  (no `KindEval`-specific outcome map) — `Applied`/`ConditionFailed`/
  `Sealed` exactly as `KindBatch` already has them, plus a new
  `KindBatchOutcome::Rejected { key, code, message }` for the two cases a
  plain false condition doesn't cover: `condition.evaluate` returning `Err`
  (a domain violation, e.g. `size()` on the wrong type) or `op`'s `Update`
  folding via `animus_item::apply_update` and returning `Err` (a malformed
  update, a type mismatch, or the post-update item over the size cap).
  `code`/`message` copy `ConditionError`'s/`UpdateError`'s own fields
  verbatim, kept as an owned `String` rather than assuming
  `"ValidationException"` (both types' only code today) stays the only one
  forever. **The wire-level mapping landed in ADR 0054 step 3**:
  `animusd::classify_kind_batch_outcome` folds `Rejected` into the same
  `NoOp` arm as `ConditionFailed`/`Sealed`, and `write_path::
  KindEvalApplied::Rejected { code, message }` maps it back to the
  identical `WireError` the pre-step-3 leader-side evaluator produced for
  the same failure (`kind_write_item_at_leader`'s doc has the full
  mapping).

  **The leader-local result payload (ADR 0054 mechanism 3) — `KindEvalResult`/
  `KindEvalResults`, deliberately a SEPARATE structure from
  `KindBatchOutcomes`, never replicated, never in a snapshot.** The reason
  is memory, not correctness: every replica derives the identical `old`/
  `new` images as a normal part of evaluating the write, but only the node
  that proposed a given entry (if any — a recovery push registers nothing)
  ever wants them back; folding the payload into the replicated outcome map
  would grow every follower's retention for a value nobody there reads.
  `RaftKvNode::propose_kind_eval` registers this node's interest in an
  accepted entry's index **while still holding the same `core` mutex the
  apply task needs to lock before it can ever drain that entry** — a real
  ordering guarantee, not a hopeful race window, on every executor
  (`SimEnv`'s single-threaded scheduler and a genuine second OS thread
  under `ProdEnv` alike), since the apply task's `drain_apply` call cannot
  proceed until the registration's own critical section releases the lock.
  `RaftKvNode::take_kind_eval_result(index, term)` removes the slot on
  read (never a peek), mirroring `kind_batch_outcome`'s identical
  index-and-term identity discipline for the identical reason (an
  uncommitted entry's index can be reoccupied by a different command after
  a leadership change). Both `interested` and `results` are bounded by the
  same generous `RETAIN = 8192` `KindBatchOutcomes` already uses.

  **Wired since ADR 0054 step 3** — `kind_write_item_at_leader`
  (`animusd::dynamo`) is the production caller: it builds the `WriteSchema`
  slice and a `KindEvalOp` mirror of the client's operation, proposes via
  `ClientCtx::cp_kind_eval_local`, and reads this payload back on confirm.
  Tests: `tests/kind_eval.rs` — a differential test against a
  hand-built `KindBatch` calling the identical `derive_kind_writes`; a
  false condition leaving every replica's row untouched; two `ADD`
  proposals issued back-to-back before either applies (the ADR's own
  motivating property) landing with zero refusals; the leader-local
  payload's three properties; the frozen/sealed gate; and a crash/restart
  replaying two entries to the identical state including the stale LSI
  row's removal. `animus-item`'s own `write_schema` module carries
  `derive_kind_writes`'s pure-function unit tests.
- **`KvCommand::KindEvalBatch` — the batched sibling of `KvCommand::
  KindEval` (ADR 0049's batched-`BatchWriteItem`-images amendment, issue
  #996 layer 1), no production caller yet.** One Raft entry, `entries:
  Vec<KindEvalEntry>` (each entry the identical `schema`/`pk`/`sk`/`op`/
  `condition`/`ttl_expired` fields `KindEval` carries, minus `ts` — shared
  by the whole entry) plus one shared `ts`, closing the same per-item-entry
  throughput gap `KindBatch.change_log`'s own `Vec` shape closed for the
  marker-table path (one entry per tablet per `BatchWriteItem` call, never
  one per item). Codec tag `17` — each entry's four rich
  nested fields ride the identical `put_json`-per-field convention
  `KindEval` established.

  **Apply order**: `assert_ts_monotonic`; `flush_pending` ONCE, up front
  (never per item — a same-entry same-key collision is handled by the
  overlay below, not by re-flushing); a whole-entry seal check over every
  item's own base key (either every item is sealed or none are, since they
  all share this tablet's one range) — sealed records a single
  `KindBatchOutcome::Sealed{key}` and materializes nothing; otherwise loop
  the entries in commit order, evaluating each via the identical
  `evaluate_kind_eval` core `KindEval` uses, threading `next_ordinal`
  across `materialize_derived` calls exactly like `TxnResolve`'s own
  multi-key commit loop (issue #852) so every item's change record lands
  at a distinct `(ts, ordinal)` pair. One item's own `ConditionFailed`/
  `Rejected` never aborts its siblings.

  **Per-entry-singular vs. per-item-local results — the same split
  `KindEval` uses, at the batch grain.** The replicated `KindBatchOutcome`
  records exactly ONE `Applied`/`Sealed{key}` for the WHOLE entry — never a
  per-item breakdown — so `classify_kind_batch_outcome`/`kind_batch_
  confirm_superseded` (`animusd`) need no changes at all. The ordered
  per-item `KindEvalItemResult` (`Applied { old, new } | ConditionFailed |
  Rejected { code, message }`) breakdown lives in a NEW, leader-local,
  never-replicated `KindEvalBatchResults` slot map — a structural clone of
  `KindEvalResults` at the batch grain (same `register`/`fill`/`take`
  shape, same `RETAIN = 8192`, same index-**and**-term identity
  discipline: `take_kind_eval_batch_result(index, term)` never trusts a
  term mismatch). `RaftKvNode::propose_kind_eval_batch` mints `ts` via
  `mint_pushed` pushed above every item's own base key (mirroring
  `put_kind_batch`'s multi-key call) and registers this node's interest
  before dropping `core`, identically to `propose_kind_eval`'s own
  race-freedom argument.

  **The overlay rule — genuinely new apply logic, plus a storage-layer
  gotcha it does NOT by itself fix.** A `BatchWriteItem` call has no
  duplicate-key validation today (unlike `TransactWriteItems`'s own
  check), and before this variant, a duplicate "worked" only because each
  item was its own Raft entry at its own, strictly-increasing `ts` — a
  later duplicate naturally overwrote an earlier one at the storage layer.
  Once every item shares one entry's one `ts`, two things are needed, not
  one:
  1. A `BTreeMap<base_key, Option<Item>>` overlay, consulted before
     `storage.get`, populated with each applied item's own `new` — so a
     later item sharing an earlier item's key evaluates against that
     earlier item's write, not a stale/absent read. This alone is new: no
     existing command needed it (`TxnResolve`'s own multi-key loop is
     guaranteed distinct keys by `TransactWriteItems`'s wire validation).
  2. **The write side, found by this layer's own test (c)**:
     `StorageEngine::merge`'s per-key LWW takes effect only when its
     version is STRICTLY greater than the key's current latest — two
     items writing the identical physical key at this entry's one shared
     `ts` carry the identical version, so without a further fix the engine
     silently keeps the FIRST push and drops the second (the opposite of
     last-write-wins). The apply arm closes this by collapsing the
     entry's own accumulated `pending` writes to at most one op per
     physical key (keeping the LAST) right before they queue for the
     engine — sound specifically because `flush_pending` ran once, up
     front, so `pending` holds exactly (and only) this entry's own writes
     at that point. See `docs/lessons/code-patterns/2026-09-20-a-batched-
     entry-sharing-one-ts-can-silently-drop-a-same-key-duplicates-write.md`
     for the generalizable version of this finding.

  **No production caller as of this PR** — `animusd`'s `BatchWriteItem`
  images-carrying arm adopts it in the stacked follow-up (layer 2), the
  same staging discipline `KindEval` itself used ahead of its own step-3
  cutover. Tests: `tests/kind_eval.rs`'s `kind_eval_batch_*` scenarios
  (distinct-key application with ordered change records, per-item
  independence under a failed sibling condition, the same-key duplicate,
  the frozen-tablet whole-entry seal, and term identity);
  `tests/kind_eval_batch_fault.rs` (leader-kill truncation, follower-kill-
  mid-commit convergence, crash/restart WAL replay); and `animus-test`'s
  `stream_lineage_corpus.rs::kind_eval_batch_delivers_every_item_exactly_
  once_in_order` (the `(hlc, ordinal)` exactly-once walk end to end,
  including a mid-commit leader kill).
- **The value envelope + transactions (`txn.rs`).** Every value the apply
  path merges into the engine is 1-byte-tagged: `0` = committed (raw value
  follows), `2` = an intent (`txn-envelope` v2) naming the staging `TxnId`,
  its record's logical key, the staged value (`None` = a staged delete) and
  the **prior** committed value it shadows (`None` = absent); `1` is the
  retired v1 intent with no prior (`txn::legacy::v1`, still decoded). Every read
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
    held immediately before the intent — **never** a tombstone over a
    committed value. **That value comes from the intent itself**
    (`IntentPrior::Known`, captured by `TxnStage`'s apply from the key's
    latest record via `stage_intent_prior`), **never from MVCC history**:
    the old `get_at(key, intent_version - 1)` lookback lost acked writes
    (ADR 0018's 2026-10-04 amendment) because `LsmEngine` compaction GC drops
    versions below a floor about 1 ms of HLC behind the newest write, and an
    `InstallSnapshot` image ships latest records only. Only a legacy v1
    intent (`IntentPrior::Unknown`) still uses the lookback. **General rule:
    no apply or read path may depend on `get_at` below the newest version
    unless something holds that history** (a held `LsmSnapshot`); the
    `MemoryEngine`-only corpora cannot catch a violation — see
    `tests/it/txn_abort_restore_history.rs` and `animus-test`'s
    `lsm_compaction_*` txn-corpus cells.
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
    **Fixed (issue #298 shape A, confirmed and closed 2026-08-26)**: this
    guard used to check only for a *different* txn's unresolved `Intent` —
    never whether the current value was already `Envelope::Committed`, so
    a stale/duplicate `TxnStage` propose landing after its own transaction
    had already fully resolved was never rejected: it would silently
    resurrect the key from `Committed` back into `Intent`, letting a later
    resolve re-materialize its derived change-log record a second time at
    a fresh HLC — caught live (`delivered=146/144`, one member of a
    transactional pair duplicated under a single sealed shard) during the
    same `SplitMode::InPlace`-unpinned soak that caught shape B. **Fixed**
    via a per-group memo, `TxnTracker::recently_resolved`, **replaced
    2026-10-06 (issue #1243)** by a durable per-key *resolved marker* (see
    below): `TxnStage`'s apply arm rejects (folds into the same `Fenced`
    bucket as `already_decided`) a stage whose target key was already
    resolved by THIS EXACT transaction on this group. **Tracing the captured trace's
    own `txn_id`s past this fix showed the LIVE trigger is narrower and
    deeper than this guard alone closes** — see `docs/engineering-
    lessons.md`'s shape A amendment for the full account: the resurrecting
    stage used a genuinely *different*, fresh `txn_id` (a client-level
    retry of an un-tokened `TransactWriteItems` racing its own
    already-committed first attempt, enabled by `cp_txn`'s own
    confirmation-loss error messages being marked retryable identically to
    a provably-safe `Fenced` refusal) — a real, still-open, deeper
    coordinator-side mechanism this fix does not close, named but not
    fixed this round per this repo's "an incidental bug gets its own PR"
    convention. Regression for the fix that DID land:
    `pr5_orphan_and_resurrection_tests::
    a_resolved_key_rejects_a_same_txn_restage_issue_298_shape_a` (this
    crate's own in-crate test module, red/green proven).
  - **`RaftKvNode::txn_record_view` uses the `stale_get_served`/
    `linearizable_get_served` "served" discipline (fixed 2026-08-26, issue
    #298 shape B)**: `Option<Option<TxnRecordView>>`, not a plain `Option`
    — outer `None` = **not served** (this replica's own read barrier
    failed, e.g. mid-fork/cutover), `Some(None)` = definitively no record
    at this key, `Some(Some(view))` = found. `animusd::ClientCtx::
    txn_recover`'s orphan-record branch makes a real decision (whether to
    synthesize an abort tombstone) directly off "no record" — before this
    fix, the plain-`Option` return conflated the two into one bare `None`,
    letting a transient barrier failure be read as a confirmed absence and
    incorrectly abort a transaction whose record was fine and merely
    unreachable by that one query. See `docs/engineering-lessons.md`'s
    issue #298 shape B amendment for the full incident and
    `tests/txn_record_view_served.rs` for the primitive's own regression.

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

- **Resolved marker (issue #1243): apply decisions read durable state, never
  process memory.** `TxnResolve`'s apply writes, for every key it actually
  resolves, a row `txn::resolved_marker_key(key)` = `token || [0x00, 0x04] ||
  key` in the base scope (value `[0xA1] || txn_id`, format
  `txn-resolved-marker` v1, fixture `tests/fixtures/formats/txn-resolved-marker/v1.bin`)
  in the same merge batch as the resolve; `TxnStage`'s apply reads it and
  rejects a stage whose `(key, txn_id)` matches. It replaced the in-memory
  `TxnTracker::recently_resolved` map, which made one committed log entry
  apply differently on a restarted / snapshot-installed / cap-evicted replica
  (stage rejected on some, intent resurrected on others; the apply-time
  read-modify-write arms then diverged permanently). One row per key
  (overwritten by the next resolve there), token-led so it moves with its key
  through splits and snapshot images; every client-facing scan skips it via
  `txn::is_internal_key` (record keys alone stay `is_record_key`, the predicate
  for code that *decodes records*). **Class G**: `engine_image` omits marker
  rows while `Gate::GlobalTables` is closed (an N-1 replica's filters would
  surface them to clients; it keeps its own in-memory guard — residual), apply
  always writes them; cell `tests/it/txn_resolved_marker_gate.rs`. Residual by design: it remembers only the
  LAST resolver of a key, so a duplicate stage of T arriving after a *later*
  transaction also resolved the same key is not caught — but that residual is
  identical on every replica for live apply and snapshot install (but see the
  replay bullet below), where the old one was per-process. A new internal row kind must be added to `is_internal_key` and
  the `animus-test` `EMBEDDED` table. Regression:
  `tests/it/resolved_restage_replica_determinism.rs` (restart + snapshot-install
  variants, `ANIMUS_RESTAGE_SEEDS`).
- **WAL replay re-applies over an engine that is already ahead (issue #1242).**
  A restart replays the log tail from `snapshot_index` over the replica's own
  durable engine, which already holds everything applied before the kill. An
  arm whose decision reads engine state sees future state on replay; the
  per-key `merge` version guard protects arms whose only effect is a plain
  per-key merge, but not whole-or-nothing multi-key ones. `TxnStage`
  therefore no-ops (`Fenced`) when any of its keys, their resolved markers, or
  (anchor) its record key carries a version strictly above the entry's `ts`
  (equal = its own partial merge, re-applies). The read is **tombstone-aware**
  (`latest_version_incl_tombstone`, over `scan_with_tombstones`): `get` hides a
  key deleted after the stage, which makes a stage rejected live by an
  own-key condition (`A` must be absent) look acceptable on replay once `A` is
  deleted. The branch is unreachable live except via `SeedBatch` (the restore
  driver merges rows at carried source-cluster versions, so a stage hitting a
  seeded key with a higher version is Fenced live too — deterministic on every
  replica, a liveness edge on a not-yet-served table only). Without it a stage
  the live apply rejected (stale restage caught by one key's marker; stage
  blocked by a since-resolved intent) resurrected an intent on the restarted
  replica only, silently dropping later acked txn appends there. The #1243
  "residual is identical on every replica" statement above held for live apply,
  not replay. Any new multi-key conditional arm needs the same "what if every
  key were from the future" review. `KindBatch`/`Batch`/`Cas`/`Delete`/`SeedBatch` make no
  engine-state decision that fans out to other keys (nothing to guard).
  **`KindEval`/`KindEvalBatch` (issue #1247, ADR 0054's 2026-10-06 amendment)**
  re-decide from engine state, and the derived rows `materialize_derived`
  writes (change-log on a unique `prefix||ts||ordinal` key, LSI/footprint rows
  keyed by item attributes) are not protected by per-key LWW. They no-op on
  replay when the decided base key (tombstone-aware, `key_reached_by_entry`)
  is **at or above** the entry's `ts` — at-or-above, not strictly above as for
  `TxnStage`, because a `KindEval*` entry's whole write set is ONE atomic
  `merge_batch` (a single WAL record), so an equal base row proves the entry
  fully landed and re-evaluating it would read its own post-state (`ADD`
  applied twice, a `not_exists` item failing on its own write and shifting
  every later item's change-record ordinal). `KindEvalBatch` is
  entry-granular (any item's key reached skips the whole entry; one pre-pass
  `get` per key is reused by the evaluation loop). `TxnResolve` decides only
  from an intent of its own `txn_id` (monotone: stage then resolve; a replayed
  resolve finds none) and `TxnStage`'s pending-write evaluation sits behind
  the `engine_ahead` gate above. Regressions:
  `tests/it/kind_eval_replay_stability.rs` (`ANIMUS_KINDEVAL_REPLAY_SEEDS`),
  `tests/it/txn_stage_replay_stability.rs`
  (`ANIMUS_TXN_REPLAY_SEEDS`); lesson
  `docs/lessons/code-patterns/2026-10-06-wal-replay-over-an-ahead-engine-must-not-re-decide-an-apply.md`.
- **`engine_applied` vs `last_applied`.** The two-task split (below) means the
  core's `last_applied` (a buffer cursor the consensus loop advances) *leads*
  the engine. Linearizable reads therefore gate on the separate
  **`engine_applied`** atomic the apply task advances after each merge —
  **never** `last_applied` (else a read could observe past the engine).
- **The engine-persisted applied watermark and the needs-snapshot state
  (issue #554, ADR 0017's 2026-09-02 addendum, ADR 0009's matching one).**
  `drive()` used to seed `engine_applied` from `core.last_applied()`, sound
  only because that value equals `snapshot_index` — a fact about the LOG's
  own compaction, not the engine — right after `RaftCore::recovered`. That
  seed silently overstates progress the instant the engine doesn't match the
  log (an engine destroyed and reopened fresh behind an already-compacted
  log — the host reconciler's engine-loss recovery, below — while the WAL
  survives): the fresh engine holds nothing, yet the seed claimed it was
  caught up through `snapshot_index`, and since the log tail still matched
  the leader's, no `InstallSnapshot` was ever triggered — the whole
  compacted prefix silently gone. **`applied.rs`** is the fix: a durable,
  engine-global marker (`seal.rs`/`ceiling.rs`'s own disjointness discipline)
  holding the highest index this tablet's OWN engine has merged, written
  **only** at compaction (`RaftCore::snapshot_upto`, before the WAL rewrite)
  and install (`install_engine_image`, same `merge_batch` as the image's own
  rows) — **deliberately not on every commit**: a first draft that did defeat
  `LsmEngine::clone_to_filtered`'s whole-file split dead-space exclusion for
  any table the marker rode along in (its key sorts above every row kind's
  own byte range, so a table carrying it no longer looks single-kind to that
  check) — never a correctness bug (`trim_split_child` backstops it
  regardless), but a real regression to that optimization on nearly every
  split instead of rarely, caught by `tests/inplace_split_dead_space.rs`.
  `drive()` reads this marker back at startup instead of `core.last_applied()`.
  **Snapshots carry markers at every version (issue #1251).** A replica must
  hold the sender's marker set or its stale-restage decision diverges (and
  "fail closed where markers are missing" diverges the other way). While
  `Gate::GlobalTables` is closed `engine_image` ships each marker under the
  wire-only image row kind `KIND_WIRE_RESOLVED_MARKER` (`0x80`, in no
  `ALL_KINDS` scope; the previous release drops an unknown kind) and
  `install_engine_image` files it back as the base-scope marker row; open, it
  is the plain base row. Never add a real row kind at `0x80`. A v1-cluster
  replica-identity check must compare intents in their v1 form (the sender also
  down-converts v2 intents, #1237).

  `RaftCore::state_machine_behind` (shared with `animus-control`, permanently
  inert there — see ADR 0009's addendum) is `true` whenever `engine_applied <
  snapshot_index`; this plane's consensus loop recomputes it **live, every
  loop iteration** (`engine_applied.load() < c.snapshot_index()`, same lock
  acquisition as `set_quiesce_engine_caught_up`, mirroring that established
  "feed the one external input the core can't see itself" pattern), never as
  a driver-latched one-shot flag — a latch cleared only by the async apply
  task left a window that produced a real, reproducible **livelock** while
  building this (see below). While behind: reads already refuse themselves
  (both linearizable and replica-local gate on `engine_applied`, seeded low —
  no extra code needed); campaigning is refused (`start_pre_vote`/
  `start_election` gain the identical `is_voter()`-style gate a learner
  already has, so a behind node can vote for others but never become leader
  over an incomplete engine); `apply_and_compact` skips draining
  `RaftCore::drain_apply` (committed effects just queue in the core —
  applying is idempotent per-key LWW, so nothing is lost once this resumes).
  `RaftMsg::AppendEntriesResp::needs_snapshot` (`#[serde(default)]`, and its
  own explicit `codec.rs` encode/decode arm, version `24` — `#[serde(default)]`
  only protects the `serde_json` WAL path) echoes this live flag on every
  response a behind replica builds; the leader's `handle_append_resp` resets
  that peer's `next_index` to 1 on `true`, which is all the pre-existing
  `replicate_to`/`snapshot_chunk_for` `next <= snapshot_index` check needs to
  ship a fresh chunked `InstallSnapshot` — built at the LEADER's own current
  applied index, reusing chunking/the resend cap/the lazy on-demand image
  build entirely unchanged.

  **Two gaps found only by running this, fixed as part of the same change:**
  `RaftCore::handle_install_snapshot`'s pre-existing "already at least this
  far along" short-circuit (`last_index <= self.snapshot_index` ⇒ drop the
  transfer, just ack) is precisely backwards for a behind replica — its own
  `snapshot_index` is exactly what it can't trust, and `last_index ==
  self.snapshot_index` is the overwhelmingly common #554 shape — so it was
  silently discarding the very offer meant to fix the gap; now gated `&&
  !self.state_machine_behind`. And the **livelock**: a leader that resets
  `next_index` to 1 on *every* `needs_snapshot: true` ack (not just the
  first) restarts a fresh transfer before the peer ever finishes digesting
  the last one, because `needs_snapshot` stays `true` on every one of the
  peer's own acks until *its* apply task actually merges the completed
  install — a window spanning several of its own heartbeat round trips.
  Fixed by `RaftCore::snapshot_served_through: BTreeMap<NodeId, u64>`
  (leader-only, volatile like `next_index`/`match_index`): once a peer is
  fully served at or past the leader's current `snapshot_index`, further
  `needs_snapshot: true` echoes are known-stale and left alone; a fresh
  compaction outpacing a still-slow peer naturally invalidates the entry.
  Confirmed live: without this guard, `next_index` oscillates between 1 and
  past-`snapshot_index` forever and `engine_applied` never leaves 0.

  **A third gap, found live 2026-09-27 under sustained bulk-seeding +
  auto-split on `--cluster-control 3 --cluster-data 5`:** the same
  `&& !state_machine_behind` override is true not only in the #554
  wipe-recovery case above, but also for the ordinary window after EVERY
  genuine `InstallSnapshot` completes — `last_applied`/`snapshot_index`
  advance synchronously inside `handle_install_snapshot`, while this
  plane's own `engine_applied` (recomputed live as `engine_applied <
  snapshot_index`, per this file's own entry above) is still draining
  `pending_install`. An unrelated, already-obsolete snapshot transfer
  landing during that window — `last_index` strictly below `last_applied`,
  a shape the wipe case never produces — used to sail through the override
  exactly like a genuine #554 offer, reinstalling and rewinding
  `last_applied`/`commit_index`/the log and re-triggering the identical
  `assert_ts_monotonic` panic the stale-snapshot-vs-`snapshot_index` fix
  above closed. Fixed in `animus-control::raft::handle_install_snapshot`
  by only excusing the exact-equality case from the redundancy check —
  `last_index < last_applied` is now unconditionally redundant regardless
  of `state_machine_behind`. Regression:
  `crates/animus-control/tests/stale_snapshot_no_rewind.rs`'s
  `stale_install_snapshot_below_last_applied_is_rejected_even_when_state_machine_behind`
  (this crate's own `tests/engine_wipe_needs_snapshot.rs` stays green,
  confirming the #554 shape — `last_index == last_applied` — is untouched).

  Regression: `tests/engine_wipe_needs_snapshot.rs` (a 3-replica `SimEnv`
  test crossing `COMPACT_THRESHOLD`, wiping a follower's — and separately
  the leader's — engine mid-run with the WAL intact) and the corpus mirror
  in `animus-test/tests/raftkv_linearizable.rs`. See `docs/engineering-
  lessons.md`'s Code-patterns entry: *a state machine's applied watermark
  must be the state machine's own, never the log's.*

  **Issue #811 (2026-09-10): a successfully-installed snapshot's `RaftCore`
  state must be forced into the SAME pass's WAL rewrite, not left for
  `behind`/`image_needed` to trigger later.** `RaftCore::
  handle_install_snapshot`'s successful-install path fixes up `snapshot_
  index`/the log/`snapshot_dirty` synchronously (consensus loop) the
  instant the last chunk lands, but nothing durably persists that: `has_
  unflushed_wal` checks only the pending log-append queue and the current
  term/vote (never `snapshot_dirty`), and `apply_and_compact`'s compaction
  branch only rewrites the WAL when `behind >= COMPACT_THRESHOLD` or a peer
  needs a fresh image — both false immediately after an install, since
  `engine_applied` and `snapshot_index` are set to the identical value in
  the same step. A replica that caught up ENTIRELY via `InstallSnapshot`
  (no log entry of its own ever logged) can therefore sit fully caught-up
  with a WAL file that still reads back empty — and a LATER genuine process
  restart (`sim.stop` + fresh `RaftKvNode::start`, never `sim.crash`/`sim.
  restart`) recovers from that empty WAL as a `fresh_group`, skipping
  `RaftCore::recovered` and leaving `snapshot_index == last_applied == 0`
  while `engine_applied` is correctly reseeded from the engine's own
  watermark — a permanent, non-convergent `behind` gap (`snapshot_upto`
  clamps to `min(engine_applied, last_applied)`, and `last_applied` is
  ALSO stuck at `0` on the fresh core) that pegs the apply task at `did_
  work = true` forever, spinning one CPU core with `run_for`/`run_until`
  never returning. **Fixed**: `apply_and_compact` now forces the
  compaction section's WAL-rewrite branch whenever this pass processed a
  `drain_pending_install`, regardless of `behind`/`image_needed`.
  Regression: `tests/restart_after_install_snapshot.rs` (`MemoryEngine` and
  `LsmEngine<SimEnv>`, both red-before/green-after). **Test-authoring
  gotcha this bug's own repro needed**: neither `run_for`'s virtual-time
  deadline nor `run_until_quiescent`'s step cap can bound this hang shape —
  the busy task's own `Future::poll` call never returns control to the
  executor at all (no `.await` point is reached between loop iterations
  once `did_work` stays `true`), so nothing short of a real OS-thread
  wall-clock watchdog (`std::thread::spawn` + `mpsc::Receiver::
  recv_timeout` around `sim.run_for`) can catch it in a test — see
  `docs/engineering-lessons.md`'s matching issue #811 entry for the full
  account, including why `crates/animus-test/tests/raftkv_linearizable.rs`'s
  own `Nemesis::StopRestart` (which always targets the current leader,
  never a snapshot-caught-up follower) structurally cannot reproduce this.
- **A stale `InstallSnapshot` can rewind a follower that already outran it
  via `AppendEntries`, corrupting `assert_ts_monotonic`'s ordering (found
  live: `panicked ... HLC ts did not strictly exceed the last applied ...
  witnessing chain is broken` on a real cluster under
  `--auto-split-bytes`).** `RaftCore::handle_install_snapshot`'s "already at
  least this far along" short-circuit (`animus-control/src/raft.rs`, the
  same #554 short-circuit documented in `animus-control/CLAUDE.md`'s
  Snapshot-transfer section) used to compare the offer's `last_index` only
  against `self.snapshot_index` — the log's last COMPACTION point, which
  lags `self.last_applied` whenever entries commit between compactions (the
  ordinary case under any real write load, since compaction is a periodic
  background sweep, not synchronous with every commit). Under a leader that
  floods/restarts snapshot transfers (compaction invalidating an in-flight
  transfer, `snapshot_upto`'s own doc — see the `COMPACT_DEFER_CEILING`
  discussion above), a follower can catch all the way up to some index N via
  ordinary `AppendEntries` while a STALE, already-obsolete chunked transfer
  built at an earlier, lower index M (`snapshot_index < M < N`) is still in
  flight; its final chunk sailed past the old guard (`M > snapshot_index`,
  "not yet redundant") and installed, resetting `last_applied`/
  `commit_index`/the log back down to M. The apply task then re-applies the
  rewound log tail, and its ts-carrying entries land strictly below the
  high-water mark the follower had already recorded before the rewind —
  `assert_ts_monotonic`'s panic (`animus-cp-data/src/lib.rs`). **Fixed** by
  comparing against `self.last_applied` instead of `self.snapshot_index` —
  always `>= snapshot_index` (the two coincide only immediately after an
  install) and the follower's true up-to-date position, independent of
  compaction cadence — keeping the pre-existing `&& !self.state_machine_
  behind` override intact (a behind node's own `last_applied`/
  `snapshot_index` are both log-derived facts it can't trust either way, per
  #554's own reasoning). Regression:
  `animus-control/tests/stale_snapshot_no_rewind.rs`, hand-driving a real
  leader/follower `RaftCore` pair so the follower's `last_applied` genuinely
  advances past a manufactured stale offer's `last_index` before delivering
  it — red before the fix (confirmed: reverting the guard to
  `self.snapshot_index` reproduces the exact rewind), green after.
  **Diagnostic improvement landed alongside**: `assert_ts_monotonic`'s panic
  now reports the offending entry's own `(index, term, KvCommand variant)`
  and the previous ts-carrying entry's — a bare "HLC ts didn't exceed the
  last applied" was undiagnosable live; this is exactly what made the live
  repro above traceable to `InstallSnapshot` at all. `kv_command_variant_
  name`/`LastAppliedTsEntry` are threaded through `apply_and_compact` like
  `max_applied_ts` itself (same lifetime, same single-writer discipline) —
  no logging added, purely richer panic context.
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
  the general lesson. **`TxnResolve` gained the identical `StageOutcome`-
  shaped outcome channel `TxnStage` already had, closing a second,
  independent gap the fence alone didn't (ADR 0018 §2 write-loss amendment
  §3/§6, closed 2026-08-29)**: a fence-miss no-op and a genuine resolve
  used to be indistinguishable to the caller — `wait_applied(index).await`
  is `true` either way. `ResolveOutcome` (`Resolved`/`Fenced`/
  `OutcomeMismatch`), recorded per apply in `ResolveOutcomes` (keyed by
  Raft log index, paired with the entry's own term — the identical
  `CasResults`/`StageOutcomes` term-identity discipline; see this file's
  `StageOutcomes` bullet above), is what a proposer now polls via
  `resolve_outcome`/`wait_resolve_outcome`; `RaftKvNode::txn_resolve`
  returns `Option<(HlcTimestamp, ResolveOutcome)>`, and **every caller must
  check the outcome** — only `Resolved` means the intent(s) actually
  changed. A second, independent bug surfaced fixing this: the apply arm
  used to clear `TxnTracker::unresolved_decided` (the entry
  `txn_resolver_loop`'s passive sweep reads to find decided-but-unresolved
  transactions) **unconditionally**, before this outcome was even computed
  — so a Fenced resolve erased this group's own memory that the
  transaction still needed resolving, even though nothing was actually
  written. Now cleared only on `ResolveOutcome::Resolved`. `animusd::
  ClientCtx::txn_resolve_participant` returns `Result<ResolveOutcome,
  String>` (a `CpRoute::None` is now a genuine `Err`, never a silent
  `Ok(())`); `txn_resolve_participant_retrying` is the bounded-retry
  wrapper (fresh `cp_route` every attempt) every production caller
  (`resolve_all`/`resolve_all_parallel`/`recovery_resolve`/
  `push_resolution_if_decided`) now goes through instead of the raw
  one-shot primitive — the actual fix for the acknowledged-write-loss bug:
  a fenced resolve now triggers a re-route-and-retry rather than being
  silently treated as done. See ADR 0018's 2026-08-29 amendment for the
  full account (including why `txn_decide`'s single-tablet convenience
  path deliberately still discards the outcome — it has no routing layer
  of its own to retry through) and `tests/txn_resolve_outcome.rs` for the
  primitive-level regression (a genuine fence-miss via a real in-place
  split, proven distinct from an ordinary resolve).
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

**`EngineFactory::local_tablets` — the reconciler's second fact source
(issue #722).** `gather_facts`/`plan` otherwise derive every fact
exclusively from replicated `Metadata` (`MetadataView`) plus this
reconciler's own in-process `LocalState` — never persisted, so a restart
starts from `LocalState::default()`. That is a real gap for one case: a
node that crashes while hosting a tablet, then restarts only after that
tablet's whole table has been dropped and the drop has already converged
everywhere else, comes back with an empty `LocalState` and a `Metadata`
view that, by the time its first tick ever runs, already never names the
dropped tablet at all — `gather_facts` then produces no fact whatsoever for
that tablet id, and `HostAction::Reclaim` (which only ever fires for a
tablet `LocalState` itself currently claims) can never target it: the
tablet's own private engine — real data, written before the crash — leaks
permanently. ADR 0024's own restart-convergence guarantee ("a replica that
was down during the drop restarts, re-hosts the tablet from its
marker/engine, then its GC loop reclaims it") depended on a durable
per-node marker that no longer exists (ADR 0050) and was never replaced —
see that ADR's 2026-09-07 amendment for the full incident.
`EngineFactory::local_tablets(&self) -> BTreeSet<TabletId>` closes it: a
second, restart-surviving fact source, listing every tablet id this node
currently has DURABLE local engine state for, independent of `Metadata`.
Default implementation returns empty (so a third-party trait implementor
still compiles); `MemoryTabletEngines` answers from its own registry keys,
`animusd`'s `LsmTabletFactory` by listing its data directory once and
parsing each file's own `db-t{tablet}-` prefix (the identical mechanism
`probe`/`destroy` already use, generalized from "does this one tablet's
prefix appear" to "which ids appear at all"). `Reconciler::tick` consults
it exactly **once**, on its very first tick after construction — never on
a later tick, since a local engine appearing after that first tick can
only be this same reconciler's own `Host`/`MaterializeSplitChild` action,
already tracked in `LocalState` — so the fix costs nothing in steady
state, only a one-time directory listing right after a restart.
`plan` gained a matching `local_tablets: &BTreeSet<TabletId>` parameter: a
new phase, before the pre-existing reclaim phase, folds any id present in
`local_tablets` but absent from a `known` set (every current
`view.tablets` key, **plus** every split child named on any tablet's own
still-live `inplace_split` intent — a pre-cutover child is materialized,
by design, before it has a map entry of its own, see
`MaterializeSplitChild`'s doc) into `LocalState::hosted`, so the
pre-existing, unmodified reclaim phase picks it up exactly like any other
hosted-but-now-absent tablet. **Safety argument**: an engine only ever
exists locally for a tablet id this exact node has, at some prior tick,
itself observed as real — `Host`/`MaterializeSplitChild` are the only two
actions that ever create one, and both fire only in reaction to observing
the tablet in `view.tablets` or a live split intent's own children — so a
locally-present id absent from `known` is never "a tablet that hasn't
appeared in `Metadata` yet" (structurally impossible by that argument),
only ever a genuine leftover. `plan`'s own doc has this argument stated in
full. Tests: `host.rs`'s own unit tests
(`a_locally_present_tablet_absent_from_the_map_is_reclaimed_even_with_
empty_state`, `a_pre_cutover_split_childs_engine_is_not_reclaimed_even_
with_empty_state`) and `tests/reconciler_corpus.rs`'s
`crash_then_drop_then_restart_reclaims_the_leftover_engine` scenario; see
`animusd/CLAUDE.md`'s own drop-table-GC entry for the `SimCluster`/
DynamoDB-wire and real-`LsmEngine` regressions this fix also carries.

- **`TabletFacts::config_excludes_me` means "this replica knows it was
  removed", by either channel (issue #1061, ADR 0058's amendment).**
  `gather_facts` sets it when the node's own log-derived voter config no
  longer lists it **or** `RaftKvNode::removed_by_leader()` — the leader's
  explicit `RaftMsg::Removed` notice, the only signal a replica that fell
  behind the leader's compacted prefix (a departing peer is never shipped a
  snapshot) or sat out a leadership change partitioned can ever get. `plan` is
  untouched: `Release` still requires replicated `Metadata` to exclude the
  node (so a stale/delayed notice to a replica that has since been re-added
  can never release it) and still debounces over `RELEASE_CONFIRM_TICKS`.
  The flag is volatile; `RaftKvNode::config()` still lists the node after a
  notice (the notice never rewrites the log-derived config). A test that
  waits for "the removed replica learned" must accept either
  (`!config().contains(me) || removed_by_leader()`) — a returning removed
  replica's own campaign usually triggers the notice before the leader's own
  schedule delivers the entry (`reconciler_corpus`'s
  `partition_blocks_release`). This added `RaftMsg::Removed`/
  `RemovedAck` (codec tags `14`/`15`). Regression:
  `tests/departing_removal_notice.rs` (a reconciler-hosted replica left behind
  the compacted log by a continuous writer: zero snapshot ships/restarts,
  told, not released while `Metadata` lists it, released once it does).
- **Release predicate and destructive-step recheck (release-vs-promote race,
  ADR 0031's 2026-09-30 addendum).** "Excluded" is `host::replica_excluded`
  (removal notice received, OR in neither `config()` nor `learners()`) — a
  **learner is a member**; `config()` is voters only, so testing it alone
  released mid-catch-up learners. `Reconciler::teardown(tablet, release)`
  re-checks before stopping the driver, and `finish_teardown` re-checks live
  state again right before `erase_tablet_files` for a `Release` (also on the
  parked `sweep_stopping` path via `StoppingNode::release`); a replica that
  is a member again keeps its files and its claim is cleared so the next
  `Host` re-adopts the intact disk. `Reclaim` never rechecks. `tick()` always
  gathers fresh facts, so a stale plan is only expressible by driving
  `finish_teardown` directly (`host.rs` unit tests). Corpus:
  `tests/release_race_corpus.rs` (`ANIMUS_RELEASE_RACE_SEEDS`). The
  leader-side follow-on (a wiped learner re-added while the leader's progress
  for it is stale being judged caught up) is fixed in `RaftCore` — see
  `animus-control/CLAUDE.md` and ADR 0058's 2026-09-30 amendment; corpus
  cells `a_readded_*_never_inherits_stale_replication_progress`.
- **Removal-notice observability (issue #1061 follow-up).** `drive`'s
  per-iteration core read also takes `RaftCore::removal_stats()` and
  `record_removal_stats` emits the growth as `Metric::CpRemovalNoticesSent`/
  `CpRemovalNoticesAcked`/`CpRemovalNoticesIgnored`/`CpDepartingPeersDropped`
  (`cp_removal_notices_sent`/`_acked`/`_ignored`, `cp_departing_peers_dropped`
  in `/admin/metrics`) — a per-node sink, like every other `Cp*` counter.
  `RaftKvNode::departing_peers()` and `snapshot_transfer_peers()` are the
  pure accessors `/admin/raftkv` surfaces as `departing` /
  `snapshot_transfer_peers` (leader-only, empty on an idle converged group; a
  group that keeps a peer in the second at zero write rate is re-offering an
  image the peer declines — `animus-control/CLAUDE.md`'s "declined offer"
  entry). A `CpSnapshotTransferRestarts` increment is **not** proof that a
  transfer restarted: `apply_and_compact` counts it whenever the idle
  ceiling overrides an in-flight transfer, and the compaction that follows
  can still no-op (clamped by a peer's `match_index` via `compaction_floor`),
  which is exactly how the declined-offer livelock inflated it every 2s.
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
- **Engine-loss recovery (issue #554, ADR 0031's 2026-09-02 addendum):
  `ensure_engine`'s (and `materialize_split_child`'s) first `open` failure
  is treated as a lost/corrupt local engine, not a transient fault to
  warn-and-retry forever.** Bounded to ONE destroy-and-reopen attempt per
  call (`factory.destroy` then a fresh `factory.open`) — a second failure
  falls back to the pre-existing warn-and-skip, so a genuinely unhealthy
  disk (every open failing, not just one tablet's) doesn't spin
  destroying/recreating tablet after tablet. Safe because this node's own
  Raft WAL/hard-state (`wal_file`) lives in a namespace an `EngineFactory`
  implementor's `destroy` never touches — no vote is forfeit, no
  double-vote is possible. `CpEngineOpenFailed`/`CpEngineRebuilt`/
  `CpEngineRebuildFailed` (ADR 0015) observe the outcome. **This
  destroy-and-reopen alone is NOT sufficient to prove recovery safe past
  a replica's own compaction point** — that is entirely the separate
  needs-snapshot mechanism's job (`applied.rs`, `RaftCore::
  state_machine_behind`, this file's own matching entry above): a first
  version of this recovery, built and regression-tested BEFORE that
  mechanism existed, passed at 20 writes (below `COMPACT_THRESHOLD` = 64)
  and silently lost the whole compacted prefix at 90 (past it) — see
  `docs/engineering-lessons.md`'s matching entry for the discovery.
  Regression: `tests/reconciler.rs::
  a_corrupt_replica_engine_is_destroyed_and_rebuilt_from_the_group` (90
  writes, past the threshold, proving the FULL recovery — destroy/reopen
  plus needs-snapshot — restores every pre-corruption key and preserves
  Raft safety, exactly one leader, no split-brain artifact).
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

### In-place split (ADR 0058 Train 2 rung 3; fork-first per ADR 0062)

**The invariant, since the issue #987 follow-up (2026-09-17): a local fork
always materializes, independent of whether this tick's view still shows
the parent.** `TabletFacts::pending_split` is gathered for EVERY hosted
tablet, every tick, regardless of what `view` says about it
(`gather_facts` used to gate the `RaftKvNode::pending_split()` call on the
view still showing an `inplace_split` intent — removed, since
`pending_split()` is a cheap point read, documented safe to call
unconditionally, and the old gate was exactly the bug: it made
`gather_facts` blind to a replica's own already-durable local fork the
instant this tick's view moved past the transient `Splitting` state). This
split the old single "phase 1.5" into two:

- **Phase 0.5, BEFORE `Host`**: for every hosted tablet whose own
  `pending_split` fact is `Some` (a local fork has already applied here,
  whether or not the CURRENT view still shows the parent or its intent),
  materialize any child not yet in `next.hosted`. Range/drop-range come
  from the parent's own still-live `range` when the parent is present in
  `view.tablets` (the ordinary in-flight-split window), or from each
  child's own already-published `Active` entry once the parent has been
  retired by `CutoverSplit` (the two are bit-identical by construction).
  Running this BEFORE phase 1 is what makes phase 1's own
  `!next.hosted.contains` gate correctly skip these child ids instead of
  hosting either one fresh through an ordinary, empty-engine `Host` — the
  defect this closed: a replica whose reconciler tick never lands inside
  the sub-second fork→cutover window (ADR 0062 rung 5) used to be handed a
  view that had already retired the parent and published both children as
  ordinary `Active` entries, materialize never fired (gated on the
  since-vanished intent), and phase 1 silently hosted both children with a
  fresh, empty engine — silent per-replica data divergence, and, if the
  skipped replica was the parent's own former leader, no replica ever
  campaigns for either child (`campaign` is only ever set on this path),
  leaving both stuck "forming" with no leader. See `host::plan`'s own
  phase 0.5 doc comment for the full incident and the safety argument for
  why every child `MaterializeSplitChild` can ever name is still provably
  a member of `known` (the issue #722 `local_tablets` safety net).
- **Phase 1.5, between `Host` and `Reconfigure`, propose-only now**: a
  tablet whose `Metadata` row still carries `Tablet::inplace_split` (the
  control plane's `MetaCommand::BeginSplitInPlace`) and whose own
  `pending_split` fact is still `None` takes this branch INSTEAD of the
  ordinary `Reconfigure` action — the two must never both fire for the
  same tablet in the same tick (an ordinary reconfigure would see the
  fork's own children as a foreign membership change and try to
  interfere). Once `pending_split` turns `Some`, phase 0.5 above already
  owns this tablet's materialization; this loop has nothing left to do
  for it.

**The Stage 1/2 learner-add-and-wait phase (ADR 0058 Train 2) is deleted
(ADR 0062 rung 5)** — `HostAction::AddSplitLearner`,
`INPLACE_SPLIT_LEARNER_CATCH_UP_THRESHOLD`, and the `TabletFacts::config`/
`learners`/`learners_caught_up` fields that fed it are all gone, along
with phase 1's "recruited via a child's own replicas" host-candidate
branch and phase 3's matching release exclusion — both provably dead
under the ADR 0062 rung 4 invariant (`SplitChild::replicas` IS the
parent's own replicas at propose time, and a `Splitting` parent's
`replicas` cannot change for the intent's whole lifetime —
`reconcile_placement`/`rebalance_placement` both skip non-`Active`
tablets — so nothing is ever named in a child's `replicas` that isn't
already a member of the parent's own).

- **Not yet forked here** (`RaftKvNode::pending_split()` answers `None`):
  the leader proposes the fork immediately (`HostAction::ProposeSplitFork`
  → `RaftKvNode::propose_split_tablet`) — no gate beyond ordinary Raft
  agreement on the fork entry itself. Every replica named in the intent's
  children already hosts the parent as an ordinary voter (ADR 0062 rung 4),
  so there is nothing left to recruit or wait on before forking.
- **Already forked here** (`pending_split()` answers `Some`):
  `HostAction::MaterializeSplitChild` fires for BOTH children on EVERY
  fork participant — not filtered by either child's own final `replicas` —
  since `pending_split().bootstrap_voters` (the parent's own current voter
  set, captured once in the data-plane's own apply) is what both children
  actually bootstrap with. Since ADR 0062 rung 4 this is no longer a
  superset of either child's own `replicas` (`SplitChild::replicas` IS the
  parent's own current replicas, see `animus-tablet`'s doc), so
  `bootstrap_voters` and both children's target replica set coincide in
  the common case and there is nothing left to trim at fork time — see
  `split.rs`'s "No more learner union" doc for the accepted fork-D
  residual this leaves (an unrelated in-flight rebalance's own learner on
  the parent, self-healed by an ordinary post-cutover `Reconfigure`).
  Claimed into `LocalState::hosted` optimistically (the same discipline
  `Host` uses) AND into `LocalState::split_forming`, which exempts a
  pre-cutover child from phase 3's reclaim check (it is `hosted` but, by
  design, absent from `Metadata` until `CutoverSplit` runs) — pruned the
  instant the child appears in `Metadata` as a real `Active` entry.
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
  campaign` — see "Deterministic first leader" below. **A `probe` hit
  alone is NOT "fully materialized"** (a fixed bug — see `trim_marker.rs`'s
  module doc for the full account): `probe` only proves `clone_engine`
  committed, not that `trim_split_child` finished on top of it, and a
  crash or a genuine `delete_range` failure between the two used to leave
  the resume branch reopening an untrimmed clone and hosting the child
  directly on it, leaking the sibling's rows and the parent's whole change
  log/cursors into the child **permanently**. `trim_split_child` now
  writes its own durable completion marker (`trim_marker::
  trim_marker_key(child)`) as its last step, and the `already_cloned`
  resume branch checks THAT — not `probe` — before skipping the trim;
  when the marker is absent it re-runs `trim_split_child` instead
  (idempotent, and provably safe: the child's Raft group can only ever
  start after the marker write succeeds, so an absent marker proves this
  replica's group has never run and holds no committed state a re-trim
  could clobber). Regression: `tests/split_trim_failure.rs`. **`clone_engine` is
  now range-aware (ADR 0058 fork closed, 2026-08-31)**: immediately before
  calling it, `materialize_split_child` computes the child's own
  physical keep-set once — its declared `range` sliced through
  `KIND_BASE`/`KIND_LSI`/`KIND_FOOTPRINT` via `StorageScope::with_kind(..)
  .physical_bounds()`, nothing of `KIND_CHANGE`/`KIND_CURSOR` (a child is
  always born empty of those two, per `trim_split_child`'s own rule below)
  — and passes it down; the backing `LsmEngine::clone_to_filtered` does
  whole-file assignment with it (a source SSTable wholly outside every
  keep range is never linked into the child's namespace at all). This does
  **not** replace `trim_split_child` — a table straddling the keep/drop
  boundary is still linked whole, so the post-clone `delete_range` over
  the sibling's own range remains necessary and still correct (deleting a
  range from a table that was never linked in the first place is a
  harmless no-op) — it only removes the whole *wholly-sibling* tables from
  the clone before trim ever runs, closing the dead-space debt a
  cold/quiesced child used to leave for its own compaction to eventually
  reclaim, and the per-engine size-accounting double-count across the two
  children. See `animus-storage/CLAUDE.md`'s `clone_to_filtered` entry for
  the primitive's own full design and test list; `tests/
  inplace_split_dead_space.rs` (this crate) is the file-level regression —
  a parent seeded with explicitly flushed, range-disjoint SSTables forks,
  and each child's own materialized engine is asserted, by SSTable
  sequence number, to have excluded its sibling's wholly-outside table.
- **`EngineFactory::clone_engine(&self, source: &S, target: TabletId, keep:
  &[(Vec<u8>, Option<Vec<u8>>)])`** takes the source's own already-open
  handle, not a bare `TabletId` — see its own doc for why re-opening would
  be unsafe. `keep` is an optimization hint, never a correctness
  requirement of the trait: `MemoryTabletEngines`'s implementor ignores it
  (no per-file dead space to save for an in-memory engine, and
  `trim_split_child` still runs immediately after and makes the result
  correct either way), while `animusd`'s `LsmTabletFactory` threads it
  straight into `LsmEngine::clone_to_filtered`.

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
  `ProposeSplitFork`). No new coordination, no new
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
    `is_voter()`), and every child's `bootstrap_voters` IS the parent's
    own voter config at the fork (ADR 0062 rung 4 — no longer a
    voter-**and**-learner union) — so it is always a voter of the child
    too; there are, in fact, no learners at all on a freshly-bootstrapped
    child (every `bootstrap_voters` member starts as a voter). `drive`
    additionally
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
`ANIMUS_INPLACE_SPLIT_SEEDS=200`, and past `=1000` locally): the fork-first
happy path (ADR 0062 rung 5 — the fork proposes immediately, no learner
phase; exact per-child data partitioning, empty change/cursor scopes at
birth, each child's bootstrap voter set exactly the parent's own voters —
no over-replication, no trim step needed — post-cutover convergence, parent
reclaim), the G4 crash window itself, a concurrent unrelated rebalance
proving non-interference in both directions with the split's own parent
handling, the immediate-campaign **fast path**
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
`tests/inplace_split_dead_space.rs` is a separate, small, single-node
`LsmEngine<SimEnv>` regression (ADR 0058 fork closed, 2026-08-31) proving
`clone_engine`'s range-awareness at the FILE level — this corpus's own
`MemoryEngine`-backed scenarios above already prove per-row correctness
but have no files to exclude, so they can't prove whole-file assignment
by themselves.

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

**Emitted in this fixed order: `MaterializeSplitChild` (phase 0.5) →
`Host` → `ProposeSplitFork` (phase 1.5) → `Reconfigure` →
`Release`/`Reclaim`.** (Since the issue #987 follow-up, 2026-09-17:
`MaterializeSplitChild` moved ahead of `Host` — see "In-place split"
above — so `next.hosted` already claims a locally-forked split's children
before phase 1's own `Host` gate ever sees them, and so a split's own
`Reclaim{parent}` — planned in the same tick whenever the parent has
already left `view.tablets` — always executes AFTER the clone from the
parent's still-open engine has completed, never before.)
`ProposeSplitFork`/`MaterializeSplitChild` (ADR 0058 Train 2 rung 3;
`AddSplitLearner` deleted by ADR 0062 rung 5) are mutually exclusive with
`Reconfigure` per tablet — see "In-place split" above. `Release`/`Reclaim`
tear down a tablet moved off or dropped/retired, respectively. Tablets are
split-only (ADR 0044) and ranges immutable (ADR 0050): the zero-copy
`ProposeSeal`/`NarrowScope` actions were deleted in the rung-7 sweep, as
merge's `WidenScope`/`Absorb` were by ADR 0044 — a hosted-but-now-absent
tablet is unconditionally `Reclaim`ed; its two causes (dropped table,
cutover-retired split parent) demand the identical action, so no
disambiguation is needed.

- **`Reconciler` teardown** (`Release`/`Reclaim`) — **the reconciler
  group-driver-stop-timing fix**: unregister from routing *before* touching
  the driver, `shutdown()`, then poll `is_stopped()` bounded by a much
  shorter `RECLAIM_STOP_GRACE` (~500ms — the common case: one persist round
  plus one apply pass). If the driver hasn't stopped by then, **park** the
  halted handle in a new `stopping` side map and return — never
  re-registered via `on_host` (a halted driver can never serve again) and
  `LocalState` left untouched, so `plan` keeps re-emitting the identical
  action every tick, which `teardown` now recognizes as a no-op for an
  already-parked tablet rather than re-running the zombie-claim path
  against a live-if-halted driver. `Reconciler::sweep_stopping`, run once
  at the very start of every `tick()` (before `plan`), finishes a parked
  teardown once `is_stopped()` actually goes true (delete the tablet's
  engine files + WAL, then `confirm_torn_down`) and — only once a parked
  teardown has sat past the full `RECLAIM_STOP_TIMEOUT` (10s, now purely an
  observability threshold, checked by the sweep rather than awaited inline)
  — logs the existing "group driver did not stop in time" warning once and
  bumps `Metric::CpReconcilerStopTimeout`. **Why this had to change**: the
  old inline wait ran *serially inside `tick()`*, so a genuinely slow
  driver (a real backlog under the apply task's own per-*pass*, not
  per-*entry*, `halted` check — since fixed in `apply_and_compact`, see
  `lib.rs`'s own effects-loop comment) blocked that tick's every OTHER
  planned action — hosting a split child, a `Reconfigure`, a
  `ProposeSplitFork` — for up to the full old timeout, repeated every tick
  since a timed-out teardown was never confirmed. Halting the driver and
  parking it decouples one slow stop from every other tablet this node
  hosts. (Merge's `Absorb` teardown — which skipped the narrow/
  `erase_scope()` and drained the committed log before halting, since the
  absorbed data was about to be served elsewhere — was removed along with
  `TeardownKind::Absorb`; see the Key invariants entry above for what
  remains of that mechanism's lesson.)
- **`Reconciler` teardown closes the tablet's stream (ADR 0026, 2026-09-28
  amendment).** `teardown` calls `self.env.close_stream(tablet.0)` in all
  three of its exit paths, each **only once the driver is confirmed
  stopped** (never eagerly — see `Network::close_stream`'s own caller
  contract, ADR 0026): the immediate-stop path (right after the `while
  !node.is_stopped()` loop above exits without parking), `sweep_stopping`'s
  own finishing branch (right after it observes `is_stopped()` true for a
  previously-parked teardown), and the zombie-claim backstop (immediately,
  since there is no driver there at all to still be polling). This is the
  actual fix for a real, live-measured leak: a tablet released from this
  node (a split parent, or a replica dropped by reconfiguration) whose
  peers keep addressing it during the teardown grace window used to have
  its frames queue in this node's `Demux`/`SimEnv` inbox forever — closing
  the stream here means such a frame is now discarded-and-counted at
  arrival instead. The stream reopens automatically the moment this node
  re-hosts the same tablet again (`RaftKvNode::start_hosted*`'s own driver
  calling `recv_stream(stream)` for the first time since the close), so a
  node dropped from a tablet's replica set and later re-added is not
  permanently locked out. **Investigated and confirmed NOT to need the
  same fix**: `HeartbeatBatcher`'s own per-group `HeartbeatInbox`
  (`heartbeat_batch.rs`) already deregisters correctly — `unregister_hosted`
  runs from the consensus loop's own `halted` branch, before `stopped` is
  set, so by the time `Reconciler::teardown`'s `is_stopped()` check can
  ever see `true`, the heartbeat demux registration is already gone. See
  ADR 0026's 2026-09-28 amendment for the full design record (the
  close/reopen/tombstone semantics, and why an explicit close was chosen
  over inferring "abandoned" from a liveness heuristic) and `tests/
  demux_stream_teardown.rs` for the fault-injecting end-to-end regression
  (a live follower released under message loss/delay, mirroring the
  `ANIMUS_RECONFIGURE_DROP_SEEDS` corpus's own shape, issue #781 — proving
  the released node's stream ends empty and tombstoned, convergence still
  holds, and a later re-add reopens it).

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
    release that needs no round number to be right). **`persist_wal` issues ONE `env.append` per round
    (all drained records concatenated) plus one `env.sync`, never one append per
    record** (issue #1092: per-record appends made a 512-entry `AppendEntries`
    batch cost 513 disk latencies on a `sync_delay` `SimEnv` disk — a joining
    learner's ack sat gated behind one round for ~10s of virtual time, which
    read as a replication stall; regression `tests/wal_round_single_append.rs`,
    see `docs/lessons/testing/2026-09-29-a-slow-disk-model-must-price-a-batch-once-not-per-record.md`). **`persist_wal` is
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
    Genuinely racing this from outside — a real `shutdown()` call
    interleaving mid-pass under `SimEnv` — has no reachable window: unlike
    `persist_wal`'s pending-write queue (a bare synchronous `core` write
    bypassing the driver loop's own check entirely, so a
    `put`-then-`shutdown()` beat reaches it deterministically),
    `apply_and_compact`'s work source (`drain_apply`) only becomes
    non-empty through the *apply task's own prior progress*, and its
    effects loop — once entered, after that same iteration's own `halted`
    check already passed — runs uninterrupted to completion under `SimEnv`
    (disk ops resolve without yielding), so there is no reachable window
    for an external test driver to inject `halted` between the check and
    this call the way `persist_wal`'s own regression does. **A dedicated
    regression exists anyway** (issue #834,
    `tests/batch_txn_resolve_apply_fault.rs`): rather than racing a real
    concurrent `shutdown()`, a `FaultyEngine` test double's own
    `merge_batch` override calls `shutdown()` itself, in-line, immediately
    before returning the injected `Err` — the deterministic stand-in for
    "halted is already true by the time `flush_pending`'s error path
    observes it," proving the tolerance without needing the unreachable
    real race. When idle it races a new
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
    `tests/apply_signal.rs`. **The safety poll is not armed while the group
    is quiesced (issue #1180):** `apply_loop` checks `is_quiesced()` under
    the core lock after each idle pass and, if quiesced, parks on
    `ApplyPending` alone, so a quiesced group has an empty `SimEnv` timeline
    (`tests/it/quiesced_apply_no_poll.rs`). The consensus loop keeps a
    loop-local `quiesced_seen` and raises `apply_signal` on every
    quiesced/awake transition it observes (comparing the previous
    iteration's state, this iteration's top sample, and its post-step
    sample, so an un-quiesce done outside the loop — local propose, `wake()`,
    `read_barrier` — is still caught, since each also wakes the loop). That
    notify is what re-arms the poll for signal-less work
    (`take_snapshot_needed` can only be set by a leader's replicate cycle,
    and `quiesce_entry_ok` requires `!snapshot_needed` / no snapshot
    machinery, so a quiesced group cannot reach it without first
    un-quiescing). A new apply-task wake source that can fire while
    quiesced must raise `apply_signal` itself.
  - **`Freeze` and `SplitTablet`'s own whole-range seal-marker writes are
    halted-gated too (issue #939)** — the same class of bare `.expect(..)`
    hard panic `flush_pending`/the WAL-compaction `replace` path above were
    already fixed for (issue #278 item 1 and its follow-up), just on a
    `storage.merge(&marker_key, ..)` call neither of those fixes reached.
    `merge_seal_marker_or_halted` (`lib.rs`, next to `flush_pending`) is the
    shared helper both arms call: tolerated iff `halted` is already set, a
    hard panic otherwise. **The tolerance is deliberately not just "don't
    panic"**: on a tolerated failure the caller must not proceed as though
    the marker were durable — no `sealed` push, no `frozen` latch, no
    `max_index` advance for that entry (a `continue` back to the loop's own
    top-of-iteration `halted` check, which — see immediately above — ends
    the pass on its next turn), and for `SplitTablet` specifically, no
    write of the separate fork-payload marker either (a fork payload
    without its own durable seal marker would let `pending_split()` answer
    `Some` for a tablet that was never actually sealed). Regression:
    `tests/seal_marker_halted_gate.rs` (a `FaultyEngine` whose `merge`
    fails-and-self-`shutdown()`s in-line before returning, the identical
    deterministic stand-in `tests/batch_txn_resolve_apply_fault.rs` uses for
    `flush_pending`'s own tolerance, since — like that file's target — this
    site's racing work source is the apply task's own in-progress entry, not
    a bypassable queue a real concurrent `shutdown()` could reach
    deterministically under `SimEnv`; see the "Not every sibling site..."
    lesson in `docs/lessons/testing/`).
  - The WAL is written by both tasks (append vs. compaction rewrite),
    serialized by the async `wal_lock`; compaction snapshots only up to
    `engine_applied` via `snapshot_upto` (not `last_applied`, which the engine
    hasn't merged). **Issue #1116: on the per-group-file path the rewrite's
    slow half no longer runs under `wal_lock`, nor on the apply task.** Under
    the lock the apply task captures the image (`wal_image`), drains the
    pending records and makes them durable in the LIVE WAL like an ordinary
    persist round (so a later round can never ack over a gap), arms
    `wal_lock.tail()` and releases the lock. A spawned task
    (`rewrite_wal_staged`) then `Disk::stage_replace`s the image (write +
    fsync of `{wal}.tmp`) unlocked, drains the tail of rounds persisted
    meanwhile into the staged file (`stage_extend`; `persist_wal` pushes each
    durable round's record bytes onto `RewriteTail`), and takes the lock only
    for the last tail drain, `markers().invalidate()` and
    `Disk::commit_staged` (rename + directory fsync). The swapped-in file is
    `image ++ every round since`, the bytes a rewrite under the lock would
    have produced; a crash before the swap leaves the live WAL (holds every
    acked record), after it the new file (tail synced before the rename).
    Residual under the lock: the rename + directory fsync, plus one small
    fsync only if a round landed since the last unlocked catch-up. At most one
    rewrite is in flight (`WalRewriteSlot`); a threshold-only trigger waits
    for it, the take-once `image_needed`/just-installed-snapshot triggers
    wait it out, and `apply_loop` will not report `apply_stopped` while one
    runs (the GC deletes the files once `is_stopped()`). Why the apply task
    too: reads confirm off `engine_applied`, which only that task advances, so
    a rewrite run inline there stalls every confirm for its whole fsync even
    with the lock free (the first cut of this fix kept the rewrite inline and
    its sim test still saw 3s). The **shared-WAL path is unchanged** (one
    physical file for all groups: `compact_group` still rewrites it under
    `wal_lock`, and its queue is FIFO behind the rewrite regardless).
    `ProdEnv` over an encrypted disk uses `Disk`'s default staged methods
    (correct, but the swap re-reads and `replace`s under the lock). Tests:
    `tests/it/wal_rewrite_no_stall.rs` (slow-rewrite `SimEnv`,
    `DiskConfig::set_replace_data_delay`; failed at 3.06s on main) and
    `tests/it/wal_rewrite_crash.rs` (whole-cluster crash at 12 offsets into a
    stalled rewrite, plain and torn/corrupt tails;
    `ANIMUS_WAL_REWRITE_CRASH_SEEDS`).
    The drain is still a **persist round** (issue #279): it goes through
    `persist_round::drain_for_round`, and the round completes as soon as the
    drained records are durable in the live WAL.
    Compaction is skipped while `halted`. `is_stopped()` requires *both* tasks
    stopped (`stopped && apply_stopped`) before the GC deletes artifacts.

    **A threshold-triggered compaction is deferred while a peer's snapshot
    transfer is in flight (issues #532/#537, `COMPACT_DEFER_CEILING`).**
    `snapshot_upto` unconditionally drops every peer's in-flight chunked
    `InstallSnapshot` progress the instant the base moves again (required
    for correctness, unchanged by this fix — see that method's own doc,
    `animus-control`). Under sustained writes, ordinary `COMPACT_THRESHOLD`
    pressure could re-cross faster than a lagging peer's own multi-chunk
    transfer could complete, restarting it from chunk 0 forever — the
    residual finding beyond `MAX_APPEND_ENTRIES_BATCH` alone (see that
    constant's own doc, `animus-control`). `apply_and_compact` now checks
    `RaftCore::snapshot_transfer_in_flight()` and skips a
    `behind >= COMPACT_THRESHOLD`-only trigger (never an `image_needed` one
    — a peer is actively waiting on that image) while some transfer is
    genuinely in flight, up to `COMPACT_DEFER_CEILING` (`COMPACT_THRESHOLD
    * 8`) past which compaction proceeds regardless, bounding the WAL even
    against a transfer that will never complete. Policy lives entirely
    here, in the driver; the core's own correctness argument is unchanged.
    Regression: `tests/learner_catchup_under_load.rs` (proven red with
    either this fix or the batch cap reverted, green with both). The real
    `ProdEnv` end-to-end bench (`animusd/tests/
    cluster_gt_rf_split_bench.rs`) still converged in only 1 of 3 runs on
    the validating host with these first two fixes alone — closed by a
    THIRD mechanism, unbounded `InstallSnapshot` chunk resend *frequency*
    (`animus-control`'s `SnapshotResend` gate, ADR 0009's third 2026-09-01
    amendment): 3 of 3 with all three fixes in place. See that amendment
    for the full account and `animus-control/CLAUDE.md`'s matching entry
    for the mechanism itself.
  - **`COMPACT_DEFER_IDLE_CEILING` (issue #898 follow-up, 2026-09-15): a
    never-acking peer (down, partitioned, or crashed) can hold
    `snapshot_transfer_in_flight()` true forever, wedging the defer above
    indefinitely once write volume stops growing `behind` past
    `COMPACT_DEFER_CEILING`.** Found regression-testing `animus-control`'s
    own issue #898 fix, which widened the shared `RaftCore::
    snapshot_transfer_in_flight()` to also count a chunk that has been SENT
    but not yet acked — correct for that plane's own gap, but it also made
    THIS plane's identical defer above hold forever for a chunk sent to a
    peer that will never ack at all, regressing the pre-existing
    `tests/hlc_differential_skew.rs::receiver_installs_the_durable_high_
    water_mark_not_just_the_rows`. Fixed by an **idle-progress-gated**
    companion ceiling, mirroring `animus-control`'s own
    `SNAPSHOT_COMPACT_DEFER_IDLE_CEILING` exactly: `apply_and_compact`
    sums `RaftCore::snapshot_chunk_advances` (a genuine forward-progress
    counter, the same one `snapshot_resend_bound.rs` uses) across every
    peer `RaftCore::snapshot_transfer_peers` names, and resets a
    `compact_defer_since: Option<Nanos>` local (owned by `apply_loop`) to
    `now` every time that sum changes — so the ceiling (2s) bounds idle
    time since the last genuine advance, never total transfer duration
    (a flat "time since streak started" ceiling was tried first and
    rejected for exactly this reason: generous enough for a real slow
    transfer's total duration is far too generous a wait for one already
    proven dead). `RaftCore::snapshot_transfer_in_flight()` itself needed
    no further change. See `animus-control/CLAUDE.md`'s matching "Fifth"/
    "Sixth" entries and `docs/lessons/testing/
    2026-09-14-control-snapshot-catch-up-stall.md` for the full incident
    — including the standing lesson that any change to shared `RaftCore`
    gates on `cargo test -p animus-cp-data` run in FULL, never `--lib`.
  - **`COMPACT_DEFER_CEILING` itself was the live flood (PR #1047,
    2026-09-27): a fixed `behind`-sized ceiling is not a "has this transfer
    stalled" signal, it is only a bound on how far `behind` can grow
    BETWEEN invalidations.** Under sustained writes fast enough relative to
    a slow/contended peer (roughly a write per millisecond against a peer
    with a ~200ms disk round trip — many hosted tablet groups contending
    for one node's single-threaded consensus loop, the field shape this
    issue was filed against), `behind` re-crossed the original ceiling
    (`COMPACT_THRESHOLD * 8` = 512) in well under a second — far faster
    than a real multi-chunk transfer to that peer could land — so
    `snapshot_upto` restarted it from chunk 0 on a tight, self-sustaining
    cycle: confirmed live at tens of thousands of chunk ships per node over
    a couple of minutes against zero completed installs, a learner's
    `match_index` pinned for the entire run. The ceiling was doing exactly
    what it was built to do; it was simply the WRONG signal to force a
    still-*advancing* transfer out early — that job already belonged to
    `COMPACT_DEFER_IDLE_CEILING` above, which measures idle time since the
    last genuine advance, not `behind`. Fixed by demoting the ceiling to a
    last-resort emergency bound and renaming it
    `COMPACT_DEFER_EMERGENCY_CEILING`, raised to `COMPACT_THRESHOLD * 64`
    (4096, 8x the old value): `threshold_hit` no longer treats `behind`
    crossing it as sufficient reason to force a transfer out on its own —
    only `COMPACT_DEFER_IDLE_CEILING`'s idle-since-last-progress check may
    do that now; the emergency ceiling exists purely to bound worst-case
    WAL/log retention for the pathological case of a transfer that keeps
    inching forward just often enough to keep resetting the idle clock
    without ever landing (a real transfer that is merely large and slow is
    never penalized by it — see that constant's own doc for the full
    reasoning). A new `Metric::CpSnapshotTransferRestarts` (`animus-env`)
    counts a forced-out-while-still-in-flight event directly, rather than
    inferring the flood from ships-vs-installs after the fact. Regression:
    `tests/snapshot_transfer_survives_compaction.rs` — proven red against
    the pre-fix `behind`-alone trigger (caught_up=false, restarts in the
    single digits to low tens over a short run, zero installs) and green
    with the fix; a second scenario in the same file (a fully partitioned,
    never-acking peer) proves the emergency ceiling still bounds log growth
    on its own. **Fixing this regressed `tests/learner_catchup_under_
    load.rs`'s own tuned drain budget** — not a correctness bug, but an
    expected consequence: the new, more patient defer policy legitimately
    lets more uncompacted log accumulate before compaction resumes, so the
    post-install `AppendEntries` catch-up has proportionally more to
    replay. Fixed by raising that test's own `DRAIN_POLLS` (see its own
    comment) — a reminder that any of this defer machinery's own sibling
    tests need re-checking, not just the one under active investigation,
    whenever its trigger conditions change. See `docs/lessons/testing/
    2026-09-27-a-defer-budget-that-resets-on-state-change-is-not-a-bound-
    on-that-state.md` for the generalized lesson.
  - This is also where `engine_applied` vs `last_applied` (Key invariants)
    comes from.
  - **Follower-aware compaction retention (ADR 0017's 2026-09-27
    amendment, PR #1047's own follow-up)**: everything above bounds how
    long an ALREADY-in-flight transfer gets before compaction forces it
    out; it does nothing about a peer that hasn't fallen behind enough to
    need a snapshot AT ALL yet, but is close to it. `apply_and_compact`
    now clamps a THRESHOLD-triggered base advance (never an `image_needed`
    one, and never the just-installed-snapshot WAL rewrite — both still
    go straight to `ea`, see the clamp's own comment) to `ea.min
    (compaction_floor)`, where `compaction_floor` is `RaftCore::
    compaction_floor(COMPACT_RETENTION_CAP_ENTRIES)` (`animus-control::
    raft`) — `min(match_index)` over every VOTER within
    `COMPACT_RETENTION_CAP_ENTRIES` (4096) of `last_log_index()`, `None`
    (compact freely) if every voter is either caught up near `ea` already
    or excluded by the cap. This is what closes the field-measured flood
    this whole PR #1047 stack was opened against (18,816 installs / 4.27M
    chunks / 1,029 restarts over 31 minutes, 25-80 groups per node): a
    voter only briefly behind now keeps catching up via `AppendEntries`
    instead of falling off the compacted log every `COMPACT_THRESHOLD`
    applies. **Learners are deliberately excluded** from this floor — see
    `RaftCore::compaction_floor`'s own doc and `docs/lessons/testing/
    2026-09-27-follower-aware-compaction-voters-only.md` for why (a bad
    interaction with the `state_machine_behind`/`needs_snapshot` machinery
    two bullets below). The control plane's own `meta_apply_and_compact`
    does not use this — see the ADR amendment for why (a single small
    per-cluster group, never the flood's own mechanism).
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
- **`SharedWal` is wired into this exact persist path behind
  `--shared-wal`/`--no-shared-wal` (C-05 PR 2 wired it, PR 3 — 2026-09-06,
  same day — cut it over to on-by-default; ADR 0028's amendments)** — an
  alternative to the per-group `raftkv.wal` file above, not a replacement
  for it: every `RaftKvNode::start_*` constructor gained a trailing
  `shared_wal: Option<Arc<animus_control::SharedWal<KvCommand, KvState>>>`
  parameter (threaded onto `DriveState`), and both drainers described in
  "What's non-obvious" above branch on it:
  - **`persist_wal`** (the consensus loop's own drainer) takes the shared
    handle plus this group's `TabletId` and the node's `MetricsHandle`
    now. With a handle present it calls `SharedWal::append_tagged(env,
    SHARED_WAL, tablet, &records)` instead of its own `env.append`-loop +
    `env.sync` — one **tagged** record per `WalRecord`, written under
    `SharedWal`'s own internal `wal_lock`-equivalent (a `futures::lock::
    Mutex` serializing every hosted group's writers into one coalesced
    `append`+`sync` round, C-05 PR 1) rather than this group's own
    private `wal_lock`. Every halted-gating/hard-panic-on-a-live-error
    rule described above for the per-group path is unchanged — a
    `SharedWal::append_tagged` error is tolerated iff `halted`, a hard
    panic otherwise, identically. Increments `Metric::CpSharedWalSyncs`
    on success (a per-physical-write counter, not per-group-append — see
    "one fsync per round" below).
  - **`apply_and_compact`** (the apply task's compaction rewrite) takes
    the shared handle too. With a handle present, compaction calls
    `SharedWal::compact_group(env, SHARED_WAL, TabletId(tablet),
    c.wal_image())` instead of building its own whole-file `buf` and
    calling `env.replace(wal, &buf)` — `compact_group` **whole-file
    rewrites the shared file**, replacing only this tablet's own record
    slice (`group_tails[tablet]`) while re-serializing every *other*
    hosted group's own latest tail verbatim
    (`encode_multiplexed_image`) — never touching a sibling group's own
    records. `erase_tablet_files` (`host.rs`) mirrors this split: with a
    handle present it calls `SharedWal::forget(env, SHARED_WAL, tablet)`
    (a compaction with an empty image for this tablet — the "GC" this
    file's Reconciler entry describes) instead of `env.remove(&wal_file
    (tablet.0))`. Increments `Metric::CpSharedWalGcRewrites` on success.
  - **Recovery** (`drive`'s startup section): with a handle present, the
    file name resolved is `SHARED_WAL` (`"raftkv.wal.shared"`, exported
    alongside `WAL`) rather than this group's own `wal_file(stream)`, and
    the initial `PersistedState` comes from `SharedWal::recovered_state
    (tablet)` — a per-tablet replay over whatever `group_tails[tablet]`
    `SharedWal::open` seeded at node startup (once, before any group's
    own `drive()` runs — see `host::Reconciler::enable_shared_wal` below)
    — instead of a direct `env.read(&wal_file(..))` +
    `PersistedState::decode`/`replay`. Every downstream witnessing-loop/
    `fresh_group`/`RaftCore::recovered` step is unchanged either way —
    both branches produce the identical `PersistedState` shape, just from
    a different physical source.
  - **`host::Reconciler::enable_shared_wal(shared: Arc<SharedWal<..>>)`**
    (mirroring `enable_heartbeat_batching`'s existing shape) is the one
    production entry point: `animusd` calls it once per node, right after
    `SharedWal::open`, before hosting anything — every one of
    `Reconciler::host`/`materialize_split_child`'s calls into
    `RaftKvNode::start_hosted*` and `erase_tablet_files`'s GC path then
    thread `self.shared_wal.clone()` through automatically. A node with
    the flag off (`self.shared_wal: None`) takes every branch above's
    `None` arm — byte-identical to pre-PR-2 behavior, including the exact
    physical file layout (`raftkv.wal.shared` is simply never created).
  - **`host::check_wal_layout(env, shared_wal) -> io::Result<()>`** (free
    function, corrected 2026-09-06 — an earlier draft of this PR shipped
    the layout-mismatch case as a silent, deliberate data reset instead;
    see the ADR amendment's own corrected paragraph for why that was
    wrong, not merely conservative) is the loud-failure half of the flag:
    called once, from `animusd`'s node-start path, **before**
    `SharedWal::open` and before any tablet's own `drive()` recovery runs.
    A directory listing only (`Env::list()`, never a file open) — it
    refuses (a plain `io::Error::other`, naming both layouts and the flag)
    whenever `shared_wal` disagrees with what the data directory already
    holds: per-group `{WAL}.<stream>` files present but `shared_wal` is
    `true`, or `SHARED_WAL` present but it's `false`. A directory holding
    neither layout yet (a genuinely fresh `--dir`) always passes. This is
    what makes flipping the flag against an existing data dir a **startup
    failure**, not a silent per-tablet Raft-state reset — see this
    section's own "What this does NOT change" bullet just below for why a
    silent reset would have been a genuine data-loss/Raft-safety hazard,
    not a convenience. **Since C-05 PR 3's default flip (2026-09-06)**, the
    two error messages read the way round the DEFAULT now runs: the
    `shared_wal: true` (default) branch tells the operator to pass
    `--no-shared-wal` (omitting the flag no longer keeps the per-group
    layout — that behavior moved to needing the opt-out named explicitly),
    and the `shared_wal: false` (`--no-shared-wal` passed) branch tells the
    operator to *omit* `--no-shared-wal` rather than to pass `--shared-wal`
    (a no-op restating the default, not a fix). Unit-tested both
    directions, `host::wal_layout_tests` (a `SimEnv` fixture, no
    `ProdEnv`/sockets needed — `Env::list()` is deterministic under
    `SimEnv` like every other `Disk` method — each direction's test now
    also asserts the exact opt-out/omit phrasing named above); real-
    `ProdEnv` proof through the actual `animusd` startup surface:
    `crates/animusd/tests/shared_wal_e2e.rs::
    a_restart_with_shared_wal_flipped_refuses_to_start`.
  - **What this does NOT change**: the per-group `wal_lock`, the
    `persist_round`/`ships_before_durable` accounting, `snapshot_upto`'s
    discard-under-lock, `COMPACT_THRESHOLD`/`COMPACT_DEFER_CEILING`, and
    every ack/durability-claim rule described above are all unmodified —
    `SharedWal` only changes *where the bytes physically land and how
    many fsyncs a round of concurrent hosted-group writes costs*, never
    *when* an ack may fire relative to those bytes landing. See
    `crates/animus-control/CLAUDE.md`'s own `shared_wal.rs` entry for the
    coordinator's internal mechanism (the tagged vs. raw API split,
    `submit_with_mutation`'s atomicity argument, `physical_write_count()`)
    and `docs/adr/0028-shared-storage-single-command-split.md`'s C-05 PR 2
    amendment for the full design record (round/ack semantics, recovery
    indexing, the GC bound, the flag's exact reach, and the layout-
    mismatch loud-failure check) — its C-05 PR 3 amendment records the
    default-flip cutover itself (unchanged mechanism, `--no-shared-wal`
    opt-out, the real-thread liveness proof). Fault-injection corpus:
    `crates/animus-cp-data/tests/sharedwal_fault_corpus.rs`
    (`ANIMUS_SHAREDWAL_SEEDS`, default 1) — cross-tablet coalescing, a
    crash mid-round with no cross-tablet contamination, `forget`-driven
    GC, a quiet tablet surviving a noisy sibling's real compaction, and
    (cell (e), issue #838) a tolerated (halted-gated) LIVE, non-crashing
    failure never leaving a phantom `group_tails` entry for a healthy
    sibling's own next compaction to durably write out, and (cell (f),
    issue #883) the complementary physical-buffer-layer shape — a
    tolerated failure whose own `env.append` already buffered real bytes
    before its own `env.sync` fails never leaving those bytes for a
    healthy sibling's own next ORDINARY (non-compacting) round to durably
    launder via its own successful `sync` — see ADR 0028's 2026-09-14 and
    2026-09-15 amendments and `animus-control/CLAUDE.md`'s `shared_wal.rs`
    entry for both mechanisms and fixes. Real-`ProdEnv`/real-disk proof:
    `crates/animusd/tests/
    shared_wal_e2e.rs` (two tables sharing one node's `SharedWal` over a
    genuine process restart).
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
- **Wiped-voter boot-time safety (issue #900, ADR 0017's matching amendment):
  `drive`'s WAL-recovery branch calls `RaftCore::begin_cluster_check`
  whenever `state.is_empty()`, except when the caller's own
  `skip_cluster_check` flag is set** (issue #945: this used to be gated on
  `campaign_immediately` directly, which only exempted the ONE replica per
  split child that campaigns — see below for why that was a real
  regression, not just a naming choice). Everything else about the
  mechanism — the `ClusterProbe`/`ClusterProbeResp` wire messages, the
  vote/campaign gating, the wait-for-every-peer aggregation — lives on the
  shared, generic `RaftCore<C, S>` (`animus-control::raft`), so it needed
  **no** cp-data-specific reimplementation, only this one driver call plus
  `RaftKvNode::cluster_check_pending()`/`refused_as_voter()` accessors.
  **Gotcha for any future boot-path change here**: wiring this in draws
  extra entropy (`env.next_u64()`) at every non-`skip_cluster_check`
  fresh-group boot, reshuffling later random draws for the rest of that
  `SimEnv` run — re-verify every fixed-seed test that starts a fresh
  replica (a new tablet, a growth join, a test harness's own second/third
  node) after touching this branch, not just the tests that exercise it
  directly; `tests/read_index.rs`'s own
  `linearizable_read_succeeds_after_a_full_membership_rotation` needed a
  seed re-pin for exactly this reason. See
  `docs/lessons/code-patterns/2026-09-15-a-generic-core-level-fix-does-
  not-wire-itself-into-every-driver.md` for the general lesson.
- **A refusal is now observable (soak finding: 3 silent refusals in 4h).**
  `start_inner` labels the core `"tablet <stream>"` (`RaftCore::
  set_group_label`, by the `stream = tablet.0` convention) so the
  `refusing to start as a voter` ERROR names its tablet
  (`group = "tablet N"`; `"control"` for the control group);
  `/admin/raftkv` carries a per-group `refused_as_voter` boolean; and the
  `Metric::CpGroupsRefusedAsVoter` level gauge (set by animusd's metrics
  sample loop from `ClusterEdgeState::refused_group_count`) counts this
  node's refused hosted replicas. Expected 0; a sustained non-zero means a
  group is silently a voter short.
- **Issue #945 (the corpus-deep regression this shipped as): `campaign_immediately`
  and `skip_cluster_check` are two different flags, not one.** The
  original issue #900 fix gated the cluster-check skip on
  `campaign_immediately` alone, reasoning (correctly) that the ONE replica
  which campaigns is safe to exempt. It missed that a real
  `materialize_split_child` fork hosts SEVERAL replicas of the same child
  at once (only one of which campaigns) — and `handle_request_vote`
  refuses a REAL vote while `cluster_check_pending` (see that function's
  own comment), so every OTHER replica of that same, equally-fresh-by-
  construction child still blocked the campaigner's own vote behind a full
  `ClusterProbe`/`ClusterProbeResp` round trip, silently degrading ADR
  0058 Train 2 rung 4's documented "no added latency" guarantee on every
  single split — caught by the nightly deep corpus (`inplace_split_
  reconciler_corpus`, `heartbeat_batch_corpus`; `ANIMUS_INPLACE_SPLIT_SEEDS`/
  `ANIMUS_HEARTBEAT_SEEDS=40`), not the per-push tier (depth 1), because
  the regression only bites on the *unlucky* simulated link-latency draws
  a handful of the 40 seeds happen to hit. Fixed by splitting the flag:
  `start_inner` now takes `skip_cluster_check` independently of
  `campaign_immediately`, and `materialize_split_child`'s non-campaigning
  branch calls the new `RaftKvNode::start_hosted_split_follower_with_
  batcher[_and_shared_wal]` constructor (skip=true, campaign=false)
  instead of the ordinary `start_hosted_with_batcher[_and_shared_wal]`
  those functions' own ordinary (non-split) callers keep using unchanged.
  **The general rule this generalizes to**: when a safety check is
  ambiguous only because a caller *hasn't yet proven itself* fresh by
  construction, and one specific caller flag proves exactly that — audit
  every OTHER party to the SAME operation that shares the caller's own
  "proven fresh" premise, not just the one with a convenient existing
  flag. A single-node exemption on a multi-node operation is usually
  incomplete.
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

## Versioned formats (ADR 0073 Phase 0)

The shared convention (magic + `u8` version, loud named `FormatError`s,
`tests/fixtures/formats/<format>/v<N>.bin` golden fixtures that are
append-only per `scripts/check-format-fixtures.sh`, a directory-iterating
decode test, a round-trip test, an `#[ignore]`d generator that refuses to
overwrite) lives in `animus-control`'s `format.rs` — see its
"Versioned formats (ADR 0073 Phase 0)" section. This crate's fixtures and
tests are in `tests/format_fixtures.rs`, except the RaftKV ones below, which
live in-crate (`src/format_fixture_tests.rs`, `#[cfg(test)]`) because the
wire/image codec is `pub(crate)`; new formats add a section in whichever fits.

- **`segment` v1** (`segment.rs`, magic `SEGF`, `VERSION = 1`): the
  stream-shard segment object, also reused by backup/PITR/export objects.
  `SegmentError` is `FormatError` (`PreBaselineFormat` for no/foreign magic,
  `UnsupportedFormatVersion` for `0`/future, `Malformed` for framing damage).
  Fixture: `tests/fixtures/formats/segment/v1.bin`. A layout change is a new
  `VERSION` plus a new fixture file — never an edit to `v1.bin`.
- **`raftkv-wire` v1 / `raftkv-image` v1** (`codec.rs`, magic byte `0xCB` +
  `u8` `VERSION = 1`, the pre-baseline single-byte-magic shape kept): the
  binary `KvWire` frame (`encode_wire`/`decode_wire`) and the
  `InstallSnapshot` engine image (`encode_image`/`decode_image`, with the
  `max_ts` header). Errors are `FormatError` with format names `raftkv-wire` /
  `raftkv-image`: empty/foreign magic is `PreBaselineFormat`, version `0` or
  above `VERSION` is `UnsupportedFormatVersion`, other framing damage is
  `Malformed`. Fixtures: `raftkv-wire/v1.bin` (`u32`-BE length-prefixed
  frames, every `RaftMsg`/`KvWire` variant, the `AppendEntries` carrying every
  `KvCommand` variant) and `raftkv-image/v1.bin` (one image, rows across
  kinds, a tombstone, nonzero `max_ts`). The `codec::tests::sample_*` helpers
  are the single construction of "every variant" — append-only, since they
  define what the fixtures must decode to.
- **`raftkv-wal` v1** (the per-group Raft WAL: `WalRecord<KvCommand, KvState>`
  lines in `animus-control`'s `CWL1` envelope; `SWL1` for the shared WAL): the
  durable, compatibility-relevant one. `KvCommand` is `serde_json` here, **not**
  the binary codec, so its serde shape (field names, enum tags) is what the
  fixture pins. `TxnWrite.stage_marker`/`pending` lost their pre-baseline
  `#[serde(default)]` (a missing field is now a `Malformed` decode error, via
  `txn::required_option` — serde otherwise defaults a missing `Option` field
  to `None` even with no attribute). Fixture: `raftkv-wal/v1.bin`.
- **`cp-engine-layout` v1** (`layout.rs`, ADR 0073 Phase 0 layer 4): the
  per-tablet engine key-layout marker. Key `escape(RESERVED_NAMESPACE) ||
  escape("cp_layout") || tablet_be` (an engine-global marker beside
  `applied`/`hwm`/`seal`/`ceiling`/`split`/`trim_marker`; skipped by
  `engine_image`/`has_data`/every kind scan), value `b"KLY1" || epoch(u8)`,
  `LAYOUT_EPOCH = 1`. It is a reserved-namespace marker, **not** a new kind
  byte (kind bytes index `ALL_KINDS`' scope table — see ADR 0073's layer 4
  paragraph). Errors are `FormatError` (`cp-engine-layout`). Fixture:
  `cp-engine-layout/v1.bin` (`u32`-BE-length-prefixed key, then value;
  in-crate tests in `src/format_fixture_tests.rs`).
  - **Check/stamp points.** `Reconciler::ensure_engine`, right after
    `factory.open`: valid marker for this tablet → ok; absent + engine empty
    (`latest_version() == 0`) → `put` the marker at version 1 (the first write
    on a fresh engine — the Raft group, `InstallSnapshot`, `SeedBatch` and the
    applied marker all start later); absent + non-empty (pre-baseline, or only
    *another* tablet's marker: the key embeds the tablet id) → refuse; present
    but undecodable/unknown epoch → refuse. The rebuilt-empty engine in the
    destroy-and-rebuild branch is stamped too. `install_engine_image` only
    merges, so an `InstallSnapshot` never deletes the marker (the image
    carries kind rows only); a wiped engine reopens empty and is re-stamped.
  - **Refusal never destroys.** A refusal logs at error, returns `None`
    (tablet not hosted, claim released, `plan` re-emits next tick) and leaves
    the engine byte-for-byte untouched.
  - **Split children.** `materialize_split_child` opens/clones the child
    engine itself (never through `ensure_engine`) and only caches it after
    trim completes; `trim_split_child` writes the child's own layout marker in
    the **same `write_batch` as its trim-completion marker**, so trim
    completion implies the layout marker. A crash between `clone_engine` and
    that batch leaves a cloned, non-empty, unstamped, untrimmed engine — the
    existing resume branch (probe → open → trim marker absent → re-trim)
    re-runs the batch and stamps it. After trim, `layout::verify` (no stamp)
    guards hosting. The parent's marker may or may not be linked into the
    clone (`clone_to_filtered` links only tables overlapping BASE/LSI/
    FOOTPRINT ranges) and names the parent's tablet id anyway, so it is never
    relied on. `ensure_engine` on a mid-materialize child (only reachable if
    `plan` ordinary-`Host`s a tablet whose cloned-but-untrimmed engine exists)
    refuses — conservative, and better than hosting an untrimmed clone.
  - **Flush at stamp time (isolating the marker).** The `0x5F` marker sorts
    above every kind scope, so left in the memtable with kind rows it would
    make the first flushed SSTable span up to it and defeat
    `clone_to_filtered`'s whole-file exclusion on a split until compaction.
    So every stamp is followed by `EngineFactory::flush_engine(&engine)` — a
    default-no-op trait hook (`StorageEngine` has no flush; `LsmEngine::
    flush_now` is inherent) that an `LsmEngine`-backed factory implements as
    `flush_now()`: after `ensure_engine` stamps a fresh (or rebuilt) engine,
    and after a split child's trim batch (tombstones + trim marker + layout
    marker, before any child kind row). A flush failure only logs (the marker
    is WAL-durable; only the isolation is lost). **Every production
    `EngineFactory` over `LsmEngine` must override `flush_engine`** —
    `animusd`'s `LsmTabletFactory` included. `tests/engine_layout.rs::
    the_stamp_lands_in_its_own_sstable_and_kind_rows_do_not_span_it` pins it
    and `tests/inplace_split_dead_space.rs` passes unmodified. The other
    engine-global markers (applied/hwm/seal/ceiling/split) are written only at
    compaction/snapshot-install/special commands, so they rarely ride in a
    first table; when they do, they widen that one table (pre-existing).
  - **`factory.open` caller audit** (all in `host.rs`): `ensure_engine`
    (host, `gather_facts`' `has_data` probe, the split parent) — checks;
    `materialize_split_child`'s two direct opens/clone — stamped by the trim
    batch and `verify`d; reclaim/`Release`/first-tick `local_tablets` reclaim
    only `destroy` (never open) — need no check; `animusd` has no direct
    `open` call (only `LsmTabletFactory`'s trait impl).
  - **Gotcha: never route a layout refusal through destroy-and-rebuild.**
    `ensure_engine`'s issue-#554 branch treats an `open` *error* as a lost
    engine and destroys it; a layout refusal is a valid, readable engine this
    build must not touch (pre-baseline or newer-version data). Keep the
    refusal a plain `return None` before that `match` arm.

- **`txn-envelope` v2** (`txn.rs`, ADR 0018's 2026-10-04 amendment): the
  per-value tag byte is the version (`0` committed, `1` v1 intent, `2` v2
  intent = v1 body + trailing `prior`). **Class G too (ADR 0073's 2026-10-05
  amendment, #1237): apply always writes v2 into the node's own engine, but
  `engine_image` (the `InstallSnapshot` image, the one place engine values leave
  a node) ships every v2 intent down-converted to v1 — plus its prior as the
  committed row one MVCC version below the intent, where the v1 lookback reads
  it — until `Gate::GlobalTables` (cluster version 2) is open; an N-1 replica
  panics on tag 2. Never branch apply on the gate; never ship an engine value
  without asking what an N-1 reader does with it.** `decode_envelope` dispatches on it;
  `txn::legacy::v1` holds the frozen v1 decoder and the v1 encoder (production
  code now, the snapshot sender uses it; not `legacy-encoders`-gated), plus `downgrade_intent_to_v1`
  (re-exported as `downgrade_txn_envelope_to_v1`): the strict whole-value v2 -> v1
  down-conversion the upgrade harness's engine-row transcode
  (`animus-test`'s `ROW_TABLE`) applies to every stored row. It parses the *entire*
  v2 shape (tag, every field, the trailing `prior`, nothing after) before touching a
  value, because an engine holds many unrelated value kinds and a row carries no
  type marker beyond the tag. Fixtures
  `txn-envelope/v1.bin` + `v2.bin` (`u32`-BE-length-prefixed envelope
  values), tests in `src/format_fixture_tests.rs`. The shared v1 body codec
  (`put_intent_v1_body`/`decode_intent_v1_body`) is frozen: a future version
  that changes those fields copies them into `legacy::v1` first.
- **Decoder dispatch + `legacy` seam (ADR 0073 Phase 1, P1-A).** Every
  format here (`backup-manifest`, `backup-data`, `segment`, `raftkv-wire`,
  `raftkv-image`, `cp-engine-layout`) keeps its `0`/`> CURRENT` named error
  in the header check, then dispatches with `match version { 1 =>
  decode_v1(body), found => Err(UnsupportedFormatVersion { found,
  max_supported, .. }) }` — the version is never bound-and-dropped
  (`codec::check_header` returns it). Each file has an empty `mod legacy {}`
  whose doc says where retired versions' frozen decoders, `VNFoo` shape types
  and `From<VNFoo>` translations go; adding v2 = move the v1 body decoder
  into `legacy::v1`, add the `2 =>` arm (checklist in the ADR). `raftkv-wal`
  is decoded by `animus-control`'s `PersistedState::decode` (P1-C owns that
  dispatch); here only its fixture test lives, and it panics on a fixture
  version with no expected value, like every other fixture test.

## Tests

`cargo test -p animus-cp-data`. All but two of the 29 test binaries drive
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
automatic reconfiguration/leadership-transfer (`tests/reconfigure_trigger.rs`
— the full failure cascade: a node crashes, is marked `Down`, and the
reconciler auto-replaces it onto a spare; `tests/reconfigure_healthy_drop.rs`
is its "drop a healthy voter" sibling, issue #781 — a direct
`CasTabletReplicas` drop of a live follower, and separately the leader
itself, through the real `spawn_reconfigure_loop`/`reconfigure_step`,
asserting the removed replica does not go on to disrupt the converged group;
depth knob `ANIMUS_RECONFIGURE_DROP_SEEDS`; `tests/demux_stream_teardown.rs`
is the ADR 0026 2026-09-28 stream-teardown sibling — the same "drop a live
follower" shape, but through the real `host::Reconciler` under injected
message loss/delay, asserting the released node's own stream ends empty and
tombstoned, convergence holds, and a later re-add reopens it; `tests/
inbox_overflow_tolerance.rs` is the ADR 0026 2026-09-28 inbox-cap sibling —
a live 3-node group with a tiny configured `InboxCap` where a leader
replicates to a node before that node's own `RaftKvNode` has even started
(the real "never-hosted" leak shape, `crates/animus-env/CLAUDE.md`'s own
entry), proving the resulting overflow-evicted frames are tolerated: the
group still converges once the late node starts and serves a linearizable
read afterward), the ADR 0026/0041/0042/0043
stream-addressing/`KindBatch`/`KIND_CURSOR`/`ClusterSegmentStore` suites,
the ADR 0018 HLC/MVCC/range-seal/transaction suites, the `host.rs`
reconciler end to end, the ADR 0044 phase-2 heartbeat-batcher baseline
(`tests/heartbeat_cost.rs`, C-02 PR 1) and fault-injection corpus
(`tests/heartbeat_batch_corpus.rs`, C-02 PR 2, below), and the real-thread
`ProdEnv` regression noted above.

### Heartbeat-batcher corpus (`tests/heartbeat_batch_corpus.rs`)

ADR 0044 phase 2 — the `heartbeat_batch` module's own fault-injection
corpus, depth knob `ANIMUS_HEARTBEAT_SEEDS` (default 1). This corpus
co-hosts several `RaftKvNode` groups sharing the SAME three physical
`NodeId`s in ONE `Simulator` — mirroring `tests/stream_addressing.rs`'s
established pattern — since batching only has anything to amortize when
several groups share one physical node's `env` (and thus one
`HeartbeatBatcher`). Eight scenarios: frame-vs-logical scaling (cell a —
every group's leader forced to the same physical node via
`start_hosted_campaigning_with_batcher`, so leadership doesn't spread
across the cluster as group count grows and confound the measurement); the
**explicit `--no-heartbeat-batch` opt-out proof** (C-02 PR 3, added at the
cutover — no batcher attached at all, independent `Simulator` worlds
exactly like `heartbeat_cost.rs`'s own pre-cutover shape below, proving
the flag genuinely restores byte-for-byte the pre-batcher scaling
behavior — moved here from `heartbeat_cost.rs` once that file's own
baseline started measuring the DEFAULT, batching-on behavior instead);
every per-group invariant (election timers, term, commit index, ReadIndex
confirmation) holding under batching over a long idle window; a genuine
partition (leaders forced to different physical nodes so the "sibling
sharing no partitioned pair is untouched" property is deterministic, not
a per-seed coin flip); a 5%-lossy-but-connected link; an unknown group in
a received frame; and leader kill / follower kill, each proving the
sibling group unaffected unless it happens to share the faulted node. Run
at depth: `ANIMUS_HEARTBEAT_SEEDS=K cargo test -p animus-cp-data --test
heartbeat_batch_corpus` (default `K=1`; held green through `K=150`
locally, `=40` in the nightly `corpus-deep.yml` tier).

**`tests/heartbeat_cost.rs`'s own baseline flipped at the C-02 PR 3
cutover** — it used to be the flag-OFF (pre-batcher) baseline (independent
`Simulator` worlds, no batcher at all, `AppendEntries` traffic scaling
with hosted-group count); now that batching is on by default, it measures
the DEFAULT behavior instead: `RaftKvNode::start_hosted_campaigning_with_
batcher`/`start_hosted_with_batcher`, co-hosted in one `Simulator` exactly
like this corpus's own cell (a), pinning `CpHeartbeatFramesSent` flat
(ratio ≈ 1.00, `frames1=238`/`frames5=238`) while `CpAppendEntriesSent`
keeps scaling with the hosted-group count (ratio ≈ 5.00,
`logical1=238`/`logical5=1190`) as today's default, not merely an opt-in
capability. The flag-OFF proof this corpus's own `batching_off_frames_
scale_with_groups_like_the_old_default` scenario now carries is exactly
`heartbeat_cost.rs`'s old shape, moved here — see that scenario's own doc
for why it stayed independent-`Simulator`-worlds rather than following
cell (a)'s co-hosted-with-injectable-batcher shape (`RaftKvNode::
start_hosted_with_batcher(.., None)` calls `env.metrics()` internally,
which is `SimEnv`'s no-op handle unless a caller injects one — there is no
co-hosted-AND-injectable-metrics-AND-no-batcher constructor, so the
unbatched proof needs `start_with_metrics`'s own independent-world shape
instead, exactly as PR 1 originally found).

### Reconciler lifecycle corpus (`tests/reconciler_corpus.rs`)

The 31 `host.rs` unit tests prove `plan` correct as a pure function; this
corpus is the **seed-reproducible fault-injection** suite for the whole
tablet lifecycle, following the house corpus doctrine (ADR 0014): a frozen,
name-seeded scenario list, a depth knob, and coverage/seed-expansion
guards. The seeding scaffolding itself (`name_seed`/`seeds_from_env`/
`seed_expand`) is `animus_test::corpus` (ADR 0061 rung B1), a dev-dependency
shared with every corpus in this repo — this file's own `Scenario` implements
`corpus::SeedVariant` rather than rolling its own expansion. See the test file
for the 19 frozen scenarios and the generic
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
  --test it reconciler_corpus::reconciler_corpus_runs_every_scenario` (default
  `K=1`; held green through `K=300` in ~52s).

## Benchmark

`benches/wal_fsync_bench.rs` (`cargo bench -p animus-cp-data --bench
wal_fsync_bench`, C-05 PR 1, ADR 0028) is a hand-rolled (no criterion,
zero new dependencies), `harness = false` **`ProdEnv` wall-clock**
benchmark — mirrors `animus-storage`'s `engine_bench`/`animusd`'s
`cluster_bench` in style. It answers the gating question `docs/roadmap.md`'s
C-05 entry named before committing to wiring `animus-control`'s unwired
`SharedWal` into this crate's persist path: whether K concurrent per-group
WAL fsyncs are already cheap on real media, or genuinely cost more than
`SharedWal`'s coalesced alternative. It measures round latency (p50/p99,
plus a real, instrumented `Disk::sync` count — never assumed from the code
shape) for a burst of one write to each of `K` active groups, three ways:
today's per-group files (concurrent and sequential), and the real,
already-built `SharedWal::append` API called directly (unwired — the bench
proves what wiring it in *would* buy, without touching production code),
plus a fixed single-group 32-write control isolating the effect to the
cross-group case. Workload knobs: `ANIMUS_BENCH_GROUPS` (default
`1,8,32,128`), `ANIMUS_BENCH_ROUNDS` (default `20`),
`ANIMUS_BENCH_VALUE_BYTES` (default `96`), `ANIMUS_BENCH_JSON` (unset — a
file path to also write results as JSON). Full method, this host's
measured numbers, the threshold used, and the recommendation are in
`docs/design/shared-wal-fsync-benchmark.md`; ADR 0028 has the matching
amendment. **Manual/local only, like its two siblings** — real disk I/O
and real elapsed wall clock make it unsuitable for a shared CI runner's
noise floor; run it locally, and never compare its numbers against a
different host/session/media (the bench prints the resolved `/proc/mounts`
filesystem type + device for whatever directory it writes into, so a
reader never has to take the media on faith).

**Upgrade-harness class (ADR 0073 P1-D):** none of this crate's formats is a whole-file `TABLE` entry; `raftkv-wal`, `cp-engine-layout` and `txn-envelope` (v2) are `EMBEDDED` in a whole-file carrier (`control-wal`/`lsm-sstable`), and `raftkv-wire`, `raftkv-image`, `segment`, `backup-manifest`, `backup-data` are `EMBEDDED` off-disk (`animus-test`'s `upgrade::transcode::EMBEDDED`).

## `wal_lock` is a FIFO-fair `FairMutex` (apply-task starvation fix)

The per-tablet `wal_lock` (serializes `persist_wal`'s append/fsync against
`apply_and_compact`'s compaction rewrite) is `animus_control::fair_lock::
FairMutex`, not `futures::lock::Mutex`. The consensus loop starts back-to-back
persist rounds while `has_unflushed_wal()`, so under continuous proposals on a
slow disk the unfair mutex let it re-lock ahead of the apply task's compaction
wait indefinitely: `engine_applied_index` froze while core `last_applied ==
commit` hid it. **`persist_wal` takes `wal_lock` before branching on `shared`,
so the `SharedWal` path had the identical bug.** Do not swap it back; `SharedWal`'s
own internal mutex (in `animus-control`) is only taken inside `append_tagged`/
`compact_group`, under `wal_lock`, and needs no change. Regression:
`tests/apply_not_starved_by_wal_lock.rs` (both paths, asserts on
`engine_applied_index`, never core `last_applied`). ADR 0017's and ADR 0038's
2026-09-30 amendments.

## Per-group WAN timing in the host reconciler (ADR 0075 section 3.4)

`MetadataView::regions` (member id -> region label, empty by default: no
behaviour change) and `Reconciler::set_max_region_rtt` feed
`Reconciler::timing_profile_for(replicas)`; the profile is applied on `host`,
on `materialize_split_child` and re-applied every `tick` to each hosted tablet
(a label or replica-set change converges; the no-change path draws no
entropy). `RaftKvNode::set_timing_profile` wakes the driver on a real change;
`RaftKvNode::election_timeout()` reads the installed base. **The ADR 0044
heartbeat batcher is unchanged**: its 50 ms tick (`DEFAULT_HEARTBEAT_BATCH_
INTERVAL`) is `<=` every profile's heartbeat. Every `MetadataView { .. }`
literal now needs `..Default::default()` (or a `regions` field). Corpus:
`tests/it/wan_timing_corpus.rs`, `ANIMUS_WAN_TIMING_SEEDS` (see the lesson
`docs/lessons/testing/2026-10-04-measure-where-the-old-setting-fails-before-
building-its-negative-control.md`: the LAN-forced control only bites on the
re-election cells).

## Preferred leader and witness replicas (ADR 0075 section 3.3/3.6, G-c M2)

`MetadataView::preferred_leader` (tablet -> `LeaderPreference { region, witness }`,
empty = no behaviour change; `animusd::leader_preferences` derives it from
`TableSchema.global` x `Tablet.table`) feeds `Reconciler::preferred_leader_step`
(end of `tick`). It acts only when this node **leads a tablet from a Region it must
not** (not the preferred one, or the witness one): the violation must hold for
`PREFERRED_LEADER_STABILITY_TIMEOUTS` (2) election timeouts and transfers are at
least `PREFERRED_LEADER_MIN_INTERVAL_TIMEOUTS` (10) apart per group; target = non-`Down`
voter in the preferred Region with the best `peer_match >= commit_index` (the exact
arm gate of `RaftCore::transfer_leadership`; its `bool` is checked and a refusal is
retried without resetting the window; metrics `CpPreferredLeaderTransfers`/`...Rejected`).
A witness-region leader falls back to any other caught-up non-witness voter. Idle
correct groups are never touched, so quiescence is not fought (arming a transfer is
the only wake). The same pass calls `RaftKvNode::set_witness`, which makes
`stale_read_ready()` false so a witness never serves a replica-local eventual read
(`animusd` `cp_stale_forward_target` also skips witness replicas). Corpus:
`tests/it/preferred_leader_corpus.rs`, `ANIMUS_MRSC_SEEDS`. Known: quiescence does not
settle on links with RTT above the heartbeat interval (issue #1226), so the quiescence
cell runs on 1 ms links.

## Fuzzing (roadmap R-01 (c))

The RaftKV codec (wire/image/WAL), segment codec, backup chunk/manifest codecs, layout marker, cursors and engine marker values are the `cp_data_formats` fuzz target; the `pub(crate)` ones are reached through the off-by-default `fuzzing` feature (`src/fuzzing.rs`). See `fuzz/README.md` (stable smoke: `cd fuzz && cargo test --release --test smoke`).

## Gate enforcement (ADR 0073 Phase 2, P2-B)

- **`gates.rs`**: exhaustive `GatedCommand for KvCommand` (all `Base`) and
  `KvWire::envelope_gate`/`required_gate`; the shared helpers `encode_for_send`
  (check the frame's envelope gate, then encode at the gate-selected frame
  version; `None` = drop) and `check_propose`. Every `env.send_stream` of a `KvWire`
  (read probe, initial probe, campaign outs, the three drive-loop sites, the
  heartbeat batcher's flush) goes through `encode_for_send`; the four
  `core.propose` sites (`propose_ordered`, `_aux`, `propose_kind_eval`,
  `propose_kind_eval_batch`) go through `RaftKvNode::gated_propose`. As in the
  control plane, **send sites check the envelope only; `AppendEntries` entry gates
  are enforced at propose.**
- **`codec.rs`**: `WIRE_VERSIONS`/`IMAGE_VERSIONS` tables `(frame version, Gate)`;
  `encode_wire(w, &features)` / `encode_image(entries, max_ts, &features)` write
  the highest version whose gate is open; decoders accept every version up to
  `VERSION`. Only v1 (`Gate::Base`) exists, so a B2 encoder emits Phase 1 bytes
  under any handle (`format_fixture_tests::pre_era_encoders_are_byte_identical_*`).
  A new frame version = a table row + body arm + decoder arm (old one to
  `legacy`) + fixture.
- **Features plumbing**: `RaftKvNode::features()`; every existing `start_*` uses a
  floor `ClusterFeatures::new()`; `RaftKvNode::start_hosted_with_options(.., HostedOptions)`
  injects the control-fed handle; `host::Reconciler::set_cluster_features` (also
  re-points its `HeartbeatBatcher` via `set_features`) is the production seam P2-C
  calls with `RaftNode::features()`. A hosted group keeps the handle it started with.


## StorageFull with every replica full (issue #1228)

A suspect-WAL node ships frozen acks (`is_frozen_ack`: success acks at or below
the frozen durable index, heartbeats) next to the `ships_before_durable`
allowlist, so followers stay in contact and the leader keeps its seat. Reads on
a full leader: `read_serve_ts` serves linearizable reads at the engine's highest
version once the ceiling lapses (no unproposable ceilings), and `read_barrier`
targets the leader's first-term entry instead of `commit_index` (the engine may
be paused short of it). `stale_read_ready` for a full replica needs only
`had_leader_contact` and no half-installed snapshot. Leader death with every
replica full is not repaired until space returns (ADR 0074 amendment). G-01
preferred-leader transfer goes through `transfer_leadership`, so it inherits the
refuse-full-target and healthy-quorum guards, and the storage-full step-down
wins over preference. `peer_health` is a diagnostic on the node's own clock
(the sim has per-node clock skew; never compare it with another node's `now`).
Tests: `animus-control` `storage_full_step_down`, `animus-test`
`raftkv_disk_full_all_replicas_*`, `quiescence` (ix), `animusd`
`sim_cluster_dynamo_disk_full`, `chaos_disk_full` phase 2. The `first`-based
ReadIndex is exercised only by real-process chaos (no sim cell produces an
engine behind commit while all are full) and is not mutation-guarded.

## StorageFull: per-tablet WAL recovery (R-01 (d), issue #1185)

`persist_wal` no longer `assert!`s on an ENOSPC append/sync (per-group file or
`SharedWal::append_tagged`): it calls `PersistProgress::mark_suspect` and runs
`recover_kv_wal`, which wraps `animus_control::persist_round::recover_suspect_wal`
with a `write_image` of `Disk::replace` (removing the `.tmp` sibling on failure)
on the per-group path or `SharedWal::compact_group` on the shared path. A
suspect group refuses writes before proposing (`RaftKvNode::is_storage_full`,
`record_storage_full_refusal` feeds `overload_storage_full`), keeps serving
reads of applied state, and `apply_and_compact` skips compaction while suspect
(an ENOSPC compaction rewrite also marks suspect; the staged-rewrite path
tolerates ENOSPC). The `persist` field on `RaftKvNode` exposes the progress
handle. A non-ENOSPC failure stays `assert!(halted)`. Engine-side ENOSPC (issue #1218):
`apply_loop` wraps its engine in `apply_stall::StallingEngine`, which retries
any `StorageError::StorageFull` call (after `Env::sleep`) instead of panicking
at the apply task's many `.expect`s; `RaftKvNode::is_storage_full()` is
`persist.is_suspect() || apply_stalled`. Soundness rests on the engine contract
that a `StorageFull` call changed nothing, and on the task being blocked inside
that call (order preserved). On `halted` a paused call sets `apply_stopped` and
parks (never panics a caller's `.expect`). Only the apply task's handle is
wrapped; other engine users still propagate. See `docs/resource-bounds.md`
section 3.

**Leader step-down (issue #1219).** The consensus loop computes
`storage_full = persist.is_suspect() || apply_stalled` every pass, in the same
lock acquisition as `set_state_machine_behind`: it calls
`RaftCore::set_storage_full` (no campaigning, `TimeoutNow` declined), vetoes
quiescence while full, and on a **leader** arms `RaftCore::storage_full_step_down`
(rotating `stepdown_last`, cooldown `2 * election_timeout`, then
`propose_signal.notify()` so `TimeoutNow` ships immediately). A refused write
(`record_storage_full_refusal`) and the apply task's ENOSPC stall start
(`StallingEngine`'s `wake`) raise `wake_signal`, so a parked/quiesced leader
re-evaluates; arming un-quiesces. A full follower never acks (the failed round
gates every ack; `persist_round` unit test + corpus pin). Do not make the step-down
conditional on follower health knowledge: there is no wire signal for it, an
aborted transfer to a full target is the (cheap) negative answer.

handle. A non-ENOSPC failure stays `assert!(halted)`. Gap: engine-side ENOSPC
(LSM flush/compaction, apply-time `merge_batch`) is NOT handled; the corpus
runs `MemoryEngine` only. See `docs/resource-bounds.md` section 3.

## A split child's log does not reproduce its engine (issue #1229)

A fork child's engine is cloned from the parent's, so its pre-fork rows are in no
log entry. `RaftKvNode`'s driver reads the durable split-trim marker at start and
calls `RaftCore::set_log_omits_base(true)`; while the leader's `snapshot_index` is 0
it then sends a *learner* no log (it raises `snapshot_needed` instead, so the engine
image is built and shipped). Never route a new replica of a fork child through log
replay. Regression: `animusd` `sim_cluster_split_relocation`. ADR 0058's
2026-10-05 amendment; lesson `docs/lessons/code-patterns/2026-10-05-state-seeded-outside-the-log-needs-a-snapshot-for-every-new-replica.md`.

- **TxnId uniqueness (R-01 F-2).** `TxnId.node` is the node qualified by the group stream (`n0#100`; primary stream = bare node id), because `ts` is per-group `Hlc` state and one node leads many groups. `txn_stage_local` (animusd) also refuses, before proposing, a stage group with any key outside the leader range (stale grouping across a split). See `docs/lessons/testing/2026-10-05-a-txn-id-must-be-unique-per-group-not-per-node.md`.

- **Seal check on every mutating apply arm (R-01 F-2).** `TxnCommit`/`TxnAbort` (and the orphan tombstone) are deterministic no-ops on a sealed record key, like every other mutation: a fork clones the parent's CURRENT engine per replica, asynchronously, so a post-fork decision landing in the parent diverges the children. Regression: `tests/it/split_tablet.rs::a_txn_decision_ordered_after_the_fork_is_a_sealed_no_op`; see `docs/lessons/testing/2026-10-05-every-mutating-apply-arm-needs-the-seal-check.md`.

- **MREC shapes (G-01 stage G-d M1).** `KindEvalOp::Replicate { item, ver }` and
  `WriteSchema.mrec` ride as JSON blobs inside `KindEval`/`KindEvalBatch`/`TxnStage`
  (no binary codec bump, wire stays v1, WAL v2). `KvCommand::required_gate` is
  **content-dependent** for those three carriers (`gates.rs::eval_gate`: MREC content
  joins to `Gate::MrecReplication`), enforced at the one `gated_propose` choke point;
  `evaluate_kind_eval` gives a `Replicate` its LWW semantics (M2, below). Shaped fixtures `raftkv-wire/v1-mrec.bin`, `raftkv-wal/v2-mrec.bin`
  (built by `codec::tests::mrec_sample_wires`; `fixture_files` skips `vN-<shape>` names).

- **MREC apply (G-01 stage G-d M2, ADR 0075 amendment).** `evaluate_kind_eval` takes
  the stored stamp (`decode_stored_item_versioned`; the `KindEvalBatch` overlay carries
  it too). `KindEvalOp::Replicate` applies iff `ver > stored` via the normal
  `derive_kind_writes` path, else `KindEvalDecision::Superseded` (no writes, not even a
  change record; leader-local `KindEvalResult::superseded` /
  `KindEvalItemResult::Superseded`, the replicated outcome stays `Applied`); a key with
  an intent gives `ConditionFailed` (the shipper's Retry); `mrec: None` rejects. A local
  op with `WriteSchema.mrec` stamps via `MrecVersion::next_local`. A `Replicate` in a
  `TxnStage` is rejected before evaluation. Tests: `src/mrec_props.rs` (pure convergence
  proptest + two negative controls, `ANIMUS_MREC_PROP_CASES`), `tests/it/mrec_apply.rs`
  (a group opened with `HostedOptions { features }` at cluster version 3: the propose
  gate panics in tests otherwise). **Adding a base-row writer for an MREC table means
  stamping it** (see the ADR's writer audit).

**MREC apply is the only consumer-facing piece here (ADR 0075 "G-d as built").** The
shipper, saga and receiver live in `animusd`; this crate owns the last-writer-wins rule
(`apply_mrec`: stored `MrecVersion` vs the incoming one, `Superseded` on a loss, stamped
tombstones, a foreign intent is `Retry`) and the content-dependent `KvCommand::required_gate`.
The cursor rows `mrec:<region>`/`mrecscan:<region>` are ordinary `KIND_CURSOR` rows written
through the existing kind ops (no new command); `trim_split_child` drops them, which is why
a split child rescans. Test-only switch `mrec_test_switch::set_lww_by_arrival` (thread-local)
backs the M5 negative control; never reachable in a release build path.
