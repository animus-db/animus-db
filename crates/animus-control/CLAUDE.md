# CLAUDE.md — animus-control

This file provides guidance to Claude Code (claude.ai/code) when working in this
crate.

## Purpose

The strongly-consistent control plane: an in-house Raft (ADR 0009, *not*
openraft, so `SimEnv` can drive it) replicating cluster metadata — membership
and the tablet map — with epoch compare-and-swap transactions. All consensus
logic is a synchronous, I/O-free state machine; a thin `Env` driver does the
I/O. The sync core is generic (ADR 0016) and is reused unchanged by the
per-tablet CP data plane (`animus-cp-data`).

## Entry points

- **`lib.rs`** — the public surface: re-exports the core types (`SharedWal`,
  `RaftCore`, `RaftNode`, `Metadata`/`MetaCommand`, the schema types,
  `FailureDetector`) plus `animus_placement::PlacementPolicy` (so a downstream
  assembler can `SetTabletPolicy` without a direct `animus-placement`
  dependency — the policy is part of this plane's public metadata surface).

  and `Metadata::rebalance` are the *pure* placement decisions (see
  Invariants). Holds members, the tablet map, placement policies, the
  table-schema catalog, node addressing (`node_addrs`), the
  DynamoDB Streams segment catalog (`stream_shards`), and the secondary-index
  backfill-completion catalog (`index_backfill`, ADR 0045 §4 — per-`(tablet,
  index)` "this tablet finished seeding change-log coverage for this index",
  populated by `MetaCommand::MarkIndexBackfilled` and consumed by `animusd`'s
  `index_backfill_loop`, the control-leader-only aggregator that flips an
  index from `Creating` to `Active` once every one of its table's *current*
  tablets has reported; same `(tablet, name)`-not-`(table, name, tablet)`
  identity convention as `stream_shards`, and the same
  `#[serde(with = "..._codec")]` tuple-key JSON workaround). Tablets are
  **split-only** (ADR 0044) — `MergeTablets` and its dual `absorbed_by`/
  `merged_tablets` provenance (ADR 0033) were removed entirely, taking
  with them the ADR 0042 §12/ADR 0043 §A5 "F1" merge-vs-streamed-table
  stopgap. See `MetaCommand`'s and `Metadata`'s own doc comments for the
  exact per-variant contract — this bullet only calls out what isn't
  derivable from those.

  **`Metadata::stream_shards`'s own field-level codec, not its natural
  `BTreeMap<(TabletId, u64), _>` shape, is what actually rides the wire.**
  `serde_json` cannot serialize a tuple (or any non-string) map key at
  all — `MapKeySerializer` errors "key must be a string" the moment the
  map is non-empty, which every pre-existing whole-`Metadata` round-trip
  test missed by never populating it. `#[serde(with =
  "stream_shards_codec")]` encodes/decodes a flat `Vec<{tablet, epoch,
  ...StreamShardRow fields}>` instead. The in-memory type is unchanged
  (still a plain `BTreeMap`); only `Metadata`'s own `Serialize`/
  `Deserialize` impl is affected. Regression:
  `meta::tests::metadata_round_trips_through_json_with_populated_stream_
  shards`. See `docs/engineering-lessons.md` for the general "an empty
  collection can't prove a map-key encoding rule" lesson this is an
  instance of.

  **The caller (never this pure state machine) is responsible for never
  removing a tablet's own current highest-epoch stream-shard row while
  that tablet still exists** — `SealStreamShard`'s own epoch derivation is
  "the chain's own highest existing row, plus one," so physically removing
  that row out from under a still-live tablet would let a future seal
  silently reuse the same epoch number; see `animusd/CLAUDE.md`'s
  `segment_janitor.rs` entry (and `docs/streams-notes.md`) for the guard
  that upholds this.

  **`RegisterNode` is the sole claim path for a fresh node identity.**
  **The CAS key is `Metadata::node_addrs` alone, not `members`**: an id
  absent from `node_addrs` claims the address slot (inserting a `Down`
  `Member` too, but only if `members` doesn't already have an entry); a
  byte-identical re-registration is `NoOp` (idempotent retry / ADR 0032
  rejoin); a *different* `NodeAddrs` already on file is `Rejected` — the
  real collision. **Keying on `node_addrs` rather than the full
  `NodeAddrs`+`labels` pair is load-bearing, not an oversight**: a
  labels-inclusive CAS breaks the moment two *independent* commands can
  each partially establish the same identity, which several call sites in
  `animusd` do — see `MetaCommand::RegisterNode`'s own doc and
  `docs/engineering-lessons.md`'s entry for the integration-test failure
  that caught the naive design.

  `Member.has_activated: bool` is **sticky**: `Metadata::apply`'s
  `UpsertMember` arm sets it the moment a member's status is *ever*
  recorded `Active` — by any caller — and never clears it again, regardless
  of any later transition. Deliberately **not** scoped to "only the
  detector's own promotion": a bootstrap-declared member starts `Active`
  directly, never passing through `Down`→`Active` at all, so a narrower
  rule would leave a founding member's `has_activated` permanently `false`
  — indistinguishable from a genuine orphan the instant it later
  legitimately crashes. This is the field the orphan-member sweep
  (`node.rs`, below) keys on to distinguish "never showed up" from "was
  alive, currently down."

- **`raft.rs`** — `RaftCore<C, S>`: the synchronous, I/O-free Raft state
  machine, **generic over its command `C` and applied state-machine `S`**
  (defaults `MetaCommand` / `Metadata`, so existing references are unchanged).
  Time and randomness are parameters (`now`, `entropy`); it returns outbound
  messages and emits `WalRecord`s. The state machine is the `StateMachine<C>`
  trait (`apply` + `noop`), implemented by `Metadata` here and by a KV store in
  the data plane. Consensus (election / replication / commit / snapshot /
  `InstallSnapshot` / pre-vote / leadership transfer / durability) is identical
  for any `S`; only `apply` and the snapshot image type are `S`-specific. The
  `RaftNode` driver stays control-plane-specific (it bakes in reconcile +
  failure detection); the KV data plane gets its own driver.

- **`node.rs`** — `RaftNode<E>`: the `Env` driver wrapping the core. Runs
  `reconcile_loop` (placement reconciler + rebalancer), `detect_loop`
  (failure detector, ADR 0012), `orphan_sweep_loop` (ADR 0040 PR6, below),
  and the heartbeat loop. Records control-plane metrics (ADR 0015);
  read-only state accessors back the `animusd` admin interface (ADR 0020).
  `metadata_watch() -> MetadataWatch` (ADR 0031) is the "applied index
  advanced" notification the per-node CP reconciler uses instead of
  polling.

  Runtime control-group membership change (`change_membership`/
  `transfer_leadership`, ADR 0037) and the control-id-native liveness
  signal (`RaftCore::peer_last_contact`, `RaftNode::
  control_peer_believed_alive`) are mechanically identical to the
  per-tablet primitives `animus-cp-data` already drives — see ADR 0037 for
  the design; the admin/CLI surface lives in `animusd` (that crate's
  `CLAUDE.md`). One gotcha worth restating: **`peer_last_contact` is
  deliberately never persisted or snapshotted**, like `next_index`/
  `match_index` — it's a volatile per-peer liveness timestamp, not
  replicated state.

  **`Metadata` is `DRIVER_APPLIED` (ADR 0038): the driver is split into a
  consensus loop (`drive`, no engine I/O — services heartbeats regardless
  of engine speed) and an async apply task (`meta_apply_loop`/
  `meta_apply_and_compact`, the sole owner of mutable `Metadata`).** See
  ADR 0038 for the full mechanics; three gotchas that aren't in the ADR
  text: (1) the apply task seeds its watermark from the **engine's own
  `_applied_index` key, not `core.last_applied()`**, which can understate
  what the engine already durably holds; (2) **every reader now reads
  `cache`, never the core** (`metadata()`/`members()`/`placement_view()`)
  — `reconcile_loop`/`detect_loop` still read leadership/term off the core
  (a consensus-level fact) but the placement view off `cache`; (3) the
  incremental `WatchMetadata` delta ring (`delta_ring.rs`) is pushed
  **before** bumping `MetadataWatch` in the same apply pass, so a watcher
  woken by that bump always finds the ring already populated. `start`/
  `start_with_metrics` **require** a `StorageEngine` — there is no
  engine-less control-plane deployment shape.

- **`delta_ring.rs`** (ADR 0038) — the apply task's bounded, per-node,
  best-effort in-memory ring of [`mirror::KeyWrite`] deltas keyed by Raft log
  index. Pure (no `Env`, no I/O); `push`/`clear`/`writes_since(last_seen,
  upto)` are its whole surface. Bounded by **both** `max_entries` and
  `max_bytes` (`DeltaRing::default` uses 1024 entries / 4 MiB), oldest
  evicted first — except a push never evicts the entry it just inserted,
  even if that single entry alone exceeds `max_bytes` (discarding your own
  freshest entry would defeat the ring's purpose). **`writes_since`'s
  contiguity check is subtle at the boundary: `last_seen + 1 ==
  front().index` is *not* a gap** (the caller's very next needed index is
  exactly the ring's oldest retained entry) — only `last_seen + 1 <
  front().index` is (see the unit tests' `byte_bound_eviction_from_one_huge_
  entry`). Unit-tested directly; `node.rs`'s own white-box apply-task tests
  and `tests/watch_deltas.rs` prove it wired up correctly against a real
  `RaftNode`.

- **`schema.rs`** — the replicated **table-schema catalog** (ADR 0013), all
  plain data (no I/O/clock/RNG): `TableSchema`, `ColumnType`,
  `SchemaCatalog`, `IndexDef`/`IndexKind`/`IndexProjection` (the replicated
  GSI/LSI *shape*, not its entry data), `StreamSpec` (a table's
  DynamoDB Streams config, ADR 0042 §2/§4), and `TtlSpec` (a table's
  DynamoDB-style TTL config, ADR 0051: just `attribute_name` — the item
  attribute holding an absolute Unix epoch second; the control plane never
  interprets an item, so it stores only the declaration). See the module's
  own doc comments for the type/accessor inventory. **`StreamViewType` is a
  read-time projection only** — a shard record always stores both images
  regardless (ADR 0043), so a view-type change never needs a backfill.
  **`TtlSpec` mints no identity label** (unlike `StreamSpec`'s `label`), so
  `MetaCommand::SetTableTtl`'s idempotency rule is the opposite of
  `SetTableStream`'s: re-enabling with the same attribute is a `NoOp`, and
  changing the attribute in place (no disable/re-enable round trip) is
  `Applied` — see the variant's own doc before copying `SetTableStream`'s
  shape for a future replicated-config command that also has no label.

- **`persist.rs`** — `WalRecord`, `PersistedState` (durability/recovery; the
  write/compact/recover flow is diagrammed in `docs/wal.md`). **`Metadata` is
  `DRIVER_APPLIED` (ADR 0038), so its WAL `Snapshot` record's `metadata` field
  is always the meaningless `Metadata::default()`** (the real durable state
  lives in the system-keyspace engine). An earlier blob-reuse snapshot encoder
  (`encode_snapshot_record_from_blob`/`RaftCore::encoded_wal_image`) that
  serialized an in-core state machine's snapshot once instead of twice was
  deleted (unreachable since ADR 0038 made every real state machine in this
  workspace `DRIVER_APPLIED`, so its own `!S::DRIVER_APPLIED` assert could
  never pass outside a toy test machine) — the live compaction path
  (`node.rs`'s `meta_apply_and_compact`) has only ever called plain
  `wal_image()`/`encode_record`.

- **`detector.rs`** — `FailureDetector` (ADR 0012): a pure, unit-tested
  interval+timeout liveness detector. No clock, no RNG.

- **`shared_wal.rs`** — `SharedWal` (ADR 0028): a multi-tenant WAL I/O
  coordinator that serializes concurrent tablet WAL writers into one file
  with coalesced `append`+`sync`. **Built and unit-tested but UNWIRED** — no
  `animusd`/`animus-cp-data` code constructs one; every tablet still writes
  its own WAL file. Wire-in-or-delete is an open decision (see ADR 0028).

- **`syskv.rs`** (ADR 0038) — the control plane's reserved **system keyspace**
  key encoding: pure functions, no I/O. `RESERVED_NAMESPACE =
  "__animus_system"` is the top-level namespace no user table/keyspace may
  claim; one `EntityKind` per `Metadata` collection (ADR 0044 dropped
  `Merged`/`AbsorbedBy` along with tablet merge), each with a typed
  `*_key`/`decode_key` helper pair used by the mirror's own engine-scan
  path (`mirror::rebuild_metadata_from_engine`) — see the module doc for
  the full type list.

  **`is_reserved_name` matching is a case-sensitive prefix test** (exact
  match *or* merely prefixed, e.g. `__animus_system_backup`) — a combined
  node's mirror writes directly through this same already-globally-
  namespaced engine with no further `StorageScope` wrapper, and a prefix
  match is the collision that scheme cannot tell apart from a real system
  key. Called from both `Metadata::apply`'s `CreateTableSchema` arm
  (state-machine-level gate) and the DynamoDB wire edge's client-side
  validation (surfaces as an immediate error instead of an opaque
  commit-wait timeout) — same two-layer idiom the duplicate-table check
  uses.

  `reserved_scan_bounds() -> (Vec<u8>, Vec<u8>)` is **the load-bearing
  bound the admin endpoint scans with instead of
  `StorageEngine::entries()`**, which would scan the whole engine (every
  user table's data too, on a combined node sharing it with the CP data
  plane, ADR 0028). See `docs/engineering-lessons.md` for why this must
  never be "simplified" to `entries()`.

- **`mirror.rs`** (ADR 0038) — the apply task's write-derivation
  (`apply_and_derive_mirror`) and restart-rebuild
  (`rebuild_metadata_from_engine`) logic; see the module doc for the split.

  **`apply_and_derive_mirror` has an explicit match arm for every
  `MetaCommand` variant, no wildcard** — a future variant fails to compile
  here until its mirror behavior is a deliberate decision. It takes
  `&mut Metadata` (not just post-apply state) **because `DropTableTablets`'s
  derived deletions depend on identities gone by the time `apply`
  returns** (its dropped-tablet-id set and its legacy `cp_member_addrs`
  prune — the dual `MergeTablets` case this once also covered was removed
  by ADR 0044) — diffing this way, rather than re-deriving the pruning
  predicate a second time, avoids the "two places must agree on a gating
  rule" hazard this crate's engineering practices warn about.

  `apply_key_write` is the single decode implementation shared by the
  bulk-rebuild and the incremental-delta consumer path (`animusd`'s
  `RemoteControlClient::observe_delta`), so they can't drift. Tested by
  this module's own unit tests, `tests/apply_engine.rs`'s `SimEnv`
  differential oracle, and `animusd`'s `tests/control_mirror_restart.rs`
  (a real `ProdEnv` restart).

## Key invariants

- **Config-in-log + current-term-commit gate (ADR 0017 C).** `LogEntry` may
  carry a `config: Option<voters>`; `RaftCore` keeps `peers`/`cluster_size` in
  sync with the latest log config (config rides snapshots + `InstallSnapshot`).
  `change_membership` appends a single-server config entry — one in flight, no
  leader self-removal, and **rejected until `commit_index >=
  first_term_index()`** (the index of the election no-op). This is the
  reconfiguration erratum guard; `first_term_index()` is also what the data
  plane's ReadIndex barrier gates on (Raft §6.4). The control plane itself never
  reconfigures, so its config stays `= initial_config`.

- **Election no-op is committed in `become_leader` itself.** After appending its
  no-op, `become_leader` advances commit, so a **single-node** group commits it
  immediately — which is what makes a restarted sole voter re-apply its
  recovered WAL tail instead of waiting for the next propose, and what stops any
  "current-term entry committed" gate from deadlocking a 1-of-1 group.

- **Commit advances only for current-term entries** via majority `matchIndex`
  (the Raft safety rule). Don't relax this.

- **Durable-before-visible, role-aware apply frontier (ADR 0009).** `apply`'s
  frontier is `min(commit_index, durable_index)` on the **leader** and
  `commit_index` on a **non-leader**. Only the leader's applied state is what a
  proposer acks on, so a command is leader-visible only after it is fsynced. A
  follower never acks a write (it only serves reads) and a committed entry
  already rests on a quorum of durable logs, so it applies on commit without
  waiting on its own fsync — gating there would only widen cross-node
  read-visibility lag. See "What's non-obvious" for the driver mechanics and
  hand-driven gotchas.

- **`BeginSplit`'s apply arm also enforces F11 token alignment on a
  streamed table (ADR 0042 §14, growth PR2).** A split key that isn't
  exactly `TOKEN_BYTES` (8) long is rejected outright when the source
  tablet's table has a stream (`self.table_stream(table).is_some()`) —
  an apply-time structural seatbelt: `animusd`'s `ClientCtx::trigger_split`
  is the one choke point that actually rounds a caller's key before ever
  proposing, so this check guards a future caller reaching apply without
  going through it, never the primary
  enforcement. See `meta::tests::split_rejects_a_non_token_aligned_key_on_a_streamed_table`/
  `split_rejects_a_token_aligned_key_equal_to_range_start` (the latter
  proving the accepted single-token hot-partition limit, Fork E, still
  rejects at the pre-existing `KeyRange::split_at` "strictly inside" guard
  rather than accepting a zero-width sibling).

- **Epoch-CAS discipline on `BeginSplit`/`CutoverSplit`/`CasTabletReplicas`.** Every
  tablet-mutating command is a compare-and-swap on the tablet's epoch, evaluated
  identically on every replica, so accept/reject is consistent and racing
  proposers can't both commit. (`MergeTablets` — ADR 0033, carrying *two*
  expected epochs since it read two tablets from one snapshot — was removed
  by ADR 0044; tablets are split-only.) Any new tablet-mutating command must
  adopt the same guard.

- **The ADR 0050 copy-based split lifecycle (Train B rung 3):
  `BeginSplit`/`CutoverSplit`.** `BeginSplit` (epoch-CAS + a state gate:
  parent must be `Active`) marks the parent `Splitting` — range and rows
  untouched, it serves until the workflow's freeze — and mints two
  `Building` children over the half-ranges at the command's own
  (proposer/placement-chosen, fork F5) replica homes, policy inherited,
  allocator floor enforced, F11 token-alignment seatbelt (above). `CutoverSplit` (epoch-CAS; parent must be
  `Splitting`; recomputes the children from the map — the two `Building`
  tablets inside the parent's range — rather than trusting carried ids)
  atomically activates both children, **removes** the parent (tablet +
  policy; the reconciler reclaims it as ordinary hosted-but-absent), and
  writes `Metadata::split_lineage[child] = SplitLineage {parent,
  parents_final_epoch, cutover_wall_ms}` — fork F9, recorded at the one
  moment the parent's shard chain is complete (never pruned; the B6
  `ParentShardId` source). Wall time rides the command
  (`cutover_wall_ms`), `SealStreamShard::seal_wall_ms`'s discipline — the
  state machine has no clock. The zero-copy split's own command and
  provenance maps (`SplitTablet`, `split_parents`, `stream_split_basis`)
  were deleted in the Train B rung-7 sweep — `split_lineage` is the sole
  split-provenance record. Placement (`reconcile_placement`/`rebalance_placement`) skips
  every non-`Active` tablet — the mid-split set is frozen. Mirror arms +
  `syskv::EntityKind::SplitLineage` follow the usual per-entity
  conventions; the `apply_engine.rs` differential oracle drives a full
  begin→cutover round.

- **The ADR 0058 Train 2 rung 3 in-place split lifecycle:
  `BeginSplitInPlace`/`CutoverSplit`'s in-place branch.** Same epoch-CAS +
  `Active`-state gate + F11 seatbelt + monotonic child-id allocator as
  `BeginSplit`, but mints **no** `Building` tablet-map rows at all — it
  records the intent directly on the parent
  (`Tablet::inplace_split = Some(InPlaceSplitIntent{split_key, children})`,
  `animus-tablet`) and marks it `Splitting`, full stop. There is nothing
  physical to place a policy on yet, so (unlike `BeginSplit`) no policy
  copy happens here. The data plane's own `KvCommand::SplitTablet`
  (`animus-cp-data`) — not this command — is what actually materializes
  the two children, entirely outside control-plane Raft; this command only
  ever sees the *intent*, never the fork itself.

  `CutoverSplit` gained an in-place branch, selected by
  `source.inplace_split.is_some()`: instead of scanning the tablet map for
  `Building` children (none exist), it creates both children's tablet-map
  rows DIRECTLY from the intent's own `(id, replicas)` pairs (each
  `replicas` is that child's placement-chosen FINAL homes — the data
  plane's own, larger `bootstrap_voters` bootstrap set is not this crate's
  concern) and inherits the parent's policy **at this moment** — the
  in-place workflow's only chance to, since there was no tablet row to
  attach it to at `BeginSplitInPlace` time. Otherwise identical to the
  copy-based branch: `split_lineage` written for both (fork F9, unchanged),
  parent removed. **G1 (ADR 0058's own "Open forks" table, decided
  2026-08-25, reversing that ADR's own Stage 4 draft text): the GSI-drain/
  backfill-seeder cutover vetoes stay PRE-cutover, caller-side, exactly as
  in the copy-based workflow** — this command's own apply never gated on
  drain state in either branch, so nothing about this in-place branch
  needed to change to honor that decision; the (not-yet-written)
  `animusd`-level in-place split driver is what will run those vetoes
  before ever proposing this command, mirroring `index_drain.rs::
  split_driver_tick`'s existing shape for the copy-based endgame. Mirror
  arms follow the usual per-entity conventions (`BeginSplitInPlace`:
  parent row + allocator counter only; `CutoverSplit`'s existing arm
  extended to also mirror each child's policy, unconditionally — a
  harmless duplicate write for the copy-based branch, the only source for
  the in-place one). Tests: `meta::tests::begin_split_in_place_*`/
  `cutover_split_in_place_*`, mirroring the copy-based tests' own shape
  scenario-for-scenario.

- **The backup catalog (ADR 0059 §3, Train 1 PR ①): `BeginBackup`/
  `RecordBackupTabletComplete`/`CompleteBackup`/`FailBackup`/`DeleteBackup`.**
  `Metadata::backups: BTreeMap<BackupId, BackupRow>` (`BackupId = String`,
  an opaque freshly-minted identity — never a table name, the ADR's own
  "scar": a name-keyed catalog would let a drop-then-recreate of the same
  table name silently poison a still-live backup row) plus `Metadata::
  backup_tablet_progress: BTreeMap<(BackupId, TabletId), BackupTabletProgress>`
  (mirroring `MarkIndexBackfilled`'s per-tablet-report shape). `BeginBackup`
  derives its whole manifest stub (an owned `TableSchema` clone + the
  table's current tablet list) from **already-agreed `Metadata` at apply
  time**, never from anything the proposer captured — the same
  determinism argument `BeginSplit`'s child ranges and `CutoverSplit`'s
  child recomputation already rest on (see `docs/engineering-lessons.md`'s
  entry on this). `CompleteBackup` requires every pinned tablet to have a
  progress row; `RecordBackupTabletComplete` is idempotent on an identical
  repeat but rejects a genuinely differing one outright (no repair-update
  path yet, unlike `SealStreamShard`'s replicas-only allowance).
  `BackupStatus` already carries an `Expired` variant for the (not yet
  built) two-phase retention janitor's mark phase, so that later PR doesn't
  reshape the enum. **`DropTableSchema`/`DropTableTablets` deliberately
  never touch `Metadata::backups`/`backup_tablet_progress`** — ADR 0024's
  explicit carve-out (ADR 0059 §3): a backup catalog row outlives its
  source table, which is what makes "restore a table dropped days ago"
  possible at all later in this train. `syskv::EntityKind::Backup`/
  `BackupProgress` follow the usual mirror conventions; `backup_progress_key`
  physically encodes `(tablet, backup_id)` (fixed-width field first, the
  `index_backfill_key` shape) even though `Metadata`'s own map key is
  `(BackupId, TabletId)` — see `docs/engineering-lessons.md`'s entry on why
  those two orders are independent decisions, not the same constraint
  twice. Scope of this PR: the catalog only — no capture driver, no
  `SegmentStore` plumbing, no wire API, no janitor loop (later PRs in the
  ADR 0059 stack).

## What's non-obvious

- **The sync/driver split is deliberate.** All consensus logic is in the sync
  `RaftCore` (unit-testable, deterministic); the driver only does I/O. When
  changing protocol behavior, change the core and keep it I/O-free — don't reach
  for the env inside it. The driver races `env.recv()` against a timer via
  `futures::select`, drawing `entropy` every iteration for randomized election
  timeouts.

- **The WAL `fsync` is raced inside that `select`, not awaited before it
  (issue #279).** `drive` used to `persist_wal` → `select` → step →
  `persist_wal` → send, both persists inline. That livelocks the control group
  whenever an `fsync` outlasts the 150 ms `election_base`: the blocked loop
  sends no heartbeats and re-arms no election deadline, followers campaign, each
  leadership change's no-op commit makes more persist work, repeat. The control
  group is not a bystander to the workload that surfaced this in the data plane
  — it is one of the replicas `fsync`ing concurrently during a
  split-during-backfill. Now only the messages that make a **durability claim**
  wait for their persist round (`RequestVoteResp{granted}`,
  `AppendEntriesResp{success}`, `RequestVote`, `InstallSnapshotResp`);
  `AppendEntries`/heartbeats and pre-vote traffic ship at once, which is what
  keeps the group alive. The accounting lives in **`persist_round.rs`, shared
  with `animus-cp-data`'s driver** — read its module doc before touching any of
  this. Two things specific to this plane:
  - **Three drainers, not two.** The consensus loop, the apply task's
    compaction rewrite, and the *public* `RaftNode::flush` (a graceful
    shutdown calls it from outside the driver). All three go through
    `persist_round::drain_for_round`, which is the only sanctioned drain
    precisely so a third or fourth one cannot take records without numbering
    the round that covers them.
  - **`flush`'s old doc claimed "the driver is parked, so this is the sole WAL
    writer".** That precondition is void now and is the exact shape of hazard
    the engineering-lessons log warns about: when a synchronous step becomes
    concurrent, every other writer's unstated "…while the loop is blocked" is
    load-bearing and must be re-examined.
  - This plane never quiesces (ADR 0048 fork G), so the timer arm is always
    present and the loop's `fully_durable` release is re-evaluated at least
    once per heartbeat interval — a belt the data plane does not have.
  Regression: `tests/slow_disk_no_livelock.rs` (verified red on the pre-fix
  driver across four seeds: 2/10 proposals accepted, the group leaderless).

- **One apply model, generic across both planes (ADR 0017, cut over to
  `Metadata` by ADR 0038 PR3).** `StateMachine::DRIVER_APPLIED = true` is now
  set for **both** `Metadata` (this crate) and the data plane's `KvState`
  (`animus-cp-data`) — the core never applies in-core for either; it buffers
  each committed-and-durable command as an effect (`drain_apply`, which only
  hands out fsynced commands, since engine I/O is async and the core is
  sync) for an async **apply task** to apply to a real `StorageEngine`.
  For `Metadata` that apply task is `node.rs`'s `meta_apply_loop`/
  `meta_apply_and_compact` — see that module's doc and the "syskv.rs"/
  "mirror.rs" entries below. `DRIVER_APPLIED = false` (in-core, synchronous
  apply) is still the trait default and is exercised by
  `generic_state_machine.rs`'s toy state machine (proving `RaftCore<C, S>`'s
  genericity in both directions), but no real state machine in this codebase
  uses it anymore.

- **Pre-vote (ADR 0009).** An election timeout does **not** campaign directly —
  the node becomes a `PreCandidate` and runs a `PreVote`/`PreVoteResp` round
  *without bumping its term*; only a pre-vote majority triggers the real,
  term-incrementing `start_election`. Peers grant a pre-vote only with **no live
  leader** (lease = `leader_id.is_some() && now < election_deadline`, or `role ==
  Leader`), so a briefly-stalled node can't inflate the term and disrupt a
  healthy leader. Pre-vote messages **bypass** higher-term step-down — the sole
  exception is a *rejecting* `PreVoteResp` with a higher real term, which reverts
  the pre-candidate to a follower at that term. Tick semantics: a multi-node
  election now needs a `PreVoteResp` grant fed before the real
  `RequestVote`/`RequestVoteResp`; a single-node group still elects on one tick
  (self is a pre-vote majority). `set_election_timeout(base, now, entropy)` makes
  the default-150ms base configurable for a node doing real disk I/O.

- **Learner (non-voting) membership class (ADR 0058 Train 1).** `RaftCore`
  gains a per-member `role`: alongside the existing voter `config`, a
  parallel `learners: BTreeSet<NodeId>` is kept in sync by the identical
  config-in-log discipline (a membership-change `LogEntry` carries both sets
  together, gated on the same `config.is_some()` check `config_at`/
  `config_change_in_flight` already use — see `LogEntry::learners`'s doc).
  Three points worth stating explicitly, since they are easy to get backwards:
  - **`peers`/`cluster_size` are derived from voters alone** (`apply_config`) —
    a learner is never in `peers`, so `maybe_advance_commit`'s replica tally
    and `majority()` need **zero** learner-awareness; this is what makes the
    "a learner never counts toward quorum" safety property hold by
    construction rather than by a scattered set of checks. A learner *is*
    still replicated to (its `match_index` tracked in the same
    `next_index`/`match_index` maps) via an explicit `peers ∪ learners` union
    at exactly three call sites: `broadcast_append`'s targets,
    `become_leader`'s next_index/match_index/last_contact seeding, and
    `quiesce_entry_ok`'s catch-up-complete check (plus `broadcast_quiesce`'s
    targets, so a fully-idle group's learners stop ticking too).
  - **A learner never campaigns for free** — `start_election`/`start_pre_vote`
    already gated on `is_voter()` before learners existed (the "pre-start a
    to-be-added node" gotcha below), so a learner (which is never in
    `config`) is simply a *durable* instance of that same transient state,
    not new logic. `handle_request_vote`/`handle_pre_vote`'s granting side
    and `handle_vote_resp`/`handle_pre_vote_resp`'s tallying side additionally
    gate on voter membership as a second, structurally-redundant line of
    defense (a learner is never solicited in normal operation — only voter
    `peers` are — so this only matters against a stray/injected message).
  - **The public surface is additive, not a signature change.** The existing
    `change_membership(voters)` keeps its exact old signature and behavior
    (learners untouched, byte-identical when no learner exists) — it gained
    one guard (`voters.is_disjoint(&self.learners)`, forcing a promotion
    through the dedicated method instead of an ambiguous direct add) but no
    new parameter, so every pre-existing call site across `animus-control`/
    `animus-cp-data`/`animusd` compiles unchanged. The new transitions are
    three sibling methods: `add_learner`/`promote_learner`/`remove_learner`
    (mirrored one-for-one as thin wrappers on `RaftNode` and
    `animus-cp-data::RaftKvNode`, exactly like `change_membership`'s own
    wrapper). `learner_caught_up(id, threshold)` is a pure predicate over
    already-tracked `match_index` state — the promotion-criterion primitive a
    later layer (the host reconciler) decides *when* to call; this train
    ships the primitive only, not the reconciler sequencing.

  Snapshot/WAL: the learner set rides the identical path the voter config
  already does — `LogEntry::learners`, `RaftMsg::InstallSnapshot::learners`,
  `WalRecord::Snapshot::learners`, `PersistedState::snapshot_learners` — so it
  survives compaction, `InstallSnapshot` catch-up, and restart the same way
  `config` does. `animus-cp-data::codec.rs`'s hand-rolled binary wire codec
  needed its own explicit encode/decode arms for both new fields (codec
  version bump to `22`) — the "grep every gating match site when a
  replicated/forwarded enum gains a variant" lesson applies just as much to a
  hand-rolled codec's field list as to a command-enum match arm; a codec that
  silently dropped `learners` on the wire would desync every replica's view
  of who is a learner the moment any message crossed it.

  **Deliberately out of scope for this primitive** (left to the reconciler
  layer, ADR 0058 Train 2 or later): notifying a *removed* learner of its own
  removal the way `departing` does for a removed voter (harmless — a removed
  learner can never campaign regardless of whether it learns about the
  removal, since it was never in `config` to begin with; only a cleanliness
  concern, not a safety one) and any policy for *when* to call
  `promote_learner` (the host reconciler's replica-move sequencing).

- **Leadership transfer (`RaftCore::transfer_leadership`, ADR 0029).**
  Originally a per-tablet CP-data primitive living here because the sync
  core is shared, described in an earlier revision of this note as
  something "the control plane never calls" — **stale since ADR 0037**:
  `animusd::ClientCtx::admin_remove_control_member`'s leader-self-removal
  branch (`POST /admin/control/member/remove`) now arms one directly on
  the **control** group too, for the identical Raft §3.10 reason (the core
  always rejects removing the current leader, so relocating it needs a
  handoff first). `change_membership` always rejects removing the current
  leader, so relocating a leader's own replica needs a Raft §3.10
  handoff: arm a transfer to a voter with `peer_match(target) >= commit_index()`
  (no config change in flight; records a **single** one-election-timeout
  deadline **at arm time, from `now`, not re-derived from replication
  progress** — see the issue #405 note below for the caller-side
  consequence of that), then
  **freeze** `propose`/`change_membership` (return `NotLeader` hinting the
  target) so the log stops growing, and send `TimeoutNow` only once the target
  **reaches `last_log_index()`** (re-sent every heartbeat until step-down). A
  target that never steps down by the deadline **aborts** (clears the arm,
  resumes proposals). Re-arming the same target is idempotent and does **not**
  push the deadline (else a perpetual retry starves the abort check). The selector
  and the arm gate must read the *same* threshold, and the return value ("did it
  arm") must never be discarded — see the engineering-lessons log (root
  `CLAUDE.md`) for the war story where they diverged.

  **A single arm attempt can legitimately fail with no retry of its own
  (issue #405, `animusd`'s own gotcha, noted here since the root cause
  lives in this method's own gate).** `transfer_leadership` is meant to be
  re-armed every tick by a caller that needs the target to *eventually*
  catch up (`RaftKvNode::reconfigure_step`'s per-tablet pattern, its own
  doc above) — a **one-shot** caller like `admin_remove_control_member`
  instead calls it exactly once, so the arm only succeeds if `peer_match
  (target)` has already caught up to `commit_index()` at that precise
  instant. Ordinary background control-group churn (a liveness `UpsertMember`
  proposal, a placement reconcile) can advance `commit_index` between an
  operator's own "are the voters converged yet" check and this call, and a
  loaded machine widens that window — see `crates/animusd/CLAUDE.md`'s
  issue #405 entry and `docs/engineering-lessons.md`'s matching entry for
  the full account and the fix (retry the *whole admin call*, not just the
  side effect, since every refusal this one-shot arm can produce is
  equally retryable and maps to the identical HTTP status).

- **Snapshot transfer is chunked and O(chunk), not O(state).** A follower
  behind the compacted prefix is caught up via a chunked `InstallSnapshot`,
  all in the sync core (deterministic). `snapshot_chunk_for` **slices
  `snapshot_blob` by reference — it does NOT re-serialize per chunk**; a
  naive per-chunk serialize on a multi-MB metadata pins the loop past the
  election timeout (a self-sustaining election storm, invisible to
  `SimEnv`'s virtual clock). Blob management differs by state-machine kind:
  - **In-core (`Metadata`):** the blob is kept **eagerly**, so the
    invariant `snapshot_index > 0 ⟹ blob.is_some()` holds and a chunk is
    never a 0-byte ship (regression:
    `install_snapshot.rs::caught_up_control_node_reships_non_empty`).
  - **`DRIVER_APPLIED` (data-plane KV):** the image is the *engine* bytes,
    built **lazily on demand** and dropped whenever it would go stale/idle,
    so no whole-tablet image is retained at rest (regression:
    `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`).

  Liveness teeth:
  `install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state` +
  `tests/prod_liveness.rs`.

- **Durable-before-visible mechanics + hand-driven gotchas.** The driver
  advances the durable watermark via `mark_durable_through` in `flush_wal`,
  immediately after `env.sync(WAL)` (passing the drain-time `last_log_index`);
  `recovered()` sets it to the recovered `last_log_index`. The leader gate closed
  the acked-before-durable window that flaked `animusd`'s
  `create_table_survives_node_restart`. Gotchas: (1) a *leader* core driven by
  hand must simulate the fsync — drain, then
  `mark_durable_through(last_log_index())` — or its `metadata()` never reflects
  proposals (see `persistence.rs`); a hand-driven *follower* applies on commit
  with no fsync (see `follower_visibility.rs`). (2) A read on a follower right
  after a leader `CreateTable` must still wait for the definition to *replicate*
  there (`await_table_*` in the `animusd` tests) — a cross-node race independent
  of the local durable gate.

- **Automatic placement + rebalancing (ADR 0005, 0029).** Policies are
  replicated (`SetTabletPolicy` → `policies`). The decision is the pure
  `Metadata::reconcile` (repair: `animus_placement::replan` over `Active`
  members, emits a `CasTabletReplicas` only for policy-violating tablets) and its
  balance-driven complement `Metadata::rebalance` (`rebalance_step` picks a
  single balance-improving healthy-replica move, wrapped as a `CasTabletReplicas`
  at the current epoch — reusing the command, so no relay-allowlist change). The
  **leader** drives both in one `reconcile_loop`: repair first each tick, and
  rebalance only if repair proposed nothing *and* `tick % REBALANCE_EVERY_N_TICKS
  == 0`. That interval is pure churn control, not a safety invariant —
  correctness rests on the epoch-CAS and the data-plane catch-up gate. Keep the
  *timing* in the driver and the *decision* pure. A split child inherits the
  source's policy (else it is invisible to both repair and rebalance).

- **Automatic failure detection (ADR 0012).** Members heartbeat the control
  group (`heartbeat_loop` → `RaftMsg::Heartbeat`, a term-less message the driver
  **intercepts** in its `recv` arm and feeds to the pure `FailureDetector` — the
  core never sees it). The **leader** drives it: `detect_loop` proposes
  `UpsertMember{Active/Down}` for any tracked member whose liveness changed
  (`liveness_transitions`, idempotent — preserves labels, skips
  `Joining`/`Leaving`, and **only judges members that have heartbeated**). A
  committed `Down` cascades into re-placement. A freshly elected leader's
  detector is **cold**, so `detect_loop` applies a post-election grace
  (`LEADER_GRACE`, one `DETECT_TIMEOUT`, tracked via `Env`-time `leader_since`)
  during which it passes `allow_down = false`, so a new leader can't falsely mark
  live members `Down` before heartbeats repopulate the detector (recoveries are
  never suppressed). These loops are driven in production (`animusd`, proven over
  `ProdEnv`/TCP in `animusd/tests/self_heal.rs`). Detector state is per-node
  volatile; only transitions are replicated.

- **Orphan-member auto-reclaim sweep (ADR 0040 PR6), same home and pattern
  as the detector above.** `orphan_sweep_loop` is the leader's own volatile
  timer that reclaims a `RegisterNode`/`admin_add_member` claim whose node
  **never showed up at all**, as opposed to a real member that's merely
  currently `Down`. See ADR 0040 for the full mechanics (candidate set,
  timers, config knobs).

  **Safety argument for a sweep proposal racing a genuine late activation**
  (the one property that must never fail): `RemoveMember`'s own apply-time
  guard rejects `Active`/`Joining` outright, so neither commit order ever
  removes an already-`Active` member; and `liveness_transitions` only
  proposes a promotion for a member present in that same tick's fresh
  `Metadata` read, so a removed claim is never resurrected by a stray late
  heartbeat either — both proven directly as pure state-machine/decision-
  function properties in
  `meta::tests::remove_member_never_removes_a_member_that_activated_first_
  regardless_of_proposal_order` and
  `node::tests::liveness_transitions_never_proposes_for_an_absent_member`.
  Full seeded fault-injection suite: `tests/orphan_sweep.rs`.

- **Replicated schema catalog (ADR 0013).** `Metadata.schemas` is mutated only by
  the `*TableSchema` commands, so it is Raft-replicated and recovered from the
  WAL/snapshot like all metadata (no `persist.rs`/`InstallSnapshot` change — the
  snapshot is a full `Metadata` image). Secondary-index *definitions* ride the
  same path (`TableSchema.indexes`, mutated by `Create/DropTableIndex`) — only
  the index *shape* is replicated; the index *entry data* stays at the wire edge,
  rebuilt from observed writes.

- **Observability metrics (ADR 0015).** All from `Env`-supplied or core-derived
  inputs (deterministic): election counters + `is_leader` gauge
  (`record_transition`); `append_entries_sent`/`_rejected` + `snapshot_installs`
  (off emitted messages, `record_outbound`); `failure_detector_down`/`_up` (the
  `Active`↔`Down` edges `detect_loop` proposes). `RaftNode::start` records into
  `env.metrics()`; use `start_with_metrics` to thread a handle a sim test can
  read (`SimEnv::metrics()` is the no-op default, so no `animus-sim` change).

- **`MetadataWatch` (ADR 0031).** A wake-a-parked-task pattern (like
  `animus-cp-data`'s `ProposeSignal`), adapted to notify an *external* caller
  rather than the driver's own loop. Three points to remember if you touch or
  copy it: (1) it carries a **monotonic watermark** (`AtomicU64`, the observed
  `last_applied()`), not a one-shot consumed flag — `changed()` re-checks
  `current > last_seen` fresh every poll, so there is no wake-before-park race
  (a change that already happened resolves on the first poll). (2) It is
  bumped from the **driver loop** (`drive`), not the proposer, via `fetch_max`
  at exactly the points `last_applied` (gated by the same role-aware frontier
  `metadata()` uses) can have moved — so defensive calls on no-op iterations
  are free. (3) It is **multi-waiter** — a `Mutex<BTreeMap<u64, Waker>>`
  registry keyed by a per-`changed()`-future slot id, not a single
  `AtomicWaker`: any number of concurrent callers (across any number of
  handle clones) park independently, and `bump` wakes all of them. It used to
  be single-waiter (one `AtomicWaker`, one intended consumer: the per-node
  reconciler) until ADR 0035 PR5 started handing the same handle to a second
  concurrent consumer (each inbound `WatchMetadata` RPC's long-poll) — see
  `docs/engineering-lessons.md` for the lost-wakeup that produced (issue
  #276) and why the fix is multi-waiter, not a single-consumer contract
  restored by convention. Don't add a propose-side wake here the way
  `animus-cp-data` did — a metadata-watch caller only ever waits to learn when
  `metadata()` *could* reflect a change, and that visibility is bound by the
  driver's flush cadence anyway.

## Tests

`cargo test -p animus-control` (use `run_for`, never `run()` — perpetual
heartbeats). One binary per behavior; the file names describe them
(`ls crates/animus-control/tests/`) — covering Raft core mechanics
(election/replication/leader-kill, the DRIVER_APPLIED apply gate, pre-vote,
leadership transfer, snapshot/InstallSnapshot), the ADR 0038 mirror/delta
differential oracles, runtime control-membership change (ADR 0037) and its
liveness guard, the ADR 0040 registration CAS and orphan sweep, placement/
failure-detection/schema-catalog/metrics end-to-end scenarios, and
`prod_liveness.rs`'s real-thread `ProdEnv` smoke tests for properties
`SimEnv`'s virtual clock can't see, and `slow_disk_no_livelock.rs`'s
slow-`fsync` driver-liveness regression (issue #279, via
`DiskConfig::set_sync_delay`).

**Test-design gotcha this file's own history records**: do not drive load with
`UpsertMember` for node ids that will never heartbeat. The leader's orphan
sweep (ADR 0040 PR6) then proposes a `RemoveMember` the state machine rejects
every tick, flooding the log with hundreds of entries — the first draft of
`slow_disk_no_livelock.rs` measured that churn's throughput instead of the
property it meant to. `CreateTableSchema` is inert: nothing in the driver
reacts to it.
