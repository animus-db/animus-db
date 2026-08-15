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

- **`meta.rs`** — the `Metadata` state machine and its command enum.
  `Metadata::apply` is the deterministic state machine; `Metadata::reconcile`
  and `Metadata::rebalance` are the *pure* placement decisions (see Invariants).
  `Metadata` holds members, the tablet map, placement policies, the table-schema
  catalog, keyspaces, `node_addrs` (member id → full `NodeAddrs { internal,
  client, admin, role }`, ADR 0032 PR1) and the legacy `cp_member_addrs` (kept
  for WAL back-compat). ADR 0040 PR1 merged the pre-existing `raftkv`/`control`
  address pair into one `internal` field — one identity per node, one internal
  env, one address to replicate per node. `NodeAddrs.role: String` (ADR 0035
  residual follow-up, `#[serde(default = "combined")]` for WAL back-compat) is
  a member's own deployment role (`"control"`/`"data"`/`"combined"`) — a plain
  string, not an `animusd`-side enum, since this crate has no dependency on
  `animusd`; a node only ever authoritatively knows its own role, so it is
  stamped once at self-registration time; `animusd`'s `/admin/peers` reads
  every *other* node's role straight off this field instead of fanning out to
  each node's own `/admin/config`. `PlacementView` is the narrow (members +
  tablets + policies, no schema) clone that `RaftCore::placement_view()` hands
  the driver loops so they evaluate off the core lock instead of cloning the
  whole `Metadata` every tick.

  `MetaCommand` variants (all applied in log order, see the enum's own doc
  comments for the exact per-variant contract): membership (`NoOp`,
  `UpsertMember`, `RemoveMember` — ADR 0032 decommission, gated on the
  member being absent already, `Leaving`/`Down`, and unreferenced by any
  tablet; extended by ADR 0040 to also prune an orphaned
  claim-without-member `node_addrs` entry, the shape the orphan-member sweep
  below proposes); tablets (`CreateTablet`/`CasTabletReplicas`, epoch-CAS;
  `SplitTablet` — ADR 0028, the *entire* split, epoch-CAS gated, no
  data-plane half; tablets are **split-only** — `MergeTablets` and its dual
  reaction existed under ADR 0033 and were removed entirely by ADR 0044;
  `SetTabletPolicy`); schema/keyspace
  (`Create/Drop/ReplaceTableSchema` — ADR 0013; `DropTableTablets` — ADR 0024
  GC; `Create/DropTableIndex`; `SetTableMode`; `SetTableStream` (ADR 0042
  §2/§4/§9 — enable/disable a table's DynamoDB Streams config,
  `schema::StreamSpec { view_type, label }` on `TableSchema.stream`; enable
  is rejected if a stream is already enabled, since a fresh `label` is only
  ever minted through an explicit disable → re-enable, never a
  same-command relabel — what makes `(table, label)` a stable identity for
  as long as the stream lives; the label itself is minted by the proposer,
  `animusd`, through its own `env.now()`, never `Metadata::apply`, which
  only ever records whatever `StreamSpec` it's handed); `SealStreamShard`/
  `ExpireStreamShards` (ADR 0042 §3/§9, ADR 0043 §A3/§A8/§A9 — the segment
  catalog, below); `Create/DropKeyspace`);
  addressing (`RegisterNodeAddrs` — update-only since ADR 0040, rejects if
  `node` is absent from both `members` and `node_addrs`; `RegisterCpAddr` —
  the predecessor, kept for WAL back-compat only); and `RegisterNode` (ADR
  0040 Decision C), below.

  **`SealStreamShard`/`ExpireStreamShards` are the segment catalog**
  (`Metadata::stream_shards: BTreeMap<(TabletId, u64), StreamShardRow>`).
  **Keyed by `(tablet, epoch)` alone, never `(table, label, tablet,
  epoch)`** — a tablet id already implies its table, and a tablet's own
  epoch counter is a property of its physical seal history (counts up from
  its first seal, never resetting across a disable/re-enable cycle), not
  of any one stream generation; `table`/`label` live inside
  `StreamShardRow` as descriptive fields. `SealStreamShard` is
  **first-committer-wins on that key's content** (round-3 PR7 amendment):
  a second proposal for an already-recorded identity whose content
  (everything but `replicas`) matches exactly is a genuine `NoOp` if
  `replicas` also matches (the sealer's own crash-retry racing itself, by
  design), or an in-place **`Applied` replicas-only update** if `replicas`
  differs — the shape the segment janitor's own replica-repair sweep
  produces (ADR 0043 §A9): it re-proposes the identical committed shard
  with a freshly-repaired `replicas` set, never touching any other field.
  A proposal whose non-`replicas` content genuinely conflicts is still
  rejected as a `NoOp`, exactly as originally designed — this is safe for
  every reader because `GetRecords`/the janitor always re-fetch the row
  fresh before consulting `replicas`, and repair is the only production
  caller that ever proposes a different `replicas` for an existing
  identity, so there is no other writer to race against. Validated
  against **either** the table's current schema `StreamSpec.label` **or**
  an existing catalog row already present for that `(table, label)`
  (F12-b: a disabled stream's un-reaped rows still license a further seal
  of the same generation, e.g. the disable-triggered final seal proposed
  after `SetTableStream{None}` already cleared the schema), plus a
  permissive-but-sane epoch-chain check (`epoch == 0` always accepted;
  `epoch > 0` needs a local `epoch - 1` row or `split_parents` provenance
  for this tablet). Relayable — a tablet leader proposing its own seal
  may run on any data node, not necessarily one control-connected at
  all. `ExpireStreamShards { rows: Vec<(TabletId, u64)>, remove: bool }`
  is the segment janitor's (`animusd::segment_janitor`, round-3 PR7)
  two-phase reclaim, reused directly for the drop-table cascade too
  (that cascade has no dedicated code path of its own — see
  `animusd/CLAUDE.md`'s `segment_janitor.rs` entry for the convergent
  design): `remove: false` **marks** every named row `expired: true`
  (idempotent; never a visibility gate — a marked-but-not-removed row is
  still fully valid to serve), `remove: true` **physically removes** it
  (idempotent). **Deliberately NOT relayable** — its only intended
  caller (the segment janitor, a control-plane-leader-only background
  loop like `detect_loop`/`orphan_sweep_loop`) always already holds a
  live `RaftNode` handle when it decides to act, so it proposes directly
  and has no structural need for a relay path; see `animusd`'s
  `is_relayable_command` for the full access-restriction argument
  (mirrors `RemoveMember`'s own exclusion). **The caller (never this pure
  state machine) is responsible for never removing a tablet's own current
  highest-epoch row while that tablet still exists** — `SealStreamShard`'s
  own epoch derivation (mirrored in
  `animusd::index_drain::seal_now`/`dynamo_streams::current_open_epoch`)
  is "the chain's own highest existing row, plus one," so physically
  removing that row out from under a still-live tablet would let a future
  seal silently reuse the same epoch number; see `animusd/CLAUDE.md`'s
  `segment_janitor.rs` entry for the guard that upholds this. `Metadata`
  accessors:
  `stream_shard_chain(table, label, tablet)` (one tablet's chain in
  ascending epoch order), `stream_shard_watermark(tablet)` (the tablet's
  own last-sealed end-HLC, regardless of label — restricted to *this*
  tablet's own chain, `None` for a fresh split child with no rows of its
  own yet), `effective_stream_shard_watermark(tablet)` (round-3 sealer PR,
  ADR 0043 §A4/§A6 — the one the sealer/hot-trim arm actually calls: walks
  `split_parents` provenance when the plain accessor above answers `None`,
  so a fresh split child inherits its parent tablet's own last-sealed
  end-HLC instead of reading as absent, transitively through a chain of
  splits), `stream_shard_rows_for_label(table, label)` (every
  row across every tablet), `stream_labels_with_rows(table)` (F12-b's
  coexistence set), `stream_shard_parent_id(tablet, epoch)` (derived
  `ParentShardId`, never stored redundantly),
  `stream_view_type(table, label)` (PR6's `DescribeStream` catalog
  amendment — the table's *current* `StreamSpec.view_type` when `label`
  is still enabled, else the last-known value carried by any of the
  label's own catalog rows; both `StreamShardRow` and `SealStreamShard`
  grew a `view_type: StreamViewType` field, `#[serde(default)]`,
  specifically so a `DISABLED`-but-unreaped stream's grace-window
  `DescribeStream` — which has no live `StreamSpec` to read once
  `SetTableStream{None}` commits — still has somewhere to read its
  last-known view type from; a view type never changes mid-stream, so
  every row of one label carries the identical value). The ADR 0042 §12/ADR
  0043 §A5 "F1" stopgap — `MergeTablets`'s apply arm rejecting outright on a
  streamed base table — is now moot: ADR 0044 removed `MergeTablets` and
  tablet merge entirely (tablets are split-only), taking the guard with it.
  Mirrored into `syskv::EntityKind::StreamShard`, keyed by the raw 16-byte
  `tablet.to_be_bytes() ++ epoch.to_be_bytes()` concatenation
  (`syskv::stream_shard_key`/`decode_stream_shard_id` — fixed-width, so no
  internal escaping is needed the way a variable-length id would).

  **`Metadata::stream_shards`'s own field-level codec, not its natural
  `BTreeMap<(TabletId, u64), _>` shape, is what actually rides the wire.**
  `serde_json` cannot serialize a tuple (or any non-string) map key at
  all — `MapKeySerializer` errors "key must be a string" the moment the
  map is non-empty, which every pre-existing whole-`Metadata` round-trip
  test missed by never populating it. `#[serde(with =
  "stream_shards_codec")]` (a small in-file module right below the field)
  encodes/decodes a flat `Vec<{tablet, epoch, ...StreamShardRow fields}>`
  instead, via `#[serde(flatten)]` — no duplicate-field struct to drift
  out of sync with `StreamShardRow` itself. The in-memory type is
  unchanged (still a plain `BTreeMap`, still `.get`/`.insert`/`.range`-able
  everywhere else in this crate); only `Metadata`'s own
  `Serialize`/`Deserialize` impl is affected. Regression:
  `meta::tests::metadata_round_trips_through_json_with_populated_stream_
  shards`. See `docs/engineering-lessons.md` for the general "an empty
  collection can't prove a map-key encoding rule" lesson this is an
  instance of.

  **`RegisterNode` is the sole claim path for a fresh node identity**,
  retiring ADR 0036's `AllocateNodeId` monotonic allocator entirely. `node`
  may be self-minted (`NodeId::mint`) or operator-/config-proposed
  (`NodeId::propose`) — treated identically. **The CAS key is
  `Metadata::node_addrs` alone, not `members`**: an id absent from
  `node_addrs` claims the address slot (inserting a `Down` `Member` too, but
  *only* if `members` doesn't already have an entry — membership can be
  independently pre-established by `UpsertMember`'s bootstrap insert or
  `admin_add_member`'s operator-labeled row); a byte-identical
  re-registration is `NoOp` (idempotent retry / ADR 0032 rejoin); a
  *different* `NodeAddrs` already on file is `Rejected` — the real
  collision. **Keying on `node_addrs` rather than the full
  `NodeAddrs`+`labels` pair is load-bearing, not an oversight**: a
  labels-inclusive CAS breaks the moment two *independent* commands can each
  partially establish the same identity, which several call sites in
  `animusd` do — see `MetaCommand::RegisterNode`'s own doc and
  `docs/engineering-lessons.md`'s entry for the integration-test failure
  that caught the naive design.

  `Member.has_activated: bool` (ADR 0040 PR6, `#[serde(default)]`) is
  **sticky**: `Metadata::apply`'s `UpsertMember` arm sets it the moment a
  member's status is *ever* recorded `Active` — by any caller, the ADR 0012
  detector's `Down`→`Active` promotion or `bootstrap`'s direct `Active`
  insert alike — and never clears it again, regardless of any later
  transition. Deliberately **not** scoped to "only the detector's own
  promotion": a bootstrap-declared member starts `Active` directly, never
  passing through `Down`→`Active` at all, so a narrower rule would leave a
  founding member's `has_activated` permanently `false` — indistinguishable
  from a genuine orphan the instant it later legitimately crashes. This is
  the field the orphan-member sweep (`node.rs`, below) keys on to
  distinguish "never showed up" from "was alive, currently down."
  `Metadata::orphan_sweep_candidates() -> BTreeSet<NodeId>` is the pure
  candidate-set predicate the sweep's driver loop calls every tick: the
  **union** of `members`' and `node_addrs`' keys (a claim can exist in
  either, or both — `admin_add_member`'s bare `UpsertMember{Down}` growth
  registration claims only `members`; a control-role `RegisterNode` claims
  only `node_addrs`), filtered to `status == Down && !has_activated &&
  tablets_referencing == 0` for a `members` row, or unconditionally
  eligible for a claim-without-member id (its only remaining safety gate —
  "is this currently a live control voter" — is the driver's job, since
  `RaftCore`'s voter config lives nowhere in `Metadata`). A candidate set,
  never a removal decision on its own — the driver still requires
  persistence across `orphan_sweep_after` and the voter exclusion before
  proposing anything.

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
  `reconcile_loop` (the leader's automatic placement reconciler + rebalancer),
  `detect_loop` (the leader's failure detector, ADR 0012), `orphan_sweep_loop`
  (ADR 0040 PR6, below), and the
  `heartbeat_loop`/`send_heartbeat` helpers a member runs to heartbeat the
  control group. Records control-plane **metrics** (ADR 0015) via
  `record_outbound`/`record_transition`; `metrics()` exposes the handle and
  `start_with_metrics` lets a sim test supply the sink. Read-only state
  accessors (`role`/`term`/`leader`/`is_leader`/`commit_index`/`last_applied`/
  `durable_index`/`snapshot_index`/`log_len`/`last_log_index`/`config`) back the
  `animusd` admin interface (ADR 0020). `metadata_watch() -> MetadataWatch`
  (ADR 0031) is the executor-agnostic "applied index advanced" notification
  the per-node CP reconciler uses to react to a `Metadata` change without
  polling.

  **Runtime control-group membership change (ADR 0037).**
  `change_membership`/`transfer_leadership` are thin wrappers over the
  identical-shaped `RaftCore` methods `animus-cp-data` already drives for a
  per-tablet group — the control plane's *own* Raft group can grow/shrink/
  replace a voter one server at a time, recorded under their own
  `ControlReconfigureAccepted`/`Rejected` metric family (kept separate from
  cp-data's per-tablet ones). Unlike cp-data's `propose_and_wake`, there is
  no propose-side wake seam here — a proposal is always serviced on the
  driver's next heartbeat tick. The admin/CLI surface lives in `animusd`
  (`admin_add_control_member`/`admin_remove_control_member`,
  `POST /admin/control/member/{add,remove}`, `animus admin
  control-{add,remove,grow}` — see that crate's `CLAUDE.md`); this crate's
  own tests stay core/driver-level only.

  **Control-id-native liveness signal.** `ControlHandle::believes_alive` is
  keyed to **raftkv** ids (the failure detector only observes heartbeats on
  the data role, ADR 0012), so it can't answer "is this control voter
  alive." Instead, `RaftCore::peer_last_contact(node) -> Option<Nanos>`
  (`raft.rs`) is the `now` of the last `AppendEntriesResp` (success **or**
  reject — either proves reachability), backed by a volatile map seeded per
  peer in `become_leader` — deliberately **never persisted or snapshotted**,
  like `next_index`/`match_index`. `RaftNode::control_peer_believed_alive`
  turns that into policy: always `true` for self or a peer never yet
  contacted this leadership stint (grace for a just-added voter), else
  gated on `CONTROL_PEER_LIVENESS_TIMEOUT = 500ms` (a separate constant
  from ADR 0012's `DETECT_TIMEOUT` — general network reachability and
  control-Raft-traffic reachability are independently tunable questions).
  `animusd`'s `admin_remove_control_member` is the consumer. Regression:
  `tests/control_membership.rs::
  last_contact_ages_out_a_partitioned_peer_but_not_a_healthy_one`.

  **`Metadata` is `DRIVER_APPLIED` (ADR 0038): the driver is split into a
  consensus loop and an async apply task**, mirroring `animus-cp-data`'s
  proven shape exactly:
  - `drive()` (the **consensus loop**) recovers from the WAL, spawns the apply
    task, then only persists (`persist_wal`), steps the core, and ships
    outbound messages — **no engine I/O**, so it always services
    heartbeats/`AppendEntries` within the election timeout regardless of how
    slow an engine merge or compaction is (the reintroduction of the
    `animus-cp-data` election-storm bug class this split exists to prevent).
  - `meta_apply_loop`/`meta_apply_and_compact` (the **apply task**, spawned by
    `drive` right after WAL recovery) owns the *only* mutable `Metadata`
    (a private `shadow`, never shared with the core). It rebuilds `shadow`
    from the engine (`mirror::rebuild_metadata_from_engine`), seeds its
    watermark from the engine's own `_applied_index` key (**not**
    `core.last_applied()`, which can understate what the engine already
    durably holds), drains `RaftCore::drain_apply()` **skipping any command
    whose index the watermark already covers** (the robust, index-based
    restart-tail filter — not reliance on incidental command idempotency),
    applies survivors via the real `Metadata::apply` (through
    `mirror::apply_and_derive_mirror`), merges the derived writes into the
    engine, and publishes into `cache: Arc<Mutex<Metadata>>` — bumping
    `MetadataWatch` only *after* that publish, so a watcher never observes a
    change before it is both durable and visible. **Every reader now reads
    `cache`, never the core** (`metadata()`/`members()`/`placement_view()`);
    `reconcile_loop`/`detect_loop` still read leadership/term off the core (a
    consensus-level fact) but the placement view off `cache`. Snapshotting
    reuses the same lazy-image machinery `animus-cp-data` uses, retargeted
    at `syskv_image`/`install_syskv_image`. `start`/`start_with_metrics`
    **require** a `StorageEngine` — there is no engine-less control-plane
    deployment shape.

  **Incremental `WatchMetadata` deltas (ADR 0038 "Phase 2").**
  `meta_apply_and_compact` also pushes one [`delta_ring::DeltaRing`] entry
  per drained command in the same pass that publishes `cache`/bumps
  `engine_applied` — *before* bumping `MetadataWatch`, so a watcher woken by
  that bump always finds the ring already populated. `RaftNode::
  watch_delta_since(last_seen) -> Option<DeltaReply>` is the public read
  side `animusd`'s `ClientCtx::watch_metadata` calls: `Some` when the ring
  contiguously covers `(last_seen, engine_applied_index()]`, `None`
  otherwise (the caller falls back to a full `metadata()` clone). The ring
  is cleared whenever `cache` is rebuilt from a jump it didn't witness (a
  received `InstallSnapshot`) — **not** on the apply task's own
  startup/restart rebuild, since a fresh ring is already empty by
  construction there.

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
  plain data (no I/O/clock/RNG): `TableSchema` (now also carrying `stream:
  Option<StreamSpec>`), `ColumnType`, `SchemaCatalog` (a
  `BTreeMap<TableName, TableSchema>` held in `Metadata`), and
  `IndexDef`/`IndexKind`/`IndexProjection` (the replicated GSI/LSI *shape*,
  not its entry data). `TableSchema::validate` is the pure malformed-schema
  check the state machine applies (unique index names; an LSI requires a
  sort attribute) — `stream` has no validation of its own (any `StreamSpec`
  a `MetaCommand::SetTableStream` hands it is already well-formed by
  construction). `StreamSpec { view_type: StreamViewType, label: String }`
  (ADR 0042 §2/§4) is a table's DynamoDB Streams configuration when
  enabled; `StreamViewType` (`NewAndOldImages`/`NewImage`/`OldImage`/
  `KeysOnly`) is a **read-time projection only** — a shard record always
  stores both images regardless (ADR 0043), so a view-type change never
  needs a backfill. `Metadata::table_stream(table) -> Option<&StreamSpec>`
  is the read accessor, alongside `table_schema`/`table_indexes`.

- **`persist.rs`** — `WalRecord`, `PersistedState` (durability/recovery; the
  write/compact/recover flow is diagrammed in `docs/wal.md`).
  `encode_snapshot_record_from_blob` encodes the WAL `Snapshot` line **reusing
  the core's cached serialized image** (`snapshot_blob`, via `serde_json`
  `RawValue`) — for an in-core state machine this serializes its whole state
  once per compaction, not twice, guarded by
  `snapshot_record_blob_reuse_round_trips`. **`Metadata` is `DRIVER_APPLIED`
  (ADR 0038), so its WAL `Snapshot` record's `metadata` field is always the
  meaningless `Metadata::default()`** (the real durable state lives in the
  system-keyspace engine) — this reuse path is exercised by this crate's
  other `DRIVER_APPLIED` uses (`driver_applied_sm.rs`'s toy state machine)
  and by `animus-cp-data`'s identical-shaped `KvState`, not by `Metadata`.

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
  claim; `entity_key(EntityKind, id)` encodes `escape(RESERVED_NAMESPACE) ||
  escape(kind) || escape(id)` reusing `animus_tablet::escape` byte-for-byte
  (this crate already depends on `animus-tablet`, unlike the wire adapters,
  which deliberately *duplicate* `escape` to stay dependency-light). One
  `EntityKind` per `Metadata` collection (`Tablet`/`Member`/`Schema`/
  `Policy`/`NodeAddrs`/`Keyspace`/`Counter`/`CpMemberAddr`/`SplitParent`/
  `StreamShard`), each with a typed `*_key` helper, plus a
  dedicated `applied_index_key()` watermark (a sibling of the entity-kind
  segment, not under one — mirrors `animus-cp-data`'s
  `engine_applied_index`). `decode_key` inverts every `*_key` helper for the
  mirror's own engine-scan path (`mirror::rebuild_metadata_from_engine`) and
  this module's round-trip tests. ADR 0036's allocator-era `NodeIdAlloc`
  kind was removed along with the allocator itself in ADR 0040 PR4 —
  `RegisterNode`'s claim lives entirely in the already-mirrored
  `Member`/`NodeAddrs` kinds, no separate ledger needed.

  **`is_reserved_name`**: called from `Metadata::apply`'s
  `CreateTableSchema`/`CreateKeyspace` arms (the state-machine-level,
  every-replica-agrees gate) and from both wire edges' `CreateTable`/`CREATE
  KEYSPACE`/`CREATE TABLE` paths (client-side, so a collision surfaces as an
  immediate `ValidationException`/`ERR_INVALID` instead of an opaque
  commit-wait timeout) — same two-layer idiom the existing duplicate-table
  check uses. Matching is a case-sensitive prefix test (exact match *or*
  merely prefixed, e.g. `__animus_system_backup`) — a combined node's
  mirror writes directly through this same already-globally-namespaced
  engine with no further `StorageScope` wrapper, and a prefix match is the
  collision that scheme cannot tell apart from a real system key.

  `EntityKind::as_str`/`from_segment` are `pub` for `animusd`'s read-only
  `GET /admin/system-table` browse endpoint. `reserved_scan_bounds() ->
  (Vec<u8>, Vec<u8>)` is the `[start, end)` pair covering the **entire**
  reserved namespace — **the load-bearing bound the admin endpoint scans
  with instead of `StorageEngine::entries()`**, which would scan the whole
  engine (every user table's data too, on a combined node sharing it with
  the CP data plane, ADR 0028). See `docs/engineering-lessons.md` for why
  this must never be "simplified" to `entries()`.

- **`mirror.rs`** (ADR 0038) — this module's two halves are the apply task's
  actual write-derivation and restart-rebuild logic (not a dual-write mirror
  of a separate in-core copy — that shadow-mode design was superseded once
  `Metadata` became `DRIVER_APPLIED`).
  - **Write derivation**: `apply_and_derive_mirror(meta: &mut Metadata,
    command: &MetaCommand) -> (ApplyOutcome, Vec<KeyWrite>)` delegates to the
    real `Metadata::apply` and derives the `syskv` writes that command
    implies. **Every `MetaCommand` variant has an explicit match arm, no
    wildcard** — a future variant fails to compile here until its mirror
    behavior is a deliberate decision. Takes `&mut Metadata` (not just
    post-apply state) to capture a small, targeted slice of *pre*-apply
    state for the one command whose derived *deletions* depend on
    identities gone by the time `apply` returns (`DropTableTablets`: both
    its dropped-tablet-id set and its legacy `cp_member_addrs` prune) —
    diffing this way, rather than
    re-deriving `Metadata::prune_cp_member_addrs`'s predicate a second time,
    avoids the exact "two places must agree on a gating rule" hazard this
    crate's engineering practices warn about. `node.rs`'s
    `meta_apply_and_compact` calls this directly, once per drained command.
  - **Read side**: `rebuild_metadata_from_engine(engine: &S) ->
    Result<Metadata, StorageError>` scans a `StorageEngine`'s live entries and
    reconstructs a `Metadata` — used by the apply task's own startup/restart
    rebuild, and by the differential-oracle tests (`apply_engine.rs`). Built
    from `apply_key_write(meta: &mut Metadata, write: &KeyWrite)` so the
    bulk-rebuild and incremental-delta paths share one decode implementation
    and can't drift. `apply_key_write` is also the incremental-delta
    consumer's whole job: `animusd`'s `RemoteControlClient::observe_delta`
    calls it once per `KeyWrite` in a `WatchMetadata` reply — see
    `delta_ring.rs`'s entry above.

  `node.rs`'s `meta_apply_loop`/`meta_apply_and_compact` are the sole
  writer/reader pair (see that entry above). The generic
  `RaftCore::pending_apply`/`drain_apply` machinery every `DRIVER_APPLIED`
  state machine has does this job with no separate flag/queue to keep in
  sync across a recovery swap — see `docs/engineering-lessons.md`'s ADR
  0038 entry for the recovery-swap race that motivated it. Deployment
  wiring lives in `animusd` (a combined node shares its CP-data engine; a
  control-only node opens its own small dedicated one; a data-only node
  gets none — see that crate's `CLAUDE.md`). Tested by `mirror.rs`'s own
  unit tests, `tests/apply_engine.rs`'s `SimEnv` differential oracle, and
  `animusd`'s `tests/control_mirror_restart.rs` (a real `ProdEnv` restart).

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

- **Epoch-CAS discipline on `SplitTablet`/`CasTabletReplicas`.** Every
  tablet-mutating command is a compare-and-swap on the tablet's epoch, evaluated
  identically on every replica, so accept/reject is consistent and racing
  proposers can't both commit. (`MergeTablets` — ADR 0033, carrying *two*
  expected epochs since it read two tablets from one snapshot — was removed
  by ADR 0044; tablets are split-only.) Any new tablet-mutating command must
  adopt the same guard.

- **`SplitTablet` records split provenance (`Metadata::split_parents`, ADR
  0018 §2 amendment) — replaces the retired `Tablet::version_floor`
  cross-group-LWW fix.** `SplitTablet` records `split_parents[new_id] =
  tablet` (the fresh sibling's immediate source). Never pruned — tablet ids
  are never reused, so an entry can never resurrect a wrong decision for a
  later id. It is a pure function of already-agreed `Metadata` state,
  computed once here so every data replica reads the identical value instead
  of deriving it locally. Consumed by `animus-cp-data`'s tablet-host
  reconciler to know **whose** range-seal marker a split child must observe
  locally before hosting — see that crate's `CLAUDE.md` and ADR 0018's PR2
  amendment for the full design this replaces `version_floor` with.
  Regression: `meta::tests::split_tablet_records_provenance_of_the_immediate_
  parent`. Also mirrored into the system keyspace
  (`syskv::EntityKind::SplitParent`, `mirror.rs`'s
  `apply_and_derive_mirror`/`apply_key_write`) so the incremental
  delta-consumer path (ADR 0038 PR5) stays byte-identical to a full
  `Metadata` fetch. (`Metadata::absorbed_by`, merge's mirror-image provenance
  field, and `Metadata::merged_tablets`, the never-pruned "this tablet was
  merged away" marker a per-node reconciler needed to tell "merged" apart
  from "table dropped" — see ADR 0033/ADR 0044 — were both removed along
  with `MergeTablets`.)

## What's non-obvious

- **The sync/driver split is deliberate.** All consensus logic is in the sync
  `RaftCore` (unit-testable, deterministic); the driver only does I/O. When
  changing protocol behavior, change the core and keep it I/O-free — don't reach
  for the env inside it. The driver races `env.recv()` against a timer via
  `futures::select`, drawing `entropy` every iteration for randomized election
  timeouts.

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

- **Leadership transfer (`RaftCore::transfer_leadership`, ADR 0029).** The
  control plane never calls it — it's a per-tablet CP-data primitive living here
  because the sync core is shared. `change_membership` always rejects removing
  the current leader, so relocating a leader's own replica needs a Raft §3.10
  handoff: arm a transfer to a voter with `peer_match(target) >= commit_index()`
  (no config change in flight; records a one-election-timeout deadline), then
  **freeze** `propose`/`change_membership` (return `NotLeader` hinting the
  target) so the log stops growing, and send `TimeoutNow` only once the target
  **reaches `last_log_index()`** (re-sent every heartbeat until step-down). A
  target that never steps down by the deadline **aborts** (clears the arm,
  resumes proposals). Re-arming the same target is idempotent and does **not**
  push the deadline (else a perpetual retry starves the abort check). The selector
  and the arm gate must read the *same* threshold, and the return value ("did it
  arm") must never be discarded — see the engineering-lessons log (root
  `CLAUDE.md`) for the war story where they diverged.

- **Snapshot transfer is chunked and O(chunk), not O(state).** A follower behind
  the compacted prefix is caught up via a chunked `InstallSnapshot`: the leader
  ships offset-addressed chunks of `SNAPSHOT_CHUNK_BYTES` (one per round trip,
  resuming from the per-peer `snapshot_offset`); the follower reassembles into
  `incoming_snapshot` and installs atomically only on the final chunk. All in the
  sync core (deterministic). `snapshot_chunk_for` **slices `snapshot_blob` by
  reference — it does NOT re-serialize per chunk**; a naive per-chunk serialize
  on a multi-MB metadata pins the loop past the election timeout (a
  self-sustaining election storm, invisible to `SimEnv`'s virtual clock). Blob
  management differs by state-machine kind:
  - **In-core (`Metadata`):** the blob is kept **eagerly** — set in
    `snapshot_upto`, on *install* completion (retain received bytes), and in
    `recovered` — so the invariant `snapshot_index > 0 ⟹ blob.is_some()` holds
    and a chunk is never a 0-byte ship (regression:
    `install_snapshot.rs::caught_up_control_node_reships_non_empty`).
  - **`DRIVER_APPLIED` (data-plane KV):** the image is the *engine* bytes, built
    **lazily on demand** — the core raises `take_snapshot_needed`, the driver
    scans the engine and calls `snapshot_upto` then `set_snapshot_blob`, and the
    core **drops** the blob whenever it would go stale/idle, so no whole-tablet
    image is retained at rest (regression:
    `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`, driving
    both hops).
  - The compaction serialize reuses the cached blob for the WAL record too (via
    `encode_snapshot_record_from_blob`), so compaction serializes `Metadata`
    once. Moving the single remaining inline compaction serialize off the loop
    was **assessed and deferred** — a bounded, non-self-sustaining ~50-120ms
    stall, not worth coupling install→WAL-rewrite ordering onto a second task on
    the most safety-critical Raft. Liveness teeth:
    `install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state` +
    `tests/prod_liveness.rs`. Deferred: cross-leader resumption (an interrupted
    transfer restarts at offset 0) and chunk-stream flow control.

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

- **Orphan-member auto-reclaim sweep (ADR 0040 PR6), same home and pattern as
  the detector above.** `orphan_sweep_loop` is the leader's own volatile
  timer for a class of `Metadata` claim the detector was never meant to
  judge: a `RegisterNode`/`admin_add_member` claim whose node **never
  showed up at all** (a crash-mid-join, or the losing racer of two
  concurrent omitted-id `control-add`s) — as opposed to a real member
  that's merely currently `Down`. On a coarse tick
  (`ORPHAN_SWEEP_CHECK_INTERVAL`, 5s — minutes-scale grace periods don't
  need liveness-detector cadence), the **leader** intersects
  `Metadata::orphan_sweep_candidates()` against `!core.config().contains(id)`
  (the live control-voter exclusion — `RaftCore`'s own config, which
  `Metadata` cannot see) and tracks, in a **volatile**
  `BTreeMap<NodeId, Nanos>` (`first_seen`, mirroring `detect_loop`'s own
  `leader_since`), when *this leadership stint* first observed each
  still-eligible candidate; once one has persisted for `orphan_sweep_after`
  (config/CLI knob, default 10 minutes, `Duration::ZERO` disables the loop
  outright — no loop is even spawned), it proposes the existing
  `MetaCommand::RemoveMember` for it. A leadership change resets
  `first_seen` wholesale — acceptable (convergent, just delayed: the new
  leader's own countdown starts over) — and a real activation cancels a
  countdown structurally (`has_activated` flips, so the next tick's
  candidate set simply no longer contains it — no explicit cancellation
  needed). **Safety argument for a sweep proposal racing a genuine late
  activation** (the one property that must never fail): `RemoveMember`'s
  own apply-time guard — unchanged, evaluated fresh against whatever
  already committed ahead of it in the log — rejects `Active`/`Joining`
  outright, so neither commit order ever removes an already-`Active`
  member; and `liveness_transitions` (the sole production producer of a
  promotion) only proposes one for a member present in that same tick's
  fresh `Metadata` read, so a removed claim is never resurrected by a
  stray late heartbeat either — both proven directly as pure
  state-machine/decision-function properties in
  `meta::tests::remove_member_never_removes_a_member_that_activated_first_
  regardless_of_proposal_order` and
  `node::tests::liveness_transitions_never_proposes_for_an_absent_member`,
  not approximated through `SimEnv` timing. Full seeded fault-injection
  suite: `tests/orphan_sweep.rs`.

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

- **`MetadataWatch` (ADR 0031).** The `AtomicWaker`-based wake-a-parked-task
  pattern (like `animus-cp-data`'s `ProposeSignal`), adapted to notify an
  *external* caller rather than the driver's own loop. Two points to remember if
  you touch or copy it: (1) it carries a **monotonic watermark** (`AtomicU64`,
  the observed `last_applied()`), not a one-shot consumed flag — `changed()`
  re-checks `current > last_seen` fresh every poll, so there is no
  wake-before-park race (a change that already happened resolves on the first
  poll). (2) It is bumped from the **driver loop** (`drive`), not the proposer,
  via `fetch_max` at exactly the points `last_applied` (gated by the same
  role-aware frontier `metadata()` uses) can have moved — so defensive calls on
  no-op iterations are free. It is **single-waiter** (one intended consumer: the
  per-node reconciler). Don't add a propose-side wake here the way
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
`SimEnv`'s virtual clock can't see.
