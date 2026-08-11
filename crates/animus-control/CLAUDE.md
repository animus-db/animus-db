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

- **`lib.rs`** — the public surface. Re-exports `SharedWal`, `RaftCore`,
  `RaftNode`, `Metadata`/`MetaCommand`, the schema types, `FailureDetector`,
  and `animus_placement::PlacementPolicy` (so a downstream assembler can
  `SetTabletPolicy` without a direct `animus-placement` dependency — the policy
  is part of this plane's public metadata surface).

- **`meta.rs`** — the `Metadata` state machine and its command enum.
  `Metadata::apply` is the deterministic state machine; `Metadata::reconcile`
  and `Metadata::rebalance` are the *pure* placement decisions (see Invariants).
  `Metadata` holds members, the tablet map, placement policies, the table-schema
  catalog, keyspaces, `node_addrs` (member id → full `NodeAddrs { internal,
  client, admin, role }`, ADR 0032 PR1) and the legacy
  `cp_member_addrs` (kept for WAL back-compat). **ADR 0040 PR1** merged the
  pre-existing `raftkv`/`control` address pair into one `internal` field —
  one identity per node, one internal env, means there is only one address
  to replicate per node now; the old `control: Option<SocketAddr>` field
  (ADR 0037 PR4, populated only for a control voter added at runtime, since
  that address had no other replication path) is gone too — a runtime-added
  voter's `internal` address is either already registered by its own
  ordinary self-registration, or supplied directly by the admin action, so
  there is no more separate gap to close there. `NodeAddrs.role: String` (ADR
  0035 residual follow-up, `#[serde(default = "combined")]` for WAL
  back-compat) is a member's own deployment role (`"control"`/`"data"`/
  `"combined"`) — a plain string, not an `animusd`-side enum, since this
  crate has no dependency on `animusd` and every other field here is already
  an opaque wire-format string this crate never interprets. A node only ever
  authoritatively knows its own role, so it is stamped once at
  self-registration time like the other fields; `animusd`'s
  `/admin/peers` reads every *other* node's role straight off this field
  instead of fanning out to each node's own `/admin/config`.
  `PlacementView` is the narrow (members + tablets + policies, no
  schema) clone that `RaftCore::placement_view()` hands the driver loops so they
  evaluate off the core lock instead of cloning the whole `Metadata` every tick.
  `MetaCommand` variants (all applied in log order):
  - `NoOp` — the election no-op a fresh leader commits.
  - `UpsertMember` — insert/update a member (labels + status).
  - `CreateTablet` / `CasTabletReplicas` — create a tablet; CAS its replica set
    (applies only if the epoch matches, then bumps it).
  - `SplitTablet` (ADR 0028) — the *entire* split: epoch-CAS gated, narrows the
    source range and mints a new sibling over the same node-shared engine. No
    data-plane half, no possible orphan.
  - `MergeTablets` (ADR 0033) — split's dual: epoch-CAS gated on *both* sides,
    rejects a cross-table merge, widens `left` to absorb `right`, and records
    `right` in `merged_tablets`.
  - `SetTabletPolicy` — set/clear a tablet's placement policy.
  - `CreateTableSchema` / `DropTableSchema` / `ReplaceTableSchema` — register /
    drop / atomically replace a table's schema (ADR 0013; `ReplaceTableSchema`
    is the atomic `ALTER TABLE … ADD`, no drop-then-recreate window).
  - `DropTableTablets` (ADR 0024) — remove every tablet scoped to a table + its
    policies in one apply (the metadata half of drop-table GC; `NoOp` if none).
  - `CreateTableIndex` / `DropTableIndex` — create/drop a secondary-index
    *definition* (ADR 0013).
  - `SetTableMode` — set a table's serving mode.
  - `CreateKeyspace` / `DropKeyspace` — keyspace lifecycle.
  - `RegisterNodeAddrs` (ADR 0032 PR1) — **update-only since ADR 0040 PR4**:
    idempotent register/overwrite of an *already-claimed* member's full
    `NodeAddrs` — rejects outright if `node` is absent from both `members`
    and `node_addrs` (nothing to update yet; see `RegisterNode` below, the
    sole path that creates a fresh claim).
  - `RegisterCpAddr` — the predecessor of `RegisterNodeAddrs`, carrying an
    optional `tablet` association (ADR 0024 GC). **Kept for WAL back-compat
    only — no longer proposed by `animusd`'s startup path.**
  - `RemoveMember` (ADR 0032 PR3 decommission) — prune a member from `members`
    plus its address entries; gated at apply on the member being absent
    already (idempotent), `Leaving`/`Down` (never `Active`/`Joining`), and
    unreferenced by any tablet (`Metadata::tablets_referencing`, also the
    drain-complete predicate `/admin/member/drain-status` reports).
    **ADR 0040 PR6 extension**: when `node` has no `members` row at all, this
    no longer treats it as a bare no-op — it prunes an orphaned
    **claim-without-member** `node_addrs` entry too (the shape a
    control-role `RegisterNode` produces, by design), so this command is a
    complete removal for every claim shape `RegisterNode` can leave behind,
    not just the data-plane one. This is what the orphan-member sweep
    (below) proposes once a claim has gone unactivated past
    `orphan_sweep_after`.
  - `RegisterNode` (ADR 0040 Decision C, PR4) — the **sole claim path** for a
    fresh node identity, retiring ADR 0036's `AllocateNodeId` monotonic
    allocator entirely (and the one-PR `alloc_node_id`/`parse_alloc_id`/
    `ALLOC_ID_BASE` string-mint shim PR3 shipped in its place). `node` may be
    self-minted (`NodeId::mint`) or operator-/config-proposed
    (`NodeId::propose`) — this command treats both identically. The CAS key
    is **`Metadata::node_addrs` alone, not `members`**: an id absent from
    `node_addrs` claims the address slot (inserting a `Down` `Member` with
    `labels` too, but *only* if `members` doesn't already have an entry —
    membership can be independently pre-established by `UpsertMember`'s
    bootstrap insert or `admin_add_member`'s operator-labeled row, both of
    which carry no address for this apply to compare against); a
    byte-identical re-registration is `NoOp` (the idempotent-retry and ADR
    0032 rejoin cases); a *different* `NodeAddrs` already on file is
    `Rejected` — the real collision. See `MetaCommand::RegisterNode`'s own
    doc for why keying on `node_addrs` rather than the full
    `NodeAddrs`+`labels` pair is load-bearing, not an oversight (a
    labels-inclusive CAS breaks the moment two *independent* commands can
    each partially establish the same identity, which several call sites in
    `animusd` do) — and `docs/engineering-lessons.md`'s entry for the
    integration-test failure that caught the naive design.

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
  `animusd` admin interface (ADR 0020). **`change_membership`/
  `transfer_leadership`** (ADR 0037 PR1, `tests/control_membership.rs`) are
  thin wrappers over the identical-shaped `RaftCore` methods `animus-cp-data`
  already drives for a per-tablet group — the control plane's *own* Raft
  group can now grow/shrink/replace a voter one server at a time, recorded
  under their own `ControlReconfigureAccepted`/`Rejected` metric family
  (kept separate from cp-data's per-tablet `CpReconfigureAccepted`/
  `Rejected`). Unlike cp-data's `propose_and_wake`, there is no propose-side
  wake seam here — `RaftNode`'s plain `propose` has never had one either, so
  a control-plane proposal (including a membership change) is always
  serviced on the driver's next heartbeat tick; no seam was added to keep
  this PR a pure thin-wrapper addition. **PR3 adds the admin/CLI surface**
  (`animusd`'s `admin_add_control_member`/`admin_remove_control_member` +
  `POST /admin/control/member/{add,remove}` + `animus admin
  control-{add,remove,grow}` — see that crate's `CLAUDE.md`); this crate's
  own tests stay core-level only. **PR4 closes PR3's address-replication gap
  and the static-vs-live `control_ids` audit** — the address-replication half
  was `NodeAddrs.control` + `animusd`'s `control_peer_sync_loop` (both since
  superseded by ADR 0040 PR1's `internal` merge and single `peer_sync_loop`
  — see that ADR); `admin_remove_member`'s control-voter refusal moving to
  `self.control.config()` (the live Raft config) instead of a static
  original-members list is unaffected — see `animusd`'s `CLAUDE.md` for
  both. `metadata_watch() -> MetadataWatch` (ADR
  0031) is the executor-agnostic "applied index advanced" notification the
  per-node CP reconciler uses to react to a `Metadata` change without polling.

  **ADR 0037 hardening PR2 (PR #136, the quorum-guard liveness fix) adds a genuinely
  control-id-native liveness signal**, closing the gap PR3's own doc and
  `docs/engineering-lessons.md`'s "id-space mismatch" entry flagged:
  `ControlHandle::believes_alive` is keyed to **raftkv** ids (the failure
  detector only ever observes heartbeats on the data role, ADR 0012), so it
  can't answer "is this control voter alive" — calling it with a control id
  is always `false`, not "unknown". Rather than bridging that id space, this
  reads a fact the leader's own control-Raft traffic already carries:
  `RaftCore::peer_last_contact(node) -> Option<Nanos>` (`raft.rs`) is the
  `now` of the last `AppendEntriesResp` (success **or** reject — either
  proves reachability) from `node`, backed by a volatile `last_contact:
  BTreeMap<NodeId, Nanos>` seeded for every peer in `become_leader` and
  stamped in `handle_append_resp` — deliberately **never persisted or
  snapshotted**, exactly like `next_index`/`match_index` (meaningless across
  a leadership change, rebuilt empty on recovery; a freshly-added peer via
  `change_membership` gets no explicit entry either, relying on the read
  side's "never contacted yet ⇒ alive" grace rather than an explicit write
  at peer-add time — see the field's own doc for why). `RaftNode::
  control_peer_believed_alive(node) -> bool` (`node.rs`) turns the raw fact
  into policy: always `true` for self; `true` if `node` has never been
  contacted this leadership stint (grace for a just-added voter, or any peer
  right after this node won an election); otherwise gated on its own
  `CONTROL_PEER_LIVENESS_TIMEOUT = 500ms` (deliberately **not** a reuse of
  `DETECT_TIMEOUT`: ADR 0040 PR1 has since put both signals in the *same* id
  space — a node has exactly one id now — but they still answer different
  questions, general network reachability (ADR 0012's heartbeat detector)
  versus control-Raft-traffic reachability specifically, so sharing one
  timeout constant would conflate two independently-tunable signals rather
  than reflect that they now happen to key on the same space).
  `animusd`'s `admin_remove_control_member` is the consumer — see that
  crate's `CLAUDE.md`. Regression: `tests/control_membership.rs::
  last_contact_ages_out_a_partitioned_peer_but_not_a_healthy_one` (a
  `SimEnv` proof at the `RaftNode` level, mirroring `pre_vote.rs`'s
  partition idiom) — this crate's own tests stay core/driver-level only,
  same discipline as PR1/PR3.

  **ADR 0038 PR3 (the cutover): `Metadata` is `DRIVER_APPLIED`, so `RaftNode`'s
  single driver is now split into a consensus loop and an async apply task**,
  mirroring `animus-cp-data`'s proven shape exactly:
  - `drive()` (the **consensus loop**) recovers from the WAL, spawns the apply
    task, then only persists (`persist_wal`), steps the core, and ships
    outbound messages — **no engine I/O**, so it always services
    heartbeats/`AppendEntries` within the election timeout regardless of how
    slow an engine merge or compaction is (the reintroduction of the
    `animus-cp-data` election-storm bug class this split exists to prevent).
  - `meta_apply_loop`/`meta_apply_and_compact` (the **apply task**, spawned by
    `drive` right after WAL recovery) owns the *only* mutable `Metadata`
    (a private `shadow`, never shared with the core). It rebuilds `shadow`
    from the engine (`mirror::rebuild_metadata_from_engine`) and seeds its
    watermark from the engine's own `_applied_index` key (**not**
    `core.last_applied()`, which after recovery only reflects the last
    *compacted* base and can understate what the engine already durably
    holds); drains `RaftCore::drain_apply()`, **skipping any command whose
    index the watermark already covers** (the robust, index-based restart-tail
    filter — not reliance on incidental command idempotency); applies
    survivors via the real, unchanged `Metadata::apply` (through
    `mirror::apply_and_derive_mirror`); merge-batches the derived writes into
    the engine; and publishes the refreshed `Metadata` into `cache:
    Arc<Mutex<Metadata>>`, gated by `engine_applied: Arc<AtomicU64>` — bumping
    `MetadataWatch` only *after* that publish, so a watcher never observes a
    change before it is both durable and visible.
  - **Every reader now reads `cache`, never the core.** `RaftNode::metadata()`/
    `members()`/`placement_view()` (the latter two promoted from
    `RaftCore<MetaCommand, Metadata>` inherent methods, which are gone —
    `self.metadata` on a `DRIVER_APPLIED` core is an unused default, mirroring
    `KvState`'s placeholder) all lock `cache` directly; `reconcile_loop`/
    `detect_loop` still read leadership/term off the core (a consensus-level
    fact, unaffected) but the placement view / membership map off `cache`.
  - Snapshotting reuses the same `take_snapshot_needed`/`set_snapshot_blob`
    lazy-image machinery `animus-cp-data` uses, retargeted at
    `syskv_image`/`install_syskv_image` (a scan/encode of the system keyspace,
    filtered by `syskv::decode_key`) instead of `engine_image`/
    `install_engine_image`'s per-tablet range.
  - `start`/`start_with_metrics` now **require** a `StorageEngine` (an added
    3rd/4th parameter) — there is no more engine-less control-plane
    deployment shape. `start_with_mirror` (PR2's shadow-mode attach point) and
    `RaftCore`'s `mirror_capture`/`mirror_log`/`enable_mirror_capture`/
    `drain_mirror_log` are **removed**: the generic `pending_apply`/
    `drain_apply` machinery this PR wires up for real already does the
    identical job, so keeping both would have been two write paths for one
    fact.
  - `RaftNode::applied()` (a bounded, `Vec<MetaCommand>` window for tests) is
    also removed — `core.applied()` is always empty once `DRIVER_APPLIED`
    (nothing pushes to it); tests that used it now compare converged
    `metadata()` across nodes instead (a strictly stronger convergence proof).

  **ADR 0038 PR5 ("Phase 2"): incremental `WatchMetadata` deltas.**
  `meta_apply_and_compact` now also pushes one [`delta_ring::DeltaRing`] entry
  per drained command (index → its derived `KeyWrite`s, possibly empty) in
  the same pass that publishes `cache`/bumps `engine_applied` — *before*
  bumping `MetadataWatch`, so a watcher woken by that bump always finds the
  ring already populated. `RaftNode::watch_delta_since(last_seen) ->
  Option<DeltaReply>` is the new public read side `animusd`'s
  `ClientCtx::watch_metadata` calls: `Some` (a cheap `DeltaReply { writes,
  watermark }`) when the ring contiguously covers `(last_seen,
  engine_applied_index()]`, `None` otherwise (the caller falls back to a full
  `metadata()` clone). The ring is cleared whenever `cache` is rebuilt from a
  jump it didn't witness (a received `InstallSnapshot`, mid-loop in
  `meta_apply_and_compact`) — **not** on `meta_apply_loop`'s own
  startup/restart rebuild, since a fresh `RaftNode`/ring is already empty by
  construction at that point. `RaftNode::start_with_ring_bounds` (bounds
  default via `DeltaRing::default`, 1024 entries / 4 MiB) is the
  "configurable" knob the design asked for — a code-level constructor
  parameter, not (yet) CLI-exposed, since no deployment-time need for a
  different bound exists today.

- **`delta_ring.rs`** (ADR 0038 PR5) — the apply task's bounded, per-node,
  best-effort in-memory ring of [`mirror::KeyWrite`] deltas keyed by Raft log
  index. Pure (no `Env`, no I/O); `push`/`clear`/`writes_since(last_seen,
  upto)` are its whole surface. Bounded by **both** `max_entries` and
  `max_bytes` (`DeltaRing::with_bounds`; `DeltaRing::default` uses
  `DEFAULT_MAX_ENTRIES = 1024`/`DEFAULT_MAX_BYTES = 4 MiB`), oldest evicted
  first — except a push never evicts the entry it just inserted, even if that
  single entry alone exceeds `max_bytes` (there's nothing smaller left to
  evict down to, and discarding your own freshest entry would defeat the
  ring's purpose). `writes_since(last_seen, upto)`'s contiguity check is
  subtle at the boundary: `last_seen + 1 == front().index` is *not* a gap
  (the caller's very next needed index is exactly the ring's oldest retained
  entry) — only `last_seen + 1 < front().index` is (see the unit tests'
  `byte_bound_eviction_from_one_huge_entry` for the case this distinction
  matters: an evicted middle entry doesn't create a gap for a caller who
  never needed it). Unit-tested directly (no `Env`); `node.rs`'s own
  white-box apply-task tests and `tests/watch_deltas.rs` prove it wired up
  correctly against a real `RaftNode`.

- **`schema.rs`** — the replicated **table-schema catalog** (ADR 0013), all
  plain data (no I/O/clock/RNG). `TableSchema` (partition key + ordered
  clustering keys + typed `ColumnDef`s + `indexes: Vec<IndexDef>`), `ColumnType`
  (union of CQL scalars + DynamoDB key families), `SchemaCatalog` (a
  `BTreeMap<TableName, TableSchema>` held in `Metadata`), and
  `IndexDef`/`IndexKind`/`IndexProjection` (the replicated GSI/LSI *shape*, not
  its entry data). `TableSchema::validate` is the pure malformed-schema check
  the state machine applies (unique index names; an LSI requires a sort
  attribute).

- **`persist.rs`** — `WalRecord`, `PersistedState` (durability/recovery; the
  write/compact/recover flow is diagrammed in `docs/wal.md`).
  `encode_snapshot_record_from_blob` encodes the WAL `Snapshot` line **reusing
  the core's cached serialized image** (`snapshot_blob`, via `serde_json`
  `RawValue`) — for an in-core state machine this serializes its whole state
  once per compaction, not twice, byte-identical to the plain encode, guarded
  by `snapshot_record_blob_reuse_round_trips`. **Since ADR 0038 PR3,
  `Metadata` is `DRIVER_APPLIED`, so its WAL `Snapshot` record's `metadata`
  field is always the meaningless `Metadata::default()`** (the real durable
  state lives in the system-keyspace engine, not this record) — this
  reuse path is exercised by this crate's other `DRIVER_APPLIED` uses
  (`driver_applied_sm.rs`'s toy state machine) and by `animus-cp-data`'s
  identical-shaped `KvState`, not by `Metadata` anymore.

- **`detector.rs`** — `FailureDetector` (ADR 0012): a pure, unit-tested
  interval+timeout liveness detector (last-heartbeat instants + `now` +
  `timeout` decide alive/dead). No clock, no RNG.

- **`shared_wal.rs`** — `SharedWal` (ADR 0028 PR1): a multi-tenant WAL I/O
  coordinator that serializes concurrent tablet WAL writers into one file with
  coalesced `append`+`sync`. **Built and unit-tested but UNWIRED** — no
  `animusd`/`animus-cp-data` code constructs one; every tablet still writes its
  own WAL file. Wire-in-or-delete is an open decision (see ADR 0028).

- **`syskv.rs`** (ADR 0038 PR1, extended PR2; wired for real by PR3) — the control plane's reserved
  **system keyspace** key encoding: pure functions, no I/O. `RESERVED_NAMESPACE
  = "__animus_system"` is the top-level namespace no user table/keyspace may
  claim; `entity_key(EntityKind, id)` encodes `escape(RESERVED_NAMESPACE) ||
  escape(kind) || escape(id)` reusing `animus_tablet::escape` byte-for-byte
  (this crate already depends on `animus-tablet`, so no new dependency edge —
  unlike the wire adapters, which deliberately *duplicate* `escape` to stay
  dependency-light of this crate, there's no such constraint here). One
  `EntityKind` per `Metadata` collection: the PR1 set
  (`Tablet`/`Member`/`Schema`/`Policy`/`NodeAddrs`/`Keyspace`/`Merged`) plus
  PR2's `Counter` (the monotonic tablet-id allocator, `next_tablet_id`, keyed
  by a fixed counter name — its ADR 0036 sibling counter, `next_alloc_id`,
  was removed in ADR 0040 PR4 along with the allocator itself),
  `CpMemberAddr` (the legacy `cp_member_addrs`/`cp_member_tablets` pair,
  combined into one value per `NodeId`) — added so the mirror can reconstruct
  a **byte-identical** `Metadata`, not one with a documented gap. A third PR2
  variant, `NodeIdAlloc` (the ADR 0036 `AllocateNodeId` idempotency ledger,
  `node_id_allocations`, keyed by nonce), was **removed in ADR 0040 PR4**
  along with the allocator it mirrored — `RegisterNode`'s claim lives
  entirely in the already-mirrored `Member`/`NodeAddrs` kinds, no separate
  ledger needed. Plus typed `tablet_key`/`member_key`/`schema_key`/
  `policy_key`/`node_addrs_key`/`keyspace_key`/`merged_key`/`counter_key`/
  `cp_member_addr_key` helpers and a dedicated `applied_index_key()`
  watermark (a sibling of the
  entity-kind segment, not under one — mirrors `animus-cp-data`'s
  `engine_applied_index`). `decode_key` inverts every `*_key` helper for the
  mirror's own engine-scan path (`mirror::rebuild_metadata_from_engine`) and
  this module's round-trip tests. **`is_reserved_name`** (wired since PR1):
  called from `Metadata::apply`'s `CreateTableSchema`/`CreateKeyspace` arms
  (the state-machine-level, every-replica-agrees gate) and from both wire
  edges' `CreateTable`/`CREATE KEYSPACE`/`CREATE TABLE` paths (client-side,
  so a reserved-name collision surfaces as an immediate
  `ValidationException`/`ERR_INVALID` instead of an opaque commit-wait
  timeout) — same two-layer idiom the existing duplicate-table check already
  uses. Matching is a case-sensitive prefix test (exact match *or* merely
  prefixed, e.g. `__animus_system_backup`) — a combined node's mirror writes
  directly through this same already-globally-namespaced engine with no
  further `StorageScope` wrapper (see `mirror.rs`'s doc for why), and a
  prefix match is the collision that scheme cannot tell apart from a real
  system key. **PR6 additions** (`animusd`'s read-only `GET
  /admin/system-table` browse surface): `EntityKind::as_str`/
  `EntityKind::from_segment` are now `pub` — the admin endpoint parses/
  renders a `?kind=` filter through them directly rather than re-deriving
  the segment table a third time. `prefix_successor(prefix) ->
  Option<Vec<u8>>` is a general byte-lexicographic-successor helper
  (increment the last non-`0xFF` byte, dropping trailing `0xFF`s first;
  `None` only for an empty or all-`0xFF` prefix — unit-tested including that
  edge case, even though the one real caller below never hits it).
  `reserved_scan_bounds() -> (Vec<u8>, Vec<u8>)` is the `[start, end)` pair
  covering the **entire** reserved namespace (every `EntityKind` plus the
  `_applied_index` watermark), built from it — **the load-bearing bound the
  admin endpoint scans with instead of `StorageEngine::entries()`**, which
  would scan the whole engine (every user table's data too, on a combined
  node sharing it with the CP data plane, ADR 0028). See
  `docs/engineering-lessons.md` for why this must never be "simplified" to
  `entries()`.

- **`mirror.rs`** (ADR 0038 PR2, promoted to the real apply path's core by
  PR3) — no longer a shadow: this module's two halves are now the apply
  task's actual write-derivation and restart-rebuild logic, not a dual-write
  mirror of a separate in-core copy.
  - **Write derivation**: `apply_and_derive_mirror(meta: &mut Metadata,
    command: &MetaCommand) -> (ApplyOutcome, Vec<KeyWrite>)` delegates to the
    real `Metadata::apply` and derives the `syskv` writes that command
    implies. Every `MetaCommand` variant has an **explicit match arm, no
    wildcard** — a future variant fails to compile here until its mirror
    behavior is a deliberate decision. Deliberately takes `&mut Metadata`
    (not just post-apply state) and captures a small, targeted slice of
    *pre*-apply state for the two commands whose derived *deletions* depend
    on identities gone by the time `apply` returns: `DropTableTablets`'s
    dropped-tablet-id set (`Metadata::tablets_for_table`, read before
    removal) and both `DropTableTablets`/`MergeTablets`'s legacy
    `cp_member_addrs` prune (a `cp_member_tablets` clone taken before, diffed
    against post-apply `tablets` absence) — diffing this way, rather than
    re-deriving `Metadata::prune_cp_member_addrs`'s predicate a second time,
    avoids the exact "two places must agree on a gating rule" hazard this
    crate's engineering practices warn about. `node.rs`'s
    `meta_apply_and_compact` calls this directly, once per drained command.
  - **Read side**: `rebuild_metadata_from_engine(engine: &S) ->
    Result<Metadata, StorageError>` scans a `StorageEngine`'s live entries and
    reconstructs a `Metadata` — used by `meta_apply_loop`'s own
    startup/restart rebuild, and by the differential-oracle tests
    (`apply_engine.rs`). **Since ADR 0038 PR5** it's built from
    `apply_key_write(meta: &mut Metadata, write: &KeyWrite)` (one `Put` per
    live entry — `entries()` never yields a tombstone, so this bulk path only
    ever exercises that half) instead of its own separate decode match, so
    the bulk-rebuild and incremental-delta paths share one decode
    implementation and can't drift. `apply_key_write` is also the
    incremental-delta consumer's whole job: `animusd`'s
    `RemoteControlClient::observe_delta` calls it once per `KeyWrite` in a
    `WatchMetadata` reply, installing them onto its own cached `Metadata`
    with no engine of its own — see `delta_ring.rs`'s entry above and ADR
    0038's "Phase 2" section.

  `node.rs`'s `meta_apply_loop`/`meta_apply_and_compact` are the sole
  writer/reader pair now — see that module's `CLAUDE.md` entry above for the
  full consensus-loop/apply-task split. PR2's shadow-only plumbing
  (`RaftCore::mirror_capture`/`mirror_log`, `RaftNode::start_with_mirror`, the
  `mirror_capture`-across-recovery-swap race its own crash test caught — see
  `docs/engineering-lessons.md`'s ADR 0038 PR2 entry, still recorded as a
  generalizable lesson even though its specific mechanism is now gone) is
  **removed**: the generic `RaftCore::pending_apply`/`drain_apply` machinery
  every `DRIVER_APPLIED` state machine already has does the identical job
  with no separate flag/queue to keep in sync across a recovery swap.

  **Deployment wiring** (`animusd`): a **combined** node's `RaftNode::start`/
  `start_with_metrics` call site passes the node's already-open **shared**
  CP-data engine directly (no `StorageScope` wrapper — `syskv` keys are
  already globally namespaced under `RESERVED_NAMESPACE`, and PR1's
  reserved-name rejection guarantees no user table can ever collide with it).
  A **control-only** node (`BoundControlNode::start_control_with`, which now
  **always** opens an engine — the `mirror_backend: Option<..>` PR2 shipped
  is gone, since there is no more engine-less shape) opens a small
  **dedicated** engine on its own `control` `ProdEnv` directory
  (`SYSKV_LSM_PREFIX`, distinct from the fixed `raft.wal` filename it shares
  that directory with) — `StorageBackend::Lsm` by default, `::Memory` under
  `--ephemeral`. A **data-only** node still gets nothing (no local control
  `RaftCore` at all).

  **Tests**: `mirror.rs`'s own unit tests cover every `MetaCommand` variant's
  derivation (including the pre-apply-diff cases) plus a unit-scale
  engine-rebuild round trip; `tests/apply_engine.rs` is the real
  `SimEnv`-driven differential oracle (see the Tests section below);
  `animusd`'s `tests/control_mirror_restart.rs` (file name kept from PR2 —
  what it proves is no longer about a shadow mirror, but the exact same
  real-disk assertion now applies to the actual source of truth) proves the
  same durability claim over a **real** `ProdEnv` process restart (real disk,
  real TCP) for a control-only node, reopening the engine from an entirely
  separate handle after the node that wrote it has been shut down.

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

- **Epoch-CAS discipline on `Split`/`Merge`/`CasTabletReplicas`.** Every
  tablet-mutating command is a compare-and-swap on the tablet's epoch, evaluated
  identically on every replica, so accept/reject is consistent and racing
  proposers can't both commit. `MergeTablets` carries *two* expected epochs
  (it reads two tablets from one snapshot). Any new tablet-mutating command must
  adopt the same guard.

- **`SplitTablet`/`MergeTablets` also seed/bump `Tablet::version_floor`
  (cross-group LWW version-floor fix, confirmed real — full writeup in
  `docs/engineering-lessons.md`).** Every tablet a node hosts shares one
  physical `StorageEngine` (ADR 0026/0028), and `animus-cp-data` stamps a
  write's MVCC version from its **own** group's local Raft log index — which
  restarts low/independent for a fresh group, so a split sibling or a merge
  survivor could otherwise carry a version no higher than what a *different*
  group already stamped for the same key, and per-key LWW would silently drop
  the write. `SplitTablet` sets the new sibling's floor to
  `source.version_floor + 1` (the source's own floor is untouched — it never
  absorbs foreign data); `MergeTablets` bumps the surviving `left`'s floor to
  `max(left, right) + 1` (checked against **both** sides deliberately —
  `left`/`right` are chosen by key-range adjacency, not allocation order, so
  `right`'s id/floor is not always the smaller one). Both are pure functions
  of already-agreed `Metadata` state, computed once here so every data
  replica reads the identical value instead of deriving it locally.
  Regressions: `meta::tests::{split_tablet_seeds_the_new_siblings_version_
  floor_past_the_sources, merge_tablets_bumps_the_survivors_version_floor_
  past_both_sides}`.

- **`merged_tablets` is never pruned.** Tablet ids are never reused, so a
  recorded merge marker can never resurrect a wrong decision for a later id.
  It is the only sound way a per-node reconciler tells "this hosted tablet
  vanished because it was merged into a sibling" (data still served — never
  erase) from "vanished because its table was dropped" (erase) — range
  containment alone is unsound, since two tables' unsplit tablets share a
  byte-identical default `KeyRange::whole()`. See ADR 0033.

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
heartbeats). The 25 binaries:

- **`control_raft.rs`** — election/replication/leader-kill + multi-seed
  convergence.
- **`persistence.rs`** — durability/recovery; the hand-driven leader `persist`
  helper (drain + `mark_durable_through`) plus (ADR 0038 PR3) `drain_and_apply`
  — since `Metadata` is `DRIVER_APPLIED`, "is this entry visible" is now "does
  `drain_apply()` yield it", drained onto a local oracle `Metadata` via
  `mirror::apply_and_derive_mirror` — the porting idiom every hand-driven
  `RaftCore<MetaCommand, Metadata>` test in this crate now uses.
- **`follower_visibility.rs`** — the role-aware apply gate (ADR 0009): a
  hand-driven follower's committed entry becomes drainable with
  `durable_index == 0`, a leader stays gated on its own proposal, a
  follower→leader transition keeps the drained entry and gates new proposals,
  and both `SimEnv` followers reflect the leader's committed command.
- **`membership_commit_gate.rs`** — core-level `change_membership`: the
  current-term-commit gate (`commit_index >= first_term_index`, leader-only) plus
  the single-server rules, driven at message granularity.
- **`generic_state_machine.rs`** — proves `RaftCore<C, S>` genericity (ADR 0016):
  drives the same core with a toy KV state machine (non-control-plane command +
  image) through propose → durable-apply → snapshot → WAL recovery.
- **`driver_applied_sm.rs`** — the `DRIVER_APPLIED` core mechanism (ADR 0017
  Stage A.1): effects are exactly the committed-durable commands in commit order,
  the in-core path is bypassed, durability still gates hand-out; plus the
  two-hop non-empty-reship regression.
- **`tablet_split_merge.rs`** — split and merge through Raft: racing splits at
  the same epoch (only the first applies), and merge rejecting a stale epoch
  racing a replica change / tablets from different tables.
- **`wal_compaction.rs`** (ADR 0038 PR3) — engine-backed WAL compaction: a real
  `RaftNode` + `MemoryEngine` under sustained load truncates the WAL and the
  engine stays the source of truth (`mirror::rebuild_metadata_from_engine`
  agrees with the live cache); a crash during sustained compaction-triggering
  load recovers to the same state a same-seed uninterrupted run reaches. The
  precise, deterministic unit-level proof of watermark-gated tail replay
  (never re-deriving writes for an already-engine-covered command) is a
  white-box `#[cfg(test)]` in `src/node.rs` itself (drives the private
  `meta_apply_and_compact` directly), not here.
- **`install_snapshot.rs`** — a partitioned follower catching up via
  `InstallSnapshot` (`RaftNode`-level, real apply task), a far-behind follower
  via a *multi-chunk* one, non-empty reship, and the wall-clock-timed
  O(chunk)-not-O(state) liveness test — the last three are hand-driven
  `RaftCore`-level tests that supply a synthetic image via `set_snapshot_blob`
  right after `snapshot()` (ADR 0038 PR3: `Metadata`'s image is now built
  lazily by an external driver, so these tests stand in for it directly,
  decoupling them from `Metadata`'s own serialization — the real engine-backed
  image path is `wal_compaction.rs`'s job).
- **`apply_engine.rs`** (ADR 0038 PR3) — the differential oracle succeeding
  PR2's `mirror_engine.rs`: no more shadow side to diff against a separate
  real side — `cache` *is* the real side. Asserts every node's published cache
  agrees with its own engine's independent rebuild through a mixed scenario
  (membership, schema, tablet create/split/drop-table, keyspace,
  node-id-allocation) and across a genuine crash + restart, seed-swept.
- **`watch_deltas.rs`** (ADR 0038 PR5) — the incremental `WatchMetadata`
  delta path's differential oracle: applying `RaftNode::watch_delta_since`'s
  writes onto a scratch `Metadata` (via `mirror::apply_key_write`) stays
  byte-identical to a full `metadata()` fetch at every checkpoint through the
  same kind of mixed scenario `apply_engine.rs` drives, including a
  `MergeTablets` step specifically to exercise a derived `Delete` (a live
  engine scan never yields one, so the bulk-rebuild oracle alone can't); plus
  the ring's own restart-reset contract (a pre-restart watcher's `last_seen`
  correctly falls back to `None`, a caught-up one still gets the trivial
  reply) and a small-bounded ring's eviction-driven fallback, proven against
  a real `RaftNode` (not just `delta_ring.rs`'s own unit tests).
- **`restart.rs`** — process restart-and-rejoin (via `Simulator::stop`); each
  node's `MemoryEngine` handle is created once and re-cloned at restart (ADR
  0038 PR3 gotcha — see `docs/engineering-lessons.md`: a restart must reuse
  the same engine handle, not construct a fresh one, or it silently loses
  everything the (real, disk-backed in production) engine would have kept).
- **`placement_reconcile.rs`** — caller-driven placement reconcile through Raft
  under a replica death + follower crash (drives `animus-placement`).
- **`placement_auto_reconcile.rs`** — leader-driven automatic reconcile from a
  replicated policy (no test-side `replan`/CAS).
- **`placement_rebalance.rs`** — leader-driven automatic load rebalancing (ADR
  0029, no test-side placement math): grow the cluster, only advance virtual
  time; spreads existing tablets to max−min ≤ 1 then goes quiet, repair defers to
  rebalance, residency + strict spread hold at every intermediate state, and it
  converges after killing the leader mid-flight; seed-swept.
- **`leadership_transfer.rs`** — core-level transfer (ADR 0029): arms only a
  caught-up current voter, retries `TimeoutNow` every heartbeat once the target
  reaches `last_log_index`, freezes `propose`/`change_membership` while armed,
  aborts and resumes if the target never catches up, an idempotent re-arm
  doesn't extend the deadline, a stale transfer doesn't survive a fresh election,
  `TimeoutNow` bypasses pre-vote, a departing peer keeps receiving the removal
  entry until it acks.
- **`failure_detection.rs`** — heartbeat-based failure detection end to end (ADR
  0012): a member crashes, the leader auto-commits `Down`, placement reconciles
  off it, the member restarts and returns to `Active`; plus detector +
  grace-gate unit tests.
- **`schema_catalog.rs`** — the replicated table-schema catalog end to end (ADR
  0013): propose, reject a duplicate + malformed on the state machine, kill the
  leader and assert survival + agreement, drop and see it replicate; plus
  `schema.rs` unit tests.
- **`schema_indexes.rs`** — replicated secondary-index definitions end to end
  (ADR 0013): create a GSI a second node sees, reject an index on a phantom
  table + a malformed LSI, restart and see the definition survive, drop
  cluster-wide; seed-reproducible.
- **`metrics.rs`** — control-plane metrics moving under known events (ADR 0015):
  a forced election bumps the election counters + leadership gauge, a crashed
  heartbeating member bumps `failure_detector_down` and its recovery `_up`; plus
  a same-seed byte-identical-snapshot reproducibility check.
- **`pre_vote.rs`** — pre-vote (ADR 0009): core-level (a live-leader lease
  rejects and never changes the term, an expired lease grants, a timeout makes a
  `PreCandidate` without bumping the term) + end-to-end (an isolated follower's
  pre-vote rounds don't move the stable leader's term and it rejoins on heal with
  no election; a genuine crash still elects at a higher term).
- **`metadata_watch.rs`** — the applied-index watch (ADR 0031): a watcher parked
  on `changed` wakes with the new index once a proposal commits and applies,
  stays parked across steady-state traffic with nothing proposed, and resolves
  on its first poll when the advance already happened (the wake-before-park case,
  safe by construction).
- **`prod_liveness.rs`** — real-thread `ProdEnv` smoke tests guarding the
  liveness properties `SimEnv`'s virtual clock can't see: a freshly-joined
  follower catches a large, compacted `Metadata` cluster up quickly without
  running leadership away (`large_metadata_catch_up_stays_live`); and (ADR
  0038 PR3) `sustained_metadata_churn_over_a_real_engine_stays_live` —
  hundreds of `MetaCommand`s at a steady drip through a 3-node group backed
  by a **real on-disk `LsmEngine`** (not `MemoryEngine`, whose I/O is
  synchronous/trivial and so can't exercise this), asserting both a bounded
  term delta *and* a bounded count of leadership transitions actually
  observed during the churn, plus bounded-deadline convergence — the direct
  real-thread proof that the apply task's real engine I/O (now on a separate
  task from consensus) never blocks heartbeat/`AppendEntries` processing
  long enough to trip the election timeout.
- **`register_node_cas.rs`** (ADR 0040 PR4, supersedes the deleted
  `node_id_allocation.rs`) — `MetaCommand::RegisterNode`'s registration CAS:
  two concurrent registrations with distinct ids both land and every
  replica agrees; a leader killed mid-registration converges to the one
  claim on an identical retry (never a second entry); a follower-connected
  proposer relays via the leader hint (the `is_relayable_command`
  regression); a *different* registration for an already-claimed id is
  rejected outright, never silently overwritten.
- **`orphan_sweep.rs`** (ADR 0040 PR6) — the auto-reclaim sweep's full
  seeded fault-injection suite, mirroring `failure_detection.rs`'s style
  for its ADR-0012 sibling: crash-mid-join swept after
  `orphan_sweep_after`; the losing racer of two concurrent omitted-id
  `control-add`s swept while the winner (now a live control voter) is
  protected; the control-role claim-without-member shape swept on its own;
  its dual — a `members`-row-only claim with no `node_addrs` entry
  (`admin_add_member`'s bare growth registration) — swept too; a
  slow-but-legit joiner that activates before the grace period elapses
  never swept; a member that was genuinely `Active` once and later went
  `Down` never swept (`has_activated` guard); a leader failover
  mid-countdown still converges, later, on the new leader's own timer; and
  the sweep disabled (`Duration::ZERO`) keeps an orphan indefinitely. The
  interleaving safety argument itself is proven separately, as a pure
  state-machine/decision-function property (not timing-dependent), by
  `meta.rs`'s and `node.rs`'s own in-crate unit tests — see this file's
  `node.rs` entry above.
- **`control_membership.rs`** — `RaftNode::change_membership`/
  `transfer_leadership` (ADR 0037 PR1): add/remove a voter and catch it up,
  reject a multi-server delta / leader self-removal / a second change while
  one is in flight, transfer-then-remove the leader, crash-mid-change
  converges either way (single-seed + a 200-seed sweep), byte-reproducible
  from a seed; plus two PR5 additions — a freshly-added voter's process
  restarting before it's caught up recovers from whatever WAL/snapshot it had
  and resumes, and removing a live voter while a different one is already
  dead is accepted **at the core level** (`RaftCore::change_membership` has
  no survivor-liveness guard, by design — that guard lives one layer up, in
  `animusd`'s admin action; see below) and demonstrably strands the group (a
  stranded 2-voter config with one dead never commits anything again) — the
  risk ADR 0037's Consequences section documents as knowingly accepted at
  the core level specifically. Plus (ADR 0037 hardening PR2, the
  quorum-guard liveness fix): `last_contact_ages_out_a_partitioned_peer_
  but_not_a_healthy_one` proves the new control-id-native liveness signal
  itself (`RaftCore::peer_last_contact`/`RaftNode::
  control_peer_believed_alive`) — partitioning one follower ages it out past
  `CONTROL_PEER_LIVENESS_TIMEOUT` while a never-partitioned one stays fresh,
  and it ages back in on heal.
- **`control_membership_prod.rs`** (ADR 0037 PR5) — the real-thread `ProdEnv`
  liveness counterpart to `control_membership.rs`: grows a real 3-node
  control group to 5 (two sequential single-server `change_membership`
  calls, exactly as `animus admin control-grow`'s client-side loop
  sequences them) under real sockets/time/threads, asserting both prompt
  catch-up and that leadership stays bounded and settles to one stable
  leader afterward — no election storm from real-thread scheduling around a
  runtime membership change.
  (PR2's shadow-mode `mirror_engine.rs`, which drove this same differential
  oracle against `RaftNode::start_with_mirror`, is superseded by
  `apply_engine.rs` above — ADR 0038 PR3.)
