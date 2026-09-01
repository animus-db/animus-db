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

  **Every WAL line carries a per-record CRC32 checksum (issue #495)**:
  `<crc32 as 8 lowercase hex chars>:<json>\n`, checked in
  `verify_checksummed_line` before the JSON is ever parsed. Before this, the
  newline-terminated-`serde_json` framing had no way to distinguish a
  bit-flip that happened to keep a record's JSON syntactically valid (e.g.
  a digit inside a packed numeric field) from a legitimate value — it
  decoded successfully into a silently wrong record instead of a decode
  error, confirmed to reach a hard panic once such a record applied past
  `animus_cp_data::assert_ts_monotonic` (`docs/engineering-lessons.md` has
  the full account). **A checksum failure is treated exactly like a torn
  trailing line**: `decode`/`decode_tagged` stop at the first bad record
  and drop it plus everything physically after it in the buffer — never
  applied, never a panic. This is a deliberately simpler rule than
  `animus-storage`'s own CRC-checked WAL framing, which additionally
  distinguishes real mid-file corruption (hard error) from a torn tail
  (tolerated) by checking whether a valid record follows; this WAL has no
  invariant that needs that finer distinction, since a dropped tail-of-log
  is always safe to recover from here. No back-compat/migration for a
  pre-existing unchecksummed WAL file (root `CLAUDE.md`'s no-back-compat
  stance) — an upgraded node needs a fresh WAL like any other format change
  in this repo.

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
  rows DIRECTLY from the intent's own `(id, replicas)` pairs. **Since ADR
  0062 (superseding this paragraph's own original text), each `replicas`
  is that child's FORK-TIME homes — the parent's own current replicas,
  verbatim, identical for both children, never placement-chosen** — a
  child's eventual *final* home is a separate decision this same apply arm
  also makes (below), not something the intent carries. It also inherits
  the parent's policy **at this moment** — the in-place workflow's only
  chance to, since there was no tablet row to attach it to at
  `BeginSplitInPlace` time. Otherwise identical to the copy-based branch:
  `split_lineage` written for both (fork F9, unchanged), parent removed.
  **G1 (ADR 0058's own "Open forks" table, decided 2026-08-25, reversing
  that ADR's own Stage 4 draft text): the GSI-drain/backfill-seeder
  cutover vetoes stay PRE-cutover, caller-side, exactly as in the
  copy-based workflow** — this command's own apply never gated on drain
  state in either branch, so nothing about this in-place branch needed to
  change to honor that decision; `animusd`'s in-place split driver
  (`index_drain.rs::inplace_split_driver_tick`) runs those vetoes before
  ever proposing this command, mirroring `split_driver_tick`'s existing
  shape for the copy-based endgame. Mirror arms follow the usual
  per-entity conventions (`BeginSplitInPlace`: parent row + allocator
  counter only; `CutoverSplit`'s existing arm extended to also mirror each
  child's policy, unconditionally — a harmless duplicate write for the
  copy-based branch, the only source for the in-place one). Tests:
  `meta::tests::begin_split_in_place_*`/`cutover_split_in_place_*`,
  mirroring the copy-based tests' own shape scenario-for-scenario.

- **ADR 0062: directed Placing — `Metadata::split_placing` +
  `MetaCommand::MarkSplitPlacingDone`.** A split child's *final* replica
  placement is decided once, separately from the fork, as a pure function
  of already-agreed `Metadata` — the same discipline `BeginBackup` already
  established for its manifest stub (fork C). `CutoverSplit`'s in-place
  branch (above) computes `select_replicas` over `active_candidates` for
  each child, once, right after minting its fork-inherited-replicas row:
  already-satisfying ⟹ no entry; a differing target ⟹
  `Metadata::split_placing[child] = SplitPlacing{target: Some(wanted),
  done: false}`; `select_replicas` erring (too few `Active`
  candidates/domains) ⟹ still written, `SplitPlacing{target: None, done:
  false}` (fork B — a visible, keep-retrying obligation, mirroring
  `reconcile_placement`'s own stance rather than staying silent). A child
  with no inherited policy gets no entry at all — nothing to place
  against. `SplitPlacing::target` is a write-once diagnostic snapshot of
  what cutover decided (or couldn't); the reconcile loop below never trusts
  or rewrites it, always recomputing `select_replicas` fresh instead
  (staleness avoidance, and it sidesteps needing a second command purely to
  update a persisted target). `node.rs`'s `reconcile_loop` gains a
  **third phase**, `Metadata::split_placing_reconcile`/`PlacementView::
  split_placing_reconcile`, run unconditionally every tick (own cadence,
  independent of repair/rebalance's gating — a split-triggered relief
  obligation shouldn't wait behind `REBALANCE_EVERY_N_TICKS`): for every
  un-`done` entry, recompute `select_replicas` fresh and propose a
  `CasTabletReplicas` for whichever ones now differ from the tablet's
  current replicas — one per entry per tick (deliberately not
  rebalance's one-move-per-tick bound). It never proposes
  `MarkSplitPlacingDone` itself — that observes *live Raft* convergence
  (`RaftKvNode::config()`/`learners()`), which this pure metadata-level
  view can't see; a leader-gated `animusd` background loop does, once a
  led tablet's live group matches `Metadata`'s current `replicas` with no
  dangling learners, held continuously for a settle window (see
  `animusd/CLAUDE.md`). `MarkSplitPlacingDone` is epoch-CAS'd against the
  **child's own** current epoch and idempotent on an already-`done` entry
  (`MarkIndexBackfilled`/`RecordBackupTabletComplete`'s idiom exactly); on
  `is_relayable_command`'s allowlist (`animus-node/src/wire.rs`), since a
  tablet's leader is frequently not the control-plane leader.
  `rebalance_placement`'s own eligibility filter (`meta.rs`) gains one more
  exclusion alongside its existing `t.state != Active` skip: a tablet
  carrying an un-`done` `split_placing` entry is skipped by ordinary
  rebalance too, so the two convergence sources never compete for the same
  tablet's epoch in the same tick. `DropTableTablets` prunes any
  `split_placing` row for a tablet it removes (`mirror.rs`), the same
  orphan-sweep `MarkIndexBackfilled`'s own doc describes for
  `index_backfill`. `syskv::EntityKind::SplitPlacing` follows the usual
  per-entity mirror conventions. Tests: `meta::tests::cutover_split_*` for
  the apply-time decision (already-satisfying/differing-target/
  unsatisfiable-at-cutover/no-policy shapes), `node::tests::` for the
  reconcile-loop phase and the rebalance exclusion,
  `drop_table_tablets_prunes_split_placing_rows_for_the_dropped_tablets`
  for the cascade. **Issue #513** (a suspected oscillation in the
  convergence primitive this phase drives — `reconfigure_step`, ADR 0058
  Train 1 — for a two-(or-more)-replica-difference target) **was
  investigated and closed as not reproducible**; see `crates/animusd/
  tests/split_placing_two_replica_diff_e2e.rs`, `crates/animus-cp-data/
  tests/reconfigure_multi_replica_diff.rs`, `docs/engineering-lessons.md`,
  and ADR 0062's #513 amendment. Directed Placing relocates a child
  regardless of how many replicas its fresh target differs by.

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
  entry on this). The tablet list feeding `pinned_tablets` filters out
  `TabletState::Building` rows — during a copy-based split's (ADR 0050)
  build/tail window `tablets_for_table` can return the `Splitting` parent
  plus its two not-yet-cutover `Building` children all at once, and only
  the parent is the current authoritative owner of the range (see
  `docs/engineering-lessons.md`'s entry on this fix). `CompleteBackup` requires every pinned tablet to have a
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
  twice. Scope of PR ①: the catalog only — no capture driver, no
  `SegmentStore` plumbing, no wire API, no janitor loop (later PRs in the
  ADR 0059 stack).

  **§6 (the backup-vs-split race), added by Train 1 PR③**: `RecordBackup
  TabletComplete`'s admission check and `CompleteBackup`'s completeness
  check both went from a bare direct-membership test to `traces_to_pinned`
  (walk a reporting tablet's own `split_lineage` chain up until a pinned
  tablet is found, or the chain runs out) / `pinned_tablet_capture_complete`
  (a pinned tablet is satisfied once every one of its current live
  `split_lineage` descendants — `live_split_descendants`, walking the
  chain the other direction — has its own progress row), so a re-planned
  split descendant's completion report is accepted even though it was
  never itself pinned. `backup_manifest_tablet_progress` is the one
  accessor every consumer of "what tablets does this backup's manifest
  actually cover" now shares (`backup_total_bytes`, the completion
  aggregator's manifest assembly, `animusd`'s `/admin/backups` view) —
  **not** a blanket scan of `Metadata::backup_tablet_progress`, because a
  pinned tablet that reported directly and only *then* happened to split
  (an ordinary, backup-unrelated split racing an already-finished tablet)
  leaves its own report behind as a harmless orphan row once its
  descendants become the authoritative reporters instead; summing both
  would double-count that range in the final manifest. `backup_ready_to_
  complete`/`backup_capture_target` are the two pure predicates the
  capture driver and completion aggregator (`animusd`, PR③) — and the
  `ANIMUS_BACKUP_SEEDS` corpus (`animus-test`) — all share rather than
  re-deriving independently. See `docs/engineering-lessons.md`'s entry on
  why this needed one canonical accessor rather than a per-consumer
  re-derivation.

  **Train 1 PR④ (wire surface + janitor) additions**: one new command,
  `MetaCommand::MarkBackupDeleted { backup_id }` — the two-phase janitor's
  own **mark** step (`Available`/`Failed` → `Expired`, idempotent once
  `Expired`, rejects `Creating` as an apply-time seatbelt behind the wire
  edge's own `BackupInUseException` check), proposed by the `DeleteBackup`
  wire operation (`animusd::dynamo::delete_backup`) — never by the janitor
  itself. The pre-existing `MetaCommand::DeleteBackup` (PR①'s own row-plus-
  progress removal) is unchanged and becomes the janitor's own
  **finalizing** command instead, proposed only once every one of a marked
  backup's objects has been reclaimed (`animusd::backup_janitor`); no new
  `BackupStatus` variant was needed (`Expired` already existed for exactly
  this). Two new `BackupRow` fields: `backup_name: String` (the client's
  `CreateBackup` request field, threaded through a new `MetaCommand::
  BeginBackup.backup_name` field — recorded verbatim, never interpreted,
  never part of this row's own identity) and `total_bytes: u64` (frozen
  **once**, by `CompleteBackup`'s own apply arm, from `Metadata::
  backup_total_bytes` at the moment every pinned tablet's live descendant is
  still resolvable — **not** re-derived live by the wire surface, which
  would silently collapse to zero the instant the source table is dropped;
  see `docs/engineering-lessons.md`'s entry on this). `backup_name` being a
  new required `MetaCommand::BeginBackup` field meant updating every
  existing `BeginBackup{..}` construction site across this crate's own
  tests, `animus-test`, and `animusd`'s tests — compiler-enumerated, the
  same "grep every site" fan-out root `CLAUDE.md`'s engineering-practices
  log already documents for this class of change; `total_bytes` needed no
  such fan-out (it is derived and stored only inside `Metadata::apply`'s own
  `BeginBackup`/`CompleteBackup` arms, never constructed by a caller).

- **The restore catalog (ADR 0059 §7, Train 2): `BeginRestore`/
  `CompleteRestore`/`FailRestore`.** `Metadata::restores: BTreeMap<RestoreId,
  RestoreRow>` (`RestoreId = String`, an opaque internally-minted identity —
  never wire-visible, unlike `BackupId`, since `RestoreTableFromBackup` has
  no AWS-defined "restore id" to echo back). `BeginRestore` mints exactly
  **one** `Building` tablet over the whole ring for the target table
  (`Tablet::with_table` + `state = Building`, the identical monotonic-
  allocator-floor seatbelt `CreateTablet`/`BeginSplit` already enforce) plus
  the `Seeding` row — the ADR's own as-built decision to mint a *fresh*
  single-tablet layout rather than mirror the backup's historical
  multi-tablet topology (see the ADR's Train 2 amendment for the full
  reasoning: this needs no `range` field anywhere, since a single
  destination tablet needs no per-row key routing at all). `CompleteRestore`
  activates that tablet (`Building` → `Active`, epoch bumped — mirroring
  `CutoverSplit`'s own activation, minus the "retire a parent" half) and
  flips the row `Done`; `FailRestore` mirrors `FailBackup`'s own idempotent-
  on-identical-repeat, rejects-a-terminal-contradiction shape, deliberately
  leaving the tablet `Building` forever (never routable, never half-serving
  — an ordinary `DeleteTable` cleans it up exactly like any other tablet,
  state-agnostic). `RestoreRow::gsi_defs: Vec<IndexDef>` carries the
  restore's own resolved GSI plan (the wire caller's
  `GlobalSecondaryIndexOverride`, or the backup manifest's own captured
  GSIs, forced to `IndexStatus::Creating` regardless of the source's status)
  from propose time to the restore driver (`animusd::backup_restore`),
  which declares them via `CreateTableIndex` only **after** `CompleteRestore`
  — declaring them earlier would let the backfill seeder observe an empty/
  `Building` tablet and mark it backfilled before any row is ever seeded,
  silently losing every restored row's GSI entry forever (see the ADR's own
  amendment for the full incident this ordering avoids). No new `syskv`
  companion progress kind exists for restore the way `Backup`/
  `BackupProgress` pair up — a restore mints exactly one destination tablet,
  so `RestoreRow` alone carries everything a restore has to say;
  `syskv::EntityKind::Restore` mirrors `Backup`'s own plain-string-key
  convention. **Deliberately no restore reclaim/delete command** yet (a
  named Train 2 residual, not a correctness gap — rows are small, bounded
  one-per-`RestoreTableFromBackup`-call, and never referenced again once
  terminal).

- **PITR (ADR 0059 §9, Train 3): `UpdateContinuousBackups`/
  `SealPitrSegment`/`ExpirePitrSegments`/`MarkBackupPitrBase`.** A fifth
  consumer's own catalog, deliberately mirroring the backup/stream ones'
  conventions rather than inventing new shapes: `TableSchema.pitr:
  Option<PitrSpec>` (generation + enable wall-clock, the `SetTableStream`/
  `SetTableTtl` schema-catalog class) toggled by `UpdateContinuousBackups`,
  which mints a fresh `generation` from `Metadata::pitr_generation`'s own
  never-rewound per-table counter (reusing `EntityKind::Counter` with a
  `"pitr_gen:{table}"`-prefixed name rather than a new entity kind).
  `Metadata::pitr_segments: BTreeMap<(TabletId, u64), PitrSegmentRow>`
  mirrors `stream_shards` exactly (same tuple-key JSON codec workaround,
  same first-committer-wins-on-content `SealPitrSegment`/two-phase
  `ExpirePitrSegments` shape, same epoch-derivation-guard obligation on the
  caller) but is a fully separate collection — a table's stream and its
  PITR coverage never share a row or gate each other. **`Metadata::
  pitr_base_backups: BTreeSet<BackupId>`, not a `BackupRow`/`BeginBackup`
  field**: tags an already-`BeginBackup`'d row as a PITR base snapshot
  without widening `MetaCommand::BeginBackup`'s own signature (which would
  have touched every one of its ~30 existing construction sites) — see
  `MetaCommand::MarkBackupPitrBase`'s own doc and the ADR's Train 3 PR①
  as-built amendment for the self-healing-tag residual this trades for.
  PITR segments/generation floor deliberately **survive**
  `DropTableSchema`/`DropTableTablets`, the identical ADR 0024 carve-out
  `backups` already gets — never gated on the source table's schema still
  existing, an explicit override of the streams drop-table retention-zero
  rule (ADR 0059 §9/§10).

  **`RestoreTableToPointInTime` (ADR 0059 §10, Train 3 PR②) reuses the
  restore catalog above rather than inventing a second one**:
  `RestoreRow`/`MetaCommand::BeginRestore` gained one optional field,
  `pitr: Option<PitrRestorePlan>` (`{target_wall_ms, segments:
  Vec<PitrReplaySegmentRef>}`), carried verbatim exactly like `gsi_defs`
  already is — a PITR restore is otherwise indistinguishable from an
  on-demand one to every downstream consumer (activation, GSI declare,
  `/admin/restores`). Two new pure accessors, both taking `&self` and
  doing no I/O:
  - `Metadata::pitr_restore_window(table) -> Option<PitrRestoreWindow>`
    (`{generation, earliest_ms, latest_ms}`) is the validation gate's
    `EarliestRestorableDateTime`/`LatestRestorableDateTime` — scoped to the
    table's **current** generation only (an earlier generation's own
    window, crossed by a disable/re-enable cycle, is never reachable
    through this accessor, which is what makes a generation-gap `T` reject
    on the ordinary out-of-bounds check rather than needing a dedicated
    error path), and it answers correctly whether or not the source
    table's schema still exists (falls back to `Metadata::pitr_generation`'s
    own surviving counter) — a deleted table's PITR history stays
    queryable, mirroring the backup catalog's own ADR 0024 carve-out.
  - `Metadata::pitr_replay_segments(base_tablet_progress, cutoff_wall_ms)
    -> Vec<PitrReplaySegmentRef>` selects which segments a restore must
    replay: a forward DFS over `split_lineage` starting from each of the
    chosen base snapshot's own pinned tablets, including **every** visited
    tablet's own `pitr_segments` rows regardless of that tablet's current
    liveness (a root tablet's floor is the base snapshot's own recorded
    cut version; a descendant's floor is 0, since ADR 0050's copy-based
    split gives every child an empty change log at birth). **Built on a
    direct DFS, deliberately not `live_split_descendants`** (§6's
    on-demand-capture re-planning accessor) — that accessor answers "live"
    descendants only and returns empty for a tablet retired by an ordinary
    `DropTableTablets` (no `split_lineage` entry, unlike a split), which
    silently dropped every segment of a deleted-and-never-split table's
    own tablet the first time this function was built that way. See the
    ADR's Train 3 PR② as-built amendment for the full incident; regression:
    `meta::tests::
    pitr_replay_segments_still_finds_a_dropped_never_split_tablets_own_segments`.

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

  **`node.rs`'s `persist_wal` has no halted-gate at all** (unlike
  `animus-cp-data`'s own `persist_wal`/`flush_pending`, which tolerate a
  live I/O error only while a group's `halted: AtomicBool` is set — see
  `docs/engineering-lessons.md`'s "halted-gated durability assert" entry,
  issues #282/#279): here `env.append(WAL, ..).await.expect("wal append")`
  and `env.sync(WAL).await.expect("wal sync")` are bare, unconditional
  `.expect()`s with no tolerated-error path whatsoever, on any node,
  live or shutting down. **Test-authoring consequence**: never point
  `DiskConfig::set_enospc_prob`/`set_error_prob` at a live node's disk in
  this crate's tests (`SimEnv` or otherwise) — an injected disk error on
  the consensus loop's own WAL append/sync panics the test process itself
  rather than exercising any application-level fault handling, since there
  is none to exercise. `DiskConfig::set_fsync_lie_prob` (never errors —
  `sync` returns `Ok` and silently leaves the bytes buffered) and
  `torn_tail_on_crash`/`corrupt_on_crash` (fire only at `Simulator::crash`,
  not mid-`.expect()`) remain safe.

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

### The control-plane machinery fault-injection corpus (`tests/control_corpus.rs`)

The seed-depth counterpart to this crate's ~30 fixed-single-seed acceptance
tests above, proving the control-plane-*unique* machinery (the ADR 0038 async
apply task, the replicated schema catalog's exclusivity guarantee) under a
real fault matrix, not just at one hand-picked seed. `learner_corpus.rs`
already covers the learner/membership-class vocabulary (ADR 0058 Train 1,
`ANIMUS_LEARNER_SEEDS`) — this is its sibling for everything else.
**Self-contained**, mirroring `animus-test`'s `raftkv_linearizable.rs`
architecture (declarative `Scenario`/`Nemesis`/`Group::apply`/`run_scenario`/
`assert_scenario_ok`) with one adaptation: a `Scenario::workload: Workload`
field selects which bespoke `spawn_*_workload` function the runner drives
(mirroring `animus-test`'s `txn_serializable.rs`'s own `Workload` struct),
since this plane's interesting scenarios need genuinely different client
shapes (concurrent schema proposers vs. plain no-contention churn), not just
different parameters of one shared loop.

**No `check_cycles` here** — a single Raft log total-orders every
`MetaCommand`, so there is no client-visible read/write history to build an
Elle dependency graph over. The property is convergence + safety invariants
instead, checked on every scenario: (1) **convergence** —
`nodes[i].metadata() == nodes[j].metadata()` for every replica pair, via the
same converged-or-timeout poll shape every corpus in this repo uses; (2)
**durability** — an effect a proposer's own retry loop actually *confirmed*
(read back after proposing, never merely `ProposeResult::Accepted`, which
only means "appended to the leader's log") must survive into the final
converged state; (3) **schema-catalog exclusivity** (a safety property,
checked unconditionally, fault or not) — `MetaCommand::CreateTableSchema`
rejects outright on an existing table name (first-committer-wins, **not**
idempotent-on-identical the way `RegisterNode`'s CAS is), so for every table
name two or more racers proposed, the surviving schema (if any) must be
byte-identical to exactly one of the racing proposals on every replica, and
never absent if any racer's proposal was ever durably confirmed; (4)
**allocator injectivity** (PR②, safety, unconditional) —
`Workload::AllocatorRace`'s `check_allocator_injectivity`: every `TabletId`
observed in any replica's tablet map, at every convergence poll AND every
fault-schedule step (not just the final state — `Shared::sample_tablets`),
must carry one stable identity (table + range) throughout the run, catching
a transient double-assignment even if a later poll happens to "correct" it
back; (5) **`RegisterNode` CAS integrity** (PR②, safety, unconditional) —
`Workload::RegisterCas`'s `check_register_cas_integrity`, mirroring check 3's
shape over `Metadata::node_addrs` instead of `Metadata::schemas`; (6)
**apply-task liveness / no-permanent-stall** (PR③, safety, unconditional on
EVERY scenario) — `poll_apply_task_caught_up`: after convergence,
`RaftNode::engine_applied_index()` must catch up to `RaftNode::commit_index()`
on every live replica within the same converged-or-timeout budget check (1)
uses. Deliberately a **separate** property from (1): a uniformly-stalled
apply task (every replica stuck at the same stale-but-consistent `Metadata`)
still looks "converged" to (1), which only ever compares replicas against
each other, never against the group's own `commit_index`. No separate
double-apply probe was added — checks (3)/(4) already catch a double-apply
if one ever happened (a double-applied `CreateTablet` would violate
injectivity or be naturally idempotent; a double-applied `CreateTableSchema`
would violate exclusivity if it somehow un-rejected on replay), so
`StopRestart`'s own cells just need to actually exercise those existing
checks post-recovery, which they do (`assert_scenario_ok` runs unconditionally).

**Gotchas this corpus's own build found** (see `docs/engineering-lessons.md`
for the full write-ups): (a) a racing proposer's confirm loop must decide
"won" vs. "lost" by **content**, never by presence — since
`CreateTableSchema` rejects rather than no-ops on an existing name, "the
table now exists" is true for every racer the instant *any* of them wins,
so a presence-only check makes a losing racer misreport itself as a winner
(`SchemaRace`, PR①; `AllocatorRace`'s `BeginSplit` phase reuses the same
discipline, PR②). (b) The inverse trap for a workload whose racing
proposals are content-**identical** except for the field being raced
(`AllocatorRace`'s `CreateTablet` phase — same shared table/range/replicas
for every racer, only the candidate id differs): a raw "confirmed at most
once" assertion over that field is checking a *stronger, false* property,
since several racers legitimately and correctly agreeing "the tablet that
landed carries my own candidate id" is expected, not a bug — only a
content/fingerprint comparison (`sample_tablets`), never an occurrence
count, states injectivity correctly. (c) A fault-finding confirmed in one
plane over a shared codec (issue #495, the WAL-corruption gap in
`animus-control::persist::WalRecord` — since fixed by a per-record CRC32
checksum, see that module's own doc — confirmed reproducible at the time
in `animus-cp-data`) does **not** automatically reproduce in a sibling
plane that shares the codec but not the downstream invariant the
corruption has to trip — confirmed absent here across an 80-combination
sweep (this plane's commands carry no HLC timestamp, and its CAS/epoch
checks reject a mismatch rather than asserting on one); see
`control_corrupt_on_crash_may_hard_panic_issue_495`'s own doc — still a
useful standing regression probe post-fix, now for whichever future
`MetaCommand` field or replay-path invariant might one day become strict
enough for a merely-dropped (rather than wrong-valued) tail record to
matter. (d) PR③:
`Simulator::crash`+`Simulator::restart` (mutes/re-arms the SAME still-live
tasks) is **not** a stand-in for a real process restart — proving the ADR
0038 apply task's restart-recovery path (`meta_apply_loop`'s engine rebuild
+ watermark reseed) needs `Simulator::stop` (removes the tasks entirely)
followed by a genuinely fresh `RaftNode::start` reopening the SAME retained
engine handle; see `docs/engineering-lessons.md` for the general form of
this lesson. (e) PR③: a real multi-chunk `InstallSnapshot` transfer, once
shipping starts, completes in on the order of single-digit milliseconds of
virtual time in this plane (no artificial per-chunk delay) — a
fixed-`Duration` fault schedule aimed at "mid-transfer" will usually miss
entirely; a condition-based poll (has the receiver started but not
finished) is what actually lands inside the window regardless of a given
seed's exact timing (`wait_for_snapshot_transfer_in_flight`).

**Scope as of PR③ (final — the stack is complete)**: `Workload::SchemaRace`
(2-3 concurrent proposers racing `CreateTableSchema`, same-table or
distinct-name), `Workload::PlainChurn` (non-contending `UpsertMember`, the
non-vacuity floor), `Workload::AllocatorRace` (several proposers racing
`CreateTablet`/`BeginSplit` against ONE shared table/tablet, hammering
`Metadata::next_tablet_id`/`next_free_tablet_id()`), `Workload::RegisterCas`
(several proposers each claiming a distinct node id then attempting one
deterministic differing-re-registration collision against their own claim —
lifts `register_node_cas.rs`'s fixed-single-seed CAS proof into this
corpus's fault matrix), and (PR③) `Workload::SustainedChurn` (like
`PlainChurn` but 50 rounds/proposer instead of 3, driving the log well past
`SNAPSHOT_THRESHOLD` so a swept `StopRestart` has real in-flight
apply-task/compaction state to interrupt) — over a nemesis set:
`LeaderKill`/`FollowerKill`/`PartitionLeader`/`SplitBrain`/`Lossy`/
`Duplicate` (`NetConfig::set_duplicate_prob`)/`FsyncLie`
(`DiskConfig::set_fsync_lie_prob`)/`TornTail` (`DiskConfig::
torn_tail_on_crash`, composed with a crash)/(PR③) `StopRestart` (a REAL
process restart — `sim.stop` + a fresh `RaftNode::start` reopening the SAME
retained `MemoryEngine` handle, `Group::engines` — categorically different
from `LeaderKill`/`FollowerKill`'s `sim.crash`, which mutes the SAME
still-live tasks and never exercises `meta_apply_loop`'s restart-recovery
path at all). `heal_all` resets **both** `NetConfig` and `DiskConfig` to
default — required for `FsyncLie`/`TornTail`, which are armed globally with
no auto-expiry (PR① never used `DiskConfig` at all). `CorruptOnCrash` is
deliberately **not** a `Nemesis` variant (issue #495 above); the one cell
exercising that composition is a dedicated, always-`#[ignore]`d test, never
part of the asserted `corpus_cells()` set.

**PR③'s two chunked-snapshot-under-fault tests** (`chunked_snapshot_
source_crash_mid_transfer_3`, `chunked_snapshot_receiver_stop_restart_3`) are
deliberately **outside** `corpus_cells()`/`Workload`/`Nemesis` — they grow a
REAL `Metadata` image through the actual `meta_apply_and_compact`/
`syskv_image` path until it forces a genuine multi-chunk `InstallSnapshot`
transfer, then inject a source-leader crash or a receiver `StopRestart`
while chunks are demonstrably still in flight
(`wait_for_snapshot_transfer_in_flight`, a condition-based poll rather than
a duration guess — an exploratory run found the whole transfer completes
within ~3ms of virtual time once shipping starts, too narrow a window for
this harness's `Vec<(Duration, Nemesis)>` schedule to land inside
reliably). Fixed-single-seed regressions (like `install_snapshot.rs`'s own
tests), not part of the `ANIMUS_CONTROL_SEEDS` seed-expansion.

Depth knob **`ANIMUS_CONTROL_SEEDS`** (default 1 = the frozen cells; held
green at `=15` and `=40` during both PR②'s and PR③'s own validation; now
wired into CI's nightly `corpus-deep.yml`, default `40`). A structural
`control_corpus_covers_the_fault_matrix` guard keeps the nemesis/workload
matrix honest, mirroring `raftkv_corpus_covers_the_fault_matrix` — it
deliberately does not (and must not) require the `#[ignore]`d
corrupt-on-crash cell to be part of the asserted set.
