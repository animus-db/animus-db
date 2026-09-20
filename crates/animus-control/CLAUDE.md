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

  **A second gotcha (issue #923): `become_leader` seeds every peer's
  `peer_last_contact` to the instant a leadership stint begins — a
  courtesy timestamp, not a genuine ack, there only so a peer that stays
  silent the whole stint can't hide behind the "never contacted yet" grace
  forever.** That seed ages out after the same `CONTROL_PEER_LIVENESS_
  TIMEOUT` a real ack would, with no wider allowance for "this leader only
  just took over" — a fresh leader's first real heartbeat round can
  legitimately take longer than that steady-state timeout under the load a
  leadership change itself creates. `control_peer_believed_alive` covers
  this with a **second**, wider, separately-named grace —
  `RaftCore::leader_since` (gated on `role == Leader`, so a stepped-down
  node reads `None` with no explicit clearing needed) +
  `CONTROL_LEADER_TAKEOVER_GRACE` — deliberately not a reuse of
  `CONTROL_PEER_LIVENESS_TIMEOUT` itself, since an equal-sized second grace
  would be a no-op (`become_leader`'s own seed already provides exactly
  that much). See ADR 0037's 2026-09-16 amendment and ADR 0012's matching
  one (the identical shape of fix for the **raftkv**-id `FailureDetector`'s
  own `LEADER_GRACE`, a structurally separate mechanism/id-space that
  needed the same kind of post-election patience).

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
  **`TableSchema::tags: BTreeMap<String, String>`** (roadmap W-06, DynamoDB
  `TagResource`/`UntagResource`/`ListTagsOfResource`) is a third shape,
  neither `StreamSpec`'s label-carrying nor `TtlSpec`'s no-label-single-
  value one: it **merges** rather than replaces, so `MetaCommand::
  TagResource`/`UntagResource`'s apply arms are per-key set/remove, and
  (unlike `SetTableTtl`) a caller checking whether its own write committed
  must compare per-key membership, never the whole map — see
  `docs/engineering-lessons.md`'s entry on this for the general form.
  `Metadata::table_tags` mirrors `table_ttl`/`table_pitr`'s read-accessor
  shape (empty map rather than `None` for a known table with no tags, since
  a tag set has no "disabled" state to distinguish from "empty").

  **`IndexDef` carries `hash_attribute_type`/`sort_attribute_type: Option<
  ColumnType>` (issue #319/W-05)** — the DynamoDB `AttributeType` an
  index's own hash/sort key attribute was declared with, resolved by
  `animus_dynamo::schema::index_to_control` from a `CreateTable`/
  `UpdateTable` call's own `AttributeDefinitions` when the caller supplied
  one; `#[serde(default)]`, `None` for a pre-existing definition or an
  attribute nobody ever declared a type for (still renders `DescribeTable`'s
  honest `S` placeholder, never a fabricated type). Every one of this
  struct's ~24 existing construction sites across this crate, `animus-node`,
  `animus-test`, and `animusd`'s tests needed both new fields added —
  compiler-enumerated via `error[E0063]`, the same fan-out pattern root
  `CLAUDE.md`'s "a future 8th port" entry describes for `RoleAddrs`.

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

- **`shared_wal.rs`** — `SharedWal<C, S>` (ADR 0028): a multi-tenant WAL I/O
  coordinator that serializes concurrent tablet WAL writers into one file
  with coalesced `append`+`sync`. **Wired into `animus-cp-data`'s persist
  path behind `--shared-wal`/`--no-shared-wal`/`cluster_settings.shared_wal`
  since C-05 PR 2 (2026-09-06) — additive default OFF; C-05 PR 3
  (2026-09-06, same day) is the cutover that flips the default ON**, the
  identical two-step shape C-02 (heartbeat batching) used — see `crates/
  animusd/CLAUDE.md`'s "Shared WAL" section for the flag/default/opt-out
  detail and the real-thread liveness proof this cutover added
  (`tests/shared_wal_liveness.rs`). Two APIs: the original raw, untyped `append`/
  `compact` pair (unchanged — still what `benches/wal_fsync_bench.rs`
  measures directly) and a **tagged, group-aware** one added by PR 2 —
  `append_tagged`/`compact_group`/`forget`/`open`/`recovered_state`,
  backed by an in-memory `group_tails: BTreeMap<TabletId, Vec<WalRecord<C,
  S>>>` cache mutated in the same critical section as each op's own queue
  enqueue (the property the round/ack and GC arguments both rest on — see
  the type's own module doc for the full "Two APIs"/"Recovery / GC
  contract" account). **A tolerated (halted-gated) failure now rolls that
  mutation back rather than leaving it permanent (issue #838, fixed
  2026-09-14)**: the eager-at-enqueue mutation is correct and load-bearing
  for the FIFO ordering argument, but it is not itself a durability claim
  — `submit_with_mutation` also snapshots the mutated tablet's own
  `group_tails` entry from immediately before `mutate` runs
  (`GroupTailsUndo{tablet, previous}`, carried on the queued `Pending`),
  and `drive()`'s failure branch applies every batch member's own undo, in
  **reverse** enqueue order (the LIFO unwind a batch coalescing several
  `append_tagged` calls for the SAME tablet needs to land back on the
  pre-batch state). Before this fix, a tolerated failure's phantom
  mutation survived to be durably written out by a completely different,
  healthy tablet's own next `compact_group`/`forget` whole-file rewrite —
  see ADR 0028's matching 2026-09-14 amendment and `docs/engineering-
  lessons.md`'s entry for the full incident and the general "eager for
  ordering is not the same claim as final" lesson. Regression:
  `crates/animus-cp-data/tests/sharedwal_fault_corpus.rs`'s cell (e).
  **A tolerated `sync`-only failure's own buffered bytes are now repaired
  before `drive()` can hand the file to the next queued op (issue #883,
  fixed 2026-09-15)**: `#838`'s undo above is a `group_tails` (in-memory
  bookkeeping) fix and has nothing to say about `flush`'s own `Append`
  branch, where `env.append` succeeding before the round's own `env.sync`
  fails leaves those bytes genuinely sitting in the file's un-synced
  buffered region — `Disk::append`/`Disk::sync` are two independently
  observable physical steps, and a failed `sync` does not retroactively
  un-write bytes an earlier `append` already buffered. Left in place, a
  completely different, healthy tablet's own next **ordinary** (not even
  compacting) `append_tagged` round would extend the SAME buffered
  region, and that tablet's own successful `sync` would durably commit
  the whole thing, doomed bytes included. `flush` now repairs this
  itself on a tolerated `Append`-batch `sync` failure: read the file
  back, confirm its tail is exactly the bytes this round itself just
  appended, and atomically `Disk::replace` the file with everything
  before that tail — caller-agnostic (no `Serialize` bound needed, so it
  covers the raw/untyped API too), no I/O added on the happy path,
  best-effort (a repair failure leaves the caller with the same original
  `sync` error as before this fix). See ADR 0028's matching 2026-09-15
  amendment for the alternatives rejected. Regression:
  `crates/animus-cp-data/tests/sharedwal_fault_corpus.rs`'s cell (f),
  which needed a new `DiskConfig::set_sync_error_prob` knob in
  `animus-sim` (every existing knob fires uniformly across every disk
  op, so none could fail only a `sync` while leaving the repair's own
  immediate `read`/`replace` free to succeed).
  `physical_write_count()` is a plain running counter
  of completed physical writes (append-batches and rewrites alike) —
  the coalescing-observability primitive a caller (or a test) reads
  before/after a burst to measure the win directly, without needing a
  `MetricsHandle` threaded through this coordinator (`animus-cp-data`'s
  own `Metric::CpSharedWalSyncs`/`CpSharedWalGcRewrites` are recorded from
  its caller side, using this as the underlying signal).
  **Gated and confirmed worth it on real disk (C-05 PR 1, 2026-09-06)**:
  `crates/animus-cp-data/benches/wal_fsync_bench.rs` measures the raw API
  directly against real `ProdEnv` I/O — on this host's real block-device-
  backed filesystem, a burst across K=128 groups costs ~10.5–11.2ms p50 as
  K separate per-group fsyncs vs. ~1.4–1.6ms p50 routed through
  `SharedWal::append` into one file, with the measured fsync count
  dropping from 128 to ~2. See `docs/design/shared-wal-fsync-benchmark.md`
  for the full method/numbers and ADR 0028's 2026-09-06 amendments (both
  PR 1's benchmark and PR 2's wiring) for the full design record —
  including the round/ack semantics, the GC policy and its bound, the
  flag shape, and layout-mismatch handling. See `crates/animus-cp-data/
  CLAUDE.md`'s own C-05 PR 2 entry for the persist-path wiring itself.

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

- **Boot-time genesis-vs-wiped-restart check (ADR 0009's 2026-09-15
  amendment, issue #667 — P0 Raft safety).** A `RaftCore` whose persisted
  state replays empty (`node.rs`'s `drive`, the branch that keeps the
  fresh `RaftCore::new` rather than calling `RaftCore::recovered`) never
  grants a real vote or campaigns (`RaftCore::cluster_check_pending`/
  `refused_as_voter`) until `begin_cluster_check`'s peer probe round
  (`RaftMsg::ClusterProbe`/`ClusterProbeResp`) resolves whether this is a
  genuine first-ever bootstrap/ADR 0060 growth join (safe — the config a
  responding peer already has committed does not yet name this node id as
  a voter) or an already-established voter's disk wiped clean (unsafe —
  the peer's config already does; refuse permanently, re-admit only
  through the learner/rejoin path, ADR 0032/0058). Gates only the real
  vote grant in `handle_request_vote`, deliberately not `handle_pre_vote`
  (touches no persisted state, so it's never part of the hazard — see the
  ADR amendment for why gating it too was tried and reverted). The
  initial probe is sent from a separate spawned task at boot (never
  inline before the first `env.recv()`, which risks a multi-node-genesis
  mutual stall) and reuses the SAME entropy `RaftCore::new`'s own
  construction already drew, never a fresh draw — seemingly-unrelated
  fixed-seed corpus cells can desync from either an extra draw or the
  extra task/wire-traffic alone; see `docs/lessons/testing/2026-09-15-
  boot-path-entropy-desyncs-fixed-seeds.md`. **CP data plane
  (`animus-cp-data`) has the identical hazard, unfixed** — tracked as
  issue #900, since a tablet's peer set is dynamic and reconstituted
  constantly (unlike the control plane's one-time genesis config), a
  materially different liveness tradeoff needing its own design decision.
  **Two further real regressions in this same mechanism, found and fixed
  the same day (ADR 0009's second 2026-09-15 amendment)**: (1)
  `RaftCore::next_deadline()` didn't account for the cluster-check's own
  independent resend deadline, so `node.rs`'s driver could oversleep past
  a due resend for as long as `election_deadline` kept getting reset by
  ordinary heartbeat traffic — fixed by including
  `cluster_check_resend_deadline` in `next_deadline()`'s own `min(..)`.
  (2) `config.contains(&self.id)` alone can never distinguish an ordinary
  genesis race (every founder's config trivially contains every other
  founder from birth) from a genuinely established restart — fixed by a
  new `RaftMsg::ClusterProbeResp` field, `ever_heard_from_prober`, backed
  by a per-core `heard_from: BTreeSet<NodeId>` marked ONLY at the three
  sites representing a genuinely durable, forgettable vote (a candidate's
  own self-vote, a vote WE granted it, or proof it won a real election) —
  see `RaftMsg::ClusterProbeResp`'s and `RaftCore::
  handle_cluster_probe_resp`'s own doc comments for the full decision
  table, and the two matching `docs/lessons/code-patterns/2026-09-15-*`
  entries for the incidents (including a real bug in the fix's own first
  draft: a rejected vote is not participation, and counting it
  reintroduced the exact false refusal the fix exists to prevent).
  **Third amendment, same day**: `ever_heard_from_prober` was originally
  wired as decisive on the FIRST peer to answer `false`, on the (false)
  assumption that an established voter's peers all keep answering `true`
  forever. `heard_from` is only marked at message sites that route through
  a candidate/leader, so two ordinary followers that never themselves
  campaign never learn of each other — any 3-voter cluster with one stable
  leader is guaranteed to have a follower-follower pair that legitimately,
  permanently answers `false` for each other. A real, deterministically
  reproducing (not intermittent) `ProdEnv` failure in `prod_liveness.rs`'s
  `wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving` caught
  this. Fixed by folding the signal into the SAME wait-for-every-peer
  aggregation the established verdict already uses (decide only once
  every peer has answered, refuse if *any* showed `true`, resolve fresh
  only if *none* did) instead of letting a single `false` short-circuit
  the wait — see `docs/lessons/testing/2026-09-15-a-per-peer-any-false-
  signal-is-not-safe-when-the.md` and the new
  `tests/wiped_voter_follower_peer_evidence.rs`.

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

- **`BeginSplitInPlace`'s apply arm enforces F11 token alignment on a
  streamed table (ADR 0042 §14, growth PR2).** A split key that isn't exactly
  `TOKEN_BYTES` (8) long is rejected outright when the source tablet's
  table has a stream (`self.table_stream(table).is_some()`) — an
  apply-time structural seatbelt: `animusd`'s `ClientCtx::trigger_split`
  is the one choke point that actually rounds a caller's key before ever
  proposing, so this check guards a future caller reaching apply without
  going through it, never the primary enforcement. See `meta::tests::
  split_in_place_rejects_a_non_token_aligned_key_on_a_streamed_table`/
  `split_in_place_rejects_a_token_aligned_key_equal_to_range_start` (the
  latter proving the accepted single-token hot-partition limit, Fork E,
  still rejects at the pre-existing `KeyRange::split_at` "strictly inside" guard
  rather than accepting a zero-width sibling).

- **Epoch-CAS discipline on `BeginSplitInPlace`/`CutoverSplit`/
  `CasTabletReplicas`.** Every tablet-mutating command is a compare-and-swap
  on the tablet's epoch, evaluated identically on every replica, so
  accept/reject is consistent and racing proposers can't both commit.
  (`MergeTablets` — ADR 0033, carrying *two* expected epochs since it read
  two tablets from one snapshot — was removed by ADR 0044; tablets are
  split-only.) Any new tablet-mutating command must adopt the same guard.

- **The copy-based split lifecycle (ADR 0050 Train B rung 3:
  `BeginSplit`/`CutoverSplit`'s copy branch) was deleted whole 2026-09-01**
  (the copy-split-deletion stack, Layers A/B1/B2 — see `docs/adr/0058-*.md`'s
  2026-09-01 as-built note and `docs/adr/0050-*.md`'s matching amendment),
  retrievable from git history. `MetaCommand::BeginSplit` — its enum
  variant, apply arm, mirror arm, and `is_relayable_command`
  classification — no longer exists; `CutoverSplit` survives as one apply
  arm, not two branches (below). The in-place lifecycle that follows is
  now the sole split mechanism.

- **The in-place split lifecycle (ADR 0058 Train 2 rung 3, directed by ADR
  0062): `BeginSplitInPlace`/`CutoverSplit`.** `BeginSplitInPlace`
  (epoch-CAS + a state gate: parent must be `Active`, F11 seatbelt above,
  monotonic child-id allocator) mints **no** `Building` tablet-map rows at
  all — it records the intent directly on the parent
  (`Tablet::inplace_split = Some(InPlaceSplitIntent{split_key, children})`,
  `animus-tablet`) and marks it `Splitting`, full stop. There is nothing
  physical to place a policy on yet, so no policy copy happens here. The
  data plane's own `KvCommand::SplitTablet` (`animus-cp-data`) — not this
  command — is what actually materializes the two children, entirely
  outside control-plane Raft; this command only ever sees the *intent*,
  never the fork itself.

  `CutoverSplit` (epoch-CAS; parent must be `Splitting` — its own apply arm
  creates both children's tablet-map rows DIRECTLY from the parent's own
  intent's `(id, replicas)` pairs; a `Splitting` parent carrying no intent
  is structurally impossible (`BeginSplitInPlace` is the sole path into
  `Splitting`) and is rejected defensively rather than trusted with an
  `.expect()`. **Issue #684 defense-in-depth**: before minting either
  child's row, this arm also rejects outright —
  `"child tablet id already occupied — allocator invariant violated"`, plus
  a `tracing::error!` — if either slot is already occupied, rather than the
  unconditional `self.tablets.insert` it used to be; the `CreateTablet`
  floor guard (below) is meant to make this structurally unreachable
  through ordinary commands, but the insert itself no longer trusts that.
  A rejected cutover here leaves the parent `Splitting` forever — safe, not
  a wedge, since `animusd`'s `index_drain::inplace_split_driver_tick`
  re-proposes `CutoverSplit` every tick regardless, idempotently, until the
  parent vanishes — only this one already-corrupt split stays pending,
  loudly) atomically activates both children, **removes** the parent
  (tablet + policy; the reconciler reclaims it as ordinary
  hosted-but-absent), and writes `Metadata::split_lineage[child] =
  SplitLineage {parent, parents_final_epoch, cutover_wall_ms}` — fork F9,
  recorded at the one moment the parent's shard chain is complete (never
  pruned; the B6 `ParentShardId` source). **`parents_final_epoch` is
  legitimately `None`** whenever the parent itself never sealed a shard of
  its own before it split further (a fast cascade's intermediate tablet —
  ADR 0043 §A3's "never seal an empty segment" — not a bug, and not the
  same thing as "no ancestor in this chain ever sealed anything").
  `Metadata::stream_shard_parent_id` — the sole reader of this field —
  accounts for this: a `None` here makes it walk one hop further up
  `split_lineage` (`lineage.parent`) rather than stopping, so a
  descendant's `ParentShardId` still resolves to the nearest REAL sealed
  ancestor, however many never-sealed hops lie in between (issue #588,
  2026-09-04; see that ADR's Fork F9 entry for the full incident — the
  fix is entirely in the derived read, `split_lineage` itself writes
  exactly as before). Wall time rides the command
  (`cutover_wall_ms`), `SealStreamShard::seal_wall_ms`'s discipline — the
  state machine has no clock. The zero-copy split's own command and
  provenance maps (`SplitTablet`, `split_parents`, `stream_split_basis`)
  were deleted in the ADR 0050 Train B rung-7 sweep, and its build/
  copy-based command and provenance stayed only through `split_lineage`
  either way — `split_lineage` is the sole split-provenance record.
  Placement (`reconcile_placement`/`rebalance_placement`) skips every
  non-`Active` tablet — the mid-split set is frozen.

  **Since ADR 0062**, each `replicas` in the intent is that child's
  FORK-TIME homes — the parent's own current replicas, verbatim, identical
  for both children, never placement-chosen — a child's eventual *final*
  home is a separate decision this same apply arm also makes (below), not
  something the intent carries. `CutoverSplit` inherits the parent's
  policy **at this moment** — the in-place workflow's only chance to,
  since there was no tablet row to attach it to at `BeginSplitInPlace`
  time. **G1 (ADR 0058's own "Open forks" table, decided 2026-08-25,
  reversing that ADR's own Stage 4 draft text): the GSI-drain/
  backfill-seeder cutover vetoes stay PRE-cutover, caller-side** — this
  command's own apply never gates on drain state; `animusd`'s in-place
  split driver (`index_drain.rs::inplace_split_driver_tick`) runs those
  vetoes before ever proposing this command. Mirror arms + `syskv::
  EntityKind::SplitLineage` follow the usual per-entity conventions
  (`BeginSplitInPlace`: parent row + allocator counter only; `CutoverSplit`'s
  arm also mirrors each child's policy). Tests: `meta::tests::
  begin_split_in_place_*`/`cutover_split_in_place_*`; `apply_engine.rs`'s
  `cache_matches_engine_through_a_mixed_scenario_and_a_restart` and
  `cache_matches_engine_through_directed_placing` for the
  differential-oracle proof; `mirror.rs`'s own
  `rebuild_from_engine_matches_direct_apply` for `BeginSplitInPlace`'s
  write-derivation shape (parent row + counter, no `Building` rows).

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
  against. **`SplitPlacing::target` is AUTHORITATIVE, not a write-once
  diagnostic (fixed for issue #528, ADR 0062's 2026-09-01 amendment)** —
  it used to be treated as a frozen snapshot of what `CutoverSplit`
  decided, with the reconcile loop always recomputing `select_replicas`
  fresh off current membership instead of trusting it; under sustained
  load the ordinary failure detector's own flap (ADR 0012) made that fresh
  recompute pick a *different* target almost every tick, faster than
  `animus-cp-data`'s learner-phased mover (`reconfigure_step`) could ever
  complete one cycle — a livelock that meant `MarkSplitPlacingDone` never
  fired. `node.rs`'s `reconcile_loop`'s **third phase**,
  `Metadata::split_placing_reconcile`/`PlacementView::
  split_placing_reconcile`, still runs unconditionally every tick (own
  cadence, independent of repair/rebalance's gating — a split-triggered
  relief obligation shouldn't wait behind `REBALANCE_EVERY_N_TICKS`), but
  now: (1) drives toward the STORED target **verbatim**, proposing a
  `CasTabletReplicas` only when it differs from the tablet's current
  replicas, as long as every one of the target's members is currently
  `Active` — never recomputed while healthy; (2) **pauses** (proposes
  nothing) for a tablet whose stored target has a transiently non-`Active`
  member; (3) only past a dwell (`node::SPLIT_PLACING_RETARGET_DWELL`, 5s,
  tracked per-`(tablet, member)` in a driver-local, `env.now()`-keyed
  `BTreeMap` — `node::retarget_ready_this_tick`) does it recompute — via
  `replan` (keeping still-live survivors of the old target, replacing only
  the genuinely-gone member, minimizing churn), never `select_replicas`
  from scratch — and propose the result as a REPLICATED
  `MetaCommand::RetargetSplitPlacing{tablet, expected_epoch, target}`
  (epoch-CAS'd against the child's own current epoch, `MarkSplitPlacingDone`'s
  discipline), so the new target is itself stable for every subsequent
  tick and every replica rather than independently re-derived by whichever
  node happens to lead. `target: None` (unsatisfiable at cutover, fork B)
  keeps its original "nothing stored to protect, keep retrying every tick"
  stance — a successful recomputation there now *establishes* the first
  stored target via the same `RetargetSplitPlacing` command. This phase
  never proposes `MarkSplitPlacingDone` itself — that observes *live Raft*
  convergence (`RaftKvNode::config()`/`learners()`), which this pure
  metadata-level view can't see; a leader-gated `animusd` background loop
  does, once a led tablet's live group matches `Metadata`'s current
  `replicas` with no dangling learners, held continuously for a settle
  window (see `animusd/CLAUDE.md`). `MarkSplitPlacingDone` is epoch-CAS'd
  against the **child's own** current epoch and idempotent on an
  already-`done` entry (`MarkIndexBackfilled`/`RecordBackupTabletComplete`'s
  idiom exactly); on `is_relayable_command`'s allowlist
  (`animus-node/src/wire.rs`), since a tablet's leader is frequently not
  the control-plane leader — `RetargetSplitPlacing` is deliberately **not**
  on that allowlist (like `CasTabletReplicas`, it's proposed directly by
  the control-plane leader off its own live `RaftNode` handle, never
  relayed). Both `rebalance_placement`'s own eligibility filter AND
  `reconcile_placement`'s (repair) now exclude a tablet carrying an
  un-`done` `split_placing` entry (the repair exclusion is the issue #528
  fix's own addition — closing a secondary, compounding race where repair
  could independently retarget the same tablet in the same tick) — the
  dwell-gated placing phase is the sole mover for that tablet until
  `done`, so none of the three convergence sources ever compete for the
  same tablet's epoch in the same tick. `DropTableTablets` prunes any
  `split_placing` row for a tablet it removes (`mirror.rs`), the same
  orphan-sweep `MarkIndexBackfilled`'s own doc describes for
  `index_backfill`. `syskv::EntityKind::SplitPlacing` follows the usual
  per-entity mirror conventions (`RetargetSplitPlacing`'s own mirror arm is
  identical to `MarkSplitPlacingDone`'s). Tests: `meta::tests::
  cutover_split_*` for the apply-time decision (already-satisfying/
  differing-target/unsatisfiable-at-cutover/no-policy shapes),
  `meta::tests::split_placing_reconcile_*`/`retarget_split_placing_*`/
  `reconcile_skips_an_undone_split_placing_tablet_even_with_a_down_replica`
  for the issue #528 fix's dwell-gate/retarget/repair-exclusion behavior at
  the pure-`Metadata` level, `tests/placement_split_placing.rs`'s
  `split_placing_phase_retargets_a_member_down_past_the_dwell`/
  `split_placing_phase_flapping_under_the_dwell_does_not_retarget` for the
  same behavior proven live over a real `reconcile_loop`/`SimEnv`,
  `drop_table_tablets_prunes_split_placing_rows_for_the_dropped_tablets`
  for the cascade. **Issue #513** (a suspected oscillation in the
  convergence primitive this phase drives — `reconfigure_step`, ADR 0058
  Train 1 — for a two-(or-more)-replica-difference target) **was
  investigated and closed as not reproducible**; see `crates/animusd/
  tests/split_placing_two_replica_diff_e2e.rs`, `crates/animus-cp-data/
  tests/reconfigure_multi_replica_diff.rs`, `docs/engineering-lessons.md`,
  and ADR 0062's #513 amendment. Directed Placing relocates a child
  regardless of how many replicas its fresh target differs by.

  **Issues #670/#921/#928 (2026-09-16, two-layer fix): an achieved
  directed-Placing target could be discarded by a failure-detector false
  positive, both before AND after `done`.** `retarget_ready_this_tick`'s
  dwell above (`SPLIT_PLACING_RETARGET_DWELL`, 5s) applied identically
  whether a tablet's stored target was still converging or already
  achieved (`t.replicas == target`) — but discarding an ALREADY-achieved
  target via a fresh `replan` is strictly more disruptive (a split child's
  only other eligible candidates are typically its own pre-split
  siblings, so this can converge the tablet right back toward the set the
  split was moving it away from) than discarding one still mid-move.
  Fixed with a separate, longer dwell,
  `node::SPLIT_PLACING_RETARGET_DWELL_ACHIEVED` (30s), used once
  `t.replicas == target`. **This alone only protects the narrow window
  before `done` fires** (`animusd`'s own settle window,
  `SPLIT_PLACING_DONE_SETTLE`, is 1.5s) — the instant `done` is set, the
  tablet falls under *ordinary* `reconcile_placement`/`rebalance_placement`,
  which had no dwell at all against the identical false positive. Closed
  by `node::recently_done_this_tick` (the same driver-local,
  `env.now()`-keyed pattern as `retarget_ready_this_tick`, tracking the
  FIRST tick each tablet's `split_placing` entry was observed `done`) and
  a new `recently_done: &BTreeSet<TabletId>` parameter on
  `Metadata::reconcile`/`rebalance` (and their `PlacementView` mirrors) —
  a tablet named in it is excluded from repair/rebalance for the same
  `SPLIT_PLACING_RETARGET_DWELL_ACHIEVED` window, counted from when `done`
  was first observed rather than from when the target was achieved (one
  continuous protection window in spirit, two mechanisms in practice
  because `reconcile_placement`/`rebalance_placement` have no
  `split_placing`-specific timing state of their own before this fix). An
  empty `recently_done` (every pure/unit-test caller) reproduces the
  pre-fix behavior exactly — this is **not** a fix to issue #928's fully
  general form (an ordinary tablet with no `split_placing` history still
  has no repair dwell at all against a false positive); only the
  directed-Placing-specific instance this crate can still name a tablet
  set for. Tests: `meta::tests::
  reconcile_does_not_repair_away_an_achieved_recently_done_target` (pure,
  proves the bug existed with an empty `recently_done` and closes with a
  populated one), `tests/placement_split_placing.rs`'s test 10
  (`split_placing_phase_holds_an_already_achieved_target_past_the_base_dwell`,
  the pre-`done` half) and test 11
  (`ordinary_reconcile_holds_a_recently_done_target_past_the_grace_window`,
  the post-`done` half, driven through the real `reconcile_loop`/`SimEnv`).
  See ADR 0062's 2026-09-16 amendment and `docs/lessons/testing/
  2026-09-16-a-directed-placement-decision-can-be-undone-by-a-transient-
  failure-detector-false-positive.md`.

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
  determinism argument `BeginSplitInPlace`'s child-id mint and
  `CutoverSplit`'s child materialization already rest on (see
  `docs/engineering-lessons.md`'s entry on this). The tablet list feeding
  `pinned_tablets` filters out `TabletState::Building` rows — a `Building`
  tablet today is a restore's not-yet-activated destination (ADR 0059 §7);
  the now-deleted copy-based split's own build/tail window used to leave up
  to three live rows covering one key range at once (the still-
  authoritative `Splitting` parent plus its two not-yet-cutover `Building`
  children), which is what originally motivated this filter (see
  `docs/engineering-lessons.md`'s entry on this fix) — the filter is a
  no-op on the in-place split path today (which mints no `Building` rows
  at all) but is still load-bearing for the restore case. `CompleteBackup` requires every pinned tablet to have a
  progress row; `RecordBackupTabletComplete` is idempotent on an identical
  repeat but rejects a genuinely differing one outright (no repair-update
  path yet, unlike `SealStreamShard`'s replicas-only allowance).
  **`BackupTabletProgress`/`RecordBackupTabletComplete` also carry
  `chunk_count: u64` (issue #856, 2026-09-14)** — the capture driver's own
  `CaptureCursor::next_chunk` at the moment its capture completed, so a
  reporting tablet's valid chunk-object indices are exactly
  `0..chunk_count`. This is restore's own recorded end-of-sequence bound
  (`animusd::backup_restore::restore_tick`, threaded through the manifest
  object via `BackupManifestTabletEntry::progress`): before this field
  existed, restore's chunk sweep used "no object at this index" as its
  sole end-of-sequence signal, which a `DeleteBackup` racing an in-flight
  restore (with the janitor reclaiming chunks out of order) could turn
  into a silently truncated table — see `docs/engineering-lessons.md`'s
  matching entry and `docs/adr/0059-backup-restore.md`'s 2026-09-14
  amendment for the full account. `#[serde(default)]` on both the field
  and its `backup_progress_codec::Entry` wire counterpart, per this repo's
  no-migration convention.
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
  itself. **Issue #856's second half (2026-09-14)**: the apply arm also
  rejects while `Metadata::backup_referenced_by_a_live_restore(backup_id)`
  answers true — any restore still `Seeding` from this backup — the
  authoritative seatbelt behind `delete_backup`'s own client-side
  `BackupInUseException` check (a `metadata_fresh` read, so it can race a
  restore that starts seeding in the narrow window between that read and
  this propose; this apply-time check is what actually closes it). A
  `Done`/`Failed` restore never blocks a delete. `MetaCommand::BeginRestore`
  gained the mirror-direction seatbelt in the same change: it rejects when
  its own `backup_id` names a row that is present but already
  `Expired`/`Failed` (the shape a `MarkBackupDeleted` that commits between
  the wire edge's own freshness read and this propose would produce) — a
  `backup_id` naming no row at all (already fully reclaimed) is
  deliberately left unrejected, the far narrower residual `RESTORE_STUCK_
  TIMEOUT`'s own eventual `FailRestore` self-heals. See `docs/adr/0059-
  backup-restore.md`'s 2026-09-14 amendment for the full account and both
  halves' own unit tests (`mark_backup_deleted_refuses_while_a_restore_is_
  seeding`/`begin_restore_rejects_an_expired_or_failed_backup`, `meta.rs`). The pre-existing `MetaCommand::DeleteBackup` (PR①'s own row-plus-
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

- **The replicated credential catalog (ADR 0066 §1/§2/§3, S-02 step 1):
  `PutCredential`/`RotateCredential`/`RevokeCredential`.** `Metadata::
  credentials: BTreeMap<AccessKeyId, CredentialRow>` (`AccessKeyId = String`,
  a plain alias like `BackupId`/`RestoreId` — an access key id is not
  secret) modelled directly on `BackupRow`'s own catalog-row shape
  (`meta.rs`). `CredentialRow`'s `secret`/`previous.secret` fields are
  `SecretKey`, a newtype whose `Debug` always renders `"SecretKey(REDACTED)"`
  regardless of the actual value — never derive/hand-roll a `Debug` for any
  future type that embeds one, and never `.as_str()` it into a log/panic
  message. **`created_at`/`updated_at`/`PreviousSecret::valid_until` are
  epoch SECONDS, not milliseconds** — a deliberate ADR 0066 convention for
  this one catalog, unlike every other wall-clock field in this crate
  (`BackupManifest::created_wall_ms`, `SealStreamShard::seal_wall_ms`, …),
  which are epoch milliseconds straight off `env.wall_now()`; a proposer
  (`animusd`) divides `wall_now()`'s milliseconds down to whole seconds
  before building any of the three commands below — this state machine
  itself reads no clock at all and performs no conversion. `Policy {
  tables: TableMatch, ops: BTreeSet<OpClass> }` is one flat `(tables, ops)`
  pair with no composition — deliberately not an authorization engine (ADR
  0066's Context section draws this line explicitly); `Policy::allow_all()`
  is the pre-S-02 "every class except `Admin`, every table" default a key
  created with no explicit policy gets, and `Policy::allows(class, table:
  Option<&str>)` is the one predicate both this crate's own tests and the
  eventual dispatch-gate consumer (S-02 step 3) share — never re-derive its
  logic at the call site.
  - `PutCredential { id, secret, policy, enabled, now }` — create-or-replace
    (unlike `BeginBackup`'s "already exists" rejection, a credential id has
    no natural collision error). Replacing the secret is an **immediate
    cutover**: `previous` is cleared, never preserved, the moment `secret`
    itself actually changes; a policy/enabled-only `Put` (identical
    `secret`) leaves `secret`/`previous` untouched. Idempotent on an
    identical repeat, `now` included.
  - `RotateCredential { id, new_secret, grace_secs, now }` — moves the
    row's current `secret` into `previous` with `valid_until = now +
    grace_secs`; a second `Rotate` inside an already-open grace window
    **replaces** `previous` outright (never chains a third secret, mirroring
    AWS IAM's own two-active-keys-per-user limit). Rejected against an
    unknown id (unlike `Put`'s create-or-replace shape). **Idempotence
    gotcha, closed already — read before touching this arm**: the
    idempotence check must recognize a retry from facts already on the
    **existing, unmutated** row (current secret already equals
    `new_secret`, `updated_at` already equals `now`, `previous.valid_until`
    already equals what `(now, grace_secs)` would produce) — it must
    **not** recompute the would-be `PreviousSecret` from `existing.secret`
    and compare that recomputation against storage, since on a genuine
    replay `existing.secret` has already moved past the row's real
    pre-rotation value, and comparing a wrong recomputation was a caught
    (not shipped) bug; see `docs/engineering-lessons.md`'s matching entry
    for the general form.
  - `RevokeCredential { id }` — removes the row outright. Idempotent on an
    already-absent id, mirroring every other catalog's "replayed
    proposal" discipline; an in-flight request already past
    `sigv4::verify` is unaffected.
  - `Metadata::credential(id) -> Option<&CredentialRow>` and `Metadata::
    verify_secret_candidates(id, now_secs) -> impl Iterator<Item =
    &SecretKey>` are the read API S-02 step 3's dispatch gate calls: the
    latter yields the current secret first, then the previous one too iff
    a grace window is still open at `now_secs` (strict `<`, so the exact
    boundary instant is already past), and yields nothing at all for an
    absent **or disabled** id (a disabled row reads exactly like an
    absent one, per ADR 0066 §3 step 2 — never let a caller distinguish
    the two). All three commands join the usual gating match sites: `is_
    relayable_command` (`animus-node/src/wire.rs`, schema-catalog class —
    the admin API proposing one of these may land on any node), the
    `mirror.rs` apply-derivation match (`Delete` for `RevokeCredential`,
    `Put` for the other two, keyed under a new `syskv::EntityKind::
    Credential`/`credential_key`), and `apply_put`/`apply_delete`'s own
    exhaustive per-kind matches. `crates/animus-control/tests/
    credentials_catalog.rs` is this catalog's own fixed-seed corpus
    (modelled on `backup_catalog.rs`: replicate, survive a leader kill, a
    real node restart, and seed-reproducibility) — no `ANIMUS_*_SEEDS`
    depth knob, mirroring `backup_catalog.rs`'s own fixed-single-seed
    shape rather than `animus-test`'s deeper corpora.

- **The restore catalog (ADR 0059 §7, Train 2): `BeginRestore`/
  `CompleteRestore`/`FailRestore`.** `Metadata::restores: BTreeMap<RestoreId,
  RestoreRow>` (`RestoreId = String`, an opaque internally-minted identity —
  never wire-visible, unlike `BackupId`, since `RestoreTableFromBackup` has
  no AWS-defined "restore id" to echo back). `BeginRestore` mints exactly
  **one** `Building` tablet over the whole ring for the target table
  (`Tablet::with_table` + `state = Building`, the identical monotonic-
  allocator-floor seatbelt `CreateTablet`/`BeginSplitInPlace` already
  enforce) plus the `Seeding` row — the ADR's own as-built decision to mint a *fresh*
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
  `SealPitrSegment`/`ExpirePitrSegments`.** A fifth consumer's own catalog,
  deliberately mirroring the backup/stream ones' conventions rather than
  inventing new shapes: `TableSchema.pitr: Option<PitrSpec>` (generation +
  enable wall-clock, the `SetTableStream`/`SetTableTtl` schema-catalog
  class) toggled by `UpdateContinuousBackups`, which mints a fresh
  `generation` from `Metadata::pitr_generation`'s own never-rewound
  per-table counter (reusing `EntityKind::Counter` with a
  `"pitr_gen:{table}"`-prefixed name rather than a new entity kind).
  `Metadata::pitr_segments: BTreeMap<(TabletId, u64), PitrSegmentRow>`
  mirrors `stream_shards` exactly (same tuple-key JSON codec workaround,
  same first-committer-wins-on-content `SealPitrSegment`/two-phase
  `ExpirePitrSegments` shape, same epoch-derivation-guard obligation on the
  caller) but is a fully separate collection — a table's stream and its
  PITR coverage never share a row or gate each other. **`Metadata::
  pitr_base_backups: BTreeSet<BackupId>`, not a `BackupRow` field**: tags a
  `BeginBackup`'d row as a PITR base snapshot via that same command's own
  `pitr_base: bool` flag, applied **atomically with the mint, in the same
  apply** (issue #593, fixed 2026-09-04) — `Metadata::apply`'s `BeginBackup`
  arm inserts into `pitr_base_backups` itself when the flag is set, so
  there is no committed state in which a PITR base snapshot exists
  untagged. **This was originally a separate side-tag command,
  `MetaCommand::MarkBackupPitrBase`, proposed only once `pitr_janitor::
  pitr_snapshot_loop` (`animusd`) observed its own `BeginBackup` row exist**
  — a real committed window (not merely theoretical) in which the row was
  an ordinary untagged `Creating` backup, closed by folding the tag into
  `BeginBackup` itself and deleting `MarkBackupPitrBase` outright (no
  self-healing sweep needed either, since there is nothing left for one to
  heal). See the ADR's 2026-09-04 as-built amendment for the full incident
  and `meta::tests::begin_backup_pitr_base_tags_atomically_with_the_mint`
  for the regression. PITR segments/generation floor deliberately
  **survive** `DropTableSchema`/`DropTableTablets`, the identical ADR 0024
  carve-out `backups` already gets — never gated on the source table's
  schema still existing, an explicit override of the streams drop-table
  retention-zero rule (ADR 0059 §9/§10).

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

- **The export catalog (ADR 0068, S-05 PR 1): `BeginExport`/
  `CompleteExport`/`FailExport`.** `Metadata::exports: BTreeMap<ExportId,
  ExportRow>` (`ExportId = String`, the export's own ARN — unlike
  `BackupId`, which is an opaque mint, `wire::export_arn` mints this one
  directly since AWS's own `ExportArn` shape is the natural identity and
  there's no name-collision hazard a table drop/recreate could exploit the
  way there would be for a name-keyed backup). Modelled directly on
  `BeginBackup`/`CompleteBackup`/`FailBackup`'s own shape (one-shot
  `Creating`/`InProgress`-style row, a terminal `Completed`/`Failed`), but
  **deliberately simpler**: there is no per-tablet progress catalog
  (`backup_tablet_progress`'s dual) and no `RecordExportTabletComplete`
  command, because an export is one job run once on whichever node
  received the wire request (`animusd::dynamo::run_export_job`, reusing
  `ctx.cp_scan` — the same primitive `Scan` uses, which already fans out
  across a table's tablets and tolerates a concurrent split transparently)
  rather than a distributed per-tablet-leader capture with a completion
  aggregator (see ADR 0068 §1 for the full reasoning and the trade-off this
  buys: no crash-resumability, a named residual). `BeginExport` records the
  export's own `s3_bucket`/`s3_prefix`/`format`/`export_type`/
  `export_time_ms`/`client_token` at apply time — every field a plain,
  already-agreed value the proposer supplies, no derivation from other
  `Metadata` state the way `BeginBackup`'s manifest-stub derivation needs
  (an export's own manifest is written by the job driver directly to the
  customer bucket, never mirrored into `Metadata`). `ExportFormat`
  (`DynamoDbJson`/`Ion` — only the former is actually reachable; the wire
  decoder rejects `ION` up front) and `ExportType` (`Full`/`Incremental` —
  only `Full` is reachable, `INCREMENTAL_EXPORT` rejected the same way) are
  both modeled even though only one variant of each is currently
  producible, so PR 2/3's own eventual `Incremental`/`Ion` support needs no
  catalog-shape change, only a wire-decoder relaxation. `CompleteExport`
  freezes `item_count`/`billed_size_bytes`/`export_manifest` (the
  customer-bucket key of `manifest-summary.json`, not a byte payload — this
  catalog never holds export content, only the pointer to where the job
  driver wrote it); `FailExport` mirrors `FailBackup`'s idempotent-on-
  identical-reason, rejects-a-terminal-contradiction shape exactly.
  `Metadata::export_by_client_token(table, token)` is the
  `ClientRequestToken` idempotency lookup `animusd::dynamo::create_export`
  uses to make a retried `ExportTableToPointInTime` call return the
  existing export rather than minting a second one — the identical
  linear-scan-over-a-small-map shape `export_by_client_token`'s own doc
  states plainly rather than indexing, since a table's export count is
  expected to stay small. `syskv::EntityKind::Export`/`export_key` follow
  the usual per-entity mirror conventions (`mirror.rs`'s `Put` arm for all
  three commands, no `Delete` arm needed — an export row is never removed,
  unlike a backup's janitor-driven reclaim); `is_relayable_command`
  (`animus-node/src/wire.rs`) allows all three — **all three**, not just
  `BeginExport`, since (unlike `CompleteBackup`/`FailBackup`, which the
  control-plane leader's own completion aggregator proposes locally) the
  export job runs on whichever node received the wire request, which may
  be any node in the cluster, control-plane leader or not. See
  `crates/animusd/CLAUDE.md`'s own ADR 0068 entry for the job driver, the
  customer-bucket store configuration/injection seam, and the object
  layout written.

- **The import catalog (ADR 0068 §6, S-05 PR 2): `BeginImport`/
  `CompleteImport`/`FailImport`.** `Metadata::imports: BTreeMap<ImportId,
  ImportRow>` (`ImportId = String`, the import's own ARN —
  `<TableArn-of-the-freshly-created-target>/import/<id>`,
  `wire::import_arn` — the identical "the id is its own ARN" shape
  `exports` already uses, since real DynamoDB's `ImportArn` is likewise a
  natural client-visible identity with no name-collision hazard to avoid).
  Modelled on **both** existing catalogs: `exports`' identity/no-delete
  shape (no `RecordImportTabletComplete`/aggregator — an import is one
  job, on whichever node hosts/leads the destination tablet, not a
  distributed per-tablet capture), and `restores`' target-provisioning
  shape (`BeginImport` mints exactly **one** fresh `Building` destination
  tablet directly, mirroring `BeginRestore`'s own mint — the mechanism
  that keeps a normal client write/read refused until the import driver
  activates it, reused rather than inventing a new tablet state; GSIs are
  resolved into `gsi_defs: Vec<IndexDef>` at `IndexStatus::Creating` but
  not declared on the schema until `CompleteImport`, the identical
  `RestoreRow::gsi_defs` backfill-race-avoidance reasoning). `BeginImport`
  additionally carries `base_schema: Box<TableSchema>` (boxed —
  `TableSchema` is by far this variant's largest field, and an unboxed
  copy would make the whole `MetaCommand` enum's in-memory size balloon to
  fit it, `clippy::large_enum_variant`'s own complaint) and `key_types`/
  `throughput`, recorded purely so `DescribeImport`'s echoed
  `TableCreationParameters` can render the original request's own
  declared shape — the wire edge (`animusd::dynamo::
  provision_import_target`) already committed the identical schema via an
  ordinary `CreateTableSchema` proposal *before* ever proposing
  `BeginImport`, so this apply arm performs no schema validation of its
  own, only the tablet mint + row insert.

  **`CompleteImport` activates the tablet** (`Building` → `Active`, epoch
  bumped — the identical `CompleteRestore` shape) **and freezes**
  `processed_item_count`/`imported_item_count`/`error_count`/
  `processed_size_bytes`. **`FailImport` deliberately leaves the tablet
  `Building` forever, exactly like `FailRestore`** — this apply arm never
  touches the tablet map on failure; the actual target-table cleanup (see
  below) is the import **driver**'s own separate, best-effort follow-up
  call through the ordinary `DropTableSchema`/`DropTableTablets` path, not
  something this pure state machine does atomically with the fail
  transition (dropping a table is itself a multi-step commit sequence).
  This is the one deliberate divergence from `restores`' own "leave it for
  manual `DeleteTable`" stance: real DynamoDB's own `ImportTable` rolls
  back a failed import's target table automatically, so `animusd::import`
  does too — but that rollback is driver-orchestrated, not catalog-time.
  `Metadata::import_by_client_token(token)` is the `ClientRequestToken`
  idempotency lookup — **not** scoped by table the way
  `export_by_client_token(table, token)` is, since a retried `ImportTable`
  call names the same target table by construction (a different name
  would already collide with the just-created target's own schema before
  the token lookup could matter), so a bare token match is enough.
  `syskv::EntityKind::Import`/`import_key` follow the usual per-entity
  mirror conventions (no `Delete` arm needed, the identical `Export`
  reasoning); `is_relayable_command` allows all three, the identical
  `BeginExport`/`CompleteExport`/`FailExport` reasoning (the import job
  may run on any node). See `crates/animusd/CLAUDE.md`'s own ADR 0068 PR 2
  entry for the driver, the item→row derivation, and the object-resolution
  mechanics.

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

  **This same "one slow thing on the shared driver task starves everyone"
  shape recurred on the *network* side (issue #661, S-07d)**: `drive`'s own
  outbound dispatch (`for (to, msg) in outs { env.send(to, bytes).await; }`)
  sequentially `.await`s one peer at a time; `ProdEnv::send_stream` used to
  run its `TcpStream::connect`+write inline with no timeout, so one
  silently-unreachable peer (a recreated pod's collapsed old network
  endpoint — exactly what a wiped-`EmptyDir` voter restart looks like)
  could ride the OS's own multi-minute TCP retry timeout and starve
  heartbeats to every *other* peer queued behind it in the same round —
  cluster-wide leaderlessness for 60+ seconds, invisible to `SimEnv` (no
  real sockets, no OS TCP timers). Fixed in `animus-env`'s `ProdEnv`
  (spawn + bounded `SEND_TIMEOUT`), not here — see
  `docs/engineering-lessons.md`'s matching entry and `wiped_voter_rejoin.rs`
  (this crate's own three `SimEnv` cells proving the *Raft protocol* side
  of a wiped-voter rejoin was never the bug: pre-vote's log-up-to-date
  check already makes a fresh, empty-log rejoiner safe).

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
  leader** — `role == Leader`, or `leader_id.is_some() && now < election_deadline`,
  or (**issue #930, 2026-09-19**) a role-gated `voted_for.is_some() && now <
  election_deadline` while still `Follower`/`Candidate` — so a briefly-stalled
  node can't inflate the term and disrupt a healthy leader. The `voted_for` arm
  closes a real gap: `leader_id` is set only by `handle_append_entries`/
  `InstallSnapshot`/`become_leader`, never by a granted real vote, so a voter
  that had just granted a real vote to the term's eventual winner had *no*
  lease at all until that winner's first `AppendEntries` arrived — a window in
  which a different, unprotected voter's own timeout could win a pre-vote (and
  then real) election, deposing the just-elected leader. **The role gate is the
  load-bearing part**: a role-less `voted_for.is_some()` check deadlocks the
  ordinary post-leader-crash re-election, because every survivor already holds
  a stale `voted_for` for the dead leader that a mere timeout never clears,
  while `start_pre_vote` keeps refreshing `election_deadline` on every retry
  round regardless — see ADR 0009's 2026-09-19 amendment and
  `docs/lessons/code-patterns/2026-09-19-a-per-term-commitment-must-not-share-
  a-deadline-a-different-retry-loop-refreshes.md`. Pre-vote messages **bypass**
  higher-term step-down — the sole
  exception is a *rejecting* `PreVoteResp` with a higher real term, which reverts
  the pre-candidate to a follower at that term. Tick semantics: a multi-node
  election now needs a `PreVoteResp` grant fed before the real
  `RequestVote`/`RequestVoteResp`; a single-node group still elects on one tick
  (self is a pre-vote majority). **`set_election_timeout` — the setter this
  used to describe as configuring the base for a node doing real disk I/O —
  was deleted (issue #313, 2026-09-01): it had zero call sites, no assembly
  layer to widen it was ever built, and the doc text describing that
  assembly layer was aspirational, not a real gap left for later.**
  `election_timeout()` (read-only) survives — `transfer_leadership` arms its
  deadline from it, and it now also backs the driver's own abort-observability
  log (see the "Leadership transfer" entry below).

  **`leader()`'s own hair-trigger clear on this timeout is exactly right for
  consensus and exactly wrong for an operational health/readiness probe
  (issue #595) — see `RaftCore::leader_within`'s own doc and ADR 0020's
  2026-09-04 amendment for the full account.** `RaftCore::
  last_leader_contact: Option<(NodeId, Nanos)>` is a second, purely
  observational field: set only at a genuine leader contact
  (`handle_append_entries`/`handle_install_snapshot`'s valid-leader-for-
  this-term path, `become_leader` recording itself) and cleared only on a
  real higher-term step-down — **never** by `start_pre_vote`/
  `start_election`'s own `leader_id = None`, which is this node's own local
  suspicion with no evidence the leader actually failed. `leader_within
  (now, max_age)` (and `RaftNode`/`ControlHandle`'s thin wrappers, the
  latter's `Remote` variant falling back to `leader()` since the wire
  carries no contact timestamp yet) reads it against a caller-chosen grace
  window; `animusd::admin::health` is the one production consumer, gated at
  `3 × election_timeout()`. **Must never be read by any election/pre-vote/
  safety/replication decision** — `leader()` itself is completely
  unchanged and still backs every one of those. Regression:
  `tests/leader_within_hysteresis.rs`.

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

  **`RaftCore::config_history()` (issue #944)**: a small bounded ring
  (`config_history`, capacity 64, oldest dropped, never rebuilt at
  recovery) of every distinct `(config, learners)` pair this core has
  adopted, appended **synchronously inside `apply_config`** — the one call
  every real transition funnels through, whether it's a leader's own
  `add_learner`/`promote_learner`/`change_membership` call (mutating this
  core directly, from whatever task calls it — for `animus-cp-data` that's
  the host reconciler's own tick, not the consensus/drive loop) or a
  follower's own per-entry `log_append` while draining a batched
  `AppendEntries`. This is deliberately **not** the same mechanism as
  `animus-cp-data`'s `VoterHistory` (issue #596), which samples
  `config()`/`learners()` from OUTSIDE, once per consensus-loop iteration —
  fine-grained enough for a voter-set change (bounded by this core's own
  per-message processing) but NOT for the learner set specifically, since a
  caller invoking two membership-changing methods back-to-back (or a
  follower draining several batched entries) can mutate this core more than
  once before an external sampler next gets scheduled, silently coalescing
  the transient learner phase away — see `crates/animus-cp-data/CLAUDE.md`'s
  matching entry and `crates/animusd/tests/learner_reconfigure.rs` for the
  flake this closed. Recording at the mutation site itself cannot miss it,
  regardless of which task or how much batching triggered it. A pure
  accessor — `config_history()` never blocks or mutates anything.

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

  **The abort above used to be cleared with no log, metric, or trace
  anywhere in the path (issue #313, fixed 2026-09-01) — now observable.**
  `RaftCore` itself stays pure/I/O-free (ADR 0003), so `node.rs`'s driver
  loop (`drive`) diffs `RaftCore::transfer_target()` across each
  `tick`/`handle` step, the same idiom `record_transition` already uses for
  election metrics: a `Some -> None` clear while still `Leader` is the
  deadline-timeout abort this section describes — logged
  (`tracing::warn!`, naming the target and the `election_timeout()` budget
  it had to fit in) and metered (`Metric::ControlTransferAborted`,
  `/metrics`'s `control_transfer_aborted`). A `Some -> None` clear while no
  longer `Leader` (this node itself stepped down to a higher term — the
  transfer likely succeeded, or was superseded by a different election) is
  logged at `info` with no metric — an operator chasing a stuck transfer
  cares about the abort case, not every routine completion.
  `RaftNode::transfer_target()`/`ControlHandle::transfer_target()` also
  surface the **live** armed target (or `None`) via `/admin/raft`'s
  `transfer_target` field, mirroring `voters`' "this replica's own view"
  diagnostic scope — `Remote` always answers `None` (no local `RaftCore`,
  and the wire carries no such signal). Regression:
  `tests/metrics.rs::aborted_leadership_transfer_is_observable` (arms a
  transfer, kills the target before it can ever see `TimeoutNow`, drives
  past the deadline, asserts the metric moved and the leader survived) +
  its seed-reproducibility sibling.

  **`RaftCore::set_election_timeout` — the setter, not the read-only
  `election_timeout()` accessor above — was deleted in the same change.**
  It had zero call sites (grep-verified) despite its own doc comment
  describing an "assembly layer" meant to widen it for a node doing real
  disk I/O; that assembly layer was never built, and — confirmed by
  reading `handle_timeout_now` — it was never the "missing `TimeoutNow`
  half" either: a received `TimeoutNow` already campaigns immediately via
  `start_election`, bypassing the election timer entirely, so this setter
  could not have made a transfer faster or more reliable even if wired up.
  Genuinely unused, aspirational API, not a documented gap worth leaving
  in limbo — see ADR 0009's matching 2026-09-01 amendment. If a real need
  to widen the timeout for a slow-disk node resurfaces, re-add the setter
  alongside its actual caller in the same change.

- **Snapshot transfer is chunked and O(chunk), not O(state).** A follower
  behind the compacted prefix is caught up via a chunked `InstallSnapshot`,
  all in the sync core (deterministic). `snapshot_chunk_for` **slices
  `snapshot_blob` by reference — it does NOT re-serialize per chunk**; a
  naive per-chunk serialize on a multi-MB metadata pins the loop past the
  election timeout (a self-sustaining election storm, invisible to
  `SimEnv`'s virtual clock). Blob management is keyed on
  `S::DRIVER_APPLIED`, **not** "in-core vs. data-plane" — since ADR 0038
  PR3's cutover, `Metadata::DRIVER_APPLIED = true` too (see `meta.rs`), so
  this plane's own blob is built **lazily on demand**, the identical shape
  to the data plane's:
  - **A non-`DRIVER_APPLIED` toy state machine** (`generic_state_machine.rs`
    only — nothing real in this workspace uses `DRIVER_APPLIED = false`
    anymore) keeps the blob **eagerly**, so the invariant `snapshot_index >
    0 ⟹ blob.is_some()` holds and a chunk is never a 0-byte ship
    (regression:
    `install_snapshot.rs::caught_up_control_node_reships_non_empty` proves
    this for `Metadata` specifically by hand-driving the eager-image
    contract as a stand-in for the real apply task — see that test file's
    own module doc for why: it decouples the chunk-mechanics tests from a
    real `StorageEngine`/`syskv_image` scan entirely).
  - **`DRIVER_APPLIED` (both planes — the data-plane KV and, since ADR 0038
    PR3, `Metadata`):** the image is the *engine* bytes (`syskv_image`/
    `install_syskv_image` here), built **lazily on demand** by the apply
    task (`meta_apply_and_compact`'s `image_needed`/`set_snapshot_blob`
    handling) only once a replication attempt actually raises
    `take_snapshot_needed`, and dropped whenever it would go stale/idle, so
    no whole-tablet/whole-metadata image is retained at rest (regression:
    `driver_applied_sm.rs::caught_up_node_reships_non_empty_snapshot`).

  Liveness teeth:
  `install_snapshot.rs::large_snapshot_ships_in_o_chunk_time_not_o_state` +
  `tests/prod_liveness.rs`.

  **A threshold-triggered compaction must defer while a peer's chunked
  transfer is genuinely in flight (issue #898, the control-plane instance of
  issues #532/#537's `animus-cp-data::COMPACT_DEFER_CEILING` finding).**
  `snapshot_upto` unconditionally invalidates every peer's in-flight
  transfer the moment the base moves again (required for correctness for a
  lazily-built `DRIVER_APPLIED` blob — see that method's own doc). Without a
  defer, ordinary sustained metadata churn — not even perpetual load, a
  handful of trailing commits landing while a fresh follower's very first
  transfer is still on the wire is enough — can re-cross
  `SNAPSHOT_THRESHOLD` before the transfer lands, restarting it from chunk 0
  forever. `meta_apply_and_compact`'s own doc claimed to mirror
  `animus-cp-data::apply_and_compact`'s shape/ordering "precisely," but
  never actually adopted this gate until issue #898 — confirmed live as the
  root cause of `prod_liveness.rs`'s intermittent CI stall (issue #898,
  recurrence of #741's shape one layer deeper: a follower stalling
  permanently mid-catch-up with `control term Δ0`, not the compaction-
  convergence poll #741 itself fixed). `node.rs`'s
  `SNAPSHOT_COMPACT_DEFER_CEILING` (`SNAPSHOT_THRESHOLD * 8`, mirroring
  `COMPACT_DEFER_CEILING`'s own derivation exactly) is the fix — a
  THRESHOLD-triggered (never `image_needed`-triggered — a peer is actively
  waiting on that exact image) compaction defers while
  `RaftCore::snapshot_transfer_in_flight()` is true, up to that ceiling,
  which still bounds the WAL even if a peer's transfer never completes.
  **`snapshot_transfer_in_flight()` itself had a second, compounding gap**:
  defined purely off `snapshot_offset` (populated only once a peer's FIRST
  ack is *processed*), it reported `false` for the whole round trip from
  "leader ships chunk 0" to "leader processes the first ack" — a window a
  slow/contended peer or link (`ProdEnv`'s real shape) can stretch
  arbitrarily far — so the defer never engaged during exactly the window it
  mattered most. Fixed by widening the accessor to also check
  `snapshot_chunk_sent` (set at SEND time, not ack time) — both maps are
  already cleared together at every existing invalidation/completion point,
  so this benefits `animus-cp-data` for free (same shared `RaftCore`).
  Regression: `tests/snapshot_compaction_race.rs` (a `SimEnv` test with an
  artificially slow leader<->follower link, since `SimEnv`'s own near-zero
  default latency lets a small synthetic transfer outrun even a bursty
  churn schedule and never exposes the race).

  **Fixing that defer unmasked a THIRD, previously-latent bug** — read this
  if you ever touch `handle_install_snapshot_resp`'s mid-transfer branch or
  the defer gate above again: a leader's per-peer `snapshot_offset`/
  `snapshot_chunk_sent` bookkeeping is never reset when that peer's
  *process* restarts (same `NodeId`, a brand-new empty `RaftCore`), and a
  restarted follower reporting `next_offset == 0` forever can never be
  reconciled against a leader that keeps resending from its own stale,
  now-unreachable non-zero offset (`handle_install_snapshot`'s `fresh &&
  offset == 0` reassembly gate can't bootstrap from a non-zero offset onto
  an empty buffer). This was **masked** before the defer fix: ordinary
  threshold-triggered recompaction fired often enough, independent of any
  peer's own transfer state, to incidentally wipe this stale bookkeeping
  clean before a restarted peer's next request ever hit it — making
  compaction fire less often removed that accidental safety net.
  `tests/control_corpus.rs`'s pre-existing
  `chunked_snapshot_receiver_stop_restart_3` caught this immediately (a
  real `StopRestart` of the receiver mid-transfer) once the defer fix
  landed. Fixed by `snapshot_offset_regressions: BTreeMap<NodeId, u32>` +
  `SNAPSHOT_OFFSET_REGRESSION_REBASE`: a peer's mid-transfer ack reporting
  an offset below the currently tracked one increments a per-peer counter
  instead of being silently absorbed by the existing monotonic `max` guard;
  once that counter crosses the (small, bounded) threshold with no
  intervening forward progress, the leader REBASES down to the peer's own
  reported truth rather than trusting its own stale record — distinguishing
  "a stale, reordered ack for the ongoing transfer" (self-heals within a
  round trip, seen once or twice — tolerate it) from "the peer's buffer
  genuinely reset" (persists indefinitely — must be honored) by consecutive
  count, not a one-shot check. See `docs/lessons/testing/
  2026-09-14-control-snapshot-catch-up-stall.md` for the full incident and
  the generalizable lesson (a fix that makes a background process fire
  *less* often can remove an accidental cleanup side effect a different
  code path was quietly relying on — re-run the FULL existing suite, not
  just the new regression, before calling a fix done).

  **Two more gaps in the same mechanism, found stress-running
  `prod_liveness.rs` under real `ProdEnv` contention (not caught by any
  `SimEnv` test)**:

  - **Fourth: the leader-only `snapshot_offset`/`snapshot_offset_
    regressions`/`snapshot_chunk_sent` bookkeeping outlived losing
    leadership.** `RaftCore::handle`'s generic higher-term step-down already
    clears a stale `transfer_target` on the exact same reasoning ("a future
    `is_leader`-independent inspection must never report a transfer in
    flight for a node that isn't leading") but never extended that to these
    three maps. Since `snapshot_transfer_in_flight()` has no `role ==
    Leader` guard of its own, a sender-side entry surviving an ordinary
    leadership handoff under real contention (not a bug by itself) made
    this node's *own* local compaction defer forever afterward, on behalf
    of a transfer with no sender left. Fixed by clearing all three at that
    step-down site too.
  - **Fifth: even with the fourth fix, a peer that never acks at all
    (down, partitioned, or configured but never started — exactly what
    `large_metadata_catch_up_stays_live`'s dark node 2 is) can still wedge
    compaction forever**, since `SNAPSHOT_COMPACT_DEFER_CEILING`'s escape
    hatch is sized in `behind` and simply never grows once a burst of
    writes stops. **Three designs were tried at this one gap; the first
    two were built, validated against `prod_liveness.rs`, and only later
    found wrong against a DIFFERENT test in the plane that shares this
    same `RaftCore`** — see the lessons doc for the full chronology:
    1. A resend-COUNT proxy for elapsed time — **rejected**: it scales
       with `heartbeat_interval`, not with how slow a genuinely live peer
       might legitimately be, and `snapshot_compaction_race.rs`'s own
       slow-link test needed ~40 un-acked heartbeat resends before a real
       peer's first-ever ack, landing right at a plausible count
       threshold and reopening the original race for a transfer that was
       actually fine.
    2. A flat "time since the defer streak started" ceiling, gated on
       `RaftCore::peer_last_contact` going stale as a faster secondary
       escape — **rejected**: `become_leader` optimistically seeds EVERY
       peer's `last_contact` to the moment leadership begins (see that
       method's own doc), so a peer that never started at all and a peer
       that stopped responding mid-transfer are both "stale relative to
       leadership start" by that field alone — indistinguishable. Worse,
       a flat "time since streak started" (rather than "idle") ceiling
       conflates "this transfer has run a while" (fine — a large
       multi-chunk snapshot legitimately takes many round trips) with
       "this transfer has made no progress in a while" (the actual
       question); `animus-cp-data/tests/hlc_differential_skew.rs`'s own
       crashed-peer scenario needed the answer within a few virtual
       seconds, far tighter than any margin safe for a large real
       transfer's TOTAL duration — a single constant cannot honestly
       answer both.
    3. **Shipped**: an **idle-progress-gated** ceiling —
       `SNAPSHOT_COMPACT_DEFER_IDLE_CEILING` (2s) — that bounds idle time
       since the LAST observed forward progress, never total transfer
       duration. `RaftCore::snapshot_transfer_peers()` (the set of peers
       with an outstanding transfer — reinstated after the rejected
       peer-recency design above, repurposed) plus the pre-existing
       `RaftCore::snapshot_chunk_advances(peer)` (a genuine
       forward-progress counter — already the exact metric
       `snapshot_resend_bound.rs` uses for the identical reason) give the
       driver a real progress signal: `meta_apply_and_compact` sums
       `snapshot_chunk_advances` across every outstanding peer each pass
       and resets its own `compact_defer_since: Option<Nanos>` to `now`
       every time that sum changes, so the idle clock only ever measures
       time since the last genuine advance. `RaftCore::
       snapshot_transfer_in_flight()` needed no change for any of the
       three designs — it stays the pure, `now`-unaware fact its own doc
       says it should be; the time-awareness belongs entirely at the
       driver layer that already has `env.now()`.
  - **Sixth, found only by running `cargo test -p animus-cp-data` in
    full (not `--lib`, which skips every `tests/*.rs` integration
    binary) — the shared `RaftCore` widening from the SECOND fix above
    (issue #898's own `snapshot_transfer_in_flight` gap-closer) regressed
    `animus-cp-data`'s own, PRE-EXISTING `hlc_differential_skew.rs`.**
    A crashed, partitioned replica's phantom, never-to-be-acked
    `InstallSnapshot` chunk held `snapshot_transfer_in_flight()` true on
    the SENDER indefinitely, wedging that replica's own compaction of an
    unrelated failed-CAS burst — the exact non-row-writing-entry
    durable-watermark advance that test exists to prove (ADR 0018 §2's
    issue #804 amendment). Fixed by mirroring the SAME idle-progress-gated
    ceiling (design 3 above) into `animus-cp-data`'s own
    `apply_and_compact`/`apply_loop` — see that crate's own
    `COMPACT_DEFER_IDLE_CEILING` and its `CLAUDE.md`'s matching entry.
    **The standing lesson this cost a full validation cycle to learn**:
    any change to `animus-control`'s `RaftCore` needs `cargo test -p
    animus-cp-data` run in FULL (integration tests included), never just
    `--lib` — the two planes share this exact state machine, and `--lib`
    only runs `lib.rs`'s own in-crate `#[cfg(test)]` module, silently
    skipping every one of that crate's ~55 `tests/*.rs` binaries. Added to
    this crate's own Tests section above — see it for the exact command.
  - None of the fourth/fifth/sixth gaps has a dedicated `SimEnv`
    regression (all are real-thread-contention/real-leadership-churn/
    cross-plane shapes); validated instead by 10-20x stress reps of
    `prod_liveness.rs` per round (0 failures with the shipped design,
    versus 20-40% with only the first three fixes) plus the full
    `animus-cp-data`/`animus-control` suites for the sixth. See the
    lessons doc above for the fuller account and the generalized lesson:
    any new bookkeeping feeding a defer/backoff gate needs an explicit
    answer to "what retires this, on *every* path that can make it stale,
    not just the happy one" — "how long is too long" is an `env.now()`
    question that belongs in the driver, never a retry count or a
    same-leadership-stint contact timestamp standing in for one — and it
    must measure idle time since the last real progress, never total
    elapsed time, or it can't tell "large and slow" from "stuck" either.

  **`state_machine_behind` + `AppendEntriesResp::needs_snapshot` (issue
  #554, ADR 0009's 2026-09-02 addendum): a follower-to-leader "I need a
  fresh `InstallSnapshot` regardless of `next_index`" signal, for a replica
  whose log tail matches its leader's but whose OWN state machine is behind
  its own log's compacted start** (an engine destroyed and reopened fresh
  behind an already-compacted log — `animus-cp-data`'s reconciler engine-
  loss recovery is the live trigger; see that crate's CLAUDE.md and ADR
  0017's matching addendum for the full mechanism this core-level piece
  supports). `RaftCore::state_machine_behind` is set only by a
  `DRIVER_APPLIED` plane's own driver — **the control plane never calls the
  setter, so it stays permanently `false` here**, making
  `AppendEntriesResp::needs_snapshot`, the `start_pre_vote`/`start_election`
  campaign gate, and `handle_install_snapshot`'s state-machine-behind
  fallthrough all dead code paths for `Metadata`. Not because the control
  plane is structurally immune to the same *class* of gap (nothing today
  destroys-and-reopens a control node's system-keyspace engine the way the
  data-plane reconciler does for a tablet — `Metadata`'s own apply task
  already seeds `engine_applied` from the engine's own durable watermark
  key, `node.rs`'s `meta_apply_loop`, not `core.last_applied()` — but that
  alone means no *active* trigger exists yet, not that one could never
  exist) — flagged, not built, since a detection/request path with no live
  caller would be speculative, untestable machinery.
  `snapshot_served_through: BTreeMap<NodeId, u64>` (leader-only, volatile
  like `next_index`/`match_index`) is the companion piece that stops a
  leader from restarting a fresh transfer on every one of a behind peer's
  still-`needs_snapshot: true` acks before the peer finishes digesting the
  last one — a real livelock without it (see ADR 0009's addendum for the
  full incident). Regression: `animus-cp-data/tests/
  engine_wipe_needs_snapshot.rs` is the only live exerciser; this crate's
  own suite stays green unchanged, by construction, since nothing here ever
  sets the flag.

  **`replicate_to`'s `AppendEntries` batch is capped, not unbounded
  (issues #532/#537, ADR 0009's 2026-09-01 amendment).**
  `MAX_APPEND_ENTRIES_BATCH` (512, derivation in that constant's own doc)
  bounds how many entries a single `AppendEntries` to a lagging peer may
  carry — without it, `replicate_to` shipped the ENTIRE outstanding tail
  every call, and `replicate_now`'s wake-on-propose (`animus-cp-data`'s own
  driver, re-invoked on every single write) resent that unbounded, growing
  tail on every propose, real unbounded CPU cost regardless of whether the
  peer had acked the last one. `RaftCore::snapshot_transfer_in_flight()` is
  the companion accessor a `DRIVER_APPLIED` driver's own compaction gate
  consults to avoid a second, independent pathology once a peer falls back
  to the chunked snapshot path — see `animus-cp-data/CLAUDE.md`'s
  `COMPACT_DEFER_CEILING` entry for that half; this core only supplies the
  fact, the policy lives entirely in the driver. See the ADR amendment for
  the full mechanism and honest residual.

  **A resend of an already-outstanding chunk is bounded per caller, not
  unconditional (issues #532/#537, ADR 0009's THIRD 2026-09-01
  amendment — the residual the batch-cap/compaction-defer fixes above left
  open).** `snapshot_chunk_for` used to unconditionally re-slice and
  re-send whatever chunk was still outstanding for a peer on every call,
  from every caller — under a sustained proposer this meant the SAME
  unacked chunk shipped again and again at write rate (confirmed live:
  96,451 sends for 196 real offset transitions), and the ack-handler's own
  equally-unconditional resend turned every response the flood provoked,
  including a no-progress duplicate ack, into another send — a
  self-sustaining loop bounded only by round-trip time. A `SnapshotResend`
  gate now bounds a resend of an UNCHANGED offset per call site: `Capped(0)`
  for `replicate_now` (wake-on-propose — fires on every write, so it may
  never resend without new progress), `Capped(SNAPSHOT_ACK_RESEND_CAP)` for
  `handle_install_snapshot_resp`'s own ack-driven resend (a small, nonzero
  bound — see that constant's own doc for why neither `0` nor unbounded
  works there), and `Always` everywhere else (heartbeat tick, a peer's own
  `AppendEntries` response, `WakeRequest`, a fresh leadership term — each
  already bounded by something other than write rate). A genuinely NEW
  offset (real ack progress) always ships immediately at every call site,
  never held back. A companion fix closes an independent defect found
  building this one: `handle_install_snapshot_resp`'s tracked offset is now
  updated via `max`, not a bare `insert` — under the pre-fix flood's own
  overlapping in-flight sends, acks could reach the leader out of
  real-progress order and regress the tracked offset backward (confirmed:
  217 such regressions in one run), which the flood's own sheer volume had
  been silently absorbing. See the ADR's third amendment for the full
  account, including why two narrower prototypes (skip wake-on-propose
  entirely; throttle it by propose count) were tried and rejected first —
  `replicate_now`'s wake is a single coalesced `AtomicBool`
  (`ProposeSignal`), so under `learner_catchup_under_load.rs`'s own
  synchronous-burst workload it was never the flood's real amplifier in
  that test to begin with; the ack-handler's cascade was.
  `animus-cp-data/tests/snapshot_resend_bound.rs` is the dedicated
  message-VOLUME regression (distinct from the pre-existing
  convergence-timing one) — see that test's own module doc for why a
  `Metric`-based measurement was necessary at all (`SimEnv` charges a
  resend flood no virtual time and little real time, so a timing-only
  test cannot see it) and why its denominator is an exact in-core counter
  (`RaftCore::snapshot_chunk_advances`) rather than externally polling
  `snapshot_offset`.

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
  `Metadata::reconcile` (repair: `animus_placement::replan_repair` over
  `Active` members, emits a `CasTabletReplicas` only for policy-violating
  tablets — **issue #957**: `replan_repair`, not plain `replan`, so a
  policy RF the current candidate pool can't fully satisfy still gets
  grown as far as it genuinely can be, e.g. RF 3 on a 2-node cluster still
  repairs a 1-replica tablet up to 2, rather than refusing to propose
  anything until a 3rd candidate ever appears — see `animus-placement/
  CLAUDE.md`'s entry for the growth-only/never-shrinks contract) and its
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
  rebuilt from observed writes. **`TableSchema.throughput: Option<
  ProvisionedThroughput>` (ADR 0065 §5(b), W-08 step 4)** — a table's
  provisioned read/write capacity units, mutated only through
  `MetaCommand::SetTableThroughput { table, spec }`, modeled directly on
  `SetTableTtl`'s own apply semantics (`ProvisionedThroughput` mints no
  identity label either): `spec: Some(new)` equal to the current value is a
  `NoOp`; a genuinely different value (including a live in-place unit
  change) is `Applied`; `spec: None` reverts to `PAY_PER_REQUEST`, itself a
  `NoOp` when already unset. `Metadata::table_throughput(table)` is the read
  accessor (`animusd`'s per-tablet throttle bucket resolves a table's
  effective limits from this, falling back to the cluster-wide default when
  `None`). On `is_relayable_command`'s allowlist (`animus-node/src/
  wire.rs`) and `mirror.rs`'s schema-catalog-class mirror bucket beside
  `SetTableTtl`/`TagResource` — a follower-connected `CreateTable`/
  `UpdateTable` carrying `BillingMode`/`ProvisionedThroughput` must reach
  the control leader like every other DDL mutation. `animus-dynamo`'s wire
  layer decodes `CreateTable`/`UpdateTable`'s `BillingMode`/
  `ProvisionedThroughput` into this type directly (re-exported, no
  wire-local duplicate) — see that crate's own `CLAUDE.md` entry for the
  decode/response-shape details, and `animusd/CLAUDE.md`'s "Per-table
  throttling" entry for the full end-to-end design (both configuration
  layers, enforcement, and tests).

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
heartbeats). **Any change to `RaftCore` (`raft.rs`) or its driver (`node.rs`)
must also gate on `cargo test -p animus-cp-data` run in FULL — never
`--lib`** (issue #898's sixth gap, above): the two planes share this exact
state machine, and `--lib` only runs `lib.rs`'s own in-crate `#[cfg(test)]`
module, silently skipping every one of that crate's `tests/*.rs` integration
binaries (`hlc_differential_skew.rs` among them — the one that caught this
gap after `--lib` alone reported green). **A `RaftCore`/boot-path change also
gates on `cargo test -p animus-control --features prod-heavy` too**: this
crate's `prod_liveness.rs` and `control_membership_prod.rs` are `[[test]]`
targets with `required-features = ["prod-heavy"]`, so a plain `cargo test -p
animus-control` silently skips both real-thread `ProdEnv` liveness binaries
(issue #667's third amendment found this the hard way — a full plain run
reported green while CI's `--all-features` run caught a real, deterministic
failure the skipped binary alone exercised). One binary per behavior; the file names describe them
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

**A second test-design gotcha (issue #741)**: any `prod_liveness.rs`/
`tests/*.rs` poll of `RaftNode::snapshot_index()` (or any other property
gated on the ADR 0038 apply task's own progress) must be **progress-gated,
not deadline-gated** — see `docs/engineering-lessons.md`'s DRIVER_APPLIED
entries for the general rule and this crate's own issue #741 entry for the
instance that caught it here. `snapshot_index()` only advances when the
apply task (`meta_apply_and_compact`) compacts, gated on **its own**
`engine_applied_index` crossing `SNAPSHOT_THRESHOLD` — a task the
consensus loop deliberately never waits on, so its forward progress has no
contention-independent latency bound. `large_metadata_catch_up_stays_live`
flaked on CI (`prod-liveness-scattered`) with one replica's
`engine_applied_index` frozen for a flat 30s deadline while its sibling's
inched forward — poll `engine_applied_index()` for forward progress
(fail only once it genuinely stalls for a generous idle window with the
target still unmet, plus a much larger overall backstop as a livelock
guard), never a flat wall-clock deadline, the same shape
`animusd/tests/support::poll_until_or_stalled` already uses for this
exact class of property one crate over.

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
back. **Issue #684 layer**: `AllocatorRace` also drives a THIRD phase (the
winner repeatedly proposes `CutoverSplit` to completion, rather than
leaving the parent `Splitting` forever) plus one dedicated extra racer
(`allocator_race_table_b_client`) that races a `CreateTablet` for a wholly
SEPARATE table against the same allocator counter for the whole run —
reproducing #684's own production interleaving under fault injection.
`check_allocator_injectivity` asserts directly over `Shared::
confirmed_tablet_tables` (every `(id, table)` a racer's own confirm loop
durably observed win) that an id, once confirmed holding one table, is
never later found holding a different one in the final converged state —
independent of (layered on top of, never instead of) the sample-based
fingerprint check above; (5) **`RegisterNode` CAS integrity** (PR②, safety, unconditional) —
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
(`SchemaRace`, PR①; `AllocatorRace`'s split phase reuses the same
discipline, PR②). (b) The inverse trap for a workload whose racing
proposals are content-**identical** except for the field being raced
(`AllocatorRace`'s `CreateTablet` phase — same shared table/range/replicas
for every racer, only the candidate id differs): a raw "confirmed at most
once" assertion over that field is checking a *stronger, false* property,
since several racers legitimately and correctly agreeing "the tablet that
landed carries my own candidate id" is expected, not a bug — only a
content/fingerprint comparison (`sample_tablets`), never an occurrence
count, states injectivity correctly. (b′, copy-split deletion stack layer
1) `AllocatorRace`'s split phase was ported from `BeginSplit` to
`BeginSplitInPlace`, which mints no tablet-map row for its `left`/`right`
ids at all (this workload never proposes the `CutoverSplit` that would);
`sample_tablets` was extended to also fingerprint a `Splitting` parent's
own `inplace_split.children` (deriving the same `(table, range.start,
range.end)` shape `CutoverSplit` would eventually assign, from the
parent's own untouched range and the intent's split key) so those ids
still get injectivity teeth, and `check_durability_meta`'s "confirmed id
must survive into the final state" check was widened the same way — a
confirmed split id now counts as present via EITHER a materialized row or
a still-recorded parent intent naming it. (c) A fault-finding confirmed in one
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
seed's exact timing (`wait_for_snapshot_transfer_in_flight`). (f) issue
#684: teaching `AllocatorRace`'s winner to actually finish via
`CutoverSplit` (rather than stopping once `BeginSplitInPlace` won) broke
`check_durability_meta`'s own confirmed-tablet-id check — Phase 1's
shared-parent id, previously permanent for the life of every scenario
(nothing ever cut it over), now legitimately gets RETIRED by a completed
cutover. The fix generalizes "still present" to also accept the id
appearing as some child's `parent` in the final `Metadata::split_lineage`
— a retired-by-a-split-it-itself-intended id is not a lost effect. General
form: widening a workload to exercise a command that legitimately
*removes* state (`CutoverSplit` removing the parent row) can turn an
existing "confirmed effect must survive" check into a false positive if
that check only ever checked raw presence — the fix is teaching the check
about the specific state transition that's expected to remove it, not
loosening the check generally.

**Scope as of PR③ (final — the stack is complete)**: `Workload::SchemaRace`
(2-3 concurrent proposers racing `CreateTableSchema`, same-table or
distinct-name), `Workload::PlainChurn` (non-contending `UpsertMember`, the
non-vacuity floor), `Workload::AllocatorRace` (several proposers racing
`CreateTablet`/`BeginSplitInPlace` against ONE shared table/tablet, hammering
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
