//! The control-plane metadata state machine: cluster membership and the tablet
//! map, mutated by replicated [`MetaCommand`]s.
//!
//! Tablet placement mutations are **compare-and-swap** transactions keyed by the
//! tablet's epoch (ADR 0002): a `CasTabletReplicas` applies only if the tablet's
//! current epoch equals the expected one, and on success bumps the epoch. Apply
//! is a deterministic pure function of the command and current state, so every
//! Raft replica computes the identical accept/reject decision.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use animus_placement::{Candidate, PlacementPolicy, rebalance_step, replan, select_replicas};
use animus_tablet::{
    Epoch, InPlaceSplitIntent, KeyRange, SplitChild, TOKEN_BYTES, Tablet, TabletId, TabletState,
};
use serde::{Deserialize, Serialize};

use crate::schema::{
    IndexDef, IndexStatus, PitrSpec, ProvisionedThroughput, SchemaCatalog, StreamSpec,
    StreamViewType, TableName, TableSchema, TtlSpec,
};

/// The default a [`StreamShardRow`]/[`MetaCommand::SealStreamShard`]'s
/// `view_type` field decodes to when loading a snapshot encoded before this
/// field existed (round-3 sealer PR predates it) — never reached by any row
/// this PR's own sealer writes (which always fills it from the table's
/// current `StreamSpec.view_type` at seal time), so any placeholder is
/// equally arbitrary; `NewAndOldImages` is chosen because it is the
/// least-surprising fallback (over-delivers images rather than silently
/// dropping one a real reader might have wanted).
fn default_stream_view_type() -> StreamViewType {
    StreamViewType::NewAndOldImages
}

/// Deliberate duplicate of `animus_dynamo::index::INDEX_TABLE_SEPARATOR` —
/// this crate cannot depend on `animus-dynamo` (dependency direction: see
/// `animus-tablet`'s `CLAUDE.md`, which documents the identical precedent for
/// duplicating `escape` rather than adding a dependency edge). Must match
/// byte-for-byte; used by `CreateTableSchema`'s apply-time rejection below.
const RESERVED_TABLE_NAME_SEPARATOR: char = '$';

/// A deliberate duplicate of `animus_cp_data::segment::shard_id`'s
/// `ShardId` format (`shardId-<tablet>-<epoch>`, ADR 0042 §2) — this crate
/// cannot depend on `animus-cp-data` (dependency direction: that crate
/// depends on *this* one), mirroring the identical `RESERVED_TABLE_NAME_SEPARATOR`
/// precedent just above. Must match byte-for-byte; used only by
/// [`Metadata::stream_shard_parent_id`] to render a `ParentShardId` string
/// without a second source of truth for the shard-id shape living in this
/// crate's own catalog rows.
fn shard_id_string(tablet: TabletId, epoch: u64) -> String {
    format!("shardId-{}-{epoch}", tablet.0)
}

/// Lifecycle status of a cluster member.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeStatus {
    /// Bootstrapping, not yet serving.
    Joining,
    /// Live and serving.
    Active,
    /// Draining ahead of removal.
    Leaving,
    /// Believed dead.
    Down,
}

/// A cluster member: its topology labels (ADR 0005) and current status.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    /// Topology labels, e.g. `region=eu-west`.
    pub labels: BTreeMap<String, String>,
    /// Current lifecycle status.
    pub status: NodeStatus,
    /// Whether this member has **ever** reached [`Active`](NodeStatus::Active)
    /// (ADR 0040 PR6) — sticky once set, never cleared back to `false` by any
    /// later transition. This is what distinguishes a member that "never
    /// showed up" (a claim whose node crashed mid-join or lost a registration
    /// race, eligible for the orphan-member sweep) from one that "was alive,
    /// currently down" (repair/decommission territory — never sweepable).
    ///
    /// Deliberately **not** scoped to "the detector's own `Down`→`Active`
    /// promotion" alone: a bootstrap-declared member starts `Active`
    /// directly (ADR 0030 §3's phantom hardening), never passing through a
    /// `Down`→`Active` transition at all, so gating this narrowly on that one
    /// transition would leave a founding cluster member's `has_activated`
    /// permanently `false` — and, the moment it later legitimately crashes
    /// and is marked `Down`, indistinguishable from a genuine never-activated
    /// orphan. `Metadata::apply`'s `UpsertMember` arm instead sets this
    /// whenever the command's own desired status is `Active`, regardless of
    /// the caller (the ADR 0012 detector's promotion, or `bootstrap`'s direct
    /// `Active` insert) — a member is "has activated" the moment it is ever
    /// recorded `Active`, by any path, which is exactly the safety property
    /// the sweep needs. `#[serde(default)]` is needed only so this compiles
    /// against every existing in-repo struct literal / WAL-format unit test
    /// unchanged — per this repo's standing "no live deployments, fresh
    /// clusters only" rule (`docs/engineering-lessons.md`), a genuine
    /// pre-ADR-0040-PR6 snapshot loading as `false` for every member
    /// (indistinguishable, at that instant, from a never-activated orphan)
    /// is explicitly **not** a supported upgrade path — no migration is
    /// attempted or required.
    #[serde(default)]
    pub has_activated: bool,
}

/// A member's full address book (ADR 0032 PR1): every listen address a node
/// exposes, replicated so any node can forward/relay to any other regardless
/// of when it joined. Keyed by the member's node id in
/// [`Metadata::node_addrs`] — the same id space as [`Metadata::cp_member_addrs`]
/// (which this supersedes for the client/admin axes; `cp_member_addrs` is kept
/// for WAL back-compat and the internal peer book).
///
/// **ADR 0040 PR1 (one identity per node)**: `raftkv` and `control` — two
/// separate listen addresses for a node's two `ProdEnv` roles — merge into
/// one `internal` field, since a node now runs exactly one internal env (the
/// control Raft on stream 0, every hosted tablet's Raft group on its own
/// stream ≥ 1, ADR 0026). This is a clean break — no wire/WAL back-compat
/// with a pre-ADR-0040 deployment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeAddrs {
    /// This node's one internal `ProdEnv` listen address (ADR 0040 PR1):
    /// carries the control-plane Raft and every per-tablet Raft group this
    /// node hosts. Always populated at self-registration, for every role
    /// (control-only, data-only, or combined) — unlike the pre-ADR-0040
    /// `control`/`raftkv` pair, there is no role for which this is legitimately
    /// absent, so a runtime-added control voter (`admin_add_control_member`)
    /// needs no separate address-replication path either: its `internal`
    /// address is either already registered (an existing node being promoted
    /// to a voter) or supplied directly by the admin action.
    ///
    /// **Naming note (ADR 0047)**: `internal` is the raw `ProdEnv`/Raft-wire
    /// transport, not the same thing as [`intra`](Self::intra) below — one
    /// letter-swap away and a recurring source of confusion. `intra` is the
    /// `ClientRequest`/`ClientResponse`-framed node-to-node RPC address
    /// (same framing as `client`, a disjoint allowed-variant set).
    pub internal: String,
    /// The plain client-protocol listen address.
    pub client: String,
    /// This node's intra-cluster RPC listen address (ADR 0047) — where
    /// every internal-only `ClientRequest` variant is actually served.
    /// Always populated at self-registration, for every role, mirroring
    /// `internal`/`client`/`admin` above.
    pub intra: String,
    /// The admin/debug HTTP listen address.
    pub admin: String,
    /// This node's deployment role (ADR 0035): `"control"` / `"data"` /
    /// `"combined"`, the same vocabulary `animusd`'s `/admin/config` already
    /// derives from its own role. A node only ever *authoritatively* knows
    /// its own role, so this is filled in and proposed once, at
    /// self-registration time (mirroring `internal`/`client`/`admin`
    /// themselves) — never inferred by a reader from anything else. Plain
    /// `String`, not an `animusd`-side enum: `animus-control` has no
    /// dependency on `animusd` (the dependency runs the other way), and
    /// every other field here is already an opaque wire-format string this
    /// crate never interprets.
    #[serde(default = "default_node_role")]
    pub role: String,
}

fn default_node_role() -> String {
    "combined".to_string()
}

/// The replicated control-plane state: membership and the (single-table) tablet
/// map.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    /// Cluster membership keyed by node id.
    pub members: BTreeMap<NodeId, Member>,
    /// The tablet map keyed by tablet id.
    pub tablets: BTreeMap<TabletId, Tablet>,
    /// Per-tablet placement policies (ADR 0005). A tablet with a policy here is
    /// reconciled automatically by the leader; tablets without one are left as
    /// placed. Keyed by tablet id, so the in-node reconciler can recompute the
    /// desired replica set deterministically on every replica.
    pub policies: BTreeMap<TabletId, PlacementPolicy>,
    /// The replicated table-schema catalog (ADR 0013): which tables exist and
    /// their key structure + typed columns, shared by both wire adapters. Mutated
    /// only through [`MetaCommand::CreateTableSchema`] /
    /// [`MetaCommand::DropTableSchema`], so it is Raft-replicated and recovered
    /// from the WAL/snapshot like every other metadata field. The adapters
    /// consume it (a deliberate follow-up) so a `CreateTable`/`CREATE TABLE`
    /// survives restart and is agreed cluster-wide.
    pub schemas: SchemaCatalog,
    /// Replicated **CP group member addresses** (Phase 2): each CP per-tablet Raft
    /// member id → the listen address of its hosting node's `raftkv` role, as an
    /// opaque string (the control plane never dials it). Mutated only through
    /// [`MetaCommand::RegisterCpAddr`]. A member created at runtime — a tablet
    /// split's co-resident sibling, or a newly-joined data node — registers its
    /// address here so every node's peer-sync loop can install it into its env peer
    /// book and the new group's internal Raft traffic routes. `#[serde(default)]`
    /// keeps pre-Phase-2 snapshots loading (empty map).
    #[serde(default)]
    pub cp_member_addrs: BTreeMap<NodeId, String>,
    /// Which **tablet** each registered CP member id belongs to (ADR 0024 GC):
    /// recorded when [`MetaCommand::RegisterCpAddr`] carries its `tablet`, and the
    /// key the address GC prunes on — when a tablet leaves the map (drop-table,
    /// merge), every member-addr entry recorded against it is removed from both
    /// maps, closing the designed leak. Keyed on **current absence** (mirroring
    /// the file GC's discipline), so a replayed historical map state cannot
    /// permanently resurrect an entry: the replayed removal prunes it again. A
    /// member registered without a tablet (legacy) is never pruned.
    /// `#[serde(default)]` keeps older snapshots loading (empty map).
    #[serde(default)]
    pub cp_member_tablets: BTreeMap<NodeId, TabletId>,
    /// The next tablet id to hand out — a **monotonic** allocator (ADR 0023): bumped
    /// past every tablet created (via `CreateTablet` or `SplitTablet`) so two
    /// concurrent `CreateTable`s can't derive the same id, and a dropped id is never
    /// reused (split member ids derive from the tablet id, so reuse could alias a
    /// stale sibling). `#[serde(default)]` keeps pre-counter snapshots loading as `0`;
    /// [`Metadata::next_free_tablet_id`] folds in the highest existing id so a loaded
    /// snapshot still allocates above its tablets.
    #[serde(default)]
    pub next_tablet_id: u64,
    /// Replicated **node address book** (ADR 0032 PR1): every member's full
    /// address set (raftkv/client/admin), keyed by its raftkv id. Mutated by
    /// [`MetaCommand::RegisterNode`] (the sole claim path, ADR 0040 Decision
    /// C) at first registration and [`MetaCommand::RegisterNodeAddrs`]
    /// (update-only) thereafter. Unlike [`Metadata::cp_member_addrs`]
    /// (internal raftkv addresses only, including transient split-sibling/CP-group
    /// member ids that are never full cluster members), this is populated once per
    /// **node** at startup, closing the ADR 0030 gap where a pre-growth node's
    /// `client_route`/admin peer list was a static, process-start-only snapshot
    /// that could never learn about a node grown in afterward. `#[serde(default)]`
    /// keeps pre-ADR-0032 snapshots loading (empty map).
    #[serde(default)]
    pub node_addrs: BTreeMap<NodeId, NodeAddrs>,
    /// **Copy-based split lineage** (ADR 0050, fork F9): every copy-based
    /// split child's id mapped to its parent's identity and final stream
    /// state, written by [`MetaCommand::CutoverSplit`]'s apply — the one
    /// moment the parent's shard chain is complete and immutable (the
    /// parent is removed from the tablet map in the same apply), so the
    /// recorded lineage can never race a later parent seal. Never pruned
    /// (tablet ids are never reused). Successor to the zero-copy split's
    /// `split_parents`/`stream_split_basis` provenance maps (deleted, Train
    /// B rung 7) for copy-based splits: children are born with **empty** change logs (no inherited
    /// backlog, no watermark inheritance — ADR 0050 strengthens ADR 0046
    /// principle 3 to "no consumer offset ever crosses a split"), so all a
    /// child needs is its parent's name and final epoch for
    /// `ParentShardId` derivation (B6). `#[serde(default)]` keeps earlier
    /// snapshots loading (empty map).
    #[serde(default)]
    pub split_lineage: BTreeMap<TabletId, SplitLineage>,
    /// **Directed Placing catalog** (ADR 0062 §2), keyed by an in-place
    /// split child's own [`TabletId`]. Written **once**, by
    /// [`MetaCommand::CutoverSplit`]'s in-place branch, as a pure function
    /// of already-agreed `Metadata` at that exact apply (fork C: the same
    /// discipline `BeginBackup` already established for deriving its
    /// manifest stub from agreed state rather than anything the proposer
    /// carried) — never by the copy-based branch, whose fork F5 already
    /// mints a child at its placement-chosen final home, so there is
    /// nothing left to place afterward. An entry exists only when the
    /// freshly-forked child's inherited (parent-current) replicas do NOT
    /// already match a fresh `select_replicas` computation under the
    /// child's own inherited policy — an already-satisfying child gets no
    /// row at all, mirroring `reconcile`/`rebalance`'s own "nothing to do,
    /// nothing proposed" convention. **Never rewritten after that one
    /// write** except by [`MetaCommand::MarkSplitPlacingDone`] flipping
    /// `done` — the reconcile loop's own directed-Placing phase
    /// ([`Metadata::split_placing_reconcile`], wired into `node.rs`'s
    /// `reconcile_loop`) always recomputes `select_replicas` fresh every
    /// tick rather than trusting or
    /// updating `target`, so this field is a diagnostic record of "what
    /// cutover itself decided," never the mechanism's own source of truth
    /// (see [`SplitPlacing::target`]'s own doc). Never pruned by anything
    /// but [`MetaCommand::DropTableTablets`]'s existing drop-table cascade,
    /// which sweeps a row whose child tablet id no longer exists at all —
    /// the same orphan-prevention `index_backfill` already gets, since
    /// nothing else will ever revisit an orphaned row. `#[serde(default)]`
    /// keeps pre-ADR-0062 snapshots loading (empty map).
    #[serde(default)]
    pub split_placing: BTreeMap<TabletId, SplitPlacing>,
    /// The stream-shard segment catalog (ADR 0042 §3, ADR 0043 §A8): every
    /// sealed shard ever committed, keyed by `(tablet, epoch)` — globally
    /// unique for a tablet's whole lifetime (a tablet's own epoch counter
    /// counts up from its first seal and never resets across a disable/
    /// re-enable cycle, ADR 0042 §2), so `table`/`label` live inside
    /// [`StreamShardRow`] as descriptive fields rather than part of the
    /// row's identity. Mutated only through [`MetaCommand::SealStreamShard`]
    /// (first-committer-wins on this same key) and
    /// [`MetaCommand::ExpireStreamShards`] (the janitor's mark-then-remove
    /// reclaim, ADR 0043 §A9). `#[serde(default)]` keeps pre-streams
    /// snapshots loading (empty map).
    ///
    /// **Wire shape (`#[serde(with = "stream_shards_codec")]`)**: encoded as
    /// a flat JSON array of `{tablet, epoch, ...StreamShardRow fields}`
    /// objects, never as a JSON object/map — `serde_json`'s
    /// `MapKeySerializer` rejects any non-string map key outright
    /// (`Error("key must be a string")`), so the natural
    /// `BTreeMap<(TabletId, u64), _>` representation cannot serialize once
    /// this map is non-empty. This bit every whole-`Metadata` JSON call site
    /// the moment a real stream shard sealed: `animusd`'s `GET
    /// /admin/status` (swallowed the error, silently returned `null`) and
    /// its `write_frame`/`ClientResponse::Status` wire path (panicked the
    /// serving connection, since that call site `expect()`s the encode to
    /// succeed) — see `docs/engineering-lessons.md`. The flat-array shape is
    /// also what a future admin/dashboard view over the raw catalog wants
    /// directly, so it doubles as that.
    #[serde(default, with = "stream_shards_codec")]
    pub stream_shards: BTreeMap<(TabletId, u64), StreamShardRow>,
    /// Per-tablet secondary-index **backfill completion** catalog (ADR 0045
    /// §4): "this tablet has finished seeding change-log coverage for this
    /// index's `Creating` scan" (the backfill seeder's own forward sweep of
    /// its `KIND_BASE` scope, a later PR). Keyed `(tablet, index name)` —
    /// same identity convention as [`stream_shards`](Self::stream_shards): a
    /// tablet id already implies its table, so `table` is not part of the
    /// key. The value is always `()` (presence alone is the fact); mutated
    /// only through [`MetaCommand::MarkIndexBackfilled`] (idempotent insert)
    /// and pruned by [`MetaCommand::DropTableTablets`]/
    /// [`MetaCommand::DropTableIndex`]'s own apply arms. `#[serde(default)]`
    /// keeps pre-backfill snapshots loading (empty map).
    ///
    /// **Wire shape (`#[serde(with = "index_backfill_codec")]`)**: same
    /// `serde_json` tuple-map-key hazard as `stream_shards` above (a
    /// `(TabletId, String)` key cannot serialize as a JSON object key either)
    /// — encoded as a flat JSON array of `{tablet, index}` objects instead.
    #[serde(default, with = "index_backfill_codec")]
    pub index_backfill: BTreeMap<(TabletId, String), ()>,
    /// The **backup catalog** (ADR 0059 §3): every on-demand backup ever
    /// begun, keyed by [`BackupId`] — an opaque, freshly-minted identity,
    /// **never the source table name** (ADR 0059 §3's scar: a name-keyed
    /// catalog would let a drop-then-recreate of the same table name
    /// silently poison a still-live backup row). Mutated only through
    /// [`MetaCommand::BeginBackup`] (mints a row), [`MetaCommand::
    /// CompleteBackup`]/[`MetaCommand::FailBackup`] (terminal transitions),
    /// and [`MetaCommand::DeleteBackup`] (removal). Deliberately **not**
    /// touched by [`MetaCommand::DropTableSchema`]/[`MetaCommand::
    /// DropTableTablets`] — a backup catalog row outlives its source table
    /// (ADR 0024's explicit carve-out, ADR 0059 §3), reclaimed only by this
    /// feature's own (not-yet-built) retention janitor or an explicit
    /// `DeleteBackup`. `#[serde(default)]` keeps pre-backup snapshots
    /// loading (empty map).
    #[serde(default)]
    pub backups: BTreeMap<BackupId, BackupRow>,
    /// Per-pinned-tablet backup **capture-completion** catalog (ADR 0059
    /// §3/§4): "this tablet has finished capturing its own share of this
    /// backup," reported by the tablet's own capture driver (a later PR).
    /// Keyed `(backup_id, tablet)` — ADR 0059 §3's own stated identity
    /// order, mirroring [`index_backfill`](Self::index_backfill)'s
    /// `(tablet, index)` shape but with the backup id leading, since a
    /// backup (not a tablet) is the natural grouping a reader wants
    /// (`DescribeBackup`'s per-tablet progress list). Mutated only through
    /// [`MetaCommand::RecordBackupTabletComplete`] (idempotent insert) and
    /// pruned by [`MetaCommand::DeleteBackup`]'s own apply arm.
    /// `#[serde(default)]` keeps pre-backup snapshots loading (empty map).
    ///
    /// **Wire shape (`#[serde(with = "backup_progress_codec")]`)**: same
    /// `serde_json` tuple-map-key hazard as `stream_shards`/`index_backfill`
    /// above (a `(BackupId, TabletId)` key cannot serialize as a JSON
    /// object key either) — encoded as a flat JSON array of `{backup_id,
    /// tablet, cut_version, bytes}` objects instead.
    #[serde(default, with = "backup_progress_codec")]
    pub backup_tablet_progress: BTreeMap<(BackupId, TabletId), BackupTabletProgress>,
    /// The **restore catalog** (ADR 0059 §7, Train 2): every
    /// `RestoreTableFromBackup` ever begun, keyed by [`RestoreId`] — an
    /// opaque, freshly-minted identity, mirroring [`backups`](Self::backups)'
    /// own "never a name" discipline (a restore's *target* table name is data
    /// on the row, not its key, so two restores racing the same target name
    /// are simply two unrelated rows — the state machine's own
    /// `CreateTableSchema` first-committer-wins guard is what actually
    /// decides which one's target table survives, ADR 0059 §7's
    /// "TableAlreadyExistsException" case). Mutated only through
    /// [`MetaCommand::BeginRestore`] (mints a row + this restore's single
    /// `Building` tablet, ADR 0059's Train 2 as-built note on the
    /// pinned-tablets-vs-fresh-layout decision), [`MetaCommand::
    /// CompleteRestore`]/[`MetaCommand::FailRestore`] (terminal transitions).
    /// Deliberately **no** delete/reclaim command yet — a restore row is
    /// small, bounded (one row per `RestoreTableFromBackup` call, never
    /// per-tablet fan-out, since Train 2 mints exactly one destination
    /// tablet per restore), and never referenced once terminal; a retention
    /// sweep is a named Train 2 residual, not a correctness gap.
    /// `#[serde(default)]` keeps pre-restore snapshots loading (empty map).
    #[serde(default)]
    pub restores: BTreeMap<RestoreId, RestoreRow>,
    /// The **PITR segment catalog** (ADR 0059 §9, Train 3): every sealed
    /// PITR segment ever committed, keyed by `(tablet, epoch)` — the
    /// identical identity convention [`stream_shards`](Self::stream_shards)
    /// uses, since a PITR segment IS a sealed-shard-shaped object over the
    /// same `KIND_CHANGE` log, just written by a separate consumer to a
    /// separate object namespace (ADR 0059 §9's "a fifth, independent
    /// consumer arm"). `table`/`generation` live inside [`PitrSegmentRow`]
    /// as descriptive fields, exactly like `StreamShardRow::table`/`label`.
    /// Mutated only through [`MetaCommand::SealPitrSegment`] (first-
    /// committer-wins on this key) and [`MetaCommand::ExpirePitrSegments`]
    /// (the janitor's mark-then-remove reclaim). `#[serde(default)]` keeps
    /// pre-PITR snapshots loading (empty map).
    ///
    /// **Wire shape (`#[serde(with = "pitr_segments_codec")]`)**: the
    /// identical `serde_json` tuple-map-key workaround `stream_shards` needs
    /// — see that field's own doc for why.
    #[serde(default, with = "pitr_segments_codec")]
    pub pitr_segments: BTreeMap<(TabletId, u64), PitrSegmentRow>,
    /// A table's PITR generation allocator (ADR 0059 §9): the highest
    /// generation number ever minted for this table name by
    /// `MetaCommand::UpdateContinuousBackups`. **Never rewound** — including
    /// across a disable (`TableSchema::pitr` goes to `None`, but this floor
    /// stays), and across a `DropTableSchema`/`CreateTableSchema` recreation
    /// of the same table name — so a re-enable, or a same-named table
    /// recreated later, always mints a strictly higher generation than any
    /// this name has ever used, the identical non-reuse discipline
    /// `next_tablet_id`'s allocator floor already gives tablet ids.
    /// `#[serde(default)]` keeps pre-PITR snapshots loading (empty map).
    #[serde(default)]
    pub pitr_generation: BTreeMap<TableName, u64>,
    /// The set of [`BackupId`]s that are **PITR base snapshots** (ADR 0059
    /// §9) rather than ordinary on-demand backups — an internally-triggered
    /// `BeginBackup` the PITR machinery proposed on its own schedule (§9's
    /// "reusing the Train 1 capture path unchanged"), tagged via
    /// [`MetaCommand::BeginBackup`]'s own `pitr_base` flag **in the same
    /// apply that mints the row** (issue #593; see that field's own doc).
    /// **Deliberately a side-set, not a [`BackupRow`] field** — this avoids
    /// widening `BackupRow` itself (and, with it, every reader of a
    /// serialized row) for a fact only the PITR janitor and
    /// `DescribeContinuousBackups` ever need to know. Pruned by
    /// [`MetaCommand::DeleteBackup`]'s existing apply arm (the same
    /// finalizing command an on-demand backup's own reclaim already uses).
    /// `#[serde(default)]` keeps pre-PITR snapshots loading (empty set).
    ///
    /// **Never observably untagged**: because the tag rides the same
    /// `BeginBackup` command as the mint, there is no committed state in
    /// which this row exists but isn't yet in this set — unlike the
    /// now-deleted two-command mint-then-tag sequence (`BeginBackup` followed
    /// by a separate `MarkBackupPitrBase`), which left exactly that window
    /// open between the two commits (a `ListBackups` default `USER` filter,
    /// or the console's per-table backups projection, could observe the row
    /// as an ordinary user backup for the instant in between). See
    /// `docs/adr/0059-backup-restore.md` §9's 2026-09-04 as-built amendment
    /// for the full incident.
    #[serde(default)]
    pub pitr_base_backups: BTreeSet<BackupId>,
}

/// Gives [`Metadata::stream_shards`] a `serde_json`-safe wire shape — see
/// that field's own doc for why the plain `BTreeMap<(TabletId, u64), _>`
/// representation cannot round-trip through JSON. Encodes/decodes a flat
/// `Vec` of `{tablet, epoch, ...StreamShardRow fields}` objects instead,
/// via `#[serde(flatten)]` on borrowed/owned `StreamShardRow` data — no
/// intermediate duplicate-field struct to keep in sync with
/// [`StreamShardRow`] itself.
mod stream_shards_codec {
    use std::collections::BTreeMap;

    use animus_tablet::TabletId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::StreamShardRow;

    /// The serialize-side entry shape: borrows the row so a full
    /// `Metadata::serialize` never clones every catalog row just to encode
    /// it.
    #[derive(Serialize)]
    struct EntryRef<'a> {
        tablet: TabletId,
        epoch: u64,
        #[serde(flatten)]
        row: &'a StreamShardRow,
    }

    /// The deserialize-side entry shape: owns the row, since decoding
    /// necessarily produces owned data.
    #[derive(Deserialize)]
    struct Entry {
        tablet: TabletId,
        epoch: u64,
        #[serde(flatten)]
        row: StreamShardRow,
    }

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<(TabletId, u64), StreamShardRow>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<EntryRef<'_>> = map
            .iter()
            .map(|(&(tablet, epoch), row)| EntryRef { tablet, epoch, row })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<(TabletId, u64), StreamShardRow>, D::Error> {
        let entries = Vec::<Entry>::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|entry| ((entry.tablet, entry.epoch), entry.row))
            .collect())
    }
}

/// Gives [`Metadata::index_backfill`] a `serde_json`-safe wire shape — the
/// same rationale as [`stream_shards_codec`] just above (a `(TabletId,
/// String)` tuple key cannot serialize as a JSON object key either). The
/// value is always `()`, so there is nothing else to carry: a flat `Vec<{
/// tablet, index}>` of just the keys.
mod index_backfill_codec {
    use std::collections::BTreeMap;

    use animus_tablet::TabletId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Entry {
        tablet: TabletId,
        index: String,
    }

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<(TabletId, String), ()>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<Entry> = map
            .keys()
            .map(|(tablet, index)| Entry {
                tablet: *tablet,
                index: index.clone(),
            })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<(TabletId, String), ()>, D::Error> {
        let entries = Vec::<Entry>::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|entry| ((entry.tablet, entry.index), ()))
            .collect())
    }
}

/// Gives [`Metadata::backup_tablet_progress`] a `serde_json`-safe wire shape
/// — the same rationale as [`stream_shards_codec`]/[`index_backfill_codec`]
/// above (a `(BackupId, TabletId)` tuple key cannot serialize as a JSON
/// object key either): a flat `Vec` of `{backup_id, tablet, cut_version,
/// bytes}` objects instead.
mod backup_progress_codec {
    use std::collections::BTreeMap;

    use animus_tablet::TabletId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::{BackupId, BackupTabletProgress};

    #[derive(Serialize, Deserialize)]
    struct Entry {
        backup_id: BackupId,
        tablet: TabletId,
        cut_version: u64,
        bytes: u64,
    }

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<(BackupId, TabletId), BackupTabletProgress>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<Entry> = map
            .iter()
            .map(|((backup_id, tablet), progress)| Entry {
                backup_id: backup_id.clone(),
                tablet: *tablet,
                cut_version: progress.cut_version,
                bytes: progress.bytes,
            })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<(BackupId, TabletId), BackupTabletProgress>, D::Error> {
        let entries = Vec::<Entry>::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|entry| {
                (
                    (entry.backup_id, entry.tablet),
                    BackupTabletProgress {
                        cut_version: entry.cut_version,
                        bytes: entry.bytes,
                    },
                )
            })
            .collect())
    }
}

/// Gives [`Metadata::pitr_segments`] a `serde_json`-safe wire shape — the
/// identical rationale as [`stream_shards_codec`] above (a `(TabletId, u64)`
/// tuple key cannot serialize as a JSON object key either). Encodes/decodes
/// a flat `Vec` of `{tablet, epoch, ...PitrSegmentRow fields}` objects.
mod pitr_segments_codec {
    use std::collections::BTreeMap;

    use animus_tablet::TabletId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::PitrSegmentRow;

    #[derive(Serialize)]
    struct EntryRef<'a> {
        tablet: TabletId,
        epoch: u64,
        #[serde(flatten)]
        row: &'a PitrSegmentRow,
    }

    #[derive(Deserialize)]
    struct Entry {
        tablet: TabletId,
        epoch: u64,
        #[serde(flatten)]
        row: PitrSegmentRow,
    }

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<(TabletId, u64), PitrSegmentRow>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<EntryRef<'_>> = map
            .iter()
            .map(|(&(tablet, epoch), row)| EntryRef { tablet, epoch, row })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<(TabletId, u64), PitrSegmentRow>, D::Error> {
        let entries = Vec::<Entry>::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|entry| ((entry.tablet, entry.epoch), entry.row))
            .collect())
    }
}

/// A copy-based split child's lineage row ([`Metadata::split_lineage`],
/// ADR 0050 fork F9), written once by [`MetaCommand::CutoverSplit`]'s apply
/// — see that field's own doc for why cutover time is the only race-free
/// moment to record it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitLineage {
    /// The parent tablet this child was copied from (removed from the
    /// tablet map by the same apply that wrote this row).
    pub parent: TabletId,
    /// The parent's **final** stream epoch — its shard chain's highest
    /// sealed epoch at cutover, immutable from then on (the parent is gone;
    /// nothing can seal for it again). `None` if the parent never sealed a
    /// shard (never streamed, or disabled before its first seal). What B6's
    /// `ParentShardId` derivation reads for a child's epoch-0 shard.
    pub parents_final_epoch: Option<u64>,
    /// Proposer-stamped wall-clock milliseconds at cutover (the
    /// [`MetaCommand::CutoverSplit::cutover_wall_ms`] payload, same
    /// discipline as `SealStreamShard`'s `seal_wall_ms`: the pure state
    /// machine has no clock, so wall time rides the command). Diagnostic /
    /// Console lineage-view data, never load-bearing for correctness.
    pub cutover_wall_ms: u64,
}

/// An in-place split child's directed-Placing row
/// ([`Metadata::split_placing`], ADR 0062 §2), written once by
/// [`MetaCommand::CutoverSplit`]'s in-place branch, driven toward by the
/// reconcile loop's third phase, and updated by
/// [`MetaCommand::RetargetSplitPlacing`] (a fresh target, dwell-gated) and
/// [`MetaCommand::MarkSplitPlacingDone`] (flipping `done`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitPlacing {
    /// This child's directed-Placing target — **authoritative, not
    /// diagnostic** (issue #528's fix; see ADR 0062 §2's 2026-09-01
    /// amendment for the full incident this corrected). `CutoverSplit`
    /// computes the first value the same way it always has: this child's
    /// policy-satisfying target at cutover, or `None` if placement was
    /// UNSATISFIABLE at that instant (fork B: too few `Active` candidates,
    /// or too few distinct failure domains for a strict spread) — still
    /// recorded as a durable, visible, keep-retrying obligation rather than
    /// silently skipped.
    ///
    /// **What changed**: the reconcile loop's own directed-Placing phase
    /// ([`Metadata::split_placing_reconcile`], wired into `node.rs`'s
    /// `reconcile_loop`) used to recompute `select_replicas` fresh off
    /// current `Metadata` every tick and drive toward *that*, treating this
    /// field as a write-once diagnostic snapshot. Under a flapping failure
    /// detector, that recompute changes the answer as often as membership
    /// flickers — faster than `animus-cp-data`'s own learner-phased mover
    /// (`reconfigure_step`) can complete a single reconfiguration cycle, a
    /// livelock that never converges (`MarkSplitPlacingDone` never fires
    /// because the predicate it needs never holds). The phase now drives
    /// toward THIS field's value **verbatim** while every member of it is
    /// currently `Active`, and only recomputes (via
    /// [`MetaCommand::RetargetSplitPlacing`], replicated, so the new value
    /// is itself stable) once a member has been continuously non-`Active`
    /// past a dwell window — see that command's own doc and
    /// `split_placing_reconcile`'s for the exact mechanics. A transiently
    /// `Down` target member pauses the drive (proposes nothing) rather than
    /// retargeting; serving is unaffected either way (fork A: this never
    /// gates serving).
    pub target: Option<Vec<NodeId>>,
    /// Set once this child's live replicas have converged to a fresh,
    /// currently-satisfying target (via [`MetaCommand::MarkSplitPlacingDone`],
    /// proposed by the tablet's own leader once it observes local Raft
    /// convergence). Never a serving gate (fork A) — a child serves
    /// `Active`, unconditionally, from the moment `CutoverSplit` commits;
    /// this only gates the derived "split fully complete" diagnostic.
    pub done: bool,
}

/// A sealed stream shard's catalog row (ADR 0042 §3, ADR 0043 §A3/§A8) — the
/// replicated record of one committed `SegmentStore` object.
///
/// **Identity note**: a row's key is `(tablet, epoch)`
/// ([`Metadata::stream_shards`]), never `(table, label, tablet, epoch)` — a
/// tablet id already implies its table, and a tablet's epoch counter is a
/// property of the tablet's own physical seal history, not of any one
/// stream generation (ADR 0042 §2's `ShardId = shardId-<tablet>-<epoch>` is
/// scoped the same way). `table`/`label` are carried here purely as
/// descriptive fields a reader needs (which stream this shard belongs to,
/// and which base table), not as part of what makes two rows distinct.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamShardRow {
    /// The base table this shard's tablet belongs to.
    pub table: TableName,
    /// The stream label active when this shard sealed (ADR 0042 §4) — the
    /// label a `DescribeStream`/`GetRecords` request against this shard
    /// must present to resolve it (F12-b's catalog-row-based resolution,
    /// ADR 0042 §4/§11).
    pub label: String,
    /// The view type declared by the table's stream when this shard sealed
    /// (ADR 0042 §3/§15, PR6's catalog amendment) — a shard's own copy of
    /// what was, at seal time, `TableSchema.stream`'s `StreamViewType`.
    /// Carried here (not re-derived from the current schema) because a
    /// `DISABLED`-but-still-readable stream's F12-b grace window has
    /// **no** live `StreamSpec` to read it from once `SetTableStream{None}`
    /// commits — a `DescribeStream` against a draining label reports its
    /// last-known view type from exactly this field. A view type never
    /// changes mid-stream (only a disable + re-enable can pick a new one,
    /// which mints a new label), so every row of one label carries the
    /// identical value. `#[serde(default)]` keeps a row encoded before this
    /// field existed loading (see [`default_stream_view_type`]'s own doc —
    /// never reached by a row this PR's own sealer writes).
    #[serde(default = "default_stream_view_type")]
    pub view_type: StreamViewType,
    /// `(start_exclusive, end_inclusive)` committed packed-HLC range — the
    /// ground truth a reader slices a fetched segment object to
    /// (`animus_cp_data::segment`'s superset-slice rule, ADR 0042 §10). A
    /// shard's `EndingSequenceNumber` (ADR 0042 §5) is `hlc_range.1`.
    pub hlc_range: (u64, u64),
    /// The number of records the sealing leader's own scan counted.
    pub count: u64,
    /// The sealing leader's own `env.now()` at seal time (never a raw OS
    /// clock, ADR 0003) — observability only, never load-bearing for a
    /// correctness decision.
    pub seal_wall_ms: u64,
    /// The replica set the segment object was pushed to
    /// (`ClusterSegmentStore::put_replicated`'s own returned set, ADR 0043
    /// §A7b/§A3 step 3) — recorded so a future reader/repair sweep knows
    /// exactly where to look without a discovery round.
    pub replicas: Vec<NodeId>,
    /// The **unique per-attempt** `SegmentStore` id this row's segment
    /// object actually lives at (the ledger-named-object as-built
    /// amendment, ADR 0042 §10/ADR 0043 §A3) —
    /// `animus_cp_data::segment::segment_object_id`'s output, never the
    /// bare deterministic `segment_id(table, label, tablet, epoch)` a
    /// reader used to recompute directly. **Every reader/sweep must resolve
    /// this field rather than recomputing an id** — recomputing would
    /// silently reintroduce the shared-id race this amendment exists to
    /// close (see `animus_cp_data::segment`'s own module doc for the full
    /// incident). No `#[serde(default)]`: this codebase has no live-
    /// deployment/back-compat requirement (fresh clusters only), so a row
    /// this field predates simply cannot exist.
    pub object_id: String,
    /// Set by [`MetaCommand::ExpireStreamShards`]'s **mark** phase
    /// (`remove: false`): the janitor's own record that it has begun
    /// reclaiming this row's segment object, so a crash-and-resume knows to
    /// go straight to deleting objects rather than re-marking (harmless
    /// either way — the mark is idempotent). **Never a visibility gate**: a
    /// marked-but-not-yet-removed row is still fully valid to serve
    /// (`DescribeStream`/`GetRecords`) — its segment object still exists
    /// until the janitor's delete step actually completes, which is what
    /// licenses removing the row itself, not this flag. `#[serde(default)]`
    /// keeps a row encoded before this field existed loading as
    /// not-yet-marked.
    #[serde(default)]
    pub expired: bool,
}

/// A sealed **PITR segment**'s catalog row (ADR 0059 §9, Train 3) — the
/// PITR consumer's own twin of [`StreamShardRow`], sharing the identical
/// `(tablet, epoch)` identity discipline and the identical `segment.rs`
/// object codec, but recorded in [`Metadata::pitr_segments`] (a fully
/// separate catalog from `Metadata::stream_shards`) and written to a
/// PITR-specific object namespace (`animus_cp_data::backup::
/// pitr_segment_object_id`) — a table's stream and its PITR coverage are
/// two independent consumers of the same change log with two independent
/// lifecycles (ADR 0059 §9), never sharing a catalog row or an object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitrSegmentRow {
    /// The base table this segment's tablet belongs to.
    pub table: TableName,
    /// The PITR generation active when this segment sealed
    /// ([`PitrSpec::generation`]) — the coverage-epoch identity a disable/
    /// re-enable cycle mints fresh, mirroring `StreamShardRow::label`'s own
    /// role for streams.
    pub generation: u64,
    /// `(start_exclusive, end_inclusive)` committed packed-HLC range —
    /// identical semantics to `StreamShardRow::hlc_range`.
    pub hlc_range: (u64, u64),
    /// The number of records the sealing leader's own scan counted.
    pub count: u64,
    /// The sealing leader's own `env.now()` at seal time — observability
    /// only, and (via the janitor) the retention-age basis for this row.
    pub seal_wall_ms: u64,
    /// The replica set the segment object was pushed to (empty for the
    /// single-directory `fs:` backup-store opt-in) — identical convention
    /// to `StreamShardRow::replicas`.
    pub replicas: Vec<NodeId>,
    /// The unique per-attempt backup-store id this row's segment object
    /// actually lives at (`animus_cp_data::backup::pitr_segment_object_id`)
    /// — the identical ledger-named-object discipline `StreamShardRow::
    /// object_id` already gives streams, applied to the PITR namespace.
    pub object_id: String,
    /// Set by [`MetaCommand::ExpirePitrSegments`]'s **mark** phase — the
    /// identical semantics to `StreamShardRow::expired`.
    #[serde(default)]
    pub expired: bool,
}

/// A backup's opaque catalog identity (ADR 0059 §3) — an ARN-shaped string at
/// the wire, freshly minted per `CreateBackup` request. **Never a table
/// name**: keying the catalog by identity rather than name is what lets a
/// backup outlive a drop-then-recreate of its source table's name (see
/// [`Metadata::backups`]'s own doc for the full "scar" this avoids).
pub type BackupId = String;

/// One tablet pinned into a backup at [`MetaCommand::BeginBackup`] time (ADR
/// 0059 §2/§3): its id and key range, as they stood the moment the backup
/// began. A plain owned snapshot — never updated in place if the tablet
/// later splits or moves; a future capture driver re-plans a retired
/// tablet's range onto its live descendants via `Metadata::split_lineage`
/// (ADR 0059 §6), a later PR's concern, not this catalog row's.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupPinnedTablet {
    /// The pinned tablet's id.
    pub tablet: TabletId,
    /// The pinned tablet's key range at pin time.
    pub range: KeyRange,
}

/// A backup's manifest stub (ADR 0059 §2), derived **entirely from
/// already-agreed `Metadata`** at [`MetaCommand::BeginBackup`]'s own apply —
/// never from anything a proposer carries beyond the backup id/table name/
/// wall-clock stamp — so every replica computes an identical stub
/// deterministically. This is the "stub" the ADR describes: the capture
/// driver (a later PR) fills in each pinned tablet's completion record
/// (`Metadata::backup_tablet_progress`) as it finishes; the full manifest
/// object written to the backup store is this stub plus that per-tablet
/// data, assembled once the row reaches [`BackupStatus::Available`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    /// An owned snapshot of the source table's schema at capture time (ADR
    /// §2's `SourceTableFeatureDetails` capture — partition/clustering
    /// keys, columns, and GSI/LSI definitions carried forward for restore;
    /// `stream`/`ttl` carried too, but purely for descriptive fidelity —
    /// `DescribeBackup` can report "this table had a stream/TTL," but
    /// restore deliberately never re-enables either, ADR 0059 §7). A plain
    /// clone, never a live reference into [`Metadata::schemas`] — mirrors
    /// [`StreamShardRow::view_type`]'s own copy-not-reference convention.
    pub schema: TableSchema,
    /// The source table's tablet list, pinned at `BeginBackup` time.
    pub pinned_tablets: Vec<BackupPinnedTablet>,
    /// Wall-clock creation time, stamped at **propose** time by the
    /// wire-serving node (`env.wall_now()`, the ADR 0051 discipline: the
    /// pure state machine has no clock, so calendar time rides the command
    /// as plain data) — never a timing input for this state machine, only
    /// `DescribeBackup`-visible metadata.
    pub created_wall_ms: u64,
}

/// One pinned tablet's capture-completion record (ADR 0059 §3/§4),
/// proposed by that tablet's own capture driver (a later PR) once it
/// finishes sweeping its share of the backup.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupTabletProgress {
    /// The packed-HLC watermark this tablet's own capture pinned (ADR 0059
    /// §2/§4) — the cut version the manifest records for this tablet.
    pub cut_version: u64,
    /// The total bytes this tablet's own data objects occupy in the backup
    /// store.
    pub bytes: u64,
}

/// A backup catalog row's lifecycle status (ADR 0059 §3/§4). Modeled with
/// room for the (not-yet-built) two-phase retention janitor's PR④ needs —
/// see [`Expired`](Self::Expired)'s own doc — so that PR doesn't have to
/// reshape this enum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupStatus {
    /// Capture in progress: [`MetaCommand::BeginBackup`] has landed, but not
    /// every pinned tablet has reported completion yet (or has, but the
    /// leader aggregator hasn't yet proposed [`MetaCommand::CompleteBackup`]).
    Creating,
    /// Terminal success — DynamoDB's own terminal on-demand-backup status.
    /// Reached only once every pinned tablet has reported and the manifest
    /// itself is durably stored (ADR 0059 §4's durable-before-visible rule;
    /// enforced by the capture driver/aggregator, a later PR — this catalog
    /// only records the resulting state).
    Available,
    /// Terminal failure: a stuck-`Creating` timeout, or any other
    /// aggregator-observed failure. `reason` is diagnostic only, never
    /// interpreted by this state machine.
    Failed {
        /// A human-readable failure reason.
        reason: String,
    },
    /// Marked for reclaim by the two-phase retention janitor's **mark**
    /// phase (ADR 0043 §A9's mold, reused verbatim by ADR 0059 §3) — no
    /// `MetaCommand` in this PR ever transitions a row into this state; it
    /// exists purely so a later PR's janitor-mark command doesn't need to
    /// widen this enum (and thus doesn't need to touch every existing match
    /// on it again).
    Expired,
}

/// A backup catalog row (`Metadata::backups`, ADR 0059 §3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupRow {
    /// The source table this backup was taken from (data only — the
    /// catalog is keyed by [`BackupId`], never by this name, ADR 0059 §3's
    /// scar).
    pub table: TableName,
    /// The client-supplied `BackupName` (ADR 0059 Train 1 PR④, DynamoDB's
    /// `CreateBackup` request field) — recorded verbatim, echoed by
    /// `DescribeBackup`/`ListBackups`, never interpreted and never part of
    /// this row's own identity (`BackupId` alone is, per this catalog's
    /// "scar" — see [`Metadata::backups`]'s doc). `#[serde(default)]` is an
    /// implementation convenience for a pre-existing snapshot/fixture that
    /// predates this field (root `CLAUDE.md`: no real migration guarantee is
    /// implied).
    #[serde(default)]
    pub backup_name: String,
    /// This row's lifecycle status.
    pub status: BackupStatus,
    /// The manifest stub (ADR 0059 §2), derived once at `BeginBackup` time.
    pub manifest: BackupManifest,
    /// The backup's total captured bytes (ADR 0059 §2's "total object
    /// sizes, for `DescribeBackup`"), frozen exactly **once**, by
    /// [`MetaCommand::CompleteBackup`]'s own apply arm, from
    /// [`Metadata::backup_total_bytes`] at the moment every pinned tablet's
    /// live descendant is still resolvable. `0` while
    /// [`Creating`](BackupStatus::Creating)/[`Failed`](BackupStatus::Failed)
    /// (Train 1 PR④, DynamoDB's own on-demand backup contract reports no
    /// size until `AVAILABLE` either) — **never** re-derived from
    /// [`Metadata::backup_manifest_tablet_progress`] after the fact, which
    /// would silently collapse to zero the moment this backup's source
    /// table (and with it every one of its tablets) is ever dropped,
    /// breaking ADR 0059 §3's own "a backup outlives its source table"
    /// promise for `DescribeBackup`'s reported size specifically.
    /// `#[serde(default)]` is an implementation convenience for a
    /// pre-existing snapshot/fixture that predates this field.
    #[serde(default)]
    pub total_bytes: u64,
}

/// A restore's opaque catalog identity (ADR 0059 §7, Train 2) — an
/// internally-minted identity, never wire-visible (unlike [`BackupId`], which
/// doubles as the wire ARN) since `RestoreTableFromBackup` has no AWS-defined
/// "restore id" of its own to echo back — a client only ever sees the
/// resulting `TableDescription`.
pub type RestoreId = String;

/// A restore-in-progress catalog row's lifecycle status (ADR 0059 §7). Unlike
/// [`BackupStatus`] there is no `Expired` — a restore row has no retention
/// janitor yet (see [`Metadata::restores`]'s own doc).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreStatus {
    /// The restore driver (`animusd::backup_restore`) is seeding the target
    /// tablet from the backup's data objects. The target table's own
    /// `TableStatus` reads `CREATING` for the whole duration this row stays
    /// in this state (derived from its tablet's own `Building` state, not
    /// stored redundantly here — see `animusd::dynamo::table_status`).
    Seeding,
    /// Terminal success: the target tablet has been fully seeded and
    /// activated (`MetaCommand::CompleteRestore`).
    Done,
    /// Terminal failure — a stuck-`Seeding` timeout, or a source backup that
    /// became unreadable mid-restore (e.g. deleted and reclaimed by the
    /// backup janitor while restore was still reading it, ADR 0059's Train 2
    /// as-built note on this narrow, accepted race). `reason` is diagnostic
    /// only, never interpreted. The target table's schema and its
    /// permanently-`Building` (never-served) tablet are left in place —
    /// `DropTableTablets`/the reconciler's ordinary `Reclaim` action already
    /// clean up a `Building` tablet exactly like any other (state-agnostic),
    /// so a failed restore's target table is a clean, ordinary `DeleteTable`
    /// away from full cleanup, never half-serving.
    Failed {
        /// A human-readable failure reason.
        reason: String,
    },
}

/// A restore-in-progress catalog row (`Metadata::restores`, ADR 0059 §7).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreRow {
    /// The backup this restore reads from.
    pub backup_id: BackupId,
    /// The backup's own source table, purely descriptive (`DescribeBackup`-
    /// adjacent observability; never re-derived from live `Metadata`, since
    /// the source table may have been dropped — the whole point of ADR
    /// 0059's "restore a table dropped days ago" case).
    pub source_table: TableName,
    /// The new table this restore is populating.
    pub target_table: TableName,
    /// The single `Building` tablet minted for this restore (ADR 0059 Train
    /// 2 as-built note: restore mints exactly **one** fresh tablet over the
    /// whole ring, matching ordinary `CreateTable`'s own provisioning
    /// convention, rather than mirroring the backup's historical
    /// multi-tablet topology — see that note for the full reasoning). The
    /// restore driver seeds this tablet from every one of the backup's data
    /// objects, regardless of which physical tablet originally captured
    /// them (a single destination tablet needs no per-row key routing).
    pub tablet: TabletId,
    /// The GSI definitions to create once this restore activates its
    /// tablet (ADR 0059 §8): the caller's own `GlobalSecondaryIndexOverride`
    /// if given, else every `Global`-kind index the backup's own manifest
    /// recorded — resolved once, client-side, by the wire handler
    /// (`animusd::dynamo::restore_table_from_backup`) before ever proposing,
    /// the identical "client-supplied, recorded verbatim" convention
    /// [`BackupRow::backup_name`] already uses. Each already carries
    /// [`IndexStatus::Creating`] (mirroring `create_index`'s own override
    /// convention) — **deliberately not declared on the target schema until
    /// [`MetaCommand::CompleteRestore`] fires** (see that command's own doc
    /// and the restore driver's module doc): declaring a GSI before the
    /// tablet is seeded would let the backfill seeder observe an empty
    /// range, mark it backfilled, and then silently miss every row this
    /// restore seeds afterward.
    pub gsi_defs: Vec<IndexDef>,
    /// `Some` for a `RestoreTableToPointInTime` restore (ADR 0059 §10, Train
    /// 3 PR②) — the segment-replay plan the wire handler resolved once,
    /// client-side, before ever proposing (the identical "client-supplied,
    /// recorded verbatim" convention [`gsi_defs`](Self::gsi_defs) already
    /// uses, and for the same reason: computing this needs the backup
    /// store's own manifest object, which this pure state machine cannot
    /// read). `None` for an ordinary `RestoreTableFromBackup` restore — in
    /// that case `backup_id` alone is everything the restore driver
    /// (`animusd::backup_restore`) needs, unchanged from Train 2. See
    /// [`PitrRestorePlan`]'s own doc for the plan shape and
    /// [`Metadata::pitr_replay_segments`] for how it is computed.
    pub pitr: Option<PitrRestorePlan>,
    /// This row's lifecycle status.
    pub status: RestoreStatus,
}

/// One PITR segment to replay, as part of a [`PitrRestorePlan`] (ADR 0059
/// §10, Train 3 PR②) — a thin reference into the already-committed
/// [`Metadata::pitr_segments`] catalog (never re-derived by the restore
/// driver, which reads only this plan), resolved once by the wire handler
/// from a **fresh** `Metadata` read at propose time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitrReplaySegmentRef {
    /// The tablet (possibly a live split descendant of the PITR base
    /// snapshot's own pinned tablet, ADR 0059 §6's re-planning technique
    /// applied here too) whose own chain this segment belongs to —
    /// diagnostic only; the driver never needs it to fetch or decode.
    pub tablet: TabletId,
    /// This segment's own epoch within `tablet`'s chain — diagnostic only.
    pub epoch: u64,
    /// The backup store object id (`PitrSegmentRow::object_id`) to fetch.
    pub object_id: String,
    /// The `(start_exclusive, end_inclusive)` packed-HLC range to slice this
    /// segment's decoded records to (`segment::decode_and_slice`) — **not**
    /// necessarily the catalog row's own full `hlc_range`: for a segment
    /// straddling the PITR base snapshot's own captured cut version, the
    /// lower bound is raised to that cut version so an already-captured
    /// record is never replayed a second time (harmless either way, since
    /// `SeedBatch`'s own merge-at-carried-version is idempotent, but
    /// avoided here rather than relied upon).
    pub replay_range: (u64, u64),
}

/// A PITR restore's own segment-replay plan (ADR 0059 §10, Train 3 PR②) —
/// resolved once, client-side, by `animusd::dynamo::
/// restore_table_to_point_in_time` from a fresh `Metadata` read plus the
/// chosen PITR base snapshot's own manifest object (fetched from the backup
/// store — something this pure state machine has no access to, which is why
/// this plan rides the command as already-resolved data rather than being
/// recomputed inside `MetaCommand::BeginRestore`'s own apply arm the way
/// `BeginBackup`'s manifest stub is). See [`Metadata::pitr_replay_segments`]
/// for the plan-computation algorithm.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitrRestorePlan {
    /// The restore's own target wall-clock cutoff, in epoch milliseconds
    /// (the requested `RestoreDateTime`, truncated to the second and pushed
    /// to that second's own last millisecond — or `LatestRestorableDateTime`
    /// verbatim for `UseLatestRestorableTime`). Every included segment's own
    /// `seal_wall_ms` is at or before this value (ADR 0059 §10's
    /// per-tablet-cutoff rule, at this codebase's own segment-granularity
    /// precision — see `pitr_replay_segments`'s own doc for the full
    /// argument). Diagnostic/observability only, once the plan below is
    /// already resolved.
    pub target_wall_ms: u64,
    /// Every PITR segment to replay, across every one of the base
    /// snapshot's own pinned tablets and their live split descendants, in
    /// no particular cross-tablet order (`SeedBatch`'s merge-at-carried-
    /// version is order-independent by construction — see
    /// `animusd::backup_restore`'s own doc for the full argument).
    pub segments: Vec<PitrReplaySegmentRef>,
}

/// The current-generation PITR restorable window for `table` (ADR 0059
/// §10) — the basis `RestoreTableToPointInTime`'s validation gate reads,
/// valid whether or not the table still exists (a dropped table's PITR
/// history survives `DropTableSchema`/`DropTableTablets`, ADR 0059 §9/§10's
/// own carve-out). Deliberately scoped to the **latest** generation this
/// table name has ever used, never an older one superseded by a disable/
/// re-enable cycle — this is what makes a `T` before this generation's own
/// coverage start (including one that falls inside an earlier disable/
/// re-enable gap) uniformly rejected as "too early," without needing a
/// separate gap-detection rule (this settles the ADR's own "Not yet
/// decided" question from the Train 3 PR① amendment: option 1, reject
/// outright, chosen over "find no coverage and use a generic error" —
/// because scoping to the latest generation alone already produces exactly
/// that rejection for free).
///
/// Distinct from (but answering the identical underlying question as)
/// `animusd::dynamo::pitr_description`'s own `DescribeContinuousBackups`
/// formula: that one is more precise for a **live** table (it takes the
/// minimum over the table's *current* tablets' own last-seal times); this
/// one instead takes the minimum over every tablet that ever sealed a
/// segment of this generation, which is the only formulation that still
/// makes sense once the table (and with it `Metadata::tablets_for_table`)
/// is gone. The two agree for a live, quiescent table but can differ
/// slightly for a live table mid-split (a retired parent's own tablet id
/// still counts here, harmlessly conservative) — never a correctness
/// concern for restore's own gate, which only needs a **safe** (never too
/// generous) bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PitrRestoreWindow {
    /// The generation this window describes.
    pub generation: u64,
    /// The earliest wall-clock instant (epoch milliseconds), **before** the
    /// retention floor is folded in (the caller, holding `env.wall_now()`,
    /// does that — this pure accessor takes no "now" input at all).
    pub earliest_ms: u64,
    /// The latest wall-clock instant (epoch milliseconds) this generation's
    /// own coverage can currently support a restore to.
    pub latest_ms: u64,
}

/// A mutation of [`Metadata`], replicated through Raft and applied in log order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaCommand {
    /// A no-op, used by a freshly elected leader to commit prior-term entries.
    NoOp,
    /// Insert or update a member.
    UpsertMember {
        node: NodeId,
        labels: BTreeMap<String, String>,
        status: NodeStatus,
    },
    /// Create a tablet (starting at [`Epoch::INITIAL`]). No-op if it exists.
    /// `table` scopes the tablet to a single table (ADR 0023): every key it owns
    /// is data of that table. `None` is the legacy whole-keyspace tablet that
    /// serves every table. `#[serde(default)]` keeps commands/snapshots written
    /// before scoping loading as `None`.
    CreateTablet {
        tablet: TabletId,
        #[serde(default)]
        table: Option<TableName>,
        range: KeyRange,
        replicas: Vec<NodeId>,
    },
    /// Compare-and-swap a tablet's replica set: applies only if the tablet's
    /// epoch equals `expected_epoch`, then bumps the epoch.
    CasTabletReplicas {
        tablet: TabletId,
        expected_epoch: Epoch,
        replicas: Vec<NodeId>,
    },
    /// Begin an **in-place split** (ADR 0058 Train 2 rung 3, Stage 1):
    /// consults placement for the two children's final homes (fork F5),
    /// mints their tablet ids up front (monotonic allocator, collision/floor
    /// checks), but mints NO `Building` tablet-map entries: no data has
    /// moved yet, so there is nothing to place a routable-but-empty row
    /// over. Instead the intent itself (split key + both children's `(id,
    /// replicas)` pairs) is recorded directly on the parent
    /// (`Tablet::inplace_split`) — the smallest representation a hosting
    /// node's own reconciler (`animus_cp_data::host`) needs to discover
    /// "this tablet is splitting in-place" and drive the
    /// learner-add/fork/materialize sequence entirely inside the data
    /// plane. Epoch-CAS on the parent (the `CasTabletReplicas` discipline);
    /// rejected unless the parent is `Active` (no re-split of a `Splitting`
    /// parent, no split of a `Building` child — e.g. one mid-restore, ADR
    /// 0059 §7, `BeginRestore`'s own mint); child ids obey the monotonic
    /// allocator floor; the F11 token-alignment seatbelt applies to a
    /// streamed table's split key. The parent's policy is
    /// inherited by both children only at
    /// [`CutoverSplit`](Self::CutoverSplit) time (there is no tablet-map
    /// row to attach a policy to before then).
    BeginSplitInPlace {
        parent: TabletId,
        expected_epoch: Epoch,
        split_key: Vec<u8>,
        /// Exactly two `(child id, replica set)` pairs, left half first —
        /// each replica set is that child's placement-chosen FINAL homes.
        children: [(TabletId, Vec<NodeId>); 2],
    },
    /// Complete an **in-place split** (ADR 0058 Train 2 rung 3, Stage 4):
    /// atomically flip both children [`TabletState::Active`], **remove the
    /// parent from the tablet map** (fork F6 — every hosting node's
    /// reconciler then reclaims it as an ordinary hosted-but-absent
    /// tablet), and record each child's [`SplitLineage`] row (fork F9) —
    /// written here, at the one moment the parent's shard chain is
    /// complete and immutable. Epoch-CAS on the parent; rejected unless
    /// the parent is `Splitting`. The parent, at that point, always
    /// carries an [`animus_tablet::InPlaceSplitIntent`]
    /// (`Tablet::inplace_split`, set by
    /// [`BeginSplitInPlace`](Self::BeginSplitInPlace) — the sole path into
    /// `Splitting`) naming both children's `(id, replicas)` pairs; this
    /// command activates them DIRECTLY from that intent, inheriting the
    /// parent's policy at THIS moment (the in-place workflow's only
    /// chance to, since there was no tablet-map row to attach it to
    /// earlier). The children are already fully formed and durable on
    /// every fork participant by the time this command ever runs (the
    /// data plane's own `SplitTablet` entry, ADR 0058 Train 2 Stage 3) —
    /// there is nothing left for a pre-cutover veto to wait for, so NO
    /// freeze/GSI-drain/backfill veto gates this command at all.
    ///
    /// **The copy-based split's own build/tail/cutover workflow (ADR
    /// 0050) — this command's former alternate branch, gated on the
    /// parent carrying no intent and instead having two `Building`
    /// tablet-map children to scan for — was deleted (copy-split deletion
    /// stack, layer B2, 2026-09-01), along with its sole minter,
    /// `BeginSplit`.** A `Splitting` parent with no intent is now
    /// structurally impossible; the apply arm still rejects that case
    /// defensively rather than panicking (see the arm's own comment).
    CutoverSplit {
        parent: TabletId,
        expected_epoch: Epoch,
        /// Proposer-stamped wall-clock ms for the lineage row (the pure
        /// state machine has no clock — `SealStreamShard::seal_wall_ms`'s
        /// discipline). Diagnostic only.
        cutover_wall_ms: u64,
    },
    /// Mark an in-place split child's directed-Placing obligation complete
    /// (ADR 0062 §3): `tablet` is the CHILD's own id, not the (long gone)
    /// parent's. Epoch-CAS'd against the **child's** own current epoch (so
    /// a stale confirm racing a later churn event on the same tablet is
    /// rejected rather than marking done against state that has since
    /// moved again) — the same `CasTabletReplicas` discipline, but this
    /// command never bumps the tablet's own epoch itself (it isn't a
    /// placement change, just a completion record). Rejected if `tablet`
    /// has no [`Metadata::split_placing`] entry at all (nothing to mark
    /// done); idempotent (`NoOp`) if the entry is already `done` — the
    /// `MarkIndexBackfilled`/`RecordBackupTabletComplete` idiom, since the
    /// proposer (a tablet leader's own background loop, `animusd`) is
    /// expected to retry on an unconfirmed propose. The row itself is
    /// never deleted by this command — it stays a permanent, bounded-size
    /// record of "this child's post-split placement finished," pruned only
    /// by `DropTableTablets`'s existing drop-table cascade.
    MarkSplitPlacingDone {
        tablet: TabletId,
        expected_epoch: Epoch,
    },
    /// Replace an in-place split child's directed-Placing STORED target
    /// (ADR 0062 §2, issue #528 fix): `tablet` is the CHILD's own id.
    /// Proposed only by the control-plane leader's own `reconcile_loop`
    /// third phase, once its driver-local dwell tracking decides a
    /// currently-stored target member has been continuously non-`Active`
    /// long enough to treat as genuinely gone (see
    /// `split_placing_reconcile`'s own doc, `meta.rs`, for the full
    /// mechanics) — never proposed for a merely transiently-`Down` member.
    /// Epoch-CAS'd against the CHILD's own current epoch, the same
    /// `MarkSplitPlacingDone` discipline — a stale recompute racing a later
    /// churn event on this same tablet (an intervening `CasTabletReplicas`,
    /// or another leader after a failover) is rejected rather than
    /// overwriting a target computed against state that has since moved on;
    /// the reconcile loop's own next tick simply recomputes fresh. Rejected
    /// if `tablet` has no [`Metadata::split_placing`] entry at all (nothing
    /// to retarget); a `NoOp` if the entry is already `done` (nothing left
    /// to retarget) or the new value is byte-identical to the stored one
    /// (no-op retry). This command never touches the tablet's own replicas
    /// or bumps its epoch — only [`Metadata::split_placing`]'s stored
    /// `target` field changes; the reconcile loop's own next tick is what
    /// actually drives replicas toward the new value via an ordinary
    /// `CasTabletReplicas`. **Not relayable** (`is_relayable_command`,
    /// `animus-node::wire`): unlike `MarkSplitPlacingDone` (a tablet
    /// leader's own report, which may run on any node), this is proposed
    /// directly by the control-plane leader off its own live `RaftNode`
    /// handle, the same class as `CasTabletReplicas` itself.
    RetargetSplitPlacing {
        tablet: TabletId,
        expected_epoch: Epoch,
        target: Option<Vec<NodeId>>,
    },
    /// Set (or clear) a tablet's placement policy (ADR 0005). Once a tablet has
    /// a policy, the leader's reconciler keeps its replica set satisfying it;
    /// `policy: None` removes the policy and stops automatic reconciliation. The
    /// tablet must exist. This replicates the policy in [`Metadata`] so it
    /// survives leader change and recovery, and so every replica computes the
    /// same desired set.
    SetTabletPolicy {
        tablet: TabletId,
        policy: Option<PlacementPolicy>,
    },
    /// Register a table's schema in the replicated catalog (ADR 0013). Rejected
    /// if a schema for `table` already exists (a `CreateTable` does not silently
    /// overwrite) or if the schema is malformed
    /// ([`TableSchema::validate`](crate::schema::TableSchema::validate) fails).
    /// Otherwise records it; because it is a replicated `MetaCommand`, the schema
    /// survives restart and is consistent on every replica.
    CreateTableSchema {
        table: TableName,
        schema: TableSchema,
    },
    /// Remove a table's schema from the catalog (ADR 0013). Idempotent: a no-op
    /// if no schema is registered for `table`.
    DropTableSchema { table: TableName },
    /// **Atomically replace** an existing table's schema (ADR 0013) — the in-place
    /// schema mutation originally behind CQL's `ALTER TABLE … ADD` (ADR 0006,
    /// since dropped by ADR 0053; appends columns to the current schema and
    /// replaces it wholesale). One command, one apply: no
    /// drop-then-recreate window in which a crash — or any reader of a replica
    /// that applied the drop but not yet the recreate — sees the table
    /// schema-less. Rejected if `table` has no schema (an ALTER cannot create a
    /// table) or if the replacement is malformed; a no-op if the schema is already
    /// identical (so a re-proposed ALTER does not churn the log).
    ReplaceTableSchema {
        table: TableName,
        schema: TableSchema,
    },
    /// Remove **every tablet scoped to `table`** from the tablet map, with their
    /// placement policies (ADR 0024 drop-table GC — the metadata half; each
    /// hosting node's GC loop reclaims its local group + engine files once the
    /// tablet leaves the map). Removes the whole set in one apply so no replica
    /// ever observes a table half-dropped. Idempotent: a no-op if the table has
    /// no tablets. The legacy whole-keyspace tablet (`table: None`) is never
    /// scoped to a table, so it is never touched.
    DropTableTablets { table: TableName },
    /// Add (or replace, by name) a **secondary index** definition on an existing
    /// table's schema (ADR 0013). Rejected if the table has no schema or if the
    /// resulting schema is malformed (duplicate index name reuse aside — an
    /// existing index of the same name is replaced — or an LSI with no sort
    /// attribute). Because it is a replicated `MetaCommand`, the index definition
    /// is durable and agreed cluster-wide. This carries the index *shape*; the
    /// index *entry data* (the actual indexed rows) is maintained at the wire edge.
    CreateTableIndex { table: TableName, index: IndexDef },
    /// Remove a secondary index definition from a table's schema (ADR 0013).
    /// Idempotent: a no-op if the table or the named index does not exist.
    DropTableIndex { table: TableName, index: String },
    /// Set a secondary index's lifecycle **status** (ADR 0045):
    /// `Creating`/`Active`/`Deleting`.
    /// Rejected if the table or the named index does not exist; a no-op if the
    /// index is already at `status`. In-place mutation via
    /// `TableSchema::set_index_status` — deliberately **not**
    /// `upsert_index`'s whole-struct replace, so a status transition never
    /// clobbers a concurrently-updated copy of the rest of the definition.
    SetIndexStatus {
        table: TableName,
        index: String,
        status: IndexStatus,
    },
    /// One tablet's completion signal for one index's backfill scan (ADR
    /// 0045 §4): "this tablet has finished seeding change-log coverage for
    /// this index." Proposed by the backfill seeder (a later PR, ADR 0045
    /// §2 step 5) once its forward sweep of `KIND_BASE` reaches the end of
    /// the tablet's current range. Idempotent insert into
    /// `Metadata::index_backfill`, keyed `(tablet, index)` — a repeat
    /// proposal (the seeder's own crash-retry) is a genuine `NoOp`. Rejected
    /// if `table` has no schema, if `index` does not name one of its current
    /// indexes, or if `tablet` is not currently scoped to `table` (a cheap
    /// `Metadata::tablets` lookup, unlike `DropTableTablets`'s own O(table's
    /// tablets) scan — see this variant's apply arm). That last check is not
    /// merely defensive: without it, a command that lands *after* its own
    /// tablet has already been dropped (a table/tablet-drop race with an
    /// in-flight seeder proposal) would insert a permanent orphan row that
    /// `DropTableTablets`'s own prune already ran past and will never revisit.
    MarkIndexBackfilled {
        table: TableName,
        index: String,
        tablet: TabletId,
    },
    /// Enable or disable a table's **DynamoDB Streams** configuration (ADR
    /// 0042 §2/§4/§9). Rejected if the table has no schema. Enabling
    /// (`spec: Some(..)`) is rejected if a stream is already enabled — the
    /// caller mints a fresh `label` only through an explicit disable →
    /// re-enable (never a same-command relabel), so `(table, label)` stays a
    /// stable identity for as long as the stream lives. Disabling
    /// (`spec: None`) is a no-op if no stream is enabled. Because this is a
    /// replicated `MetaCommand`, the stream configuration is durable and
    /// agreed cluster-wide, like the rest of the catalog.
    SetTableStream {
        table: TableName,
        spec: Option<StreamSpec>,
    },
    /// Enable, reconfigure, or disable a table's **DynamoDB-style TTL**
    /// configuration (ADR 0051). Rejected if the table has no schema.
    ///
    /// Unlike [`MetaCommand::SetTableStream`], TTL mints no identity label
    /// (see [`TtlSpec`]'s own doc for why), so the apply semantics are
    /// simpler and deliberately **not** modeled on the stream command's
    /// enabled-already-rejects-re-enable shape:
    /// - `spec: Some(new_spec)` where `new_spec` equals the currently
    ///   recorded spec (including the disabled → same-attribute-name
    ///   re-enable case) is a **no-op** — idempotent, since there is no
    ///   label to go stale.
    /// - `spec: Some(new_spec)` that names a *different* attribute than
    ///   what is currently recorded — including changing it in place while
    ///   already enabled — is **applied**: a live attribute-name change is
    ///   a legal DynamoDB `UpdateTimeToLive` call, not an error.
    /// - `spec: None` is a **no-op** if TTL is already disabled, else
    ///   applied.
    ///
    /// Because this is a replicated `MetaCommand`, the TTL configuration is
    /// durable and agreed cluster-wide, like the rest of the catalog.
    SetTableTtl {
        table: TableName,
        spec: Option<TtlSpec>,
    },
    /// Enable, reconfigure, or disable a table's **provisioned throughput**
    /// (ADR 0065 §5(b)) — the `CreateTable`/`UpdateTable` `BillingMode`/
    /// `ProvisionedThroughput` wire fields' own catalog mutation. Rejected
    /// if the table has no schema.
    ///
    /// Modeled directly on [`MetaCommand::SetTableTtl`]'s own apply
    /// semantics (`ProvisionedThroughput` mints no identity label either,
    /// see that type's own doc), **not** `SetTableStream`'s
    /// enabled-already-rejects-re-enable shape:
    /// - `spec: Some(new_spec)` where `new_spec` equals the currently
    ///   recorded spec is a **no-op** — idempotent, since there is no label
    ///   to go stale.
    /// - `spec: Some(new_spec)` that differs from what is currently
    ///   recorded — including changing the units in place while already
    ///   `PROVISIONED` — is **applied**: a live `UpdateTable
    ///   ProvisionedThroughput` change is a legal DynamoDB call, not an
    ///   error.
    /// - `spec: None` (`PAY_PER_REQUEST`) is a **no-op** if throughput is
    ///   already unset, else applied.
    ///
    /// Because this is a replicated `MetaCommand`, the throughput
    /// configuration is durable and agreed cluster-wide, like the rest of
    /// the catalog. `animusd`'s per-tablet throttle bucket (ADR 0065
    /// Decision 1) re-derives its own share from this field (or the
    /// cluster-wide default when `None`) on every refill — never cached
    /// across a change.
    SetTableThroughput {
        table: TableName,
        spec: Option<ProvisionedThroughput>,
    },
    /// Add or overwrite tags on a table (ADR-less, roadmap W-06): the
    /// `TagResource` wire operation's own catalog mutation. Rejected if the
    /// table has no schema. Modelled on [`MetaCommand::SetTableTtl`]'s own
    /// simplicity rather than [`MetaCommand::SetTableStream`]'s label
    /// discipline — a tag set has no identity to go stale, so there is
    /// nothing to reject: an existing key's value is overwritten (last
    /// writer wins, matching DynamoDB's own `TagResource` semantics), a new
    /// key is inserted, and the whole command is a **no-op** only when every
    /// given `(key, value)` pair already matches what is recorded (so a
    /// caller's retry of an already-applied `TagResource` is idempotent, the
    /// same shape `SetTableTtl`'s identical-spec case gets).
    TagResource {
        table: TableName,
        tags: BTreeMap<String, String>,
    },
    /// Remove tags from a table by key (the `UntagResource` wire
    /// operation's own catalog mutation). Rejected if the table has no
    /// schema. A no-op if none of the named keys are currently present
    /// (mirroring `TagResource`'s own idempotent-retry shape); a key not
    /// present is silently ignored rather than treated as an error, matching
    /// DynamoDB's own `UntagResource` behavior.
    UntagResource {
        table: TableName,
        tag_keys: Vec<String>,
    },
    /// Enable or disable a table's **point-in-time recovery (PITR)**
    /// configuration (ADR 0059 §9) — the `UpdateContinuousBackups` wire
    /// operation's own catalog toggle. Rejected if the table has no schema.
    ///
    /// `enabled: true`: a no-op if PITR is already enabled (re-enabling an
    /// already-enabled table is a legal, idempotent DynamoDB call — real
    /// AWS simply returns the current description); otherwise mints a fresh
    /// generation from [`Metadata::pitr_generation`]'s own never-rewound
    /// per-table floor (bumping it by exactly one) and records
    /// `TableSchema.pitr = Some(PitrSpec { generation, enabled_wall_ms:
    /// wall_ms })` — the ADR's "enable starts the window at now" rule.
    ///
    /// `enabled: false`: a no-op if PITR is already disabled; otherwise
    /// clears `TableSchema.pitr` (the allocator floor in
    /// `Metadata::pitr_generation` is deliberately left untouched, so a
    /// later re-enable never reuses this generation number — see that
    /// field's own doc). Existing `Metadata::pitr_segments` rows and any
    /// PITR base snapshot are **not** touched here — they survive at the
    /// catalog's own retention janitor's pace (ADR 0059 §9/§3), exactly
    /// like a disabled-but-still-draining stream's un-reaped rows.
    UpdateContinuousBackups {
        table: TableName,
        enabled: bool,
        /// `env.wall_now()` at propose time (ADR 0051's discipline) — the
        /// new generation's `PitrSpec::enabled_wall_ms` when `enabled` is
        /// `true`; ignored when `enabled` is `false`.
        wall_ms: u64,
    },
    /// Record a sealed stream shard segment in the replicated catalog (ADR
    /// 0042 §3/§9, ADR 0043 §A3/§A8) — the tablet leader's own commit of a
    /// `SegmentStore::put` it has already durably completed.
    ///
    /// **First-committer-wins on `(tablet, epoch)`'s CONTENT** (round-3 PR7
    /// amendment) — mirroring `CreateTablet`'s own race-safety shape,
    /// generalized from "first tablet" to "first seal of this epoch": a
    /// second proposal for an already-recorded `(tablet, epoch)` whose
    /// content (everything but `replicas`) matches exactly is a **no-op**
    /// if `replicas` also matches (the sealer's own crash-retry loop racing
    /// itself by design, ADR 0043 §A3: a crash before this command commits
    /// simply re-runs the whole seal, landing here again with the identical
    /// `(tablet, epoch)`), or a **replicas-only update, still `Applied`**,
    /// if `replicas` differs — the shape the segment janitor's own
    /// replica-repair sweep produces (ADR 0043 §A9): it re-proposes the
    /// identical committed shard with a freshly-repaired `replicas` set,
    /// never touching any other field. A proposal whose non-`replicas`
    /// content genuinely conflicts with what is already recorded is
    /// rejected as a no-op, exactly as the original design. The proposer is
    /// expected to log whichever outcome it gets itself, since this pure
    /// state machine performs no I/O.
    ///
    /// **Label validation (F12-b)**: `label` must be licensed either by the
    /// table's *current* schema `StreamSpec.label`, or by an existing
    /// catalog row already present for this exact `(table, label)` pair (a
    /// disabled stream's un-reaped rows still license a further seal of the
    /// same generation — e.g. the disable-triggered final seal itself,
    /// proposed after `SetTableStream{None}` has already cleared the
    /// schema's own `stream` field). A label matching **neither** is
    /// rejected — nothing ever licensed sealing under it.
    ///
    /// **Epoch-chain sanity**: `epoch == 0` is always accepted (a tablet's
    /// genuine root — a copy-based split child's own first seal starts its
    /// own chain at 0, ADR 0050). `epoch > 0` requires this tablet's own
    /// `epoch - 1` row to already exist — this guard's job is to catch a
    /// genuinely nonsensical gap, not to re-derive the sealer's own
    /// scheduling.
    SealStreamShard {
        table: TableName,
        label: String,
        tablet: TabletId,
        epoch: u64,
        /// This shard's own view type (PR6's catalog amendment) — see
        /// [`StreamShardRow::view_type`]'s doc for why it rides the row
        /// rather than being re-derived from the current schema.
        /// `#[serde(default)]` for the same reason the row field has it.
        #[serde(default = "default_stream_view_type")]
        view_type: StreamViewType,
        hlc_range: (u64, u64),
        count: u64,
        seal_wall_ms: u64,
        replicas: Vec<NodeId>,
        /// The unique per-attempt object id this proposal's own segment
        /// object was already durably written at (the ledger-named-object
        /// amendment — see [`StreamShardRow::object_id`]'s own doc). Part of
        /// this command's own "content" for the first-committer-wins
        /// comparison below: two attempts computed from different snapshots
        /// always mint different ids (even when every other field happens
        /// to agree), so this field is what makes a genuine race between
        /// independently-computed attempts a **content** mismatch — never
        /// silently treated as the identical-content no-op case that a
        /// true same-attempt retry (same id, reused) is.
        object_id: String,
    },
    /// The segment-janitor's two-phase reclaim of already-sealed catalog
    /// rows (ADR 0043 §A9), and the drop-table cascade's own removal path
    /// — reused directly rather than a separate drop-specific command,
    /// since "these rows should no longer exist" is the same fact whether
    /// the reason is retention or a table drop.
    ///
    /// `remove: false` **marks** every named `(tablet, epoch)` row
    /// `expired: true` (idempotent: a no-op for an absent row, or one
    /// already marked) — the janitor's own record that it has begun
    /// reclaiming that row's segment object, so a crash mid-sweep resumes
    /// by going straight to deleting objects rather than re-marking.
    /// `remove: true` **physically removes** every named row (idempotent: a
    /// no-op for an absent row, regardless of its `expired` flag — a
    /// drop-table cascade may remove a row that was never marked at all).
    /// One command, proposed twice by the janitor (mark, then — once every
    /// recorded replica has confirmed its segment object deleted — remove),
    /// is ADR 0043 §A9's whole "two-phase: mark expired, delete objects,
    /// drop rows" sequence; the object-deletion step in between is not
    /// itself a `MetaCommand` (it is a `SegmentStore::delete` call against
    /// each row's own recorded `replicas`, a later PR).
    ExpireStreamShards {
        rows: Vec<(TabletId, u64)>,
        remove: bool,
    },
    /// Record a sealed **PITR segment** in the replicated catalog (ADR 0059
    /// §9, Train 3) — the PITR consumer's own twin of
    /// [`SealStreamShard`](Self::SealStreamShard), sharing its exact
    /// first-committer-wins-on-content shape, replicas-only-update
    /// allowance, and epoch-chain sanity check, but validated against
    /// [`PitrSegmentRow::generation`] instead of a stream label.
    ///
    /// **Generation validation**: `generation` must be licensed either by
    /// the table's *current* `TableSchema.pitr.generation`, or by an
    /// existing [`Metadata::pitr_segments`] row already present for this
    /// exact `(table, generation)` pair (a disabled PITR's un-reaped rows
    /// still license a further seal of the same generation — the
    /// disable-triggered final seal, mirroring `SealStreamShard`'s F12-b
    /// label rule exactly). A generation matching neither is rejected.
    SealPitrSegment {
        table: TableName,
        generation: u64,
        tablet: TabletId,
        epoch: u64,
        hlc_range: (u64, u64),
        count: u64,
        seal_wall_ms: u64,
        replicas: Vec<NodeId>,
        object_id: String,
    },
    /// The PITR retention janitor's two-phase reclaim of already-sealed
    /// [`Metadata::pitr_segments`] rows (ADR 0059 §9) — the PITR twin of
    /// [`ExpireStreamShards`](Self::ExpireStreamShards), identical
    /// mark/remove semantics, applied to the separate PITR segment catalog.
    ExpirePitrSegments {
        rows: Vec<(TabletId, u64)>,
        remove: bool,
    },
    /// Register (or update) a **CP group member's address** (Phase 2): the
    /// `raftkv`-role listen address of member `id`, stored opaquely in
    /// [`Metadata::cp_member_addrs`] and replicated so every node's peer-sync loop
    /// can reach a runtime-created group member (a split sibling or a joined data
    /// node). Idempotent: a no-op if `id` already maps to `addr` (with the same
    /// tablet association).
    ///
    /// `tablet` (ADR 0024 GC, `#[serde(default)]` for older commands) associates
    /// the member with the tablet whose group it serves, so the address is
    /// **garbage-collected when that tablet leaves the map** (drop-table, merge)
    /// instead of leaking forever. `Some(tablet)` is rejected while the tablet is
    /// not in the map (the registrar's propose-and-await loop simply retries once
    /// it lands — the same convergent discipline as the file GC); `None` (legacy)
    /// registers an address that is never pruned.
    RegisterCpAddr {
        id: NodeId,
        addr: String,
        #[serde(default)]
        tablet: Option<TabletId>,
    },
    /// Update (or re-affirm) an **already-claimed** node's full address book
    /// (ADR 0032 PR1; tightened to update-only by ADR 0040 PR4): the
    /// client/admin/internal listen addresses of member `id`, stored in
    /// [`Metadata::node_addrs`]. Superset of [`MetaCommand::RegisterCpAddr`]
    /// for the client/admin axes — every already-established node proposes
    /// this at startup (and whenever an address changes) so any other node
    /// (including one that joined earlier and never restarted) can resolve
    /// it as a forward/relay target.
    ///
    /// **ADR 0040 Decision C**: this command **never claims a fresh id** —
    /// `id` must already be present in [`Metadata::members`] or
    /// [`Metadata::node_addrs`] (a config-bootstrapped member, or one already
    /// registered via [`MetaCommand::RegisterNode`]), or apply **rejects**
    /// it. [`MetaCommand::RegisterNode`] is now the *sole* path that inserts
    /// a brand-new member — see its own doc for why: an unguarded "blind
    /// idempotent insert" here would let two racing proposers for the same
    /// never-before-seen id land on two different `NodeAddrs` with no CAS to
    /// catch it, first-committer-silently-wins. Idempotent: a no-op if `id`
    /// already maps to an identical [`NodeAddrs`]; otherwise overwrites (an
    /// address genuinely changed, e.g. a replacement process on new ports).
    RegisterNodeAddrs { node: NodeId, addrs: NodeAddrs },
    /// **Decommission** a drained member (ADR 0032 PR3): the second half of
    /// `drain` (which only marks a member `Leaving` and lets the placement
    /// reconciler + rebalancer + release-GC relocate its tablets off it with no
    /// new mechanism). Applied under three preconditions, enforced here — at
    /// APPLY time, not just by whichever caller proposed it — because a racing
    /// second proposer's propose-time view could be stale:
    /// - member **absent**: idempotent no-op (`Applied`) — a retried removal
    ///   (e.g. the proposer's confirm timed out but the command actually
    ///   landed) converges instead of erroring;
    /// - member present but `status` is `Active`/`Joining`: **rejected** —
    ///   removing a still-serving member could strand a tablet's replication
    ///   factor with no warning;
    /// - [`Metadata::tablets_referencing`]`(node) > 0`: **rejected** — the
    ///   member is still a replica of some tablet, and removing it from
    ///   `members` would drop that tablet below its replication factor with
    ///   the member gone from the placement candidate pool entirely, with no
    ///   repair path left (placement can only choose from `Active` members).
    ///
    /// On success, `node` is pruned from `members` **and** its entries in
    /// [`Metadata::node_addrs`]/[`Metadata::cp_member_addrs`]/
    /// [`Metadata::cp_member_tablets`] are pruned in the same apply — mirroring
    /// the existing ADR 0024 GC discipline for tablet-scoped `cp_member_addrs`
    /// entries (keyed on current absence, so a replayed historical state can't
    /// resurrect a removed member's addresses).
    ///
    /// **Removal is not a fence**: it only stops this node's own automatic
    /// self-registration from ever re-asserting it (that happens once, at
    /// process startup) — a node whose *process* is restarted at the same
    /// raftkv id re-registers and rejoins exactly like a fresh join. The
    /// decommission flow's real last step is stopping the process.
    RemoveMember { node: NodeId },
    /// **Registration compare-and-swap** (ADR 0040 Decision C): the *sole*
    /// path that claims a fresh node identity, retiring ADR 0036's
    /// `AllocateNodeId` monotonic allocator entirely. `node` may be a
    /// self-minted random id ([`NodeId::mint`](animus_env::NodeId::mint)) or
    /// an operator-/config-proposed one
    /// ([`NodeId::propose`](animus_env::NodeId::propose)) — this command
    /// doesn't care which; uniqueness is enforced identically either way, by
    /// this apply, not by how the id was chosen.
    ///
    /// Apply semantics (evaluated identically on every replica, so a race
    /// between two proposers always resolves the same way everywhere) are
    /// keyed on [`Metadata::node_addrs`] **alone** — not on
    /// [`Metadata::members`], deliberately:
    /// - `node` **absent from `node_addrs`**: claims the address slot
    ///   (`Applied`), and inserts a [`Down`](NodeStatus::Down) [`Member`]
    ///   with `labels` too, *iff* `members` doesn't already have an entry for
    ///   `node` — an already-existing member row (from `UpsertMember`'s
    ///   bootstrap insert, or `admin_add_member`'s operator-labeled `Down`
    ///   row, both wholly decoupled commands that carry no address) is left
    ///   untouched, never overwritten with this call's own (possibly
    ///   label-less) view. The existing `Down → Active` promotion chain (ADR
    ///   0030 §1, the failure detector's first-heartbeat observation) is
    ///   unchanged from here either way.
    /// - `node` **present in `node_addrs`, byte-identical** to what's
    ///   proposed: `NoOp` (or `Applied` if it *also* had to repair a missing
    ///   `members` row) — the idempotent case, covering *both* a proposer's
    ///   retry after an accepted-but-unconfirmed propose (the
    ///   durable-before-visible discipline every proposer here must respect,
    ///   root `CLAUDE.md`) *and* the ADR 0032 same-identity rejoin (a
    ///   restarted process at the same operator-proposed id, registering the
    ///   identical address book again).
    /// - `node` **present in `node_addrs`, but a *different* entry**:
    ///   `Rejected` — the id's address is already held by someone else. A
    ///   caller with a **minted** id re-mints and retries (ports are never
    ///   derived from ids under this scheme, so nothing needs rebinding); a
    ///   caller with a **proposed** id fails loudly instead (an operator/config
    ///   collision is a real conflict to report, not to paper over).
    ///
    /// **Why `node_addrs`, not `members`, is the CAS key**: this command is
    /// the *one* self-registration call every node shape makes at startup —
    /// including a fresh bootstrap node whose id `bootstrap()`'s own
    /// `UpsertMember` also claims, a growth node whose id `admin_add_member`
    /// also claims, and (with no other claim path *at all*) a permanently-
    /// non-voter control-only growth node. Comparing `labels`/`members` here
    /// too would make this call race destructively against whichever of
    /// those decoupled commands wins first (a labels mismatch is not a real
    /// identity collision — it's just "some other command got here first
    /// with its own view of this member's labels"); the actual identity
    /// collision this CAS exists to prevent is always visible in
    /// `node_addrs` alone, since that's the one field only this command ever
    /// writes.
    ///
    /// **Never claims [`Metadata::members`] for a control-only registration**
    /// (`addrs.role == "control"`, plain string comparison — this crate never
    /// otherwise interprets `NodeAddrs.role`, but this one structural
    /// invariant is load-bearing enough to enforce here rather than trust
    /// every caller to preserve by convention): `members` is *data-plane*
    /// membership, and the placement engine's `active_candidates` (ADR 0005)
    /// treats any `Active` entry there as a real tablet-replica candidate — a
    /// control-only node has no `raftkv` role or storage engine and can never
    /// actually host one, so letting it appear in `members` at all (even
    /// `Down`, since the failure detector promotes any heartbeating `Down`
    /// entry on its own, ADR 0012) would silently corrupt placement the
    /// moment it's picked. `node_addrs` still claims normally either way —
    /// only the membership side effect is gated.
    ///
    /// An abandoned join attempt (the process crashes before ever becoming
    /// `Active`) leaves its claimed id `Down` forever — accepted, not a leak
    /// to fix: ids are never reused (mirroring tablet ids), and the entry is
    /// prunable through the existing [`MetaCommand::RemoveMember`] path
    /// exactly like any other drained, unreferenced member, once an operator
    /// notices it (an automatic sweep is future work, ADR 0040 PR6).
    RegisterNode {
        node: NodeId,
        addrs: NodeAddrs,
        labels: BTreeMap<String, String>,
    },
    /// Begin an on-demand backup (ADR 0059 §3/§4): mints a
    /// [`BackupStatus::Creating`] catalog row at `backup_id`. The manifest
    /// stub (schema snapshot + pinned tablet list) is derived **entirely
    /// from already-agreed `Metadata`** at apply time — never from anything
    /// the proposer carries beyond `backup_id`/`table`/`created_wall_ms`/
    /// `backup_name` — so every replica computes an identical stub. Rejected
    /// if `backup_id` already names a row (a fresh id is minted per request,
    /// so there is nothing to CAS against — first-committer-wins on a
    /// brand-new identity, mirroring [`CreateTablet`](Self::CreateTablet)'s
    /// own shape) or if `table` has no schema.
    BeginBackup {
        /// The freshly-minted, never-reused backup identity (an ARN-shaped
        /// string at the wire, ADR 0059 §3).
        backup_id: BackupId,
        /// The source table to back up.
        table: TableName,
        /// Stamped at PROPOSE time by the wire-serving node
        /// (`env.wall_now()`, ADR 0051's discipline) — the pure state
        /// machine has no clock.
        created_wall_ms: u64,
        /// The client-supplied `BackupName` (ADR 0059 Train 1 PR④, DynamoDB's
        /// `CreateBackup` request field) — recorded verbatim in
        /// [`BackupRow::backup_name`] purely for later `DescribeBackup`/
        /// `ListBackups` echo; never interpreted, never part of the backup's
        /// own identity (that's `backup_id` alone, per this catalog's own
        /// "scar," [`Metadata::backups`]'s doc).
        backup_name: String,
        /// Whether this backup is a **PITR base snapshot** (ADR 0059 §9)
        /// rather than an ordinary on-demand one — set by the PITR periodic
        /// snapshot loop, `false` for every other proposer (an explicit
        /// `CreateBackup`, the admin seeder, every existing test fixture).
        /// `true` inserts `backup_id` into [`Metadata::pitr_base_backups`]
        /// **in this same apply**, atomically with minting the row itself
        /// (issue #593) — replacing a now-deleted two-command sequence
        /// (`BeginBackup` followed by a separate `MetaCommand::
        /// MarkBackupPitrBase`) that left a PITR base snapshot observable,
        /// for the instant between the two commits, as an ordinary untagged
        /// user backup (a `ListBackups` default `USER` filter, or the
        /// console's per-table backups projection, could catch it in that
        /// window). See [`Metadata::pitr_base_backups`]'s own doc and
        /// `docs/adr/0059-backup-restore.md` §9's 2026-09-04 as-built
        /// amendment for the full incident this closes.
        pitr_base: bool,
    },
    /// One pinned tablet's capture-completion report (ADR 0059 §3/§4) —
    /// mirroring [`MarkIndexBackfilled`](Self::MarkIndexBackfilled)'s
    /// per-tablet shape exactly, including its identity convention: keyed
    /// `(backup_id, tablet)`, not `(table, backup_id, tablet)` (a tablet id
    /// already implies its table). Idempotent: an identical repeat (the
    /// capture driver's own crash-retry) is a `NoOp`. Rejected if
    /// `backup_id` is unknown, the backup is not
    /// [`Creating`](BackupStatus::Creating), `tablet` does not name one of
    /// the backup's own pinned tablets, or a *different* completion is
    /// already recorded for this `(backup_id, tablet)` (a genuine conflict
    /// — unlike `SealStreamShard`'s replicas-only-update allowance, this
    /// row has no repair concept in this PR, so a conflicting resubmission
    /// is rejected outright rather than silently overwritten).
    RecordBackupTabletComplete {
        /// The backup this completion belongs to.
        backup_id: BackupId,
        /// The pinned tablet reporting completion.
        tablet: TabletId,
        /// The packed-HLC watermark this tablet's own capture pinned.
        cut_version: u64,
        /// The total bytes this tablet's own data objects occupy.
        bytes: u64,
    },
    /// Complete a backup (ADR 0059 §3/§4) once every pinned tablet has
    /// reported — proposed by the control-plane-leader aggregator (a later
    /// PR). Rejected if `backup_id` is unknown, not
    /// [`Creating`](BackupStatus::Creating), or any pinned tablet has not
    /// yet reported a completion record. Flips the row to
    /// [`BackupStatus::Available`] — DynamoDB's own terminal on-demand
    /// status.
    CompleteBackup {
        /// The backup to complete.
        backup_id: BackupId,
    },
    /// Fail a backup (ADR 0059 §3/§4) — a stuck-`Creating` timeout, or any
    /// other aggregator-observed failure. Rejected if `backup_id` is
    /// unknown, or if the row is already terminal in a way that
    /// contradicts failure (already [`Available`](BackupStatus::Available)
    /// — a completed backup cannot subsequently "fail" — or already
    /// [`Expired`](BackupStatus::Expired)). Idempotent: a no-op if the row
    /// is already `Failed` with the identical `reason`.
    FailBackup {
        /// The backup to fail.
        backup_id: BackupId,
        /// A human-readable failure reason.
        reason: String,
    },
    /// Remove a backup catalog row outright (ADR 0059 §3) — an operator/
    /// retention action, distinct from [`CompleteBackup`](Self::CompleteBackup)/
    /// [`FailBackup`](Self::FailBackup). Idempotent: a no-op if `backup_id`
    /// is unknown. Also prunes every one of its per-tablet completion rows
    /// from [`Metadata::backup_tablet_progress`]. Deliberately **never**
    /// reached as a side effect of [`DropTableSchema`](Self::DropTableSchema)/
    /// [`DropTableTablets`](Self::DropTableTablets) — ADR 0024's explicit
    /// carve-out (ADR 0059 §3): a backup catalog row outlives its source
    /// table.
    DeleteBackup {
        /// The backup to remove.
        backup_id: BackupId,
    },
    /// Mark a backup for reclaim (ADR 0059 §3, Train 1 PR④) — the two-phase
    /// retention janitor's own **mark** step (ADR 0043 §A9's mold, reused
    /// verbatim), driven here by the `DeleteBackup` wire operation
    /// (`animusd::dynamo`) rather than by any retention clock (on-demand
    /// backups never auto-expire): flips [`Available`](BackupStatus::Available)
    /// or [`Failed`](BackupStatus::Failed) to
    /// [`Expired`](BackupStatus::Expired), the same terminal-reclaim state
    /// [`BackupStatus::Expired`]'s own doc already reserved for exactly this
    /// purpose. The row itself is **not** removed here — [`DeleteBackup`]
    /// (Self::DeleteBackup) is the existing, unmodified **finalizing**
    /// command the backup janitor (`animusd::backup_janitor`) proposes once
    /// it has reclaimed every object this backup's manifest/data occupy in
    /// the backup store. Idempotent: a no-op if the row is already `Expired`
    /// (the janitor's own crash-resume, or a repeated `DeleteBackup` wire
    /// call, must never re-mark). Rejected if `backup_id` is unknown, or if
    /// the row is still [`Creating`](BackupStatus::Creating) — the wire
    /// edge's own `BackupInUseException` check happens first in the common
    /// case, but this is the apply-time seatbelt for any other caller,
    /// mirroring every other command's defense-in-depth precondition.
    MarkBackupDeleted {
        /// The backup to mark for reclaim.
        backup_id: BackupId,
    },
    /// Begin a restore (ADR 0059 §7, Train 2): mints a fresh
    /// [`RestoreRow`] in [`RestoreStatus::Seeding`] plus this restore's
    /// single `Building` tablet, bound to `target_table` (whose schema must
    /// already exist — the wire caller proposes `CreateTableSchema`/
    /// `CreateTableIndex` for the LSIs first, exactly the ordering
    /// `provision_tablet` already requires of `CreateTable`). Rejected if
    /// `restore_id` already exists (first-committer-wins, mirroring
    /// [`BeginBackup`](Self::BeginBackup)'s own fresh-identity collision
    /// case) or if `tablet` already exists or sits below the monotonic
    /// allocator floor (the identical seatbelt
    /// [`BeginSplitInPlace`](Self::BeginSplitInPlace)/
    /// [`CreateTablet`](Self::CreateTablet) already enforce). No epoch-CAS:
    /// like `BeginBackup`, there is nothing
    /// to CAS against for a freshly-minted identity.
    BeginRestore {
        /// This restore's own opaque identity.
        restore_id: RestoreId,
        /// The backup being restored from.
        backup_id: BackupId,
        /// The backup's own source table (descriptive only, see
        /// [`RestoreRow::source_table`]'s doc).
        source_table: TableName,
        /// The new table being populated.
        target_table: TableName,
        /// This restore's single destination tablet id (caller-allocated,
        /// same convention as `CreateTablet`/`BeginSplitInPlace`'s own children).
        tablet: TabletId,
        /// The destination tablet's initial replica set.
        replicas: Vec<NodeId>,
        /// The GSI definitions to create once this restore completes — see
        /// [`RestoreRow::gsi_defs`]'s own doc for why these are recorded now
        /// but not declared on the schema until then.
        gsi_defs: Vec<IndexDef>,
        /// `Some` for a `RestoreTableToPointInTime` restore (ADR 0059 §10,
        /// Train 3 PR②) — see [`RestoreRow::pitr`]'s own doc. Carried
        /// verbatim onto the new [`RestoreRow`], never inspected or
        /// recomputed by this apply arm.
        pitr: Option<PitrRestorePlan>,
    },
    /// Complete a restore (ADR 0059 §7 step 5): activates this restore's
    /// tablet (`Building` → `Active`, epoch bumped — mirroring
    /// [`CutoverSplit`](Self::CutoverSplit)'s own activation, minus the
    /// "retire a parent" half, since restore has no parent) and flips the
    /// row to [`RestoreStatus::Done`]. Rejected if `restore_id` is unknown,
    /// not currently [`Seeding`](RestoreStatus::Seeding), or its tablet is
    /// not `Building` (defense-in-depth against a doubly-proposed
    /// completion racing something else entirely — this command's own
    /// producer, the restore driver, `animusd::backup_restore`, only ever
    /// proposes it once per restore in practice).
    CompleteRestore {
        /// The restore to complete.
        restore_id: RestoreId,
    },
    /// Fail a restore past a bounded stuck-timeout, or on an unreadable
    /// source backup (ADR 0059 §7's crash/liveness contract — mirroring
    /// [`FailBackup`](Self::FailBackup)'s own idempotent terminal-transition
    /// shape). Idempotent on an identical repeat; contradicts (rejects) an
    /// already-[`Done`](RestoreStatus::Done) row. The tablet is
    /// deliberately left `Building` (never activated, never routable) —
    /// see [`RestoreStatus::Failed`]'s own doc for why this still leaves a
    /// cleanly deletable target table.
    FailRestore {
        /// The restore to fail.
        restore_id: RestoreId,
        /// A human-readable failure reason.
        reason: String,
    },
}

/// The deterministic result of applying a [`MetaCommand`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The command changed state.
    Applied,
    /// The command was a no-op by design.
    NoOp,
    /// The command's precondition failed; state is unchanged.
    Rejected(&'static str),
}

/// The subset of [`Metadata`] the placement reconciler actually reads (ADR 0005):
/// members (for liveness/labels), the tablet map, and the per-tablet policies —
/// **not** the schema catalog or the CP address book, which dominate a grown
/// `Metadata`'s size. The leader's `reconcile_loop` clones this narrow view under
/// the `RaftCore` lock (via `RaftCore::placement_view`) and evaluates
/// [`reconcile`](PlacementView::reconcile) **off the lock**, instead of cloning
/// the whole blob every tick (the clone-churn fix).
#[derive(Clone, Debug)]
pub struct PlacementView {
    /// Cluster membership (liveness + topology labels).
    pub members: BTreeMap<NodeId, Member>,
    /// The tablet map.
    pub tablets: BTreeMap<TabletId, Tablet>,
    /// Per-tablet placement policies.
    pub policies: BTreeMap<TabletId, PlacementPolicy>,
    /// The directed-Placing catalog (ADR 0062 §2) — the reconcile loop's
    /// third phase needs it alongside `tablets`/`policies` to know which
    /// un-`done` children still need converging.
    pub split_placing: BTreeMap<TabletId, SplitPlacing>,
}

impl PlacementView {
    /// The pure placement decision over this view — identical to
    /// [`Metadata::reconcile`] (both delegate to the same body).
    #[must_use]
    pub fn reconcile(&self) -> Vec<MetaCommand> {
        reconcile_placement(
            &self.members,
            &self.tablets,
            &self.policies,
            &self.split_placing,
        )
    }

    /// The pure load-rebalancing decision over this view — identical to
    /// [`Metadata::rebalance`] (both delegate to the same body).
    #[must_use]
    pub fn rebalance(&self) -> Option<MetaCommand> {
        rebalance_placement(
            &self.members,
            &self.tablets,
            &self.policies,
            &self.split_placing,
        )
    }

    /// The pure directed-Placing convergence decision (ADR 0062 §2) —
    /// identical to [`Metadata::split_placing_reconcile`]. `retarget_ready`
    /// is the driver's own dwell-gate decision (`node.rs`'s
    /// `retarget_ready_this_tick`) — see the free function's own doc for
    /// the full per-entry state machine (issue #528 fix).
    #[must_use]
    pub fn split_placing_reconcile(&self, retarget_ready: &BTreeSet<TabletId>) -> Vec<MetaCommand> {
        split_placing_reconcile(
            &self.members,
            &self.tablets,
            &self.policies,
            &self.split_placing,
            retarget_ready,
        )
    }
}

/// Build placement candidates from the `Active` members and their labels.
/// Liveness is the control plane's job (ADR 0005): only `Active` members are
/// offered to the placement engine, which then enforces *policy* (residency
/// + spread). Iteration is over a `BTreeMap`, so the order is deterministic.
fn active_candidates(members: &BTreeMap<NodeId, Member>) -> Vec<Candidate> {
    members
        .iter()
        .filter(|(_, m)| m.status == NodeStatus::Active)
        .map(|(id, m)| Candidate::new(id.clone(), m.labels.clone()))
        .collect()
}

/// The shared body of [`Metadata::reconcile`] / [`PlacementView::reconcile`]: a
/// pure, deterministic function of exactly the placement-relevant maps, so the
/// caller can evaluate it on a narrow clone off the `RaftCore` lock.
fn reconcile_placement(
    members: &BTreeMap<NodeId, Member>,
    tablets: &BTreeMap<TabletId, Tablet>,
    policies: &BTreeMap<TabletId, PlacementPolicy>,
    split_placing: &BTreeMap<TabletId, SplitPlacing>,
) -> Vec<MetaCommand> {
    let candidates = active_candidates(members);
    policies
        .iter()
        .filter_map(|(tablet, policy)| {
            let t = tablets.get(tablet)?;
            // ADR 0050 (fork F5 rider): placement is frozen for a tablet
            // mid-split — a `Building` child must not be moved while the
            // split driver seeds it, and a `Splitting` parent is torn down
            // at cutover anyway.
            if t.state != TabletState::Active {
                return None;
            }
            // ADR 0062 §2 (issue #528 fix): an un-done directed-Placing
            // obligation owns this tablet's convergence exclusively —
            // the dwell-gated placing phase is the sole mover until
            // `done`, so repair must not independently retarget it too
            // (the same exclusion `rebalance_placement` already applies,
            // below).
            if split_placing.get(tablet).is_some_and(|entry| !entry.done) {
                return None;
            }
            let desired = replan(&t.replicas, &candidates, policy).ok()?;
            // `replan` returns a sorted set; `t.replicas` is normalized
            // (sorted + deduped) by `Tablet::new` / `CasTabletReplicas`, so a
            // direct comparison is a faithful "already satisfied" check.
            if desired == t.replicas {
                None
            } else {
                Some(MetaCommand::CasTabletReplicas {
                    tablet: *tablet,
                    expected_epoch: t.epoch,
                    replicas: desired,
                })
            }
        })
        .collect()
}

/// The shared body of [`Metadata::rebalance`] / [`PlacementView::rebalance`]: the
/// pure load-rebalancing decision (ADR 0029). Builds the
/// `(TabletId, &[NodeId], &PlacementPolicy)` slice for every policied tablet and
/// asks [`rebalance_step`] for a single balance-improving move, wrapping it as a
/// `CasTabletReplicas` at the tablet's current epoch. Returns at most one command
/// per call; `None` when the cluster is already balanced or no policy-legal move
/// exists. Deterministic (only `BTreeMap` iteration + the pure planner), so every
/// replica agrees — though only the leader ever *proposes* the result.
///
/// **ADR 0062 §2's ordering exclusion**: a tablet carrying an un-`done`
/// [`split_placing`](Metadata::split_placing) entry is skipped here too, the
/// same way a non-`Active` tablet already is — a freshly-cutover child is
/// already policy-satisfying on its inherited replicas (invisible to
/// `reconcile_placement`'s violation-driven repair) but not yet at its
/// directed-Placing target, so leaving it eligible for ordinary balance-driven
/// rebalance would race the faster, dedicated Placing phase for the same
/// tablet's epoch on the same tick — harmless (the loser's CAS just rejects),
/// but avoidable churn. Once `done` flips, the tablet rejoins this population
/// like any other.
fn rebalance_placement(
    members: &BTreeMap<NodeId, Member>,
    tablets: &BTreeMap<TabletId, Tablet>,
    policies: &BTreeMap<TabletId, PlacementPolicy>,
    split_placing: &BTreeMap<TabletId, SplitPlacing>,
) -> Option<MetaCommand> {
    let candidates = active_candidates(members);
    let entries: Vec<(TabletId, &[NodeId], &PlacementPolicy)> = policies
        .iter()
        .filter_map(|(tablet, policy)| {
            let t = tablets.get(tablet)?;
            // ADR 0050: same mid-split placement freeze as `reconcile_placement`.
            if t.state != TabletState::Active {
                return None;
            }
            // ADR 0062 §2: an un-done directed-Placing obligation owns this
            // tablet's convergence exclusively until it finishes.
            if split_placing.get(tablet).is_some_and(|entry| !entry.done) {
                return None;
            }
            Some((*tablet, t.replicas.as_slice(), policy))
        })
        .collect();
    let (tablet, replicas) = rebalance_step(&entries, &candidates)?;
    let epoch = tablets.get(&tablet)?.epoch;
    Some(MetaCommand::CasTabletReplicas {
        tablet,
        expected_epoch: epoch,
        replicas,
    })
}

/// The shared body of [`Metadata::split_placing_reconcile`] /
/// [`PlacementView::split_placing_reconcile`]: ADR 0062 §2's third
/// reconcile-loop phase, fixed for issue #528 (see that field's doc and the
/// ADR's 2026-09-01 amendment for the full incident).
///
/// For every un-`done` [`SplitPlacing`] entry:
///
/// - **`target: Some(list)`, every member of `list` currently `Active`**:
///   drive toward `list` **verbatim** — propose a `CasTabletReplicas`
///   iff `list` differs from the tablet's current replicas. The target is
///   never recomputed while it is healthy, which is what makes it stable
///   under ordinary failure-detector flap (the root cause this rung fixes:
///   a fresh `select_replicas` answer every tick moves the target faster
///   than `animus-cp-data`'s learner-phased mover can complete a cycle).
/// - **`target: Some(list)`, some member of `list` NOT currently `Active`,
///   but `child` is not in `retarget_ready`**: paused — propose nothing
///   this tick. `retarget_ready` is the driver's own dwell-gate decision
///   (`node.rs`'s `retarget_ready_this_tick`, `Env`-time based, never a
///   pure function of `Metadata` alone) — a transiently-`Down` target
///   member must not itself trigger a retarget, only a continuously-down
///   one past the dwell window. Serving is unaffected either way (fork A:
///   this never gates serving).
/// - **`target: Some(list)`, some member of `list` NOT currently `Active`,
///   AND `child` IS in `retarget_ready`**: the dwell has elapsed for at
///   least one member of `list` — recompute via [`replan`] (not
///   `select_replicas`: `list` is the "current" set here, so a still-live
///   member of it is kept and only the genuinely-gone one is replaced,
///   minimizing churn exactly the way ordinary repair already does for
///   ANY tablet). A differing result proposes
///   [`MetaCommand::RetargetSplitPlacing`] — a REPLICATED update of the
///   stored target, so the new value is itself stable next tick, never a
///   direct `CasTabletReplicas` (that still waits for a future tick once
///   the new target is stored and found fully `Active`).
/// - **`target: None`** (unsatisfiable at cutover, fork B): no stored
///   target to protect, so keep retrying every tick regardless of
///   `retarget_ready` — [`select_replicas`] fresh, and a success proposes
///   `RetargetSplitPlacing` to establish the first stored target. A
///   still-unsatisfiable recomputation is silently skipped this tick and
///   re-attempted next tick, restating fork B's stance.
///
/// A tablet whose row no longer exists (the table was dropped —
/// `DropTableTablets` prunes the `split_placing` row itself, so this is
/// likely unreachable in practice, but is guarded defensively anyway) or
/// which inherited no policy is skipped outright — nothing to converge
/// toward. Deterministic given its inputs (only `BTreeMap` iteration + the
/// pure planners) — `retarget_ready` itself is driver-local, `Env`-time
/// state, but every replica that computes this function agrees given the
/// same inputs, and only the leader ever *proposes* the result. Unlike
/// [`rebalance_placement`], this returns **every** eligible move in one
/// call, not just one — ADR 0062 §2's own pseudocode bounds churn per entry
/// (at most one command per un-done entry per tick), not globally across
/// entries, since a split-triggered relief obligation should not queue
/// behind an unrelated split's own convergence.
fn split_placing_reconcile(
    members: &BTreeMap<NodeId, Member>,
    tablets: &BTreeMap<TabletId, Tablet>,
    policies: &BTreeMap<TabletId, PlacementPolicy>,
    split_placing: &BTreeMap<TabletId, SplitPlacing>,
    retarget_ready: &BTreeSet<TabletId>,
) -> Vec<MetaCommand> {
    let candidates = active_candidates(members);
    let active: BTreeSet<NodeId> = candidates.iter().map(|c| c.node.clone()).collect();
    split_placing
        .iter()
        .filter(|(_, entry)| !entry.done)
        .filter_map(|(&child, entry)| {
            let t = tablets.get(&child)?;
            let policy = policies.get(&child)?;
            match &entry.target {
                Some(target) if target.iter().all(|m| active.contains(m)) => {
                    if *target == t.replicas {
                        None
                    } else {
                        Some(MetaCommand::CasTabletReplicas {
                            tablet: child,
                            expected_epoch: t.epoch,
                            replicas: target.clone(),
                        })
                    }
                }
                Some(target) if retarget_ready.contains(&child) => {
                    let fresh = replan(target, &candidates, policy).ok()?;
                    if Some(&fresh) == entry.target.as_ref() {
                        None
                    } else {
                        Some(MetaCommand::RetargetSplitPlacing {
                            tablet: child,
                            expected_epoch: t.epoch,
                            target: Some(fresh),
                        })
                    }
                }
                Some(_) => None, // paused: dwelling on a down target member
                None => {
                    let fresh = select_replicas(&candidates, policy).ok()?;
                    Some(MetaCommand::RetargetSplitPlacing {
                        tablet: child,
                        expected_epoch: t.epoch,
                        target: Some(fresh),
                    })
                }
            }
        })
        .collect()
}

impl Metadata {
    /// The narrow placement view ([`PlacementView`]) — clones only the
    /// placement-relevant maps, never the schema catalog / CP address book.
    #[must_use]
    pub fn placement_view(&self) -> PlacementView {
        PlacementView {
            members: self.members.clone(),
            tablets: self.tablets.clone(),
            policies: self.policies.clone(),
            split_placing: self.split_placing.clone(),
        }
    }

    /// Recompute placement for every tablet that has a policy and return the
    /// [`CasTabletReplicas`](MetaCommand::CasTabletReplicas) commands needed to
    /// bring the cluster into compliance — only for tablets whose current set
    /// already violates the policy (a member went `Down`/`Leaving`, or the set
    /// otherwise no longer satisfies residency + spread).
    ///
    /// This is a **pure, deterministic** function of the metadata: it does no
    /// I/O, draws no randomness, and iterates over `BTreeMap`s, so every replica
    /// (and a replay) computes the same proposals. The leader's reconciler
    /// (`node.rs`) calls it on a timer and proposes the result through Raft; a
    /// tablet already satisfying its policy yields nothing, so the loop is
    /// **idempotent** (no churn at steady state). A tablet whose policy cannot be
    /// satisfied with the current candidates (e.g. too few eligible nodes) is
    /// skipped, leaving the existing replicas in place rather than shrinking the
    /// set. **ADR 0062 §2 (issue #528 fix)**: a tablet carrying an un-`done`
    /// [`split_placing`](Self::split_placing) entry is skipped here too — the
    /// dwell-gated directed-Placing phase
    /// ([`split_placing_reconcile`](Self::split_placing_reconcile)) is the
    /// sole mover for that tablet until `done`, so this repair pass must not
    /// independently retarget it (the same exclusion
    /// [`rebalance`](Self::rebalance) already applies).
    #[must_use]
    pub fn reconcile(&self) -> Vec<MetaCommand> {
        reconcile_placement(
            &self.members,
            &self.tablets,
            &self.policies,
            &self.split_placing,
        )
    }

    /// The single load-rebalancing move to make right now (ADR 0029), or `None`
    /// if the cluster is already balanced or no policy-legal move exists.
    ///
    /// Where [`reconcile`](Self::reconcile) is **violation-driven** (moves a
    /// replica off a `Down`/ineligible node), this is **balance-driven**: it moves
    /// a *healthy* replica from a most-loaded node onto a least-loaded one so a
    /// cluster grown from N to M members spreads its existing tablets onto the new
    /// members (the reconciler never does, since surviving eligible replicas are
    /// pinned). Also **pure + deterministic**, returning at most one
    /// `CasTabletReplicas` per call — a deliberate one-CAS-per-tick churn bound;
    /// the leader's `reconcile_loop` calls it (paced) only once repair had nothing
    /// to do. Safety rests on the epoch-CAS (a stale move is epoch-rejected) and the
    /// data-plane catch-up gate, not on the cadence.
    #[must_use]
    pub fn rebalance(&self) -> Option<MetaCommand> {
        rebalance_placement(
            &self.members,
            &self.tablets,
            &self.policies,
            &self.split_placing,
        )
    }

    /// The directed-Placing convergence phase (ADR 0062 §2, fixed for issue
    /// #528): for every un-`done` [`split_placing`](Self::split_placing)
    /// entry, drive toward its STORED target while every member of it is
    /// `Active`, pause while a member is transiently down, and only
    /// recompute (via a replicated [`MetaCommand::RetargetSplitPlacing`])
    /// once `retarget_ready` (the driver's own dwell-gate decision) says a
    /// member has been down long enough to treat as genuinely gone — see
    /// the free `split_placing_reconcile` function's own doc for the full
    /// per-entry state machine. Also **pure + deterministic given its
    /// inputs**; the leader's `reconcile_loop` runs this every tick,
    /// unconditionally — no `REBALANCE_EVERY_N_TICKS`-style throttle, since
    /// a split child's own relief obligation should not wait behind a
    /// cadence meant for slow, cluster-wide balance churn. Returns one
    /// command per eligible un-done entry (not capped to one total per
    /// call, unlike [`rebalance`](Self::rebalance) — see
    /// [`split_placing_reconcile`]'s own doc for why that's the correct
    /// bound here). `MarkSplitPlacingDone` is never proposed from here — a
    /// separate, later mechanism (ADR 0062 §3) observes live Raft
    /// convergence, which this pure metadata-level function has no way to
    /// see.
    #[must_use]
    pub fn split_placing_reconcile(&self, retarget_ready: &BTreeSet<TabletId>) -> Vec<MetaCommand> {
        split_placing_reconcile(
            &self.members,
            &self.tablets,
            &self.policies,
            &self.split_placing,
            retarget_ready,
        )
    }

    /// Apply a command, returning the (deterministic) outcome.
    pub fn apply(&mut self, command: &MetaCommand) -> ApplyOutcome {
        match command {
            MetaCommand::NoOp => ApplyOutcome::NoOp,
            MetaCommand::UpsertMember {
                node,
                labels,
                status,
            } => {
                // `has_activated` is sticky (ADR 0040 PR6): once a member has
                // ever been recorded `Active` — by any caller, the ADR 0012
                // detector's `Down`→`Active` promotion or `bootstrap`'s direct
                // `Active` insert alike — it stays `true` forever, regardless
                // of any later status this same command (or a future one)
                // sets. See `Member::has_activated`'s own doc for why this is
                // computed here, structurally, rather than left to whichever
                // caller happens to drive a promotion.
                let has_activated = self.members.get(node).is_some_and(|m| m.has_activated)
                    || *status == NodeStatus::Active;
                self.members.insert(
                    node.clone(),
                    Member {
                        labels: labels.clone(),
                        status: *status,
                        has_activated,
                    },
                );
                ApplyOutcome::Applied
            }
            MetaCommand::CreateTablet {
                tablet,
                table,
                range,
                replicas,
            } => {
                if self.tablets.contains_key(tablet) {
                    ApplyOutcome::Rejected("tablet already exists")
                } else if table
                    .as_deref()
                    .is_some_and(|t| self.tablets_for_table(t).next().is_some())
                {
                    // One `CreateTablet` per table (ADR 0023): the *first* tablet is
                    // provisioned at `CreateTable`; further tablets of a table come
                    // only from `SplitTablet`. This makes provision-at-create
                    // race-safe — two nodes racing with different allocated ids both
                    // propose `CreateTablet` for the table, but only the first applies.
                    ApplyOutcome::Rejected("table already has a tablet")
                } else {
                    self.tablets.insert(
                        *tablet,
                        Tablet::with_table(*tablet, table.clone(), range.clone(), replicas.clone()),
                    );
                    self.next_tablet_id = self.next_tablet_id.max(tablet.0 + 1);
                    ApplyOutcome::Applied
                }
            }
            MetaCommand::CasTabletReplicas {
                tablet,
                expected_epoch,
                replicas,
            } => match self.tablets.get_mut(tablet) {
                None => ApplyOutcome::Rejected("no such tablet"),
                Some(t) if t.epoch != *expected_epoch => ApplyOutcome::Rejected("epoch mismatch"),
                Some(t) => {
                    let mut replicas = replicas.clone();
                    replicas.sort_unstable();
                    replicas.dedup();
                    t.replicas = replicas;
                    t.epoch = t.epoch.next();
                    ApplyOutcome::Applied
                }
            },
            MetaCommand::BeginSplitInPlace {
                parent,
                expected_epoch,
                split_key,
                children,
            } => {
                let [(left_id, left_replicas), (right_id, right_replicas)] = children;
                if left_id == right_id {
                    return ApplyOutcome::Rejected("child ids must be distinct");
                }
                for id in [left_id, right_id] {
                    if self.tablets.contains_key(id) {
                        return ApplyOutcome::Rejected("child tablet id already exists");
                    }
                    // Same monotonic-allocator floor as `CreateTablet`/
                    // `BeginRestore` (and for the same dropped-data-
                    // resurrection reason).
                    if id.0 < self.next_free_tablet_id().0 {
                        return ApplyOutcome::Rejected(
                            "child tablet id below the monotonic allocator",
                        );
                    }
                }
                let Some(source) = self.tablets.get(parent) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                // Epoch-CAS (the `CasTabletReplicas` discipline) plus the
                // state gate: only an `Active` tablet may begin a split — a
                // `Splitting` parent is already mid-workflow (one split at a
                // time) and a `Building` tablet has no committed contents to
                // split.
                if source.epoch != *expected_epoch {
                    return ApplyOutcome::Rejected("epoch mismatch");
                }
                if source.state != TabletState::Active {
                    return ApplyOutcome::Rejected("tablet is not Active");
                }
                // F11 seatbelt (ADR 0042 §14, growth PR2).
                if split_key.len() != TOKEN_BYTES
                    && source
                        .table
                        .as_deref()
                        .is_some_and(|table| self.table_stream(table).is_some())
                {
                    return ApplyOutcome::Rejected(
                        "split key not token-aligned for a streamed table",
                    );
                }
                // Validate the split key is strictly inside the parent's own
                // range (`KeyRange::split_at`'s own "strictly inside" guard)
                // WITHOUT keeping the halves — the in-place workflow derives
                // each child's own range later, from this same split key,
                // wherever it's actually needed (the data plane's own
                // `SplitTablet` apply, and this command's own `CutoverSplit`
                // counterpart below); there is no tablet-map row to place
                // them on yet.
                if source.range.split_at(split_key).is_none() {
                    return ApplyOutcome::Rejected("split key not strictly inside range");
                }
                let intent = InPlaceSplitIntent {
                    split_key: split_key.clone(),
                    children: [
                        SplitChild {
                            id: *left_id,
                            replicas: left_replicas.clone(),
                        },
                        SplitChild {
                            id: *right_id,
                            replicas: right_replicas.clone(),
                        },
                    ],
                };
                self.next_tablet_id = self.next_tablet_id.max(left_id.0 + 1);
                self.next_tablet_id = self.next_tablet_id.max(right_id.0 + 1);
                let source = self.tablets.get_mut(parent).expect("tablet present");
                source.state = TabletState::Splitting;
                source.inplace_split = Some(intent);
                source.epoch = source.epoch.next();
                ApplyOutcome::Applied
            }
            MetaCommand::CutoverSplit {
                parent,
                expected_epoch,
                cutover_wall_ms,
            } => {
                let Some(source) = self.tablets.get(parent) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                if source.epoch != *expected_epoch {
                    return ApplyOutcome::Rejected("epoch mismatch");
                }
                if source.state != TabletState::Splitting {
                    return ApplyOutcome::Rejected("tablet is not Splitting");
                }
                // An in-place split's parent carries its own intent directly
                // — the children are already fully formed on every fork
                // participant (the data plane's own `SplitTablet` apply), so
                // this arm creates their tablet-map rows straight from the
                // intent. `BeginSplitInPlace` is now the sole path into
                // `TabletState::Splitting` (the copy-based workflow's own
                // minter, `BeginSplit`, was deleted in the copy-split
                // deletion stack's layer B2 — ADR 0050/ADR 0058 rung 4), and
                // it always records an intent in the very apply that sets
                // the state, so a `Splitting` tablet with no intent is now
                // structurally impossible in practice. Still handled
                // defensively rather than with an `.expect()`/panic — the
                // same posture every other invalid-state check in this arm
                // takes (`epoch mismatch`/`tablet is not Splitting` above) —
                // since nothing prevents a future replicated command, or a
                // hand-crafted/corrupted one, from reaching this arm with a
                // `Splitting` tablet it didn't itself intend.
                let Some(intent) = source.inplace_split.clone() else {
                    return ApplyOutcome::Rejected("splitting parent has no in-place split intent");
                };
                let Some((left_range, right_range)) = source.range.split_at(&intent.split_key)
                else {
                    // Structurally unreachable — `BeginSplitInPlace`
                    // already validated this exact split against this
                    // exact (immutable-since-then) range.
                    return ApplyOutcome::Rejected("split key not strictly inside range");
                };
                let table = source.table.clone();
                let policy = self.policies.get(parent).cloned();
                let parents_final_epoch = self
                    .stream_shards
                    .range((*parent, 0)..=(*parent, u64::MAX))
                    .next_back()
                    .map(|(&(_, epoch), _)| epoch);
                // ADR 0062 §2 (fork C): the directed-Placing target, if
                // any, is decided ONCE here, as a pure function of
                // already-agreed `Metadata` at this exact apply — the
                // same candidate pool every child below is measured
                // against, computed once rather than per child (the
                // active-member set doesn't change mid-apply).
                let candidates = active_candidates(&self.members);
                for (child, range) in [
                    (&intent.children[0], left_range),
                    (&intent.children[1], right_range),
                ] {
                    let mut t =
                        Tablet::with_table(child.id, table.clone(), range, child.replicas.clone());
                    t.state = TabletState::Active;
                    t.epoch = t.epoch.next();
                    let child_replicas = t.replicas.clone();
                    self.tablets.insert(child.id, t);
                    if let Some(policy) = policy.clone() {
                        self.policies.insert(child.id, policy);
                    }
                    self.split_lineage.insert(
                        child.id,
                        SplitLineage {
                            parent: *parent,
                            parents_final_epoch,
                            cutover_wall_ms: *cutover_wall_ms,
                        },
                    );
                    // A child inherits no policy at all ⟹ nothing to
                    // place against, mirroring `reconcile`/`rebalance`'s
                    // own "no policy, no automatic placement" rule — no
                    // `split_placing` entry either.
                    if let Some(policy) = &policy {
                        match select_replicas(&candidates, policy) {
                            // Already satisfying the freshest placement
                            // decision at fork time: no entry at all —
                            // there is nothing for a directed-Placing
                            // convergence phase to ever do here.
                            Ok(wanted) if wanted == child_replicas => {}
                            Ok(wanted) => {
                                self.split_placing.insert(
                                    child.id,
                                    SplitPlacing {
                                        target: Some(wanted),
                                        done: false,
                                    },
                                );
                            }
                            // Fork B: unsatisfiable at cutover (too few
                            // `Active` candidates, or too few distinct
                            // failure domains) is still written as a
                            // durable, visible, keep-retrying
                            // obligation — never silently skipped.
                            Err(_) => {
                                self.split_placing.insert(
                                    child.id,
                                    SplitPlacing {
                                        target: None,
                                        done: false,
                                    },
                                );
                            }
                        }
                    }
                }
                self.tablets.remove(parent);
                self.policies.remove(parent);
                ApplyOutcome::Applied
            }
            MetaCommand::MarkSplitPlacingDone {
                tablet,
                expected_epoch,
            } => {
                // Epoch-CAS against the CHILD's own current epoch (the
                // `CasTabletReplicas` discipline) — a stale confirm racing
                // a later churn event on this same tablet is rejected
                // rather than marking done against state that has since
                // moved again.
                match self.tablets.get(tablet) {
                    None => ApplyOutcome::Rejected("no such tablet"),
                    Some(t) if t.epoch != *expected_epoch => {
                        ApplyOutcome::Rejected("epoch mismatch")
                    }
                    Some(_) => match self.split_placing.get_mut(tablet) {
                        None => ApplyOutcome::Rejected("no split_placing entry for this tablet"),
                        Some(entry) if entry.done => ApplyOutcome::NoOp,
                        Some(entry) => {
                            entry.done = true;
                            ApplyOutcome::Applied
                        }
                    },
                }
            }
            MetaCommand::RetargetSplitPlacing {
                tablet,
                expected_epoch,
                target,
            } => {
                // Same epoch-CAS discipline as `MarkSplitPlacingDone` just
                // above: a stale recompute racing a later churn event on
                // this same tablet is rejected rather than overwriting a
                // target computed against state that has since moved on.
                match self.tablets.get(tablet) {
                    None => ApplyOutcome::Rejected("no such tablet"),
                    Some(t) if t.epoch != *expected_epoch => {
                        ApplyOutcome::Rejected("epoch mismatch")
                    }
                    Some(_) => match self.split_placing.get_mut(tablet) {
                        None => ApplyOutcome::Rejected("no split_placing entry for this tablet"),
                        Some(entry) if entry.done => ApplyOutcome::NoOp,
                        Some(entry) if entry.target == *target => ApplyOutcome::NoOp,
                        Some(entry) => {
                            entry.target = target.clone();
                            ApplyOutcome::Applied
                        }
                    },
                }
            }
            MetaCommand::SetTabletPolicy { tablet, policy } => {
                if !self.tablets.contains_key(tablet) {
                    return ApplyOutcome::Rejected("no such tablet");
                }
                match policy {
                    Some(p) => {
                        self.policies.insert(*tablet, p.clone());
                    }
                    None => {
                        self.policies.remove(tablet);
                    }
                }
                ApplyOutcome::Applied
            }
            MetaCommand::CreateTableSchema { table, schema } => {
                if crate::syskv::is_reserved_name(table) {
                    return ApplyOutcome::Rejected(
                        "table name collides with the reserved system namespace",
                    );
                }
                // A hidden GSI/LSI index table (`<base>$<index>`, ADR 0041
                // §1) can never collide with a user table's own name **only
                // if** a user table name is itself forbidden from containing
                // the separator that hidden-table convention builds on —
                // checked here, at the one place a user table name is ever
                // registered (a hidden table gets only a tablet-map row via
                // `CreateTablet`, never a catalog schema entry of its own).
                // `RESERVED_TABLE_NAME_SEPARATOR` is a deliberate duplicate of
                // `animus_dynamo::index::INDEX_TABLE_SEPARATOR` — this crate
                // cannot depend on `animus-dynamo` (dependency direction; see
                // `animus-tablet`'s `CLAUDE.md` for the identical `escape`
                // duplication precedent) — and must match it byte-for-byte.
                if table.contains(RESERVED_TABLE_NAME_SEPARATOR) {
                    return ApplyOutcome::Rejected(
                        "table name may not contain the reserved `$` separator",
                    );
                }
                if self.schemas.contains(table) {
                    return ApplyOutcome::Rejected("table schema already exists");
                }
                if schema.validate().is_err() {
                    return ApplyOutcome::Rejected("malformed table schema");
                }
                self.schemas.insert(table.clone(), schema.clone());
                ApplyOutcome::Applied
            }
            MetaCommand::DropTableSchema { table } => {
                if self.schemas.remove(table) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            MetaCommand::ReplaceTableSchema { table, schema } => {
                let Some(existing) = self.schemas.get(table) else {
                    return ApplyOutcome::Rejected("no schema to replace for table");
                };
                if schema.validate().is_err() {
                    return ApplyOutcome::Rejected("malformed table schema");
                }
                if existing == schema {
                    return ApplyOutcome::NoOp;
                }
                self.schemas.insert(table.clone(), schema.clone());
                ApplyOutcome::Applied
            }
            MetaCommand::DropTableTablets { table } => {
                let dropped: Vec<TabletId> =
                    self.tablets_for_table(table).map(|(&id, _)| id).collect();
                if dropped.is_empty() {
                    return ApplyOutcome::NoOp;
                }
                for id in &dropped {
                    self.tablets.remove(id);
                    // A dropped tablet can no longer be reconciled.
                    self.policies.remove(id);
                }
                // ADR 0045: a dropped tablet can no longer be backfilled either
                // — prune its rows so a gone tablet id never lingers in the
                // catalog forever (nothing will ever prune it otherwise).
                self.index_backfill
                    .retain(|(tablet, _), ()| !dropped.contains(tablet));
                // ADR 0062 §2: a dropped tablet can no longer be placed
                // either — the identical orphan-prevention prune
                // `index_backfill` gets just above (nothing else will ever
                // revisit a `split_placing` row for a tablet id no longer
                // in the live tablet map).
                self.split_placing
                    .retain(|tablet, _| !dropped.contains(tablet));
                // Reclaim the dropped tablets' CP member addresses (ADR 0024 GC —
                // the address-book counterpart of the hosting nodes' file GC).
                self.prune_cp_member_addrs();
                ApplyOutcome::Applied
            }
            MetaCommand::CreateTableIndex { table, index } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                // Tentatively apply, then validate the resulting schema so a
                // malformed index (e.g. an LSI with no sort attribute) is rejected
                // deterministically and leaves the schema unchanged.
                let mut candidate = schema.clone();
                candidate.upsert_index(index.clone());
                if candidate.validate().is_err() {
                    return ApplyOutcome::Rejected("malformed table index");
                }
                *schema = candidate;
                ApplyOutcome::Applied
            }
            MetaCommand::DropTableIndex { table, index } => {
                let removed = self
                    .schemas
                    .get_mut(table)
                    .is_some_and(|schema| schema.remove_index(index));
                if removed {
                    // ADR 0045: prune every backfill-completion row for this
                    // index, scoped to `table`'s own tablets (never a bare
                    // "match on index name alone" — a distinct table could
                    // happen to declare a same-named index, and its own rows
                    // must not be swept up by this table's drop).
                    let table_tablets: Vec<TabletId> =
                        self.tablets_for_table(table).map(|(&id, _)| id).collect();
                    self.index_backfill.retain(|(tablet, idx), ()| {
                        !(idx == index && table_tablets.contains(tablet))
                    });
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            MetaCommand::SetIndexStatus {
                table,
                index,
                status,
            } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                let Some(current) = schema.index(index) else {
                    return ApplyOutcome::Rejected("no such table index");
                };
                if current.status == *status {
                    return ApplyOutcome::NoOp;
                }
                schema.set_index_status(index, *status);
                ApplyOutcome::Applied
            }
            MetaCommand::MarkIndexBackfilled {
                table,
                index,
                tablet,
            } => {
                let Some(schema) = self.schemas.get(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                if schema.index(index).is_none() {
                    return ApplyOutcome::Rejected("no such table index");
                }
                let belongs_to_table = self
                    .tablets
                    .get(tablet)
                    .is_some_and(|t| t.table.as_deref() == Some(table.as_str()));
                if !belongs_to_table {
                    return ApplyOutcome::Rejected("tablet is not scoped to this table");
                }
                if self.index_backfill.contains_key(&(*tablet, index.clone())) {
                    return ApplyOutcome::NoOp;
                }
                self.index_backfill.insert((*tablet, index.clone()), ());
                ApplyOutcome::Applied
            }
            MetaCommand::BeginBackup {
                backup_id,
                table,
                created_wall_ms,
                backup_name,
                pitr_base,
            } => {
                if self.backups.contains_key(backup_id) {
                    return ApplyOutcome::Rejected("backup id already exists");
                }
                let Some(schema) = self.schemas.get(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                // The manifest stub is derived entirely from already-agreed
                // state (this schema, this table's current tablet map) —
                // never from anything the proposer carried beyond the three
                // fields above — so every replica computes an identical
                // stub deterministically (see `BackupManifest`'s own doc).
                //
                // **State-filtered, unlike `tablets_for_table`'s other
                // callers**: the now-deleted ADR 0050 copy-based split's
                // build/tail window used to leave up to THREE live rows
                // covering one key range at once — the still-authoritative
                // `Splitting` parent, plus its two not-yet-cutover
                // `Building` children (minted immediately by `BeginSplit`,
                // long before `CutoverSplit` ever ran). A `Building`
                // tablet is never routable (`topology::tablet_for_key`
                // excludes it — see `animusd/CLAUDE.md`'s "Write fences are
                // GONE" entry) and is not yet a complete copy, so pinning
                // it here would either double-count its range (parent also
                // pinned) or pin an incomplete copy as if it were
                // authoritative. Only `Active` and `Splitting` tablets are
                // ever the CURRENT authoritative owner of their range — a
                // `Splitting` parent keeps serving reads/writes throughout
                // the whole build/tail window. This filter is now a no-op
                // on the sole surviving split path (ADR 0058's in-place
                // split mints no `Building` rows at all — `CutoverSplit`
                // flips authority straight from `Splitting` parent to two
                // now-`Active` children in one apply) but is kept as a
                // still-load-bearing exclusion for a `Building` tablet
                // mid-restore (`BeginRestore`, ADR 0059 §7). See
                // `docs/engineering-lessons.md`'s entry on this for the
                // general "a read site with a `tablets_for_table`-shaped
                // scan must filter to the current authoritative owner
                // explicitly" rule.
                let pinned_tablets: Vec<BackupPinnedTablet> = self
                    .tablets_for_table(table)
                    .filter(|(_, t)| t.state != TabletState::Building)
                    .map(|(&tablet, t)| BackupPinnedTablet {
                        tablet,
                        range: t.range.clone(),
                    })
                    .collect();
                self.backups.insert(
                    backup_id.clone(),
                    BackupRow {
                        table: table.clone(),
                        backup_name: backup_name.clone(),
                        status: BackupStatus::Creating,
                        manifest: BackupManifest {
                            schema: schema.clone(),
                            pinned_tablets,
                            created_wall_ms: *created_wall_ms,
                        },
                        total_bytes: 0,
                    },
                );
                // Atomic with the mint (issue #593): a PITR base snapshot is
                // never observable as an untagged, ordinary user backup —
                // see `BeginBackup::pitr_base`'s own doc and
                // `Metadata::pitr_base_backups`'s doc for the incident this
                // closes.
                if *pitr_base {
                    self.pitr_base_backups.insert(backup_id.clone());
                }
                ApplyOutcome::Applied
            }
            MetaCommand::RecordBackupTabletComplete {
                backup_id,
                tablet,
                cut_version,
                bytes,
            } => {
                let Some(row) = self.backups.get(backup_id) else {
                    return ApplyOutcome::Rejected("no such backup");
                };
                if !matches!(row.status, BackupStatus::Creating) {
                    return ApplyOutcome::Rejected("backup is not Creating");
                }
                // ADR 0059 §6: `tablet` need not be directly pinned — a
                // re-planned live descendant of a retired (split) pinned
                // tablet is admitted too, via its own `split_lineage` chain.
                if !self.traces_to_pinned(&row.manifest.pinned_tablets, *tablet) {
                    return ApplyOutcome::Rejected("tablet is not pinned in this backup");
                }
                let key = (backup_id.clone(), *tablet);
                if let Some(existing) = self.backup_tablet_progress.get(&key) {
                    if existing.cut_version == *cut_version && existing.bytes == *bytes {
                        return ApplyOutcome::NoOp;
                    }
                    return ApplyOutcome::Rejected(
                        "tablet already reported a different completion",
                    );
                }
                self.backup_tablet_progress.insert(
                    key,
                    BackupTabletProgress {
                        cut_version: *cut_version,
                        bytes: *bytes,
                    },
                );
                ApplyOutcome::Applied
            }
            MetaCommand::CompleteBackup { backup_id } => {
                let Some(row) = self.backups.get(backup_id) else {
                    return ApplyOutcome::Rejected("no such backup");
                };
                if !matches!(row.status, BackupStatus::Creating) {
                    return ApplyOutcome::Rejected("backup is not Creating");
                }
                // ADR 0059 §6: a pinned tablet that retired via a split is
                // satisfied once every one of its LIVE `split_lineage`
                // descendants has its own progress row, not by a (now
                // impossible) direct report from the retired id itself.
                let all_reported = row
                    .manifest
                    .pinned_tablets
                    .iter()
                    .all(|t| self.pinned_tablet_capture_complete(backup_id, t.tablet));
                if !all_reported {
                    return ApplyOutcome::Rejected(
                        "not every pinned tablet has reported completion",
                    );
                }
                // Freeze the final byte total NOW, while every pinned
                // tablet's live descendant is still resolvable
                // (`backup_total_bytes`'s own doc: it sums only the
                // *currently authoritative* reporter per pinned tablet,
                // never a stale split-superseded orphan) — `DescribeBackup`/
                // `ListBackups` (Train 1 PR④) read this frozen field
                // directly rather than re-deriving it, since a live
                // re-derivation goes to exactly zero the moment the source
                // table (and with it every one of this backup's tablets) is
                // ever dropped (ADR 0059 §3's own "outlives the source
                // table" promise would otherwise silently break the size
                // this row reports, not just the table lookup).
                let total_bytes = self.backup_total_bytes(backup_id);
                let row = self
                    .backups
                    .get_mut(backup_id)
                    .expect("checked present above");
                row.status = BackupStatus::Available;
                row.total_bytes = total_bytes;
                ApplyOutcome::Applied
            }
            MetaCommand::FailBackup { backup_id, reason } => {
                let Some(row) = self.backups.get_mut(backup_id) else {
                    return ApplyOutcome::Rejected("no such backup");
                };
                match &row.status {
                    BackupStatus::Failed { reason: existing } if existing == reason => {
                        ApplyOutcome::NoOp
                    }
                    BackupStatus::Creating | BackupStatus::Failed { .. } => {
                        row.status = BackupStatus::Failed {
                            reason: reason.clone(),
                        };
                        ApplyOutcome::Applied
                    }
                    BackupStatus::Available | BackupStatus::Expired => {
                        ApplyOutcome::Rejected("backup is not in a failable state")
                    }
                }
            }
            MetaCommand::DeleteBackup { backup_id } => {
                if self.backups.remove(backup_id).is_none() {
                    return ApplyOutcome::NoOp;
                }
                self.backup_tablet_progress
                    .retain(|(id, _), _| id != backup_id);
                // ADR 0059 §9: a PITR base snapshot's own tag row is exactly
                // as reclaimable as the backup row it tags — never a reason
                // by itself to keep the row alive (retention is decided
                // upstream of this command, same as an ordinary on-demand
                // backup's).
                self.pitr_base_backups.remove(backup_id);
                ApplyOutcome::Applied
            }
            MetaCommand::MarkBackupDeleted { backup_id } => {
                let Some(row) = self.backups.get_mut(backup_id) else {
                    return ApplyOutcome::Rejected("no such backup");
                };
                match &row.status {
                    BackupStatus::Expired => ApplyOutcome::NoOp,
                    BackupStatus::Creating => {
                        ApplyOutcome::Rejected("backup is not in a deletable state")
                    }
                    BackupStatus::Available | BackupStatus::Failed { .. } => {
                        row.status = BackupStatus::Expired;
                        ApplyOutcome::Applied
                    }
                }
            }
            MetaCommand::BeginRestore {
                restore_id,
                backup_id,
                source_table,
                target_table,
                tablet,
                replicas,
                gsi_defs,
                pitr,
            } => {
                if self.restores.contains_key(restore_id) {
                    return ApplyOutcome::Rejected("restore id already exists");
                }
                if self.tablets.contains_key(tablet) {
                    return ApplyOutcome::Rejected("tablet already exists");
                }
                // Same monotonic-allocator floor as `CreateTablet`/
                // `BeginSplitInPlace` (and for the same dropped-data-
                // resurrection reason).
                if tablet.0 < self.next_free_tablet_id().0 {
                    return ApplyOutcome::Rejected("tablet id below the monotonic allocator");
                }
                let mut t = Tablet::with_table(
                    *tablet,
                    Some(target_table.clone()),
                    KeyRange::whole(),
                    replicas.clone(),
                );
                t.state = TabletState::Building;
                self.tablets.insert(*tablet, t);
                self.next_tablet_id = self.next_tablet_id.max(tablet.0 + 1);
                self.restores.insert(
                    restore_id.clone(),
                    RestoreRow {
                        backup_id: backup_id.clone(),
                        source_table: source_table.clone(),
                        target_table: target_table.clone(),
                        tablet: *tablet,
                        gsi_defs: gsi_defs.clone(),
                        pitr: pitr.clone(),
                        status: RestoreStatus::Seeding,
                    },
                );
                ApplyOutcome::Applied
            }
            MetaCommand::CompleteRestore { restore_id } => {
                let Some(row) = self.restores.get(restore_id) else {
                    return ApplyOutcome::Rejected("no such restore");
                };
                if !matches!(row.status, RestoreStatus::Seeding) {
                    return ApplyOutcome::Rejected("restore is not Seeding");
                }
                let tablet_id = row.tablet;
                let Some(t) = self.tablets.get_mut(&tablet_id) else {
                    return ApplyOutcome::Rejected("restore's tablet no longer exists");
                };
                if t.state != TabletState::Building {
                    return ApplyOutcome::Rejected("restore's tablet is not Building");
                }
                t.state = TabletState::Active;
                t.epoch = t.epoch.next();
                self.restores
                    .get_mut(restore_id)
                    .expect("checked present above")
                    .status = RestoreStatus::Done;
                ApplyOutcome::Applied
            }
            MetaCommand::FailRestore { restore_id, reason } => {
                let Some(row) = self.restores.get_mut(restore_id) else {
                    return ApplyOutcome::Rejected("no such restore");
                };
                match &row.status {
                    RestoreStatus::Failed { reason: existing } if existing == reason => {
                        ApplyOutcome::NoOp
                    }
                    RestoreStatus::Seeding | RestoreStatus::Failed { .. } => {
                        row.status = RestoreStatus::Failed {
                            reason: reason.clone(),
                        };
                        ApplyOutcome::Applied
                    }
                    RestoreStatus::Done => ApplyOutcome::Rejected("restore already completed"),
                }
            }
            MetaCommand::SetTableStream { table, spec } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                match spec {
                    Some(new_spec) => {
                        if schema.stream.is_some() {
                            return ApplyOutcome::Rejected(
                                "table stream already enabled (disable before re-enabling \
                                 to mint a new label)",
                            );
                        }
                        schema.stream = Some(new_spec.clone());
                    }
                    None => {
                        if schema.stream.is_none() {
                            return ApplyOutcome::NoOp;
                        }
                        schema.stream = None;
                    }
                }
                ApplyOutcome::Applied
            }
            MetaCommand::SetTableTtl { table, spec } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                if schema.ttl == *spec {
                    // Covers both idempotent shapes at once: re-enabling
                    // with the same attribute name, and disabling when
                    // already disabled (`spec` and `schema.ttl` both
                    // `None`).
                    return ApplyOutcome::NoOp;
                }
                schema.ttl = spec.clone();
                ApplyOutcome::Applied
            }
            MetaCommand::SetTableThroughput { table, spec } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                if schema.throughput == *spec {
                    // Covers both idempotent shapes at once: re-asserting an
                    // identical spec, and reverting to `PAY_PER_REQUEST`
                    // when already unset (`spec` and `schema.throughput`
                    // both `None`) — the identical `SetTableTtl` idiom.
                    return ApplyOutcome::NoOp;
                }
                schema.throughput = *spec;
                ApplyOutcome::Applied
            }
            MetaCommand::TagResource { table, tags } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                let mut changed = false;
                for (key, value) in tags {
                    if schema.tags.get(key) != Some(value) {
                        schema.tags.insert(key.clone(), value.clone());
                        changed = true;
                    }
                }
                if changed {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            MetaCommand::UntagResource { table, tag_keys } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                let mut changed = false;
                for key in tag_keys {
                    if schema.tags.remove(key).is_some() {
                        changed = true;
                    }
                }
                if changed {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            MetaCommand::UpdateContinuousBackups {
                table,
                enabled,
                wall_ms,
            } => {
                let Some(schema) = self.schemas.get(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                if *enabled {
                    if schema.pitr.is_some() {
                        return ApplyOutcome::NoOp;
                    }
                    let generation = self.pitr_generation.get(table).copied().unwrap_or(0) + 1;
                    self.pitr_generation.insert(table.clone(), generation);
                    self.schemas
                        .get_mut(table)
                        .expect("checked present above")
                        .pitr = Some(PitrSpec {
                        generation,
                        enabled_wall_ms: *wall_ms,
                    });
                } else {
                    if schema.pitr.is_none() {
                        return ApplyOutcome::NoOp;
                    }
                    self.schemas
                        .get_mut(table)
                        .expect("checked present above")
                        .pitr = None;
                }
                ApplyOutcome::Applied
            }
            MetaCommand::SealStreamShard {
                table,
                label,
                tablet,
                epoch,
                view_type,
                hlc_range,
                count,
                seal_wall_ms,
                replicas,
                object_id,
            } => {
                // First-committer-wins on CONTENT, not merely on identity
                // (round-3 PR7 amendment, ADR 0043 §A3/§A9): a second
                // proposal for an already-recorded (tablet, epoch) is
                // evaluated against the existing row's own content fields
                // (everything except `replicas`/`expired`). Two shapes are
                // legitimate and must both apply cleanly:
                //   - the sealer's own crash-retry loop racing itself
                //     (identical content, including `replicas` — a true
                //     no-op); and
                //   - the segment janitor's replica-repair sweep (ADR 0043
                //     §A9) re-proposing the *same* committed shard with an
                //     updated `replicas` set, once it has re-replicated a
                //     copy lost to a dead/removed member onto a fresh
                //     target — everything else about the shard is, by
                //     construction, unchanged (repair never re-derives
                //     `hlc_range`/`count`/etc., it only moves bytes).
                // A proposal whose non-`replicas` content genuinely
                // differs from what is already recorded is rejected as a
                // no-op exactly as before — this state machine never lets
                // a second, conflicting seal silently overwrite a
                // committed shard's own facts, only its replica location.
                // Safe for every reader: `GetRecords`/the janitor always
                // re-fetch the row fresh before consulting `replicas`, so
                // an in-place update is observed atomically, never a torn
                // read of a half-updated set.
                if let Some(existing) = self.stream_shards.get_mut(&(*tablet, *epoch)) {
                    // `object_id` is part of CONTENT here, not merely
                    // descriptive (ledger-named-object amendment): two
                    // independently-computed attempts for the same epoch —
                    // the dueling-seal race this amendment closes — always
                    // mint different ids even when every other field
                    // happens to agree, so including it here is what makes
                    // that race a genuine content mismatch (correctly
                    // rejected below) rather than misclassified as the
                    // replicas-only-update path. A true same-attempt retry
                    // (the sealer's own crash-retry loop racing itself)
                    // reuses its own already-written id unchanged, so it
                    // still matches here exactly as before.
                    let content_matches = existing.table == *table
                        && existing.label == *label
                        && existing.view_type == *view_type
                        && existing.hlc_range == *hlc_range
                        && existing.count == *count
                        && existing.seal_wall_ms == *seal_wall_ms
                        && existing.object_id == *object_id;
                    if !content_matches || existing.replicas == *replicas {
                        return ApplyOutcome::NoOp;
                    }
                    existing.replicas = replicas.clone();
                    return ApplyOutcome::Applied;
                }
                // Label validation (F12-b): licensed by the table's
                // *current* schema stream spec, or by an existing catalog
                // row already present for this exact (table, label) pair
                // (a disabled stream's un-reaped rows still license a
                // further seal of the same generation).
                let current_label_matches = self
                    .schemas
                    .get(table)
                    .and_then(|s| s.stream.as_ref())
                    .is_some_and(|spec| spec.label == *label);
                let existing_row_for_label = self
                    .stream_shards
                    .values()
                    .any(|row| row.table == *table && row.label == *label);
                if !current_label_matches && !existing_row_for_label {
                    return ApplyOutcome::Rejected(
                        "stream label has no current schema entry and no existing catalog rows \
                         to extend",
                    );
                }
                // Epoch-chain sanity (see this command's own doc): epoch 0
                // always accepted; epoch > 0 needs a local predecessor row.
                if *epoch > 0 && !self.stream_shards.contains_key(&(*tablet, *epoch - 1)) {
                    return ApplyOutcome::Rejected(
                        "epoch chain gap: no prior epoch row for this tablet",
                    );
                }
                self.stream_shards.insert(
                    (*tablet, *epoch),
                    StreamShardRow {
                        table: table.clone(),
                        label: label.clone(),
                        view_type: *view_type,
                        hlc_range: *hlc_range,
                        count: *count,
                        seal_wall_ms: *seal_wall_ms,
                        replicas: replicas.clone(),
                        object_id: object_id.clone(),
                        expired: false,
                    },
                );
                ApplyOutcome::Applied
            }
            MetaCommand::ExpireStreamShards { rows, remove } => {
                let mut changed = false;
                for (tablet, epoch) in rows {
                    if *remove {
                        changed |= self.stream_shards.remove(&(*tablet, *epoch)).is_some();
                    } else if let Some(row) = self.stream_shards.get_mut(&(*tablet, *epoch))
                        && !row.expired
                    {
                        row.expired = true;
                        changed = true;
                    }
                }
                if changed {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            MetaCommand::SealPitrSegment {
                table,
                generation,
                tablet,
                epoch,
                hlc_range,
                count,
                seal_wall_ms,
                replicas,
                object_id,
            } => {
                // First-committer-wins on CONTENT — the identical shape
                // `SealStreamShard` uses, see that arm's own doc for the
                // full reasoning (crash-retry no-op vs. replica-repair
                // update vs. genuine conflict).
                if let Some(existing) = self.pitr_segments.get_mut(&(*tablet, *epoch)) {
                    let content_matches = existing.table == *table
                        && existing.generation == *generation
                        && existing.hlc_range == *hlc_range
                        && existing.count == *count
                        && existing.seal_wall_ms == *seal_wall_ms
                        && existing.object_id == *object_id;
                    if !content_matches || existing.replicas == *replicas {
                        return ApplyOutcome::NoOp;
                    }
                    existing.replicas = replicas.clone();
                    return ApplyOutcome::Applied;
                }
                // Generation validation, mirroring `SealStreamShard`'s label
                // rule: licensed by the table's *current* PITR generation,
                // or by an existing catalog row already present for this
                // exact (table, generation) pair.
                let current_generation_matches = self
                    .schemas
                    .get(table)
                    .and_then(|s| s.pitr.as_ref())
                    .is_some_and(|spec| spec.generation == *generation);
                let existing_row_for_generation = self
                    .pitr_segments
                    .values()
                    .any(|row| row.table == *table && row.generation == *generation);
                if !current_generation_matches && !existing_row_for_generation {
                    return ApplyOutcome::Rejected(
                        "PITR generation has no current schema entry and no existing catalog \
                         rows to extend",
                    );
                }
                // Epoch-chain sanity, identical to `SealStreamShard`.
                if *epoch > 0 && !self.pitr_segments.contains_key(&(*tablet, *epoch - 1)) {
                    return ApplyOutcome::Rejected(
                        "epoch chain gap: no prior epoch row for this tablet",
                    );
                }
                self.pitr_segments.insert(
                    (*tablet, *epoch),
                    PitrSegmentRow {
                        table: table.clone(),
                        generation: *generation,
                        hlc_range: *hlc_range,
                        count: *count,
                        seal_wall_ms: *seal_wall_ms,
                        replicas: replicas.clone(),
                        object_id: object_id.clone(),
                        expired: false,
                    },
                );
                ApplyOutcome::Applied
            }
            MetaCommand::ExpirePitrSegments { rows, remove } => {
                let mut changed = false;
                for (tablet, epoch) in rows {
                    if *remove {
                        changed |= self.pitr_segments.remove(&(*tablet, *epoch)).is_some();
                    } else if let Some(row) = self.pitr_segments.get_mut(&(*tablet, *epoch))
                        && !row.expired
                    {
                        row.expired = true;
                        changed = true;
                    }
                }
                if changed {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            MetaCommand::RegisterCpAddr { id, addr, tablet } => {
                // A tablet-scoped registration for a tablet not (yet or anymore)
                // in the map is rejected: accepting it would either leak (the GC
                // prunes on the recorded tablet's *current absence*, so it would
                // be swept at the next removal anyway) or resurrect a dropped
                // tablet's entry. The registrar's propose-and-await loop retries
                // until its tablet lands, so a benign register-before-create race
                // converges.
                if let Some(t) = tablet
                    && !self.tablets.contains_key(t)
                {
                    return ApplyOutcome::Rejected("no such tablet for cp addr");
                }
                if self.cp_member_addrs.get(id) == Some(addr)
                    && self.cp_member_tablets.get(id) == tablet.as_ref()
                {
                    ApplyOutcome::NoOp
                } else {
                    self.cp_member_addrs.insert(id.clone(), addr.clone());
                    match tablet {
                        Some(t) => {
                            self.cp_member_tablets.insert(id.clone(), *t);
                        }
                        None => {
                            self.cp_member_tablets.remove(id);
                        }
                    }
                    ApplyOutcome::Applied
                }
            }
            MetaCommand::RegisterNodeAddrs { node, addrs } => {
                // ADR 0040 PR4: update-only — never claims a fresh id.
                // `MetaCommand::RegisterNode` is the sole claim path now; a
                // node absent from both `members` and `node_addrs` has no
                // existing claim this command could legitimately update.
                if !self.members.contains_key(node) && !self.node_addrs.contains_key(node) {
                    return ApplyOutcome::Rejected(
                        "node id not yet a registered member — register it via \
                         MetaCommand::RegisterNode first",
                    );
                }
                if self.node_addrs.get(node) == Some(addrs) {
                    ApplyOutcome::NoOp
                } else {
                    self.node_addrs.insert(node.clone(), addrs.clone());
                    ApplyOutcome::Applied
                }
            }
            MetaCommand::RemoveMember { node } => {
                let Some(member) = self.members.get(node) else {
                    // No `members` row for this id. Two shapes (ADR 0040
                    // PR6): a genuinely already-removed id (an idempotent
                    // retry, e.g. a proposer whose confirm timed out after
                    // the command actually committed) — `NoOp`; or a
                    // **claim-without-member** id (a control-role
                    // `RegisterNode` claims `node_addrs` alone, never a
                    // `members` row — see that command's own doc) whose
                    // orphaned address-book claim this command should still
                    // clean up, so `RemoveMember` is a complete removal for
                    // every shape `RegisterNode` can produce, not just the
                    // data-plane one. Distinguished purely by whether
                    // `node_addrs` still has an entry to prune: nothing else
                    // to gate on here — a claim-without-member id can never
                    // be `Active`/`Joining` (it never claims `members` at
                    // all) and can never be referenced by a tablet's replica
                    // set (placement only ever chooses from `members`). The
                    // one real safety check this shape needs — "is `node`
                    // currently a live **control** voter" — is not this
                    // state machine's to make: `RaftCore`'s voter config
                    // lives in a wholly different part of the system, not in
                    // `Metadata` at all, so the caller (the orphan-sweep
                    // driver, or an admin action) must check it *before*
                    // ever proposing this command.
                    if self.node_addrs.remove(node).is_some() {
                        self.cp_member_addrs.remove(node);
                        self.cp_member_tablets.remove(node);
                        return ApplyOutcome::Applied;
                    }
                    return ApplyOutcome::NoOp;
                };
                if matches!(member.status, NodeStatus::Active | NodeStatus::Joining) {
                    return ApplyOutcome::Rejected("not drained: member is Active or Joining");
                }
                if self.tablets_referencing(node) > 0 {
                    return ApplyOutcome::Rejected("still referenced by a tablet's replica set");
                }
                self.members.remove(node);
                self.node_addrs.remove(node);
                self.cp_member_addrs.remove(node);
                self.cp_member_tablets.remove(node);
                ApplyOutcome::Applied
            }
            MetaCommand::RegisterNode {
                node,
                addrs,
                labels,
            } => {
                // The CAS is keyed on `node_addrs` alone, not `members` —
                // membership can legitimately already exist via a wholly
                // decoupled path (`UpsertMember`'s bootstrap insert,
                // `admin_add_member`'s operator-labeled `Down` row) *before*
                // this node ever gets to self-register its own address book,
                // and none of those paths carry an address for this apply to
                // compare against. Treating "member present, addrs absent"
                // as an unclaimed address slot — rather than folding
                // `members` into the collision test — is what lets a single
                // shared self-registration call site serve every node shape
                // (a fresh bootstrap node racing its own `bootstrap()`
                // insert, a growth node racing its own `admin_add_member`,
                // and a permanently-non-voter control-only growth node with
                // *no other claim path at all*) without any of them
                // fighting over which command's labels/status wins — those
                // stay whatever the *other*, decoupled command already set,
                // untouched here.
                // **Never claim `members` for a control-only registration**
                // (`addrs.role == "control"`): `Metadata::members` is
                // *data-plane* membership — the placement engine's
                // `active_candidates` (ADR 0005) treats every `Active` entry
                // there as a real tablet-replica candidate, and the failure
                // detector promotes any heartbeating `Down` entry to
                // `Active` on its own (ADR 0012) — a control-only node has
                // no `raftkv` role, no storage engine, and can never
                // actually host a tablet, so letting it appear in `members`
                // at all (even transiently `Down`) would make it a
                // placement candidate that silently corrupts serving the
                // moment it's picked. Before ADR 0040 PR4, no command ever
                // proposed membership for a control-only node (self-
                // registration only ever touched `node_addrs`, via
                // `RegisterNodeAddrs`); this is that same invariant, now
                // enforced structurally in the one command that could
                // otherwise violate it, instead of merely by no caller
                // happening to ask.
                let claims_membership = addrs.role != "control";
                match self.node_addrs.get(node) {
                    Some(existing) if existing == addrs => {
                        // Idempotent: a proposer retry after an accepted-
                        // but-unconfirmed propose, or the ADR 0032
                        // same-identity rejoin (a restarted process
                        // re-registering its own, unchanged address book).
                        // Still make sure the member row exists (a repair,
                        // never a label/status overwrite of one that does).
                        if claims_membership && !self.members.contains_key(node) {
                            self.members.insert(
                                node.clone(),
                                Member {
                                    labels: labels.clone(),
                                    status: NodeStatus::Down,
                                    // A fresh claim: never activated yet.
                                    has_activated: false,
                                },
                            );
                            return ApplyOutcome::Applied;
                        }
                        ApplyOutcome::NoOp
                    }
                    Some(_) => {
                        // A genuinely different address book is already on
                        // file for this id — the real collision case.
                        ApplyOutcome::Rejected(
                            "node id already claimed by a different registration",
                        )
                    }
                    None => {
                        // Unclaimed address slot: claim it, and claim
                        // membership too iff nothing else already has (and
                        // this is a data-capable registration).
                        self.node_addrs.insert(node.clone(), addrs.clone());
                        if claims_membership && !self.members.contains_key(node) {
                            self.members.insert(
                                node.clone(),
                                Member {
                                    labels: labels.clone(),
                                    status: NodeStatus::Down,
                                    // A fresh claim: never activated yet.
                                    has_activated: false,
                                },
                            );
                        }
                        ApplyOutcome::Applied
                    }
                }
            }
        }
    }

    /// Drop every CP member-addr entry recorded against a tablet that is **no
    /// longer in the map** (ADR 0024 — the address-book half of drop-table GC,
    /// closing the designed `cp_member_addrs` leak). Called from the apply arm
    /// that removes tablets (`DropTableTablets`); keyed purely on
    /// current absence, so it is deterministic on every replica and **convergent
    /// under replay**: a re-applied historical sequence re-registers and then
    /// re-prunes in the same order, never leaving a resurrected entry. Members
    /// registered without a tablet association (legacy) are untouched.
    fn prune_cp_member_addrs(&mut self) {
        let dead: Vec<NodeId> = self
            .cp_member_tablets
            .iter()
            .filter(|(_, t)| !self.tablets.contains_key(t))
            .map(|(id, _)| id.clone())
            .collect();
        for id in dead {
            self.cp_member_tablets.remove(&id);
            self.cp_member_addrs.remove(&id);
        }
    }

    /// The schema registered for `table`, if any (ADR 0013). A read accessor for
    /// the wire adapters that consume the replicated catalog.
    #[must_use]
    pub fn table_schema(&self, table: &str) -> Option<&TableSchema> {
        self.schemas.get(table)
    }

    /// Whether a schema is registered for `table`.
    #[must_use]
    pub fn has_table_schema(&self, table: &str) -> bool {
        self.schemas.contains(table)
    }

    /// All `(name, schema)` pairs in the catalog, in ascending name order.
    pub fn table_schemas(&self) -> impl Iterator<Item = (&TableName, &TableSchema)> {
        self.schemas.iter()
    }

    /// The secondary-index definitions registered for `table` (ADR 0013), in
    /// ascending index-name order. Empty if the table is unknown or has no
    /// indexes. A read accessor for the wire adapters that consume the replicated
    /// index definitions.
    #[must_use]
    pub fn table_indexes(&self, table: &str) -> &[IndexDef] {
        self.schemas.get(table).map_or(&[], |s| &s.indexes)
    }

    /// This table's DynamoDB Streams configuration (ADR 0042), if enabled.
    /// `None` for an unknown table or one with no stream. A read accessor for
    /// the wire adapters that consume the replicated catalog.
    #[must_use]
    pub fn table_stream(&self, table: &str) -> Option<&StreamSpec> {
        self.schemas.get(table).and_then(|s| s.stream.as_ref())
    }

    /// This table's DynamoDB-style TTL configuration (ADR 0051), if enabled.
    /// `None` for an unknown table or one with no TTL declared. A read
    /// accessor for the wire adapters that consume the replicated catalog,
    /// mirroring [`table_stream`](Self::table_stream) exactly.
    #[must_use]
    pub fn table_ttl(&self, table: &str) -> Option<&TtlSpec> {
        self.schemas.get(table).and_then(|s| s.ttl.as_ref())
    }

    /// This table's provisioned throughput configuration (ADR 0065 §5(b)),
    /// if `BillingMode` is `PROVISIONED`. `None` for an unknown table or one
    /// with no throughput declared (`PAY_PER_REQUEST`), mirroring
    /// [`table_stream`](Self::table_stream)/[`table_ttl`](Self::table_ttl)
    /// exactly. A read accessor for `animusd`'s per-tablet throttle bucket
    /// and the wire adapters that consume the replicated catalog.
    #[must_use]
    pub fn table_throughput(&self, table: &str) -> Option<&ProvisionedThroughput> {
        self.schemas.get(table).and_then(|s| s.throughput.as_ref())
    }

    /// This table's point-in-time recovery (PITR) configuration (ADR 0059
    /// §9), if enabled. `None` for an unknown table or one with no PITR
    /// declared, mirroring [`table_stream`](Self::table_stream)/
    /// [`table_ttl`](Self::table_ttl) exactly.
    #[must_use]
    pub fn table_pitr(&self, table: &str) -> Option<&PitrSpec> {
        self.schemas.get(table).and_then(|s| s.pitr.as_ref())
    }

    /// This table's **resource tags** (roadmap W-06), if any. `None` for an
    /// unknown table; an empty map (never `None`) for a known table with no
    /// tags — mirroring [`TableSchema::tags`]'s own "always present, may be
    /// empty" shape rather than [`table_stream`](Self::table_stream)/
    /// [`table_ttl`](Self::table_ttl)/[`table_pitr`](Self::table_pitr)'s
    /// "absent means disabled" one, since a tag set has no such notion. A
    /// read accessor for the wire adapters that consume the replicated
    /// catalog.
    #[must_use]
    pub fn table_tags(&self, table: &str) -> Option<&BTreeMap<String, String>> {
        self.schemas.get(table).map(|s| &s.tags)
    }

    /// `tablet`'s own catalog rows for `(table, label)`, in ascending epoch
    /// order (ADR 0042 §2/§3) — the chain a `DescribeStream`/lineage-walk
    /// consumer needs for one tablet. `BTreeMap<(TabletId, u64), _>`'s own
    /// key order already sorts by tablet then epoch, so a bounded range
    /// query over this one tablet's key space, filtered to the requested
    /// `(table, label)`, comes back in the right order for free — no
    /// separate sort.
    pub fn stream_shard_chain<'a>(
        &'a self,
        table: &'a str,
        label: &'a str,
        tablet: TabletId,
    ) -> impl Iterator<Item = (u64, &'a StreamShardRow)> {
        self.stream_shards
            .range((tablet, 0)..=(tablet, u64::MAX))
            .filter(move |((_, _), row)| row.table == table && row.label == label)
            .map(|((_, epoch), row)| (*epoch, row))
    }

    /// `tablet`'s effective stream watermark (ADR 0042 §8/ADR 0043 §A6,
    /// F10): the tablet's own shard chain's **last sealed end-HLC** —
    /// `None` if it has never sealed (the safe, trim-blocking default every
    /// cold consumer already gets). Scoped to the tablet's own chain
    /// **regardless of label** — a tablet's physical seal history is one
    /// continuous sequence across however many stream generations
    /// (enable/disable/re-enable cycles) it has lived through, so the
    /// watermark the hot-trim arm needs is "how far has this tablet's own
    /// log been sealed," not "how far has *this* label's log been sealed."
    #[must_use]
    pub fn stream_shard_watermark(&self, tablet: TabletId) -> Option<u64> {
        self.stream_shards
            .range((tablet, 0)..=(tablet, u64::MAX))
            .next_back()
            .map(|(_, row)| row.hlc_range.1)
    }

    /// `tablet`'s own most recent seal's wall-clock time (ADR 0042 fork G) —
    /// the `seal_wall_ms` of the newest row in this tablet's own chain, by
    /// `(tablet, epoch)` order (the same `next_back()` lookup
    /// [`stream_shard_watermark`] uses just above), or `None` if this tablet
    /// has never sealed a shard of its own. This is the cheap, catalog-only
    /// basis the seal arm's age trigger derives its "time since the last
    /// seal" from (`animusd::index_drain::seal_tick`) — no `KIND_CHANGE`
    /// scan needed.
    ///
    /// A never-sealed tablet (including a copy-based split child before its
    /// own first seal — children are born with empty change logs, ADR 0050)
    /// reads as `None`; the seal arm's own never-sealed fallback (a
    /// one-time real scan of the true oldest pending record's HLC, memoized
    /// per tablet — see `animusd::index_drain::seal_tick`'s own doc) is the
    /// answer for that case.
    #[must_use]
    pub fn last_seal_wall_ms(&self, tablet: TabletId) -> Option<u64> {
        self.stream_shards
            .range((tablet, 0)..=(tablet, u64::MAX))
            .next_back()
            .map(|(_, row)| row.seal_wall_ms)
    }

    /// Every catalog row for `(table, label)`, across every tablet, in
    /// ascending `(tablet, epoch)` order (ADR 0042 §3 — `DescribeStream`'s
    /// own read, and a later PR's lineage-walk consumer).
    pub fn stream_shard_rows_for_label<'a>(
        &'a self,
        table: &'a str,
        label: &'a str,
    ) -> impl Iterator<Item = (TabletId, u64, &'a StreamShardRow)> {
        self.stream_shards
            .iter()
            .filter(move |((_, _), row)| row.table == table && row.label == label)
            .map(|((tablet, epoch), row)| (*tablet, *epoch, row))
    }

    /// `(table, label)`'s view type (PR6's DescribeStream read, ADR 0042
    /// §3/§15): the table's *current* `StreamSpec.view_type` when `label` is
    /// still the enabled one, else the last-known value carried by any of
    /// the label's own catalog rows (a view type never changes mid-stream —
    /// every row of one label carries the identical value, see
    /// [`StreamShardRow::view_type`]'s doc) — `None` only when `label` is
    /// neither the current schema label nor has ever sealed a row (F12-b: a
    /// caller should already have rejected such a label as
    /// `ResourceNotFoundException` before asking this).
    #[must_use]
    pub fn stream_view_type(&self, table: &str, label: &str) -> Option<StreamViewType> {
        if let Some(spec) = self.table_stream(table)
            && spec.label == label
        {
            return Some(spec.view_type);
        }
        self.stream_shard_rows_for_label(table, label)
            .next()
            .map(|(_, _, row)| row.view_type)
    }

    /// Every distinct label of `table` that still has at least one catalog
    /// row (F12-b coexistence, ADR 0042 §4/§11): a `DISABLED`-but-unreaped
    /// stream's label stays in this set for as long as any of its rows
    /// haven't been reaped, alongside the table's current (if any) enabled
    /// label — this is what lets `ListStreams` show both during a disable
    /// grace window.
    #[must_use]
    pub fn stream_labels_with_rows(&self, table: &str) -> BTreeSet<String> {
        self.stream_shards
            .values()
            .filter(|row| row.table == table)
            .map(|row| row.label.clone())
            .collect()
    }

    /// `(tablet, epoch)`'s own `ParentShardId` (ADR 0042 §2/ADR 0043 §A4). An
    /// epoch above 0 names the same tablet's own previous epoch, always
    /// derived (a routine seal can never race itself). An epoch-0 shard
    /// names the NEAREST split-lineage ancestor's own FINAL sealed shard
    /// (ADR 0050, fork F9; the walk-past-an-unsealed-ancestor behavior is
    /// ADR 0043's 2026-09-04 amendment, issue #588). `None` only for a
    /// genuine root: an epoch-0 shard whose tablet was never a split child
    /// at all, or whose entire split-lineage chain — every ancestor, all
    /// the way back — never sealed a shard of its own.
    #[must_use]
    pub fn stream_shard_parent_id(&self, tablet: TabletId, epoch: u64) -> Option<String> {
        if epoch > 0 {
            return Some(shard_id_string(tablet, epoch - 1));
        }
        // A split child's epoch-0 shard names its parent's FINAL sealed
        // shard via `split_lineage` — written at `CutoverSplit`'s own
        // apply, the one moment the parent's chain is complete and
        // immutable, so this needs no freezing defense layers at all
        // (fork F9).
        //
        // **Issue #588**: `SplitLineage::parents_final_epoch` is
        // legitimately `None` whenever the immediate parent itself never
        // sealed anything of its own before it split further — not a race,
        // a documented case (ADR 0043 §A3's "never seal an empty segment"):
        // a fast cascade can produce an intermediate tablet that inherits
        // bytes from ITS OWN parent's fork but takes zero direct writes
        // before splitting again. Stopping at that `None` used to strand
        // every descendant with a permanently-null `ParentShardId`, even
        // though an earlier ancestor's real sealed history is right there
        // in `split_lineage` one hop further up — walk past it instead: a
        // never-sealed parent's own epoch-0 "parent shard" is, by this
        // exact same definition, ITS parent's final sealed shard, so
        // recursing on `(lineage.parent, 0)` finds the nearest ancestor
        // that ever actually sealed something, however many un-sealed
        // hops lie between. Bounded by `split_lineage.len() + 1` recursive
        // steps at most (it is a tree, never cyclic — the same bound
        // `live_split_descendants`'s own forward DFS uses) so a corrupted/
        // cyclic map can never spin this forever.
        self.stream_shard_parent_id_bounded(tablet, self.split_lineage.len())
    }

    /// [`stream_shard_parent_id`](Self::stream_shard_parent_id)'s own
    /// epoch-0 recursive step (issue #588) — `budget` is decremented on
    /// every hop through an unsealed ancestor and the walk gives up
    /// (`None`) if it ever reaches zero, rather than trusting
    /// `split_lineage` to stay acyclic under a hypothetically corrupted
    /// state. A legitimate tree can never exhaust this budget: each hop
    /// visits a distinct tablet id (`split_lineage` is a tree keyed
    /// child → parent) and `split_lineage.len()` upper-bounds how many
    /// distinct tablets can possibly be children in it at all.
    fn stream_shard_parent_id_bounded(&self, tablet: TabletId, budget: usize) -> Option<String> {
        let lineage = self.split_lineage.get(&tablet)?;
        match lineage.parents_final_epoch {
            Some(parent_epoch) => Some(shard_id_string(lineage.parent, parent_epoch)),
            None if budget == 0 => None,
            None => self.stream_shard_parent_id_bounded(lineage.parent, budget - 1),
        }
    }

    /// `tablet`'s effective PITR watermark (ADR 0059 §9) — the identical
    /// "last sealed end-HLC" semantics as
    /// [`stream_shard_watermark`](Self::stream_shard_watermark), over the
    /// separate [`pitr_segments`](Self::pitr_segments) catalog. `None` if
    /// this tablet has never sealed a PITR segment.
    #[must_use]
    pub fn pitr_segment_watermark(&self, tablet: TabletId) -> Option<u64> {
        self.pitr_segments
            .range((tablet, 0)..=(tablet, u64::MAX))
            .next_back()
            .map(|(_, row)| row.hlc_range.1)
    }

    /// `tablet`'s own most recent PITR seal's wall-clock time — the PITR
    /// twin of [`last_seal_wall_ms`](Self::last_seal_wall_ms).
    #[must_use]
    pub fn last_pitr_seal_wall_ms(&self, tablet: TabletId) -> Option<u64> {
        self.pitr_segments
            .range((tablet, 0)..=(tablet, u64::MAX))
            .next_back()
            .map(|(_, row)| row.seal_wall_ms)
    }

    /// Every PITR generation `table` has at least one catalog row for (ADR
    /// 0059 §9) — the PITR twin of
    /// [`stream_labels_with_rows`](Self::stream_labels_with_rows), licensing
    /// a disable-triggered final PITR seal exactly the way that accessor
    /// licenses one for streams.
    pub fn pitr_generations_with_rows(&self, table: &str) -> BTreeSet<u64> {
        self.pitr_segments
            .values()
            .filter(|row| row.table == table)
            .map(|row| row.generation)
            .collect()
    }

    /// Every [`BackupRow`] tagged as a PITR base snapshot
    /// ([`Metadata::pitr_base_backups`]) for `table`, in ascending
    /// `created_wall_ms` order — the janitor's and `DescribeContinuousBackups`'
    /// shared "which base snapshots does this table's PITR history have"
    /// read, so the accessor logic lives in exactly one place.
    pub fn pitr_base_backups_for_table<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Iterator<Item = (&'a BackupId, &'a BackupRow)> {
        let mut rows: Vec<(&'a BackupId, &'a BackupRow)> = self
            .pitr_base_backups
            .iter()
            .filter_map(|id| self.backups.get(id).map(|row| (id, row)))
            .filter(|(_, row)| row.table == table)
            .collect();
        rows.sort_by_key(|(_, row)| row.manifest.created_wall_ms);
        rows.into_iter()
    }

    /// **ADR 0059 §10 (Train 3 PR②)**: the ordered plan of PITR segments to
    /// replay forward from a chosen PITR base snapshot's own per-tablet cut
    /// versions (`base_tablet_progress`, taken verbatim from that backup's
    /// frozen manifest **object** — never from this catalog, which never
    /// held that per-tablet detail in the first place) up to
    /// `cutoff_wall_ms`.
    ///
    /// **Split-lineage aware, but deliberately NOT built on
    /// [`live_split_descendants`]** (ADR 0059 §6's own on-demand-capture
    /// re-planning accessor) — a real bug found building this function's
    /// own first e2e test, not by design review: `live_split_descendants`
    /// answers "live" descendants, and a tablet retired by
    /// `DropTableTablets` (an ordinary table drop, never a split) has
    /// **no** `split_lineage` entry of its own to descend through, so it
    /// reads as having no live descendant at all — exactly correct for
    /// on-demand capture (which has nothing left to *capture* once its own
    /// table is gone) but wrong here, where the whole point is to keep
    /// replaying a DROPPED table's own already-sealed segments (ADR 0059
    /// §9/§10's own outlives-the-source-table carve-out). This function
    /// instead walks the `split_lineage` **tree forward** from each base
    /// tablet — itself, unconditionally, plus every split descendant
    /// however many generations deep — and includes each visited tablet's
    /// own segments regardless of whether that tablet (or any of its
    /// ancestors) is currently live: a table drop never touches
    /// `split_lineage` or `pitr_segments` at all, so "was this tablet ever
    /// live" is simply not a fact this function needs to know. A cascading
    /// split (parent → mid → {a, b}) therefore replays the parent's own
    /// tail, then `mid`'s own full chain, then `a`'s and `b`'s own full
    /// chains, in one direct forward traversal with no separate ancestor
    /// walk-back needed.
    ///
    /// **Precision, stated plainly rather than left to be discovered by
    /// diffing this against the ADR's own prose**: [`PitrSegmentRow::
    /// seal_wall_ms`] is pinned once **per segment**, never per record — a
    /// segment is included or excluded as a whole unit against
    /// `cutoff_wall_ms`, never split mid-body. This is a **safe**
    /// (never-includes-a-later-write) approximation of "the packed-HLC
    /// cutoff corresponding to wall-clock second `T`": every record inside
    /// an included segment was durably committed strictly before that
    /// segment's own seal, itself at or before `cutoff_wall_ms` by this
    /// function's own filter. The cost is the opposite direction: a record
    /// truly committed just before `T` but batched into a segment sealed
    /// just after it is excluded, not included — bounded to at most one
    /// seal interval's worth of imprecision, and the identical direction
    /// ADR 0059 §9's own "never claim `now`" `LatestRestorableDateTime`
    /// rule already accepts.
    ///
    /// Deliberately **not** sorted or interleaved across tablets in the
    /// returned `Vec` — see [`PitrRestorePlan::segments`]'s own doc for why
    /// the restore driver's replay order never matters.
    #[must_use]
    pub fn pitr_replay_segments(
        &self,
        base_tablet_progress: &[(TabletId, u64)],
        cutoff_wall_ms: u64,
    ) -> Vec<PitrReplaySegmentRef> {
        let mut out = Vec::new();
        for &(base_tablet, base_cut_version) in base_tablet_progress {
            // Forward DFS over the `split_lineage` subtree rooted at
            // `base_tablet` — `base_tablet` itself first (`is_root: true`,
            // the only stack entry floored at the base's own cut version),
            // then every split descendant, however many generations deep.
            // Bounded by `split_lineage.len() + 1` pushes at most (each
            // tablet is discovered as a CHILD at most once, since a child
            // key is unique in the map), so a malformed/cyclic chain —
            // which should never occur; `split_lineage` is a tree, written
            // once per child — still cannot loop forever.
            let mut stack = vec![(base_tablet, true)];
            let mut visited = 0usize;
            while let Some((tablet, is_root)) = stack.pop() {
                visited += 1;
                if visited > self.split_lineage.len() + 1 {
                    break; // defensive bound; see doc above
                }
                let floor = if is_root { base_cut_version } else { 0 };
                for (&(_, epoch), row) in self.pitr_segments.range((tablet, 0)..=(tablet, u64::MAX))
                {
                    if row.hlc_range.1 <= floor {
                        continue; // fully covered by the base snapshot already
                    }
                    if row.seal_wall_ms > cutoff_wall_ms {
                        continue; // sealed after the requested cutoff
                    }
                    out.push(PitrReplaySegmentRef {
                        tablet,
                        epoch,
                        object_id: row.object_id.clone(),
                        replay_range: (floor.max(row.hlc_range.0), row.hlc_range.1),
                    });
                }
                for (&child, lineage) in &self.split_lineage {
                    if lineage.parent == tablet {
                        stack.push((child, false));
                    }
                }
            }
        }
        out
    }

    /// See [`PitrRestoreWindow`]'s own doc for the full contract.
    #[must_use]
    pub fn pitr_restore_window(&self, table: &str) -> Option<PitrRestoreWindow> {
        let live_spec = self.table_pitr(table);
        let generation = live_spec
            .map(|s| s.generation)
            .or_else(|| self.pitr_generation.get(table).copied())?;
        if generation == 0 {
            return None; // this table name has never enabled PITR at all
        }
        let enabled_ms = live_spec
            .filter(|s| s.generation == generation)
            .map(|s| s.enabled_wall_ms);

        let bases: Vec<&BackupRow> = self
            .pitr_base_backups_for_table(table)
            .map(|(_, row)| row)
            .collect();
        let segment_tablets: BTreeSet<TabletId> = self
            .pitr_segments
            .iter()
            .filter(|(_, row)| row.table == table && row.generation == generation)
            .map(|((t, _), _)| *t)
            .collect();

        let earliest_ms = enabled_ms
            .or_else(|| bases.first().map(|r| r.manifest.created_wall_ms))
            .or_else(|| {
                self.pitr_segments
                    .iter()
                    .filter(|(_, row)| row.table == table && row.generation == generation)
                    .map(|(_, row)| row.seal_wall_ms)
                    .min()
            })?;

        let latest_ms = if segment_tablets.is_empty() {
            // Nothing has sealed yet. A live, just-(re)enabled generation
            // with no base snapshot either still has a trivially valid
            // (zero-width) window — "restorable to the moment it was
            // enabled" — mirroring `pitr_description`'s own identical
            // `.unwrap_or(spec.enabled_wall_ms)` fallback; only a table
            // whose generation is known **only** through history (no live
            // `PitrSpec`, i.e. disabled or dropped) with no base snapshot
            // either has genuinely nothing to report.
            bases
                .last()
                .map(|r| r.manifest.created_wall_ms)
                .or(enabled_ms)
        } else {
            segment_tablets
                .iter()
                .map(|&t| self.last_pitr_seal_wall_ms(t).unwrap_or(earliest_ms))
                .min()
        }?;

        Some(PitrRestoreWindow {
            generation,
            earliest_ms,
            latest_ms: latest_ms.max(earliest_ms),
        })
    }

    /// The tablets scoped to `table` (ADR 0023), in ascending tablet-id order.
    /// Empty if no table-scoped tablet exists for it yet (a freshly created table
    /// whose tablet has not committed, or a legacy cluster whose only tablet is the
    /// whole-keyspace `None` one). The legacy whole-keyspace tablet is **not**
    /// returned here — it is the routing fallback, not a tablet *of* the table.
    pub fn tablets_for_table<'a>(
        &'a self,
        table: &'a str,
    ) -> impl Iterator<Item = (&'a TabletId, &'a Tablet)> {
        self.tablets
            .iter()
            .filter(move |(_, t)| t.table.as_deref() == Some(table))
    }

    /// Whether at least one tablet is scoped to `table` (ADR 0023). When false, a
    /// key of `table` routes to the legacy whole-keyspace tablet if present.
    #[must_use]
    pub fn has_table_tablet(&self, table: &str) -> bool {
        self.tablets_for_table(table).next().is_some()
    }

    /// Count of tablets whose **current** replica set still names `node` (ADR
    /// 0032 PR3): the drain-complete predicate — a member is safe to remove
    /// only once this is `0` — and the same invariant
    /// [`MetaCommand::RemoveMember`]'s apply-time guard enforces. Removing a
    /// member while any tablet still lists it as a replica would silently drop
    /// that tablet below its replication factor with the member gone from the
    /// placement candidate pool entirely, and no repair path left (placement
    /// only ever chooses from `Active` members).
    #[must_use]
    pub fn tablets_referencing(&self, node: &NodeId) -> usize {
        self.tablets
            .values()
            .filter(|t| t.replicas.contains(node))
            .count()
    }

    /// **ADR 0040 PR6**: every node-identity claim in this snapshot that is
    /// eligible for the orphan-member sweep, judged purely from what
    /// `Metadata` itself can see. This is a **candidate set, not a removal
    /// decision** — the caller (the control-plane leader's own volatile
    /// timer, `node.rs::orphan_sweep_loop`) still has to (a) require a
    /// candidate to persist across `orphan_sweep_after` before proposing
    /// anything (a one-tick candidate is not itself sweep-worthy — the whole
    /// point is a *grace period*), and (b) exclude anything in the **current
    /// control-voter set**, which is `RaftCore`'s own live config and lives
    /// nowhere in `Metadata` — this state machine has no way to know it and
    /// must not guess.
    ///
    /// Iterates the **union** of [`Metadata::members`] and
    /// [`Metadata::node_addrs`]'s keys — a claim can exist in either, or
    /// both, and this must catch every shape:
    /// - **A `members` row exists** (whether or not `node_addrs` also has
    ///   one — e.g. `admin_add_member`'s bare `UpsertMember{Down}` growth
    ///   registration, which claims no address until the node itself
    ///   self-registers one later): eligible iff `status ==
    ///   `[`Down`](NodeStatus::Down)` (excludes `Active`/`Joining` — still
    ///   live or forming — and `Leaving` — decommission territory, always
    ///   already `has_activated` in practice since a member must have been
    ///   `Active` to ever become `Leaving`), `!has_activated` (never showed
    ///   up, as opposed to a real member that later went down), and it is
    ///   unreferenced by any tablet's replica set (mirroring
    ///   [`RemoveMember`](MetaCommand::RemoveMember)'s own apply-time guard —
    ///   this predicate is a superset check, never a substitute for it: the
    ///   sweep still proposes the real command, whose own guard is the
    ///   actual safety net).
    /// - **No `members` row at all**: the claim-without-member shape (a
    ///   control-role [`RegisterNode`](MetaCommand::RegisterNode) claims only
    ///   `node_addrs`, by design — see that command's doc). Always eligible
    ///   by this predicate alone: it can never be `Active`/`Joining` (it
    ///   never claims `members`) and can never be tablet-referenced
    ///   (placement only ever chooses from `members`) — its only real
    ///   safety gate is the control-voter exclusion the caller applies.
    #[must_use]
    pub fn orphan_sweep_candidates(&self) -> BTreeSet<NodeId> {
        self.members
            .keys()
            .chain(self.node_addrs.keys())
            .filter(|id| self.is_orphan_sweep_candidate(id))
            .cloned()
            .collect()
    }

    fn is_orphan_sweep_candidate(&self, node: &NodeId) -> bool {
        match self.members.get(node) {
            None => true,
            Some(m) => {
                m.status == NodeStatus::Down
                    && !m.has_activated
                    && self.tablets_referencing(node) == 0
            }
        }
    }

    /// The next tablet id a proposer should request when creating a tablet (ADR
    /// 0023). Folds the monotonic `next_tablet_id` counter together with the highest
    /// existing id (so a pre-counter snapshot still allocates above its tablets) and
    /// a floor of `1` (id `0` is reserved/unused). Race-safe with retry: if two
    /// proposers pick the same id, the second's `CreateTablet` is rejected as a
    /// duplicate and it re-reads this for a fresh id.
    #[must_use]
    pub fn next_free_tablet_id(&self) -> TabletId {
        let highest = self.tablets.keys().map(|t| t.0).max().unwrap_or(0);
        TabletId(self.next_tablet_id.max(highest + 1).max(1))
    }

    /// **ADR 0059 §6 (the backup-vs-split race)**: every currently-**live**
    /// tablet whose `split_lineage` chain traces back to `ancestor`,
    /// transitively. `ancestor` itself, as a single-element vec, if it is
    /// still live (the common case — most tablets never split during a
    /// backup's own capture window); otherwise the union of its live
    /// descendants, recursing through however many generations a cascading
    /// split chain produced (`split_lineage` is a tree keyed child → parent,
    /// walked here in the opposite direction via a linear scan — cheap
    /// enough for this catalog's size, and this accessor is a background-
    /// loop-tick concern, never a hot path). Empty only if `ancestor` is
    /// neither live nor has ever recorded a child in `split_lineage` — a
    /// tablet retired some way other than a split (today, only a dropped
    /// table's `DropTableTablets`) has no live descendant to substitute, and
    /// a caller sees "nothing to capture from," not a false completion (see
    /// [`pinned_tablet_capture_complete`](Self::pinned_tablet_capture_complete)'s
    /// own empty-set handling).
    #[must_use]
    pub fn live_split_descendants(&self, ancestor: TabletId) -> Vec<TabletId> {
        if self.tablets.contains_key(&ancestor) {
            return vec![ancestor];
        }
        let mut out = Vec::new();
        for (&child, lineage) in &self.split_lineage {
            if lineage.parent == ancestor {
                out.extend(self.live_split_descendants(child));
            }
        }
        out
    }

    /// **ADR 0059 §6**: does `tablet`'s own `split_lineage` chain trace back
    /// to one of `pinned`'s tablets (directly, or through one or more
    /// cascading splits)? The admission test
    /// [`MetaCommand::RecordBackupTabletComplete`]'s apply arm uses this in
    /// place of a bare direct-membership check, so a re-planned split
    /// descendant's own completion report is accepted — the tablet id it
    /// carries was never itself pinned, but its lineage proves it captured
    /// exactly the range a pinned (now-retired) ancestor would have.
    /// Bounded by `split_lineage`'s own size against a malformed/cyclic
    /// chain, which should never occur (`split_lineage` is a tree, written
    /// once per child at `CutoverSplit`) but must never spin forever if it
    /// somehow did.
    fn traces_to_pinned(&self, pinned: &[BackupPinnedTablet], tablet: TabletId) -> bool {
        let mut cur = tablet;
        for _ in 0..=self.split_lineage.len() {
            if pinned.iter().any(|t| t.tablet == cur) {
                return true;
            }
            let Some(lineage) = self.split_lineage.get(&cur) else {
                return false;
            };
            cur = lineage.parent;
        }
        false
    }

    /// **ADR 0059 §4/§6**: has `pinned` tablet's own share of `backup_id`
    /// been fully captured? True directly (a progress row at `pinned`
    /// itself) when `pinned` is still live and never split; otherwise —
    /// `pinned` retired via a split — true once **every** one of its live
    /// `split_lineage` descendants
    /// ([`live_split_descendants`](Self::live_split_descendants)) has its
    /// own progress row. `false` when that descendant set is empty (a
    /// tablet retired some way other than a split has nothing to wait on
    /// that could ever report — see `live_split_descendants`'s own doc), so
    /// this never treats a vacuous "every descendant reported" over an
    /// empty set as completion.
    fn pinned_tablet_capture_complete(&self, backup_id: &str, pinned: TabletId) -> bool {
        let live = self.live_split_descendants(pinned);
        !live.is_empty()
            && live.iter().all(|t| {
                self.backup_tablet_progress
                    .contains_key(&(backup_id.to_owned(), *t))
            })
    }

    /// **ADR 0059 §3/§4/§6**: is every one of `backup_id`'s pinned tablets
    /// fully captured (directly, or via re-planned live split descendants)?
    /// The pure readiness predicate the completion aggregator (`animusd`, a
    /// later PR) polls before proposing
    /// [`MetaCommand::CompleteBackup`] — sharing
    /// [`pinned_tablet_capture_complete`](Self::pinned_tablet_capture_complete)
    /// with that command's own apply-time re-check rather than
    /// re-deriving the same decision twice. `false` for an unknown backup id
    /// or one not currently [`Creating`](BackupStatus::Creating).
    #[must_use]
    pub fn backup_ready_to_complete(&self, backup_id: &str) -> bool {
        let Some(row) = self.backups.get(backup_id) else {
            return false;
        };
        if !matches!(row.status, BackupStatus::Creating) {
            return false;
        }
        row.manifest
            .pinned_tablets
            .iter()
            .all(|t| self.pinned_tablet_capture_complete(backup_id, t.tablet))
    }

    /// **ADR 0059 §4/§6**: is `tablet` (a **live** tablet) currently a
    /// capture target of `backup_id` — i.e. should a capture driver leading
    /// `tablet` be doing work for this backup right now? True when
    /// `backup_id` is [`Creating`](BackupStatus::Creating), `tablet` is
    /// live, and `tablet` is directly pinned or traces back to a pinned
    /// (now-retired) ancestor via `split_lineage`
    /// ([`traces_to_pinned`](Self::traces_to_pinned)). The one predicate
    /// both the capture driver (a later PR, deciding what to work on) and
    /// this corpus's own driver-tick mirror evaluate — never re-derived
    /// independently, so the two can't drift on what counts as "pinned."
    #[must_use]
    pub fn backup_capture_target(&self, backup_id: &str, tablet: TabletId) -> bool {
        let Some(row) = self.backups.get(backup_id) else {
            return false;
        };
        matches!(row.status, BackupStatus::Creating)
            && self.tablets.contains_key(&tablet)
            && self.traces_to_pinned(&row.manifest.pinned_tablets, tablet)
    }

    /// **ADR 0059 §3/§4/§6**: the manifest object's own authoritative
    /// per-tablet completion-record list for `backup_id` — every one of its
    /// pinned tablets' **current live** `split_lineage` frontier
    /// ([`live_split_descendants`](Self::live_split_descendants)), each
    /// paired with its own progress row (`None` until it has actually
    /// reported).
    ///
    /// Deliberately **not** a blanket iteration of every
    /// `Metadata::backup_tablet_progress` row tagged with this backup id.
    /// Consider a pinned tablet that reports its own completion directly
    /// and *only then* happens to split (an ordinary, backup-unrelated
    /// split racing a backup that had already finished that one tablet's
    /// share before the split ever committed): its direct report at the
    /// now-retired id is genuine and harmless to leave on file, but once
    /// the split lands that id is no longer part of its own
    /// `live_split_descendants` frontier — its two children are — so a
    /// naive "every progress row tagged with this backup" scan would put
    /// **three** overlapping entries in the final manifest (the retired
    /// parent's full-range capture, plus each child's own independent
    /// re-capture of a sub-range of that same range), double-counting rows
    /// a restore/verification pass would then see twice. This accessor is
    /// what keeps that stale, superseded report out of the manifest: the
    /// authoritative reporting tablet set for a pinned ancestor is always
    /// exactly its current live frontier, never "whatever tablet id
    /// happened to report, ever."
    #[must_use]
    pub fn backup_manifest_tablet_progress(
        &self,
        backup_id: &str,
    ) -> Vec<(TabletId, Option<BackupTabletProgress>)> {
        let Some(row) = self.backups.get(backup_id) else {
            return Vec::new();
        };
        row.manifest
            .pinned_tablets
            .iter()
            .flat_map(|p| self.live_split_descendants(p.tablet))
            .map(|t| {
                let progress = self
                    .backup_tablet_progress
                    .get(&(backup_id.to_owned(), t))
                    .copied();
                (t, progress)
            })
            .collect()
    }

    /// The backup catalog row for `backup_id`, if any (ADR 0059 §3). A read
    /// accessor for the admin observer surface and a future wire edge.
    #[must_use]
    pub fn backup(&self, backup_id: &str) -> Option<&BackupRow> {
        self.backups.get(backup_id)
    }

    /// The restore catalog row for `restore_id`, if any (ADR 0059 §7). A
    /// read accessor for the restore driver and observability surfaces.
    #[must_use]
    pub fn restore(&self, restore_id: &str) -> Option<&RestoreRow> {
        self.restores.get(restore_id)
    }

    /// `backup_id`'s own per-tablet completion records (ADR 0059 §3/§4) —
    /// `DescribeBackup`'s per-tablet progress list.
    pub fn backup_tablet_progress_for<'a>(
        &'a self,
        backup_id: &'a str,
    ) -> impl Iterator<Item = (TabletId, &'a BackupTabletProgress)> {
        self.backup_tablet_progress
            .iter()
            .filter(move |((id, _), _)| id == backup_id)
            .map(|((_, tablet), progress)| (*tablet, progress))
    }

    /// `backup_id`'s total captured bytes so far (ADR 0059 §2's "total
    /// object sizes, for `DescribeBackup`") — the sum of every pinned
    /// tablet's own **currently authoritative** reported
    /// [`BackupTabletProgress::bytes`]
    /// ([`backup_manifest_tablet_progress`](Self::backup_manifest_tablet_progress),
    /// never a blanket sum over every `backup_tablet_progress` row tagged
    /// with this id — that accessor's own doc explains why a pinned
    /// tablet's stale, split-superseded direct report must never be
    /// double-counted alongside its live descendants' own shares). `0`
    /// before any (still-authoritative) tablet has reported.
    #[must_use]
    pub fn backup_total_bytes(&self, backup_id: &str) -> u64 {
        self.backup_manifest_tablet_progress(backup_id)
            .iter()
            .filter_map(|(_, progress)| progress.map(|p| p.bytes))
            .sum()
    }
}

/// `Metadata` is the control plane's replicated state machine. **ADR 0038
/// PR3 (the cutover): `DRIVER_APPLIED = true`.** `RaftCore` no longer applies
/// commands in-core — it agrees the order and durability of `MetaCommand`s and
/// buffers each committed-and-durable one as an effect (`RaftCore::drain_apply`)
/// for `node.rs`'s async apply task to apply to its own privately-owned
/// `Metadata` (still via the real, unchanged inherent [`Metadata::apply`],
/// which returns an [`ApplyOutcome`] the apply task uses to decide what to
/// mirror into the system keyspace) and publish into the engine-backed,
/// `engine_applied`-gated cache every reader (`RaftNode::metadata`, `admin.rs`,
/// the dashboard, `reconcile_loop`/`detect_loop`) now reads. This trait impl's
/// own `apply` is consequently never called — mirroring
/// `animus-cp-data::KvState`'s identical `unreachable!()` shape for its own
/// `DRIVER_APPLIED` state machine.
///
/// [`RaftCore`]: crate::raft::RaftCore
impl crate::raft::StateMachine<MetaCommand> for Metadata {
    const DRIVER_APPLIED: bool = true;

    fn apply(&mut self, _command: &MetaCommand) {
        unreachable!(
            "Metadata is DRIVER_APPLIED (ADR 0038 PR3); node.rs's apply task \
             applies to its own owned Metadata and the system-keyspace engine"
        )
    }

    fn noop() -> MetaCommand {
        MetaCommand::NoOp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, ColumnType};

    /// `ReplaceTableSchema` (the atomic `ALTER TABLE` primitive): replaces an
    /// existing table's schema in **one apply** — rejected when there is no schema
    /// to replace (an ALTER cannot create a table) or when the replacement is
    /// malformed; a no-op when identical (a re-proposed ALTER does not churn the
    /// log). At no point between commands can a reader see the table schema-less
    /// (the failure mode of the old drop-then-recreate).
    #[test]
    fn replace_table_schema_is_atomic_and_validated() {
        let mut m = Metadata::default();
        let base = TableSchema::simple("pk", ColumnType::String);

        // No schema yet: replace is rejected (not an upsert).
        assert_eq!(
            m.apply(&MetaCommand::ReplaceTableSchema {
                table: "ks.users".to_owned(),
                schema: base.clone(),
            }),
            ApplyOutcome::Rejected("no schema to replace for table")
        );

        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "ks.users".to_owned(),
                schema: base.clone(),
            }),
            ApplyOutcome::Applied
        );

        // Replacing with an identical schema is a no-op.
        assert_eq!(
            m.apply(&MetaCommand::ReplaceTableSchema {
                table: "ks.users".to_owned(),
                schema: base.clone(),
            }),
            ApplyOutcome::NoOp
        );

        // The ALTER shape: the current schema with a column appended, in one apply.
        let mut extended = base.clone();
        extended
            .columns
            .push(ColumnDef::new("age", ColumnType::Number));
        assert_eq!(
            m.apply(&MetaCommand::ReplaceTableSchema {
                table: "ks.users".to_owned(),
                schema: extended.clone(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.schemas.get("ks.users"), Some(&extended));

        // A malformed replacement is rejected and the schema is untouched.
        let malformed = TableSchema::with_columns("pk", Vec::new(), Vec::new());
        assert!(malformed.validate().is_err(), "test premise");
        assert_eq!(
            m.apply(&MetaCommand::ReplaceTableSchema {
                table: "ks.users".to_owned(),
                schema: malformed,
            }),
            ApplyOutcome::Rejected("malformed table schema")
        );
        assert_eq!(m.schemas.get("ks.users"), Some(&extended));
    }

    /// `SetIndexStatus` (ADR 0045): rejects an absent table, rejects an
    /// absent index on a table that does exist, is a no-op when the index is
    /// already at the target status, and otherwise transitions the status
    /// visibly through `table_indexes` (leaving every other field of the
    /// index definition untouched).
    #[test]
    fn set_index_status_apply_arm() {
        let mut m = Metadata::default();

        // No such table at all.
        assert_eq!(
            m.apply(&MetaCommand::SetIndexStatus {
                table: "ghost".to_owned(),
                index: "by-email".to_owned(),
                status: IndexStatus::Active,
            }),
            ApplyOutcome::Rejected("no such table schema")
        );

        let base = TableSchema::simple("id", ColumnType::String);
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "users".to_owned(),
                schema: base,
            }),
            ApplyOutcome::Applied
        );

        // Table exists, but the named index does not.
        assert_eq!(
            m.apply(&MetaCommand::SetIndexStatus {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                status: IndexStatus::Active,
            }),
            ApplyOutcome::Rejected("no such table index")
        );

        let index = IndexDef {
            name: "by-email".to_owned(),
            kind: crate::schema::IndexKind::Global,
            hash_attribute: "email".to_owned(),
            sort_attribute: None,
            projection: crate::schema::IndexProjection::All,
            status: IndexStatus::Creating,
            hash_attribute_type: None,
            sort_attribute_type: None,
        };
        assert_eq!(
            m.apply(&MetaCommand::CreateTableIndex {
                table: "users".to_owned(),
                index,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.table_indexes("users")[0].status,
            IndexStatus::Creating,
            "test premise: index starts Creating"
        );

        // Already at the target status: a no-op, and the definition is
        // otherwise untouched.
        assert_eq!(
            m.apply(&MetaCommand::SetIndexStatus {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                status: IndexStatus::Creating,
            }),
            ApplyOutcome::NoOp
        );

        // A genuine transition applies and is visible via `table_indexes`.
        assert_eq!(
            m.apply(&MetaCommand::SetIndexStatus {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                status: IndexStatus::Active,
            }),
            ApplyOutcome::Applied
        );
        let idx = &m.table_indexes("users")[0];
        assert_eq!(idx.status, IndexStatus::Active);
        assert_eq!(idx.name, "by-email");
        assert_eq!(idx.hash_attribute, "email");
    }

    /// A fixture for the `MarkIndexBackfilled` tests below: a table with one
    /// `Creating` GSI and one tablet actually scoped to it.
    fn table_with_index_and_tablet(m: &mut Metadata, table: &str, index: &str, tablet: TabletId) {
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTableIndex {
                table: table.to_owned(),
                index: IndexDef {
                    name: index.to_owned(),
                    kind: crate::schema::IndexKind::Global,
                    hash_attribute: "email".to_owned(),
                    sort_attribute: None,
                    projection: crate::schema::IndexProjection::All,
                    status: IndexStatus::Creating,
                    hash_attribute_type: None,
                    sort_attribute_type: None,
                },
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet,
                table: Some(table.to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );
    }

    /// `MarkIndexBackfilled` (ADR 0045 §4): rejects an absent table, rejects
    /// an absent index on a table that does exist, is idempotent on a repeat
    /// proposal for the same `(tablet, index)`, and otherwise records a
    /// visible row in `Metadata::index_backfill`.
    #[test]
    fn mark_index_backfilled_apply_arm() {
        let mut m = Metadata::default();

        // No such table at all.
        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "ghost".to_owned(),
                index: "by-email".to_owned(),
                tablet: TabletId(1),
            }),
            ApplyOutcome::Rejected("no such table schema")
        );

        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "users".to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );

        // Table exists, but the named index does not.
        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                tablet: TabletId(1),
            }),
            ApplyOutcome::Rejected("no such table index")
        );

        // Add the index and a tablet scoped to it (the schema already
        // exists from just above, so drive `CreateTableIndex`/`CreateTablet`
        // directly here instead of the shared fixture, which would try to
        // recreate the schema).
        assert_eq!(
            m.apply(&MetaCommand::CreateTableIndex {
                table: "users".to_owned(),
                index: IndexDef {
                    name: "by-email".to_owned(),
                    kind: crate::schema::IndexKind::Global,
                    hash_attribute: "email".to_owned(),
                    sort_attribute: None,
                    projection: crate::schema::IndexProjection::All,
                    status: IndexStatus::Creating,
                    hash_attribute_type: None,
                    sort_attribute_type: None,
                },
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );

        // A genuine mark applies and is visible.
        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                tablet: TabletId(1),
            }),
            ApplyOutcome::Applied
        );
        assert!(
            m.index_backfill
                .contains_key(&(TabletId(1), "by-email".to_owned()))
        );

        // A repeat proposal (the seeder's own crash-retry) is idempotent.
        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                tablet: TabletId(1),
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(m.index_backfill.len(), 1);
    }

    /// `MarkIndexBackfilled` rejects a tablet that does not currently belong
    /// to the named table — either because it belongs to a different table,
    /// or because it has never existed at all.
    #[test]
    fn mark_index_backfilled_rejects_a_tablet_not_scoped_to_the_table() {
        let mut m = Metadata::default();
        table_with_index_and_tablet(&mut m, "users", "by-email", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(2),
                table: Some("orders".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );

        // A tablet that belongs to a different table.
        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                tablet: TabletId(2),
            }),
            ApplyOutcome::Rejected("tablet is not scoped to this table")
        );

        // A tablet id that has never existed at all.
        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                tablet: TabletId(999),
            }),
            ApplyOutcome::Rejected("tablet is not scoped to this table")
        );
        assert!(m.index_backfill.is_empty());
    }

    // --- ADR 0059 §3: the backup catalog ---------------------------------

    /// A fixture for the backup-catalog tests below: a table with one
    /// tablet, ready for `BeginBackup`.
    fn table_with_one_tablet(m: &mut Metadata, table: &str, tablet: TabletId) {
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet,
                table: Some(table.to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );
    }

    /// `BeginBackup` (ADR 0059 §3): rejected against an unknown table;
    /// otherwise mints a `Creating` row whose manifest stub is derived
    /// entirely from already-agreed state (the table's current schema and
    /// tablet list) — never from anything else the command carries. A
    /// second `BeginBackup` for the same id is rejected outright, even
    /// against a different table (a fresh id is minted per request, so
    /// there is nothing to legitimately retry against).
    #[test]
    fn begin_backup_apply_arm() {
        let mut m = Metadata::default();

        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "ghost".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Rejected("no such table schema")
        );

        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        let row = m.backup("backup-1").expect("row present");
        assert_eq!(row.table, "users");
        assert_eq!(row.status, BackupStatus::Creating);
        assert_eq!(row.manifest.created_wall_ms, 1000);
        assert_eq!(row.manifest.schema, *m.table_schema("users").unwrap());
        assert_eq!(
            row.manifest.pinned_tablets,
            vec![BackupPinnedTablet {
                tablet: TabletId(1),
                range: KeyRange::whole(),
            }]
        );

        // A second table, ready for a would-be collision.
        table_with_one_tablet(&mut m, "orders", TabletId(2));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "orders".to_owned(),
                created_wall_ms: 2000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Rejected("backup id already exists")
        );
        // Untouched by the rejected collision.
        assert_eq!(m.backup("backup-1").unwrap().table, "users");
    }

    /// `RecordBackupTabletComplete` (ADR 0059 §3/§4): rejected against an
    /// unknown backup, a backup not `Creating`, or a tablet not pinned in
    /// it; idempotent on an identical repeat (the capture driver's own
    /// crash-retry); rejected as a genuine conflict on a differing repeat.
    #[test]
    fn record_backup_tablet_complete_apply_arm() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "ghost".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Rejected("no such backup")
        );

        // A tablet never pinned in this backup.
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(2),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Rejected("tablet is not pinned in this backup")
        );

        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.backup_tablet_progress
                .get(&("backup-1".to_owned(), TabletId(1))),
            Some(&BackupTabletProgress {
                cut_version: 10,
                bytes: 100,
            })
        );

        // Identical repeat: the capture driver's own crash-retry.
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::NoOp
        );

        // A genuinely differing repeat is rejected, not silently applied.
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 11,
                bytes: 100,
            }),
            ApplyOutcome::Rejected("tablet already reported a different completion")
        );

        // Once complete, a further report — even an identical one — is
        // rejected: the backup is no longer `Creating`.
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Rejected("backup is not Creating")
        );
    }

    /// `CompleteBackup` (ADR 0059 §3/§4): rejected against an unknown
    /// backup or one not `Creating`; rejected while any pinned tablet has
    /// not yet reported; `Applied` (flips to `Available`) once every pinned
    /// tablet has.
    #[test]
    fn complete_backup_apply_arm() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "ghost".to_owned(),
            }),
            ApplyOutcome::Rejected("no such backup")
        );

        // No pinned tablet has reported yet.
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Rejected("not every pinned tablet has reported completion")
        );

        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.backup("backup-1").unwrap().status,
            BackupStatus::Available
        );
        assert_eq!(
            m.backup("backup-1").unwrap().total_bytes,
            100,
            "CompleteBackup freezes the final byte total onto the row"
        );

        // Already `Available`: a second `CompleteBackup` is rejected, not
        // silently re-applied.
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Rejected("backup is not Creating")
        );
    }

    /// `CompleteBackup` over a backup with **two** pinned tablets: rejected
    /// until BOTH have reported, not just one.
    #[test]
    fn complete_backup_requires_every_pinned_tablet() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "users".to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        // `CreateTablet` allows only one tablet per table (ADR 0023) — a
        // genuine second tablet on the same table comes only from a real
        // split, so drive one (via `split_tablet`, in-place) to end up with
        // two `Active` tablets scoped to `users`.
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );
        let split_key = [0x80; TOKEN_BYTES].to_vec();
        split_tablet(&mut m, TabletId(1), split_key, TabletId(2));

        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.backup("backup-1").unwrap().manifest.pinned_tablets.len(),
            2
        );

        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(2),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Rejected("not every pinned tablet has reported completion")
        );

        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(3),
                cut_version: 20,
                bytes: 200,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.backup_total_bytes("backup-1"), 300);
    }

    /// `FailBackup` (ADR 0059 §3/§4): rejected against an unknown backup;
    /// applies from `Creating`; idempotent on an identical repeat reason;
    /// applies again on a differing reason while still `Failed`; rejected
    /// once the backup is `Available` (a completed backup cannot
    /// subsequently "fail").
    #[test]
    fn fail_backup_apply_arm() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::FailBackup {
                backup_id: "ghost".to_owned(),
                reason: "timeout".to_owned(),
            }),
            ApplyOutcome::Rejected("no such backup")
        );

        assert_eq!(
            m.apply(&MetaCommand::FailBackup {
                backup_id: "backup-1".to_owned(),
                reason: "timeout".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.backup("backup-1").unwrap().status,
            BackupStatus::Failed {
                reason: "timeout".to_owned()
            }
        );

        // Identical repeat is idempotent.
        assert_eq!(
            m.apply(&MetaCommand::FailBackup {
                backup_id: "backup-1".to_owned(),
                reason: "timeout".to_owned(),
            }),
            ApplyOutcome::NoOp
        );

        // A differing reason while still `Failed` applies (updates the
        // recorded reason).
        assert_eq!(
            m.apply(&MetaCommand::FailBackup {
                backup_id: "backup-1".to_owned(),
                reason: "store unreachable".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.backup("backup-1").unwrap().status,
            BackupStatus::Failed {
                reason: "store unreachable".to_owned()
            }
        );

        // An `Available` backup cannot subsequently fail.
        let mut avail = Metadata::default();
        table_with_one_tablet(&mut avail, "users", TabletId(1));
        assert_eq!(
            avail.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-2".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            avail.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-2".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            avail.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-2".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            avail.apply(&MetaCommand::FailBackup {
                backup_id: "backup-2".to_owned(),
                reason: "too late".to_owned(),
            }),
            ApplyOutcome::Rejected("backup is not in a failable state")
        );
    }

    /// `DeleteBackup` (ADR 0059 §3): removes the row and every one of its
    /// own per-tablet progress records; idempotent on an unknown id.
    #[test]
    fn delete_backup_apply_arm() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::DeleteBackup {
                backup_id: "ghost".to_owned(),
            }),
            ApplyOutcome::NoOp
        );

        assert_eq!(
            m.apply(&MetaCommand::DeleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert!(m.backup("backup-1").is_none());
        assert!(m.backup_tablet_progress_for("backup-1").next().is_none());

        // Idempotent: an already-deleted id is a no-op.
        assert_eq!(
            m.apply(&MetaCommand::DeleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::NoOp
        );
    }

    /// `MarkBackupDeleted` (ADR 0059 §3, Train 1 PR④): the janitor's own
    /// two-phase **mark** step, driven by the `DeleteBackup` wire operation —
    /// rejects an unknown id or a still-`Creating` backup, transitions
    /// `Available`/`Failed` to `Expired`, and is idempotent once `Expired`.
    /// The row itself survives this command (only the existing, unmodified
    /// `DeleteBackup` command removes it).
    #[test]
    fn mark_backup_deleted_apply_arm() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: "ghost".to_owned(),
            }),
            ApplyOutcome::Rejected("no such backup")
        );

        // Still `Creating` — rejected (the wire edge's own
        // `BackupInUseException` check happens first in practice; this is
        // the apply-time seatbelt).
        assert_eq!(
            m.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Rejected("backup is not in a deletable state")
        );

        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.backup("backup-1").unwrap().status,
            BackupStatus::Available
        );

        assert_eq!(
            m.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.backup("backup-1").unwrap().status, BackupStatus::Expired);

        // Idempotent once `Expired`.
        assert_eq!(
            m.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::NoOp
        );

        // A `Failed` backup is markable too (the janitor treats `Failed`
        // identically to `Expired` for reclaim purposes).
        let mut failed = Metadata::default();
        table_with_one_tablet(&mut failed, "users", TabletId(1));
        assert_eq!(
            failed.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-2".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            failed.apply(&MetaCommand::FailBackup {
                backup_id: "backup-2".to_owned(),
                reason: "timeout".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            failed.apply(&MetaCommand::MarkBackupDeleted {
                backup_id: "backup-2".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            failed.backup("backup-2").unwrap().status,
            BackupStatus::Expired
        );
    }

    // --- ADR 0059 §7, Train 2: the restore catalog -----------------------

    /// `BeginRestore` (ADR 0059 §7): mints exactly one fresh `Building`
    /// tablet over the whole ring, scoped to the target table, plus a
    /// `Seeding` restore row — rejected on a duplicate restore id, a
    /// colliding tablet id, or a tablet id below the monotonic allocator
    /// floor.
    #[test]
    fn begin_restore_apply_arm() {
        let mut m = Metadata::default();
        // The target table's schema is created first, exactly as
        // `provision_tablet` requires ahead of an ordinary `CreateTable`.
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "restored".to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::BeginRestore {
                restore_id: "restore-1".to_owned(),
                backup_id: "backup-1".to_owned(),
                source_table: "users".to_owned(),
                target_table: "restored".to_owned(),
                tablet: TabletId(5),
                replicas: vec![nid(1)],
                gsi_defs: Vec::new(),
                pitr: None,
            }),
            ApplyOutcome::Applied
        );
        let row = m.restore("restore-1").expect("row present");
        assert_eq!(row.backup_id, "backup-1");
        assert_eq!(row.source_table, "users");
        assert_eq!(row.target_table, "restored");
        assert_eq!(row.tablet, TabletId(5));
        assert_eq!(row.status, RestoreStatus::Seeding);
        let tablet = &m.tablets[&TabletId(5)];
        assert_eq!(tablet.state, TabletState::Building);
        assert_eq!(tablet.range, KeyRange::whole());
        assert_eq!(tablet.table.as_deref(), Some("restored"));
        assert!(!tablet.is_routable());

        // A second `BeginRestore` at the same restore id is rejected
        // outright, even naming a different (also-fresh) tablet.
        assert_eq!(
            m.apply(&MetaCommand::BeginRestore {
                restore_id: "restore-1".to_owned(),
                backup_id: "backup-2".to_owned(),
                source_table: "users".to_owned(),
                target_table: "restored".to_owned(),
                tablet: TabletId(6),
                replicas: vec![nid(1)],
                gsi_defs: Vec::new(),
                pitr: None,
            }),
            ApplyOutcome::Rejected("restore id already exists")
        );

        // A colliding tablet id.
        assert_eq!(
            m.apply(&MetaCommand::BeginRestore {
                restore_id: "restore-2".to_owned(),
                backup_id: "backup-2".to_owned(),
                source_table: "users".to_owned(),
                target_table: "restored2".to_owned(),
                tablet: TabletId(5),
                replicas: vec![nid(1)],
                gsi_defs: Vec::new(),
                pitr: None,
            }),
            ApplyOutcome::Rejected("tablet already exists")
        );

        // Below the monotonic allocator floor.
        assert_eq!(
            m.apply(&MetaCommand::BeginRestore {
                restore_id: "restore-3".to_owned(),
                backup_id: "backup-2".to_owned(),
                source_table: "users".to_owned(),
                target_table: "restored3".to_owned(),
                tablet: TabletId(0),
                replicas: vec![nid(1)],
                gsi_defs: Vec::new(),
                pitr: None,
            }),
            ApplyOutcome::Rejected("tablet id below the monotonic allocator")
        );
    }

    /// `CompleteRestore`/`FailRestore` (ADR 0059 §7): completion activates
    /// the tablet and flips the row `Done`; failure leaves the tablet
    /// `Building` (never served) and flips the row `Failed`. Both reject a
    /// terminal-contradicting call, and `FailRestore` is idempotent on an
    /// identical repeat.
    #[test]
    fn complete_and_fail_restore_apply_arms() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "restored".to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginRestore {
                restore_id: "restore-1".to_owned(),
                backup_id: "backup-1".to_owned(),
                source_table: "users".to_owned(),
                target_table: "restored".to_owned(),
                tablet: TabletId(5),
                replicas: vec![nid(1)],
                gsi_defs: Vec::new(),
                pitr: None,
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::CompleteRestore {
                restore_id: "ghost".to_owned(),
            }),
            ApplyOutcome::Rejected("no such restore")
        );

        let before_epoch = m.tablets[&TabletId(5)].epoch;
        assert_eq!(
            m.apply(&MetaCommand::CompleteRestore {
                restore_id: "restore-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.restore("restore-1").unwrap().status, RestoreStatus::Done);
        let tablet = &m.tablets[&TabletId(5)];
        assert_eq!(tablet.state, TabletState::Active);
        assert!(tablet.is_routable());
        assert!(tablet.epoch > before_epoch);

        // Already `Done` — a second completion is rejected, not idempotent
        // (mirroring `CompleteBackup`'s own already-`Available` rejection).
        assert_eq!(
            m.apply(&MetaCommand::CompleteRestore {
                restore_id: "restore-1".to_owned(),
            }),
            ApplyOutcome::Rejected("restore is not Seeding")
        );
        // ...and `FailRestore` cannot contradict a completed restore either.
        assert_eq!(
            m.apply(&MetaCommand::FailRestore {
                restore_id: "restore-1".to_owned(),
                reason: "too late".to_owned(),
            }),
            ApplyOutcome::Rejected("restore already completed")
        );

        // A second, independent restore that fails instead: the tablet
        // stays `Building` (never served, but cleanly droppable).
        assert_eq!(
            m.apply(&MetaCommand::BeginRestore {
                restore_id: "restore-2".to_owned(),
                backup_id: "backup-2".to_owned(),
                source_table: "users".to_owned(),
                target_table: "restored2".to_owned(),
                tablet: TabletId(6),
                replicas: vec![nid(1)],
                gsi_defs: Vec::new(),
                pitr: None,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::FailRestore {
                restore_id: "restore-2".to_owned(),
                reason: "stuck".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.restore("restore-2").unwrap().status,
            RestoreStatus::Failed {
                reason: "stuck".to_owned()
            }
        );
        assert_eq!(m.tablets[&TabletId(6)].state, TabletState::Building);
        // Idempotent on an identical repeat.
        assert_eq!(
            m.apply(&MetaCommand::FailRestore {
                restore_id: "restore-2".to_owned(),
                reason: "stuck".to_owned(),
            }),
            ApplyOutcome::NoOp
        );
        // A genuinely differing reason still transitions (mirrors
        // `FailBackup`'s own "not a repair path, but a differing repeat
        // still applies" shape).
        assert_eq!(
            m.apply(&MetaCommand::FailRestore {
                restore_id: "restore-2".to_owned(),
                reason: "stuck again".to_owned(),
            }),
            ApplyOutcome::Applied
        );
    }

    /// ADR 0024/ADR 0059 §3's explicit carve-out: `DropTableSchema`/
    /// `DropTableTablets` must NOT touch the backup catalog — a backup row
    /// (and its progress records) survives a drop of its source table.
    #[test]
    fn backup_catalog_survives_a_table_drop() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );

        assert_eq!(
            m.apply(&MetaCommand::DropTableTablets {
                table: "users".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::DropTableSchema {
                table: "users".to_owned(),
            }),
            ApplyOutcome::Applied
        );

        // The table is genuinely gone...
        assert!(!m.has_table_schema("users"));
        assert!(!m.has_table_tablet("users"));
        // ...but the backup row and its progress record are untouched.
        let row = m
            .backup("backup-1")
            .expect("backup row survives table drop");
        assert_eq!(row.status, BackupStatus::Available);
        assert_eq!(row.table, "users");
        assert_eq!(
            m.backup_tablet_progress
                .get(&("backup-1".to_owned(), TabletId(1))),
            Some(&BackupTabletProgress {
                cut_version: 10,
                bytes: 100,
            })
        );
        // `total_bytes` was frozen at `CompleteBackup` time and survives the
        // drop unchanged — `backup_total_bytes`'s own live re-derivation
        // would instead report 0 here (every one of this backup's tablets
        // is gone from `Metadata::tablets`), which is exactly the silent
        // regression `BackupRow::total_bytes` exists to avoid.
        assert_eq!(row.total_bytes, 100);
        assert_eq!(
            m.backup_total_bytes("backup-1"),
            0,
            "the live accessor legitimately goes to zero post-drop — it is \
             not what DescribeBackup/ListBackups read from, `BackupRow::\
             total_bytes` is"
        );
    }

    /// ADR 0059 §6: a pinned tablet that splits mid-capture re-plans onto
    /// its live descendants — `live_split_descendants`/
    /// `backup_capture_target`/`traces_to_pinned` all agree, and the split
    /// child's own completion report (never itself pinned) is admitted.
    #[test]
    fn backup_survives_a_split_of_its_pinned_tablet() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1_000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        let pinned = m
            .backup("backup-1")
            .unwrap()
            .manifest
            .pinned_tablets
            .clone();
        assert_eq!(
            pinned,
            vec![BackupPinnedTablet {
                tablet: TabletId(1),
                range: KeyRange::whole(),
            }]
        );

        // Before the split: tablet 1 is its own sole live descendant, and is
        // the one and only capture target.
        assert_eq!(m.live_split_descendants(TabletId(1)), vec![TabletId(1)]);
        assert!(m.backup_capture_target("backup-1", TabletId(1)));
        assert!(!m.backup_ready_to_complete("backup-1"));

        // Split tablet 1 into 2 (left) / 3 (right) mid-capture — the parent
        // retires from `Metadata::tablets` entirely.
        split_tablet(&mut m, TabletId(1), b"m".to_vec(), TabletId(2));
        assert!(!m.tablets.contains_key(&TabletId(1)));

        // The retired parent is no longer itself a capture target (it's
        // gone); its two live children now are, via `split_lineage`.
        assert!(!m.backup_capture_target("backup-1", TabletId(1)));
        assert!(m.backup_capture_target("backup-1", TabletId(2)));
        assert!(m.backup_capture_target("backup-1", TabletId(3)));
        let mut descendants = m.live_split_descendants(TabletId(1));
        descendants.sort();
        assert_eq!(descendants, vec![TabletId(2), TabletId(3)]);

        // A tablet unrelated to this lineage is never a target.
        assert!(!m.backup_capture_target("backup-1", TabletId(99)));

        // A retired parent's own completion report is still ADMITTED (it is
        // directly pinned — `traces_to_pinned` doesn't care whether the id
        // is still live), covering the legitimate race where the parent
        // genuinely finished its whole-range capture before an unrelated
        // split retired it. But it does NOT count toward completion (its id
        // is no longer part of its own `live_split_descendants` frontier —
        // the two children are), so it must not let the backup complete
        // with only the children's shares half-done, and (per
        // `backup_manifest_tablet_progress`'s own doc) never becomes a
        // stale, double-counted entry in the eventual manifest.
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 1,
                bytes: 1,
            }),
            ApplyOutcome::Applied
        );
        assert!(!m.backup_ready_to_complete("backup-1"));
        assert!(
            m.backup_manifest_tablet_progress("backup-1")
                .iter()
                .all(|(t, _)| *t != TabletId(1)),
            "the retired parent's own orphaned report must never surface in the manifest's \
             authoritative tablet list, which is always the current live frontier"
        );

        // Only the left child reports: not ready yet — the right child's
        // own share is still outstanding.
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(2),
                cut_version: 10,
                bytes: 111,
            }),
            ApplyOutcome::Applied
        );
        assert!(!m.backup_ready_to_complete("backup-1"));
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Rejected("not every pinned tablet has reported completion")
        );

        // The right child reports too: now ready, and `CompleteBackup`
        // succeeds — the manifest's own `pinned_tablets` list still names
        // the retired parent (a historical, never-mutated stub), while the
        // catalog's actual progress is keyed by the two real descendants.
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(3),
                cut_version: 12,
                bytes: 222,
            }),
            ApplyOutcome::Applied
        );
        assert!(m.backup_ready_to_complete("backup-1"));
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.backup("backup-1").unwrap().status,
            BackupStatus::Available
        );
        assert_eq!(m.backup_total_bytes("backup-1"), 111 + 222);
        assert_eq!(
            m.backup("backup-1").unwrap().manifest.pinned_tablets,
            vec![BackupPinnedTablet {
                tablet: TabletId(1),
                range: KeyRange::whole(),
            }],
            "the manifest stub's pinned-tablet list is a frozen historical \
             snapshot, never rewritten onto the re-planned descendants"
        );
    }

    /// A cascading split (a re-planned descendant splitting again before it
    /// finishes its own share) re-plans transitively — `traces_to_pinned`/
    /// `live_split_descendants` walk however many generations it takes.
    #[test]
    fn backup_survives_a_cascading_split_of_a_split_descendant() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1_000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        split_tablet(&mut m, TabletId(1), b"m".to_vec(), TabletId(2));
        // Split the LEFT child (tablet 2) again before it ever reports.
        split_tablet(&mut m, TabletId(2), b"g".to_vec(), TabletId(4));
        assert!(!m.tablets.contains_key(&TabletId(2)));

        let mut descendants = m.live_split_descendants(TabletId(1));
        descendants.sort();
        assert_eq!(
            descendants,
            vec![TabletId(3), TabletId(4), TabletId(5)],
            "tablet 1's live frontier is now three generations-2 tablets: \
             the once-split-then-split-again left branch's two children, \
             plus the untouched right child"
        );
        for &t in &descendants {
            assert!(m.backup_capture_target("backup-1", t));
        }
        assert!(!m.backup_capture_target("backup-1", TabletId(1)));
        assert!(!m.backup_capture_target("backup-1", TabletId(2)));

        for &t in &descendants {
            assert_eq!(
                m.apply(&MetaCommand::RecordBackupTabletComplete {
                    backup_id: "backup-1".to_owned(),
                    tablet: t,
                    cut_version: 1,
                    bytes: 10,
                }),
                ApplyOutcome::Applied
            );
        }
        assert!(m.backup_ready_to_complete("backup-1"));
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "backup-1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
    }

    /// A tablet with no lineage relationship to `backup_id`'s pinned list at
    /// all is never admitted, split or not — `traces_to_pinned` must not
    /// accept an unrelated tablet just because *some* `split_lineage` entry
    /// exists somewhere in the catalog.
    #[test]
    fn record_backup_tablet_complete_rejects_an_unrelated_tablets_report_even_with_other_splits_on_file()
     {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        table_with_one_tablet(&mut m, "other", TabletId(10));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1_000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        // An unrelated table's tablet splits — its lineage must never leak
        // into backup-1's own admission decision.
        split_tablet(&mut m, TabletId(10), b"m".to_vec(), TabletId(11));
        assert!(!m.backup_capture_target("backup-1", TabletId(11)));
        assert!(!m.backup_capture_target("backup-1", TabletId(12)));
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(11),
                cut_version: 1,
                bytes: 1,
            }),
            ApplyOutcome::Rejected("tablet is not pinned in this backup")
        );
    }

    /// `Metadata` round-trips through JSON with a populated backup catalog
    /// (mirroring the `stream_shards`/`index_backfill` tuple-key-codec
    /// regression this crate already guards against — see
    /// `metadata_round_trips_through_json_with_populated_stream_shards`).
    #[test]
    fn metadata_round_trips_through_json_with_populated_backups() {
        let mut m = Metadata::default();
        table_with_one_tablet(&mut m, "users", TabletId(1));
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "backup-1".to_owned(),
                table: "users".to_owned(),
                created_wall_ms: 1000,
                backup_name: "backup".to_string(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::RecordBackupTabletComplete {
                backup_id: "backup-1".to_owned(),
                tablet: TabletId(1),
                cut_version: 10,
                bytes: 100,
            }),
            ApplyOutcome::Applied
        );

        let json = serde_json::to_string(&m).expect("serializes");
        let round_tripped: Metadata = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(m, round_tripped);
    }

    /// ADR 0059 §7 (Train 2): `Metadata::restores` round-trips through JSON
    /// with a populated row (its key is a plain `String`, not a tuple, so
    /// this doesn't hit the `serde_json` non-string-map-key hazard the
    /// `stream_shards`/`index_backfill`/`backup_tablet_progress` tests above
    /// exist for — but an empty collection still can't prove the *value*
    /// shape round-trips, including its nested `gsi_defs: Vec<IndexDef>`).
    #[test]
    fn metadata_round_trips_through_json_with_populated_restores() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "restored".to_owned(),
                schema: TableSchema::simple("id", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginRestore {
                restore_id: "restore-1".to_owned(),
                backup_id: "backup-1".to_owned(),
                source_table: "users".to_owned(),
                target_table: "restored".to_owned(),
                tablet: TabletId(5),
                replicas: vec![nid(1)],
                gsi_defs: vec![IndexDef {
                    name: "by-status".to_owned(),
                    kind: crate::schema::IndexKind::Global,
                    hash_attribute: "status".to_owned(),
                    sort_attribute: None,
                    projection: crate::schema::IndexProjection::All,
                    status: IndexStatus::Creating,
                    hash_attribute_type: None,
                    sort_attribute_type: None,
                }],
                pitr: None,
            }),
            ApplyOutcome::Applied
        );

        let json = serde_json::to_string(&m).expect("serializes");
        let round_tripped: Metadata = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(m, round_tripped);
    }

    /// `DropTableTablets` (ADR 0045): pruning a table's tablets also prunes
    /// every `index_backfill` row keyed to one of those tablet ids, whatever
    /// index it names — a gone tablet id can never be reported against
    /// again — while leaving another table's rows (even a same-named index
    /// on a different table) untouched.
    #[test]
    fn drop_table_tablets_prunes_index_backfill_rows_for_the_dropped_tablets() {
        let mut m = Metadata::default();
        table_with_index_and_tablet(&mut m, "users", "by-email", TabletId(1));
        table_with_index_and_tablet(&mut m, "orders", "by-email", TabletId(2));
        for (table, tablet) in [("users", TabletId(1)), ("orders", TabletId(2))] {
            assert_eq!(
                m.apply(&MetaCommand::MarkIndexBackfilled {
                    table: table.to_owned(),
                    index: "by-email".to_owned(),
                    tablet,
                }),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(m.index_backfill.len(), 2);

        assert_eq!(
            m.apply(&MetaCommand::DropTableTablets {
                table: "users".to_owned(),
            }),
            ApplyOutcome::Applied
        );

        assert!(
            !m.index_backfill
                .contains_key(&(TabletId(1), "by-email".to_owned())),
            "the dropped table's row must be pruned"
        );
        assert!(
            m.index_backfill
                .contains_key(&(TabletId(2), "by-email".to_owned())),
            "the other table's same-named-index row must survive"
        );
    }

    /// `DropTableTablets` (ADR 0062 §2): pruning a table's tablets also
    /// prunes every `split_placing` row keyed to one of those dropped
    /// tablet ids — the identical `index_backfill` orphan-prevention prune
    /// above, for the new catalog — while leaving another table's row
    /// untouched.
    #[test]
    fn drop_table_tablets_prunes_split_placing_rows_for_the_dropped_tablets() {
        let mut m = Metadata::default();
        for (table, tablet) in [("users", TabletId(1)), ("orders", TabletId(2))] {
            assert_eq!(
                m.apply(&MetaCommand::CreateTablet {
                    tablet,
                    table: Some(table.to_owned()),
                    range: KeyRange::whole(),
                    replicas: vec![nid(1)],
                }),
                ApplyOutcome::Applied
            );
            m.split_placing.insert(
                tablet,
                SplitPlacing {
                    target: Some(vec![nid(9)]),
                    done: false,
                },
            );
        }
        assert_eq!(m.split_placing.len(), 2);

        assert_eq!(
            m.apply(&MetaCommand::DropTableTablets {
                table: "users".to_owned(),
            }),
            ApplyOutcome::Applied
        );

        assert!(
            !m.split_placing.contains_key(&TabletId(1)),
            "the dropped table's row must be pruned"
        );
        assert!(
            m.split_placing.contains_key(&TabletId(2)),
            "the other table's row must survive"
        );
    }

    /// `DropTableIndex` (ADR 0045): dropping an index prunes every
    /// `index_backfill` row for that index name, scoped to the owning
    /// table's own tablets — a distinct table's row for a same-named index
    /// must survive untouched.
    #[test]
    fn drop_table_index_prunes_index_backfill_rows_for_that_index() {
        let mut m = Metadata::default();
        table_with_index_and_tablet(&mut m, "users", "by-email", TabletId(1));
        table_with_index_and_tablet(&mut m, "orders", "by-email", TabletId(2));
        for (table, tablet) in [("users", TabletId(1)), ("orders", TabletId(2))] {
            assert_eq!(
                m.apply(&MetaCommand::MarkIndexBackfilled {
                    table: table.to_owned(),
                    index: "by-email".to_owned(),
                    tablet,
                }),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(m.index_backfill.len(), 2);

        assert_eq!(
            m.apply(&MetaCommand::DropTableIndex {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
            }),
            ApplyOutcome::Applied
        );

        assert!(
            !m.index_backfill
                .contains_key(&(TabletId(1), "by-email".to_owned())),
            "the dropped index's row must be pruned"
        );
        assert!(
            m.index_backfill
                .contains_key(&(TabletId(2), "by-email".to_owned())),
            "the other table's same-named index must be untouched"
        );

        // Idempotent: a repeat drop is a no-op that prunes nothing further
        // (there is nothing left to prune).
        assert_eq!(
            m.apply(&MetaCommand::DropTableIndex {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
            }),
            ApplyOutcome::NoOp
        );
    }

    /// `CreateTableSchema` rejects a table name that collides with the
    /// reserved system-keyspace namespace (ADR 0038), both an exact match and
    /// a name merely prefixed by it, and leaves the catalog untouched.
    #[test]
    fn create_table_schema_rejects_reserved_namespace() {
        let mut m = Metadata::default();
        let schema = TableSchema::simple("pk", ColumnType::String);

        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: crate::syskv::RESERVED_NAMESPACE.to_owned(),
                schema: schema.clone(),
            }),
            ApplyOutcome::Rejected("table name collides with the reserved system namespace")
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: format!("{}_backup", crate::syskv::RESERVED_NAMESPACE),
                schema: schema.clone(),
            }),
            ApplyOutcome::Rejected("table name collides with the reserved system namespace")
        );
        assert!(m.schemas.is_empty(), "no schema should have been recorded");

        // An ordinary name is unaffected.
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "ks.orders".to_owned(),
                schema,
            }),
            ApplyOutcome::Applied
        );
    }

    /// `CreateTableSchema` rejects any user table name containing the
    /// reserved `$` separator (the collision-safety argument ADR 0041's
    /// hidden index-table naming convention depends on).
    #[test]
    fn create_table_schema_rejects_the_reserved_separator() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders$byCustomer".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Rejected("table name may not contain the reserved `$` separator")
        );
    }

    /// `RegisterCpAddr` records a CP member's address, updates on change, and is a
    /// no-op when re-registering the same address (Phase 2 address distribution).
    /// It applies in the deterministic state machine like every other `MetaCommand`,
    /// so it replicates + recovers through Raft by construction.
    #[test]
    fn register_cp_addr_records_updates_and_is_idempotent() {
        let mut m = Metadata::default();
        let reg = |id, addr: &str| MetaCommand::RegisterCpAddr {
            id,
            addr: addr.to_owned(),
            tablet: None,
        };

        // First registration applies and is readable.
        assert_eq!(
            m.apply(&reg(nid(301), "127.0.0.1:9001")),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.cp_member_addrs.get(&nid(301)).map(String::as_str),
            Some("127.0.0.1:9001")
        );

        // Re-registering the same address is a no-op (so a periodic re-register
        // does not churn the Raft log).
        assert_eq!(
            m.apply(&reg(nid(301), "127.0.0.1:9001")),
            ApplyOutcome::NoOp
        );

        // A changed address updates the entry.
        assert_eq!(
            m.apply(&reg(nid(301), "127.0.0.1:9002")),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.cp_member_addrs.get(&nid(301)).map(String::as_str),
            Some("127.0.0.1:9002")
        );

        // A distinct member coexists.
        assert_eq!(
            m.apply(&reg(nid(401), "127.0.0.1:9101")),
            ApplyOutcome::Applied
        );
        assert_eq!(m.cp_member_addrs.len(), 2);
    }

    /// `RegisterNodeAddrs` (ADR 0032 PR1) records a node's full address book,
    /// is idempotent on an identical re-register, and overwrites on a real
    /// change — mirroring `RegisterCpAddr`'s own contract. **ADR 0040 PR4**:
    /// `RegisterNodeAddrs` is update-only now, so this pre-establishes each
    /// id's claim via `UpsertMember` first (standing in for a config-
    /// bootstrapped member) — `register_node_addrs_rejects_an_unclaimed_id`
    /// covers the "no prior claim" rejection this test used to also exercise
    /// implicitly.
    #[test]
    fn register_node_addrs_records_updates_and_is_idempotent() {
        let mut m = Metadata::default();
        let addrs = |suffix: u16| NodeAddrs {
            internal: format!("127.0.0.1:{}", 9300 + suffix),
            client: format!("127.0.0.1:{}", 9000 + suffix),
            admin: format!("127.0.0.1:{}", 9500 + suffix),
            intra: format!("127.0.0.1:{}", 9600 + suffix),
            role: "combined".to_string(),
        };
        for node in [300, 301] {
            m.apply(&MetaCommand::UpsertMember {
                node: nid(node),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            });
        }

        // First registration applies and is readable.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(300),
                addrs: addrs(0),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.node_addrs.get(&nid(300)), Some(&addrs(0)));

        // Re-registering an identical address book is a no-op.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(300),
                addrs: addrs(0),
            }),
            ApplyOutcome::NoOp
        );

        // A changed address book overwrites the entry.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(300),
                addrs: addrs(1),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.node_addrs.get(&nid(300)), Some(&addrs(1)));

        // A distinct member coexists.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(301),
                addrs: addrs(2),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.node_addrs.len(), 2);
    }

    /// A `Metadata` snapshot serialized before ADR 0032 (no `node_addrs` field
    /// in the JSON) still decodes, defaulting to an empty map — the same
    /// `#[serde(default)]` back-compat contract every other additive field on
    /// `Metadata` already carries.
    #[test]
    fn metadata_without_node_addrs_field_still_decodes() {
        let m = Metadata::default();
        let mut value = serde_json::to_value(&m).expect("metadata serializes");
        value
            .as_object_mut()
            .expect("metadata is a JSON object")
            .remove("node_addrs");
        let decoded: Metadata =
            serde_json::from_value(value).expect("metadata without node_addrs still decodes");
        assert!(decoded.node_addrs.is_empty());
    }

    /// `NodeAddrs.role` (ADR 0035 residual follow-up) replicates alongside the
    /// rest of the address book — a control-only, data-only, and combined
    /// registration each record their own distinct role string, readable by
    /// every replica off `Metadata.node_addrs` alone (no fan-out to the
    /// node's own `/admin/config` needed).
    #[test]
    fn register_node_addrs_records_the_role() {
        let mut m = Metadata::default();
        for (node, role) in [(0, "control"), (300, "data"), (301, "combined")] {
            // ADR 0040 PR4: `RegisterNodeAddrs` is update-only — establish
            // each id's claim first (standing in for a config-bootstrapped
            // member), mirroring `register_node_addrs_records_updates_and_
            // is_idempotent`'s adaptation above.
            m.apply(&MetaCommand::UpsertMember {
                node: nid(node),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            });
            assert_eq!(
                m.apply(&MetaCommand::RegisterNodeAddrs {
                    node: nid(node),
                    addrs: NodeAddrs {
                        internal: format!("127.0.0.1:{}", 9300 + node),
                        client: format!("127.0.0.1:{}", 9000 + node),
                        admin: format!("127.0.0.1:{}", 9500 + node),
                        intra: format!("127.0.0.1:{}", 9600 + node),
                        role: role.to_string(),
                    },
                }),
                ApplyOutcome::Applied
            );
            assert_eq!(
                m.node_addrs.get(&nid(node)).map(|a| a.role.as_str()),
                Some(role)
            );
        }
    }

    /// A `NodeAddrs` JSON shape serialized before ADR 0035 (no `role` field)
    /// still decodes, defaulting to `"combined"` — every node that ever
    /// proposed `RegisterNodeAddrs` before this field existed was, by
    /// construction, running in combined mode (the `Control`/`Data` split
    /// didn't exist yet), so this is the historically accurate default, the
    /// same back-compat discipline as every other additive field here.
    #[test]
    fn node_addrs_without_role_field_defaults_to_combined() {
        let addrs = NodeAddrs {
            internal: "127.0.0.1:9300".to_owned(),
            client: "127.0.0.1:9000".to_owned(),
            admin: "127.0.0.1:9500".to_owned(),
            intra: "127.0.0.1:9600".to_owned(),
            role: "combined".to_string(),
        };
        let mut value = serde_json::to_value(&addrs).expect("NodeAddrs serializes");
        value
            .as_object_mut()
            .expect("NodeAddrs is a JSON object")
            .remove("role");
        let decoded: NodeAddrs =
            serde_json::from_value(value).expect("NodeAddrs without role still decodes");
        assert_eq!(decoded.role, "combined");
        assert_eq!(decoded.internal, addrs.internal);
    }

    /// Idempotency guard for a tablet whose recorded policy targets more
    /// replicas than there are eligible candidates (the exact shape
    /// `animusd::ClientCtx::provision_tablet` now deliberately creates on a
    /// small cluster — a fix to ADR 0005's placement policy, see
    /// `docs/engineering-lessons.md`): `replan` returns
    /// `PlacementError::InsufficientCandidates`, and `reconcile_placement`'s
    /// `.ok()?` must silently skip that tablet, not panic or somehow force a
    /// too-small set through. Calling `reconcile()` repeatedly against the
    /// identical, still-under-candidated state must keep yielding **zero**
    /// proposals every time — proof there is no proposal storm (a
    /// leader that kept re-proposing the same doomed-to-reject command every
    /// tick would still be harmless *correctness*-wise, epoch-CAS makes a
    /// second acceptance impossible, but would be needless churn this test
    /// rules out directly).
    #[test]
    fn reconcile_with_insufficient_candidates_is_a_stable_noop() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(1),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("t".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );
        // The target RF (3), not the single available candidate — exactly
        // what the fixed `provision_tablet` now always records.
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("cp-rf", 3)),
            }),
            ApplyOutcome::Applied
        );

        // Only 1 of the 3 required candidates exists: `replan` must error,
        // and `reconcile()` must propose nothing — repeatedly, across
        // several simulated ticks, with no state mutation in between.
        for tick in 0..5 {
            assert_eq!(
                m.reconcile(),
                Vec::new(),
                "tick {tick}: expected zero proposals with only 1 of 3 required \
                 candidates Active — a proposal here would be a storm against a \
                 policy that can never be satisfied yet"
            );
        }

        // Once enough candidates exist, the exact same policy (unchanged
        // since creation) now correctly grows the tablet — proving this
        // isn't merely "reconcile never grows anything," but specifically
        // "reconcile correctly waits for real eligibility."
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(2),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(3),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            }),
            ApplyOutcome::Applied
        );
        let proposals = m.reconcile();
        assert_eq!(
            proposals.len(),
            1,
            "expected exactly one CasTabletReplicas proposal now that 3 candidates exist: {proposals:?}"
        );
        assert!(matches!(
            &proposals[0],
            MetaCommand::CasTabletReplicas { tablet, replicas, .. }
                if *tablet == TabletId(1) && replicas.len() == 3
        ));
    }

    /// ADR 0024 address GC: a tablet-scoped `RegisterCpAddr` entry is pruned from
    /// both maps when its tablet leaves the map (`DropTableTablets`);
    /// a registration for an absent tablet is rejected (the
    /// registrar retries); legacy tablet-less entries are never pruned; and the
    /// whole thing is **convergent under replay** — re-applying the same command
    /// sequence to a fresh state machine reaches the identical pruned state, so a
    /// replayed historical map state cannot permanently resurrect an entry.
    #[test]
    fn cp_member_addrs_are_pruned_when_their_tablet_leaves_the_map() {
        let commands = vec![
            MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            },
            // Tablet-scoped members of tablet 1.
            MetaCommand::RegisterCpAddr {
                id: nid(1301),
                addr: "127.0.0.1:9301".to_owned(),
                tablet: Some(TabletId(1)),
            },
            MetaCommand::RegisterCpAddr {
                id: nid(1302),
                addr: "127.0.0.1:9302".to_owned(),
                tablet: Some(TabletId(1)),
            },
            // A legacy (tablet-less) member: never pruned.
            MetaCommand::RegisterCpAddr {
                id: nid(301),
                addr: "127.0.0.1:9001".to_owned(),
                tablet: None,
            },
            MetaCommand::DropTableTablets {
                table: "users".to_owned(),
            },
        ];
        let replay = |cmds: &[MetaCommand]| {
            let mut m = Metadata::default();
            for c in cmds {
                m.apply(c);
            }
            m
        };

        let m = replay(&commands);
        // The dropped tablet's members were reclaimed from BOTH maps…
        assert!(!m.cp_member_addrs.contains_key(&nid(1301)));
        assert!(!m.cp_member_addrs.contains_key(&nid(1302)));
        assert!(m.cp_member_tablets.is_empty());
        // …the legacy entry survives.
        assert_eq!(
            m.cp_member_addrs.get(&nid(301)).map(String::as_str),
            Some("127.0.0.1:9001")
        );

        // Convergent under replay: a fresh replica applying the same log reaches
        // the identical state (no resurrected entries).
        assert_eq!(replay(&commands), m);

        // A registration against the now-absent tablet is rejected, so it cannot
        // resurrect the pruned entry after the drop replays.
        let mut m = m;
        assert_eq!(
            m.apply(&MetaCommand::RegisterCpAddr {
                id: nid(1301),
                addr: "127.0.0.1:9301".to_owned(),
                tablet: Some(TabletId(1)),
            }),
            ApplyOutcome::Rejected("no such tablet for cp addr")
        );
        assert!(!m.cp_member_addrs.contains_key(&nid(1301)));
    }

    /// ADR 0058 Train 2 rung 3: `BeginSplitInPlace`'s own fixture — one
    /// `Active` parent tablet (id 1, whole range, RF 3, a recorded policy)
    /// plus the command splitting it at the ring midpoint into children 2
    /// and 3 at hand-picked (distinct, DISJOINT from the parent's own
    /// replicas — this train's own Stage 1 needs at least one genuinely new
    /// home) homes.
    fn begin_split_in_place_fixture() -> (Metadata, MetaCommand) {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("users", 3)),
            }),
            ApplyOutcome::Applied
        );
        let cmd = MetaCommand::BeginSplitInPlace {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            children: [
                (TabletId(2), vec![nid(1), nid(2), nid(4)]),
                (TabletId(3), vec![nid(2), nid(3), nid(5)]),
            ],
        };
        (m, cmd)
    }

    /// `BeginSplitInPlace` marks the parent `Splitting` (range/replicas
    /// untouched — the data plane's own fork, not this command, is what
    /// moves data) and records the intent verbatim, WITHOUT minting any
    /// `Building` tablet-map rows or copying the policy (there is nothing to
    /// attach it to yet — see [`CutoverSplit`]'s own in-place test for where
    /// that happens instead) — but DOES advance the allocator floor, so the
    /// two minted ids can never be reused even before any tablet row for
    /// them exists.
    #[test]
    fn begin_split_in_place_records_the_intent_and_marks_the_parent_splitting() {
        let (mut m, cmd) = begin_split_in_place_fixture();
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);

        let parent = &m.tablets[&TabletId(1)];
        assert_eq!(parent.state, TabletState::Splitting);
        assert_eq!(parent.range, KeyRange::whole(), "parent range untouched");
        assert_eq!(
            parent.replicas,
            vec![nid(1), nid(2), nid(3)],
            "parent replicas untouched — the intent, not this field, carries the children's homes"
        );
        assert_eq!(parent.epoch, Epoch::INITIAL.next());

        let intent = parent.inplace_split.as_ref().expect("intent recorded");
        let mid = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        assert_eq!(intent.split_key, mid);
        assert_eq!(intent.children[0].id, TabletId(2));
        assert_eq!(intent.children[0].replicas, vec![nid(1), nid(2), nid(4)]);
        assert_eq!(intent.children[1].id, TabletId(3));
        assert_eq!(intent.children[1].replicas, vec![nid(2), nid(3), nid(5)]);

        // No `Building` tablet-map rows — the in-place workflow mints
        // nothing physical until the data plane's own fork.
        assert!(!m.tablets.contains_key(&TabletId(2)));
        assert!(!m.tablets.contains_key(&TabletId(3)));
        assert!(!m.policies.contains_key(&TabletId(2)));
        assert!(!m.policies.contains_key(&TabletId(3)));
        // The allocator floor still advances, so ids 2/3 can never be
        // reissued to something else even before any row exists for them.
        assert!(m.next_free_tablet_id().0 >= 4);
        assert!(m.split_lineage.is_empty());
    }

    /// `BeginSplitInPlace` rejects on the epoch/state/child-id gates the
    /// now-deleted copy-based `BeginSplit` (ADR 0050) used to share —
    /// same discipline, same fixture shape. This is the SOLE coverage of
    /// that gate discipline: the copy-based mirror of this test
    /// (`begin_split_rejects_bad_epoch_state_and_child_ids`) was deleted as
    /// redundant once this one existed (copy-split deletion stack, layer
    /// 1), and `BeginSplit` itself followed in layer B2 — nothing here was
    /// ever specific to the in-place command.
    #[test]
    fn begin_split_in_place_rejects_bad_epoch_state_and_child_ids() {
        let (mut m, cmd) = begin_split_in_place_fixture();
        let MetaCommand::BeginSplitInPlace {
            parent,
            split_key,
            children,
            ..
        } = cmd.clone()
        else {
            unreachable!()
        };

        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent,
                expected_epoch: Epoch::INITIAL.next(),
                split_key: split_key.clone(),
                children: children.clone(),
            }),
            ApplyOutcome::Rejected("epoch mismatch")
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent,
                expected_epoch: Epoch::INITIAL,
                split_key: split_key.clone(),
                children: [(TabletId(2), vec![nid(4)]), (TabletId(2), vec![nid(5)])],
            }),
            ApplyOutcome::Rejected("child ids must be distinct")
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent,
                expected_epoch: Epoch::INITIAL,
                split_key: split_key.clone(),
                children: [(TabletId(1), vec![nid(4)]), (TabletId(9), vec![nid(5)])],
            }),
            ApplyOutcome::Rejected("child tablet id already exists")
        );

        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent,
                expected_epoch: Epoch::INITIAL.next(),
                split_key,
                children: [(TabletId(10), vec![nid(4)]), (TabletId(11), vec![nid(5)])],
            }),
            ApplyOutcome::Rejected("tablet is not Active"),
            "one split at a time — a re-split of an already-Splitting parent is rejected"
        );
    }

    /// ADR 0058 Train 2 rung 3, Stage 4: `CutoverSplit`'s in-place branch
    /// activates both children DIRECTLY from the parent's own intent — no
    /// `Building` scan — inheriting the parent's policy at THIS moment (the
    /// in-place workflow's only chance to), writing `split_lineage`
    /// identically to the copy-based branch, and removing the parent.
    ///
    /// Also carries three assertions ported from the deleted copy-based
    /// `cutover_split_activates_children_removes_parent_and_freezes_lineage`/
    /// `cutover_split_records_the_parents_final_stream_epoch` tests
    /// (copy-split deletion stack, layer B2) — none of this command's own
    /// branch-selection logic (epoch-CAS, `Splitting`-state gate, stream-epoch
    /// derivation, post-cutover parent absence) is copy-branch-specific, so
    /// there is no reason to prove it twice: a cutover attempted while the
    /// parent is still `Active` (pre-begin) is rejected, the parent's final
    /// stream epoch (the chain's highest sealed epoch, not its first) is
    /// still recorded correctly on this branch, and a duplicate cutover after
    /// the parent is gone is rejected on "no such tablet".
    #[test]
    fn cutover_split_in_place_activates_both_children_from_the_intent() {
        let (mut m, cmd) = begin_split_in_place_fixture();

        // Cutover before any begin: the parent is `Active`, not `Splitting`.
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                cutover_wall_ms: 99,
            }),
            ApplyOutcome::Rejected("tablet is not Splitting")
        );

        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        let parent_epoch = m.tablets[&TabletId(1)].epoch;

        // The parent's final stream epoch: the chain's highest sealed epoch
        // (7, via two catalog rows), not its first.
        for epoch in [3u64, 7] {
            m.stream_shards.insert(
                (TabletId(1), epoch),
                StreamShardRow {
                    table: "users".to_owned(),
                    label: "L".to_owned(),
                    view_type: default_stream_view_type(),
                    hlc_range: (epoch * 10, epoch * 10 + 9),
                    count: 1,
                    seal_wall_ms: 0,
                    replicas: vec![nid(1)],
                    object_id: format!("users/L/1/{epoch}/x"),
                    expired: false,
                },
            );
        }

        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: parent_epoch,
                cutover_wall_ms: 99,
            }),
            ApplyOutcome::Applied
        );

        assert!(!m.tablets.contains_key(&TabletId(1)), "parent removed");
        assert!(
            !m.policies.contains_key(&TabletId(1)),
            "parent policy removed"
        );

        let mid = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        let left = &m.tablets[&TabletId(2)];
        assert_eq!(left.state, TabletState::Active);
        assert_eq!(left.table.as_deref(), Some("users"));
        assert_eq!(left.replicas, vec![nid(1), nid(2), nid(4)]);
        assert_eq!(left.range.end.as_deref(), Some(mid.as_slice()));
        assert_eq!(left.epoch, Epoch::INITIAL.next());
        assert!(
            left.inplace_split.is_none(),
            "the intent does not persist onto the child"
        );

        let right = &m.tablets[&TabletId(3)];
        assert_eq!(right.state, TabletState::Active);
        assert_eq!(right.replicas, vec![nid(2), nid(3), nid(5)]);
        assert_eq!(right.range.start, mid);

        // The policy, only ever attachable at cutover for this workflow,
        // is inherited by both children.
        assert!(m.policies.contains_key(&TabletId(2)));
        assert!(m.policies.contains_key(&TabletId(3)));

        for child in [TabletId(2), TabletId(3)] {
            let lineage = &m.split_lineage[&child];
            assert_eq!(lineage.parent, TabletId(1));
            assert_eq!(
                lineage.parents_final_epoch,
                Some(7),
                "the chain's highest sealed epoch, not its first"
            );
            assert_eq!(lineage.cutover_wall_ms, 99);
        }

        // A duplicate cutover finds no parent left.
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: parent_epoch,
                cutover_wall_ms: 100,
            }),
            ApplyOutcome::Rejected("no such tablet")
        );
    }

    /// ADR 0062 §2, case 1 ("already satisfying"): when a fresh
    /// `select_replicas` computation under the child's inherited policy
    /// agrees with the replicas the child was just forked onto, `CutoverSplit`
    /// writes NO `split_placing` entry at all — there is nothing for a
    /// directed-Placing convergence phase to ever do.
    #[test]
    fn cutover_split_in_place_writes_no_placing_entry_when_already_satisfying() {
        let mut m = Metadata::default();
        for n in [1u64, 2, 3] {
            assert_eq!(
                m.apply(&MetaCommand::UpsertMember {
                    node: nid(n),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                }),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("users", 3)),
            }),
            ApplyOutcome::Applied
        );
        // Fork-first (ADR 0062 §1): both children inherit the parent's own
        // current replicas verbatim — the ONLY three active candidates that
        // exist, so `select_replicas` can only ever re-derive this exact set.
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
                children: [
                    (TabletId(2), vec![nid(1), nid(2), nid(3)]),
                    (TabletId(3), vec![nid(1), nid(2), nid(3)]),
                ],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL.next(),
                cutover_wall_ms: 1,
            }),
            ApplyOutcome::Applied
        );

        assert!(
            m.split_placing.is_empty(),
            "an already-satisfying fork needs no directed-Placing obligation"
        );
    }

    /// ADR 0062 §2, case 2 (a real, satisfiable, non-trivial target): when
    /// `select_replicas` prefers a DIFFERENT set than the child's
    /// fork-inherited replicas, `CutoverSplit` writes `SplitPlacing{target:
    /// Some(wanted), done: false}` for each child.
    #[test]
    fn cutover_split_in_place_writes_a_placing_target_when_a_better_placement_exists() {
        let mut m = Metadata::default();
        // Four active candidates ("n1".."n4", string-sorted in that exact
        // order); RF 3 over them prefers the three lowest ids — [n1, n2, n3]
        // — which the parent's own current replicas ([n2, n3, n4]) are not.
        for n in [1u64, 2, 3, 4] {
            assert_eq!(
                m.apply(&MetaCommand::UpsertMember {
                    node: nid(n),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                }),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(2), nid(3), nid(4)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("users", 3)),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
                children: [
                    (TabletId(2), vec![nid(2), nid(3), nid(4)]),
                    (TabletId(3), vec![nid(2), nid(3), nid(4)]),
                ],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL.next(),
                cutover_wall_ms: 1,
            }),
            ApplyOutcome::Applied
        );

        for child in [TabletId(2), TabletId(3)] {
            let entry = &m.split_placing[&child];
            assert_eq!(entry.target, Some(vec![nid(1), nid(2), nid(3)]));
            assert!(!entry.done);
        }
    }

    /// ADR 0062 §2 fork B, case 3 (unsatisfiable at cutover): when no
    /// `Active` candidates exist at all (`begin_split_in_place_fixture`'s own
    /// `Metadata::default()`, RF 3), `select_replicas` errs — `CutoverSplit`
    /// still writes an entry, `SplitPlacing{target: None, done: false}`,
    /// rather than silently skipping it. This is what makes an
    /// unsatisfiable-at-cutover child a visible, keep-retrying obligation
    /// instead of a gap nothing will ever revisit.
    #[test]
    fn cutover_split_in_place_writes_a_pending_placing_entry_when_unsatisfiable() {
        let (mut m, cmd) = begin_split_in_place_fixture();
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        let parent_epoch = m.tablets[&TabletId(1)].epoch;
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: parent_epoch,
                cutover_wall_ms: 1,
            }),
            ApplyOutcome::Applied
        );

        for child in [TabletId(2), TabletId(3)] {
            let entry = &m.split_placing[&child];
            assert_eq!(entry.target, None);
            assert!(!entry.done);
        }
    }

    /// `MarkSplitPlacingDone` happy path: epoch-CAS against the CHILD's own
    /// current epoch, flips `done` on an existing un-done entry.
    #[test]
    fn mark_split_placing_done_happy_path() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(9)]),
                done: false,
            },
        );
        let epoch = m.tablets[&TabletId(2)].epoch;

        assert_eq!(
            m.apply(&MetaCommand::MarkSplitPlacingDone {
                tablet: TabletId(2),
                expected_epoch: epoch,
            }),
            ApplyOutcome::Applied
        );
        assert!(m.split_placing[&TabletId(2)].done);
        // `target` is left exactly as `CutoverSplit` wrote it — a diagnostic
        // record, never updated by this command.
        assert_eq!(m.split_placing[&TabletId(2)].target, Some(vec![nid(9)]));
    }

    /// `MarkSplitPlacingDone` rejects an epoch mismatch against the child's
    /// own current epoch — a stale confirm racing a later churn event on
    /// this same tablet is rejected, not marked done against moved-on state.
    #[test]
    fn mark_split_placing_done_rejects_epoch_mismatch() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(9)]),
                done: false,
            },
        );
        let stale_epoch = m.tablets[&TabletId(2)].epoch.next();

        assert_eq!(
            m.apply(&MetaCommand::MarkSplitPlacingDone {
                tablet: TabletId(2),
                expected_epoch: stale_epoch,
            }),
            ApplyOutcome::Rejected("epoch mismatch")
        );
        assert!(!m.split_placing[&TabletId(2)].done);
    }

    /// `MarkSplitPlacingDone` rejects a tablet with no `split_placing` entry
    /// at all — nothing to mark done (also covers a nonexistent tablet, via
    /// the same "no such tablet" epoch-CAS gate every other tablet-mutating
    /// command uses).
    #[test]
    fn mark_split_placing_done_rejects_when_no_entry_exists() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        let epoch = m.tablets[&TabletId(2)].epoch;

        assert_eq!(
            m.apply(&MetaCommand::MarkSplitPlacingDone {
                tablet: TabletId(2),
                expected_epoch: epoch,
            }),
            ApplyOutcome::Rejected("no split_placing entry for this tablet")
        );

        assert_eq!(
            m.apply(&MetaCommand::MarkSplitPlacingDone {
                tablet: TabletId(999),
                expected_epoch: Epoch::INITIAL,
            }),
            ApplyOutcome::Rejected("no such tablet")
        );
    }

    /// `MarkSplitPlacingDone` is idempotent on an already-`done` entry — a
    /// re-propose from the proposer's own retry is a harmless no-op, the
    /// `MarkIndexBackfilled`/`RecordBackupTabletComplete` idiom.
    #[test]
    fn mark_split_placing_done_is_idempotent_on_an_already_done_entry() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(9)]),
                done: false,
            },
        );
        let epoch = m.tablets[&TabletId(2)].epoch;
        let cmd = MetaCommand::MarkSplitPlacingDone {
            tablet: TabletId(2),
            expected_epoch: epoch,
        };
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        assert_eq!(m.apply(&cmd), ApplyOutcome::NoOp);
        assert!(m.split_placing[&TabletId(2)].done);
    }

    /// Shared fixture for the issue #528 regression tests below: an un-done
    /// `split_placing` entry with a stored target `[n1, n2, n3]` over four
    /// `Active` candidates (`n1..n4`) and an RF3 policy — mirrors the
    /// through-Raft `placement_split_placing.rs` suite's own 4-candidates
    /// shape, at the pure-`Metadata` level.
    fn split_placing_dwell_fixture() -> Metadata {
        let mut m = Metadata::default();
        for n in [1u64, 2, 3, 4] {
            assert_eq!(
                m.apply(&MetaCommand::UpsertMember {
                    node: nid(n),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                }),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(2),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                // Deliberately DIFFERENT from the stored target below (n4
                // instead of n3), mirroring a freshly-cutover, not-yet-
                // converged child, so the reconcile phase has real work to
                // do until it converges.
                replicas: vec![nid(1), nid(2), nid(4)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(2),
                policy: Some(PlacementPolicy::simple("users", 3)),
            }),
            ApplyOutcome::Applied
        );
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(1), nid(2), nid(3)]),
                done: false,
            },
        );
        m
    }

    /// **Issue #528 regression (red on the pre-fix code, which recomputed
    /// `select_replicas` fresh every tick)**: a stored target whose members
    /// are ALL `Active` is driven toward verbatim, and flipping one member
    /// `Down` then back `Active` — a flap, never past the dwell gate — never
    /// changes the computed proposal set away from that same original
    /// target, even mid-flap. On the pre-fix code, `n1` going `Down` would
    /// have made `select_replicas` pick a fresh 3-of-{n2,n3,n4} target
    /// immediately; here it does not, because `n1` never crosses
    /// `retarget_ready`.
    #[test]
    fn split_placing_reconcile_does_not_retarget_on_a_flap() {
        let mut m = split_placing_dwell_fixture();
        let empty: BTreeSet<TabletId> = BTreeSet::new();

        // Healthy: proposes the CAS toward the stored target (replicas
        // still the fork-inherited set, differing from it).
        let proposals = m.split_placing_reconcile(&empty);
        assert_eq!(proposals.len(), 1, "{proposals:?}");
        assert!(matches!(
            &proposals[0],
            MetaCommand::CasTabletReplicas { tablet, replicas, .. }
                if *tablet == TabletId(2) && *replicas == vec![nid(1), nid(2), nid(3)]
        ));

        // Flap: n1 goes Down. `retarget_ready` is still empty (the driver's
        // dwell has not elapsed — indeed this is the very first tick it's
        // been observed down) — the phase must propose NOTHING for this
        // tablet: not a retarget, and not a `CasTabletReplicas` toward a
        // target with a now-dead member.
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(1),
                labels: BTreeMap::new(),
                status: NodeStatus::Down,
            }),
            ApplyOutcome::Applied
        );
        let proposals = m.split_placing_reconcile(&empty);
        assert!(
            proposals.is_empty(),
            "expected a pause (no proposal) while n1 is down but not yet retarget-ready: {proposals:?}"
        );

        // Flap recovers: n1 back to Active before ever crossing the dwell.
        // The stored target is untouched (`RetargetSplitPlacing` was never
        // proposed), so the phase resumes driving toward the SAME original
        // target — no churn from the flap at all.
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(1),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.split_placing[&TabletId(2)].target,
            Some(vec![nid(1), nid(2), nid(3)]),
            "the stored target must be untouched by a flap that never reached the dwell gate"
        );
        let proposals = m.split_placing_reconcile(&empty);
        assert_eq!(proposals.len(), 1, "{proposals:?}");
        assert!(matches!(
            &proposals[0],
            MetaCommand::CasTabletReplicas { tablet, replicas, .. }
                if *tablet == TabletId(2) && *replicas == vec![nid(1), nid(2), nid(3)]
        ));
    }

    /// A paused tick (target member down, not yet `retarget_ready`)
    /// proposes nothing — the pause half of the dwell gate, isolated from
    /// the flap-recovery scenario above.
    #[test]
    fn split_placing_reconcile_pauses_while_a_target_member_is_down_and_not_ready() {
        let mut m = split_placing_dwell_fixture();
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(2),
                labels: BTreeMap::new(),
                status: NodeStatus::Down,
            }),
            ApplyOutcome::Applied
        );
        let proposals = m.split_placing_reconcile(&BTreeSet::new());
        assert!(proposals.is_empty(), "{proposals:?}");
        // The stored target itself is untouched by a paused tick.
        assert_eq!(
            m.split_placing[&TabletId(2)].target,
            Some(vec![nid(1), nid(2), nid(3)])
        );
    }

    /// Once `retarget_ready` names the tablet (the driver's dwell having
    /// elapsed for a down target member), the phase proposes a REPLICATED
    /// `RetargetSplitPlacing` — via `replan`, so the still-`Active` members
    /// of the old target (`n1`, `n3`) are kept and only the down one (`n2`)
    /// is replaced by the sole remaining eligible candidate (`n4`) — never
    /// a direct `CasTabletReplicas` in the same tick (the new target must
    /// itself become stable before anything drives toward it).
    #[test]
    fn split_placing_reconcile_retargets_once_ready_keeping_live_survivors() {
        let mut m = split_placing_dwell_fixture();
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(2),
                labels: BTreeMap::new(),
                status: NodeStatus::Down,
            }),
            ApplyOutcome::Applied
        );
        let ready: BTreeSet<TabletId> = [TabletId(2)].into_iter().collect();
        let proposals = m.split_placing_reconcile(&ready);
        assert_eq!(proposals.len(), 1, "{proposals:?}");
        assert!(matches!(
            &proposals[0],
            MetaCommand::RetargetSplitPlacing { tablet, target, .. }
                if *tablet == TabletId(2) && *target == Some(vec![nid(1), nid(3), nid(4)])
        ));
    }

    /// `RetargetSplitPlacing` happy path: epoch-CAS against the child's own
    /// current epoch, replaces the stored `target`, never touches `done` or
    /// the tablet's own replicas/epoch.
    #[test]
    fn retarget_split_placing_happy_path() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(9)]),
                done: false,
            },
        );
        let epoch = m.tablets[&TabletId(2)].epoch;

        assert_eq!(
            m.apply(&MetaCommand::RetargetSplitPlacing {
                tablet: TabletId(2),
                expected_epoch: epoch,
                target: Some(vec![nid(8)]),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.split_placing[&TabletId(2)].target, Some(vec![nid(8)]));
        assert!(!m.split_placing[&TabletId(2)].done);
        assert_eq!(m.tablets[&TabletId(2)].replicas, vec![nid(1)]);
        assert_eq!(m.tablets[&TabletId(2)].epoch, epoch);
    }

    /// `RetargetSplitPlacing` rejects an epoch mismatch — the identical
    /// stale-confirm guard `MarkSplitPlacingDone` already has.
    #[test]
    fn retarget_split_placing_rejects_epoch_mismatch() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(9)]),
                done: false,
            },
        );
        let stale_epoch = m.tablets[&TabletId(2)].epoch.next();
        assert_eq!(
            m.apply(&MetaCommand::RetargetSplitPlacing {
                tablet: TabletId(2),
                expected_epoch: stale_epoch,
                target: Some(vec![nid(8)]),
            }),
            ApplyOutcome::Rejected("epoch mismatch")
        );
        assert_eq!(m.split_placing[&TabletId(2)].target, Some(vec![nid(9)]));
    }

    /// `RetargetSplitPlacing` rejects a tablet with no `split_placing` entry
    /// (also a nonexistent tablet) and is idempotent on an already-`done`
    /// entry — the identical shape `MarkSplitPlacingDone`'s own tests cover.
    #[test]
    fn retarget_split_placing_rejects_missing_entry_and_is_a_noop_once_done() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        let epoch = m.tablets[&TabletId(2)].epoch;
        assert_eq!(
            m.apply(&MetaCommand::RetargetSplitPlacing {
                tablet: TabletId(2),
                expected_epoch: epoch,
                target: Some(vec![nid(8)]),
            }),
            ApplyOutcome::Rejected("no split_placing entry for this tablet")
        );
        assert_eq!(
            m.apply(&MetaCommand::RetargetSplitPlacing {
                tablet: TabletId(999),
                expected_epoch: Epoch::INITIAL,
                target: Some(vec![nid(8)]),
            }),
            ApplyOutcome::Rejected("no such tablet")
        );

        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(9)]),
                done: true,
            },
        );
        assert_eq!(
            m.apply(&MetaCommand::RetargetSplitPlacing {
                tablet: TabletId(2),
                expected_epoch: epoch,
                target: Some(vec![nid(8)]),
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(m.split_placing[&TabletId(2)].target, Some(vec![nid(9)]));
    }

    /// **Repair-exclusion regression (issue #528)**: an un-done
    /// `split_placing` tablet with a genuinely `Down` replica (one that is
    /// NOT even part of the stored target, so the placing phase itself
    /// would also skip it) must NOT be retargeted by the repair path
    /// (`Metadata::reconcile`) either — the placing phase (dwell-gated) is
    /// the sole mover for this tablet until `done`. Mirrors the existing
    /// exclusion `rebalance_placement` already has (`rebalance_never_
    /// touches_a_split_placing_tablet`, if present) but for `reconcile`.
    #[test]
    fn reconcile_skips_an_undone_split_placing_tablet_even_with_a_down_replica() {
        let mut m = Metadata::default();
        for n in [1u64, 2, 3, 4] {
            assert_eq!(
                m.apply(&MetaCommand::UpsertMember {
                    node: nid(n),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                }),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(2),
                table: Some("users".to_owned()),
                range: KeyRange::whole(),
                // n5 is a replica but never an `Active` member — an
                // ordinary policy violation `reconcile` would otherwise
                // repair immediately.
                replicas: vec![nid(1), nid(2), nid(5)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(2),
                policy: Some(PlacementPolicy::simple("users", 3)),
            }),
            ApplyOutcome::Applied
        );
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(1), nid(2), nid(3)]),
                done: false,
            },
        );

        // Sanity: without the un-done split_placing entry, `reconcile`
        // WOULD repair this violation — proves the exclusion is actually
        // doing something, not merely vacuously true.
        let mut without_entry = m.clone();
        without_entry.split_placing.remove(&TabletId(2));
        assert!(
            !without_entry.reconcile().is_empty(),
            "expected the sanity baseline to actually repair the violation"
        );

        assert!(
            m.reconcile().is_empty(),
            "repair must not touch a tablet with an un-done split_placing entry"
        );

        // Marking it done reopens the tablet to ordinary repair.
        m.split_placing.get_mut(&TabletId(2)).unwrap().done = true;
        assert!(
            !m.reconcile().is_empty(),
            "a done split_placing entry must no longer exclude the tablet from repair"
        );
    }

    /// `Metadata` round-trips through JSON with a populated `split_placing`
    /// catalog (ADR 0062 §2) — same-version WAL/snapshot fidelity for the
    /// new collection, mirroring `metadata_round_trips_through_json_with_
    /// populated_backups`' own shape.
    #[test]
    fn metadata_round_trips_through_json_with_populated_split_placing() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(2),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        m.split_placing.insert(
            TabletId(2),
            SplitPlacing {
                target: Some(vec![nid(9), nid(10)]),
                done: false,
            },
        );
        m.split_placing.insert(
            TabletId(3),
            SplitPlacing {
                target: None,
                done: true,
            },
        );

        let json = serde_json::to_string(&m).expect("serializes");
        let round_tripped: Metadata = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(m, round_tripped);
    }

    /// ADR 0058 Train 2 rung 3: placement is frozen mid-split — neither the
    /// repair reconciler nor the rebalancer proposes a move for a
    /// `Splitting` parent carrying an in-place split intent, even when the
    /// replica set violates policy (here: RF 3 policy, 1-replica sets,
    /// which repair would otherwise fix immediately); an ordinary `Active`
    /// tablet in the same view still gets repaired. This workflow never
    /// mints any `Building` child rows at all — there is nothing else here
    /// to freeze (`reconcile_placement`/`rebalance_placement` skip on the
    /// parent's own non-`Active` state alone).
    #[test]
    fn placement_is_frozen_for_a_splitting_parent_with_an_in_place_intent() {
        let mut m = Metadata::default();
        for n in 1..=6u64 {
            assert_eq!(
                m.apply(&MetaCommand::UpsertMember {
                    node: nid(n),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                }),
                ApplyOutcome::Applied
            );
        }
        // An under-replicated Active tablet: repair proposes for it.
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("a".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(1),
                policy: Some(PlacementPolicy::simple("p", 3)),
            }),
            ApplyOutcome::Applied
        );
        // A second table, mid-in-place-split: equally under-replicated, but
        // frozen on the parent's own `Splitting` state alone.
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(2),
                table: Some("b".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(2)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(2),
                policy: Some(PlacementPolicy::simple("p", 3)),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: TabletId(2),
                expected_epoch: Epoch::INITIAL,
                split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
                children: [(TabletId(3), vec![nid(2)]), (TabletId(4), vec![nid(2)])],
            }),
            ApplyOutcome::Applied
        );

        let proposed = m.reconcile();
        let targets: Vec<TabletId> = proposed
            .iter()
            .map(|c| match c {
                MetaCommand::CasTabletReplicas { tablet, .. } => *tablet,
                other => panic!("unexpected command: {other:?}"),
            })
            .collect();
        assert_eq!(
            targets,
            vec![TabletId(1)],
            "repair touches only the Active tablet; the Splitting parent (in-place, no \
             Building rows at all) is frozen"
        );
        // The rebalancer proposes nothing for the frozen set either (the
        // Active tablet's set violates policy, so rebalance skips it by its
        // own pre-existing rule; nothing else is eligible at all).
        assert!(m.rebalance().is_none());
    }

    /// `DropTableTablets` (ADR 0024): removes every tablet scoped to the table —
    /// split children included — with their policies, in one apply; leaves other
    /// tables' and the legacy unscoped tablet alone; no-op when the table has no
    /// tablets (so a re-proposed drop does not churn the Raft log).
    #[test]
    fn drop_table_tablets_removes_the_tables_tablets_and_policies() {
        let mut m = Metadata::default();
        let create = |id: u64, table: Option<&str>| MetaCommand::CreateTablet {
            tablet: TabletId(id),
            table: table.map(str::to_owned),
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2), nid(3)],
        };
        assert_eq!(m.apply(&create(1, Some("users"))), ApplyOutcome::Applied);
        assert_eq!(m.apply(&create(2, Some("orders"))), ApplyOutcome::Applied);
        assert_eq!(m.apply(&create(3, None)), ApplyOutcome::Applied); // legacy
        // Split `users` (`split_tablet`, in-place) so the table owns two
        // tablets (ids 4 and 5; tablet 1 is retired by the cutover).
        split_tablet(
            &mut m,
            TabletId(1),
            0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            TabletId(4),
        );
        for id in [4u64, 5, 2] {
            assert_eq!(
                m.apply(&MetaCommand::SetTabletPolicy {
                    tablet: TabletId(id),
                    policy: Some(PlacementPolicy::simple("cp-rf", 3)),
                }),
                ApplyOutcome::Applied
            );
        }

        let drop = MetaCommand::DropTableTablets {
            table: "users".to_owned(),
        };
        assert_eq!(m.apply(&drop), ApplyOutcome::Applied);
        // Both `users` tablets are gone, with their policies…
        assert!(m.tablets_for_table("users").next().is_none());
        assert!(!m.tablets.contains_key(&TabletId(4)));
        assert!(!m.tablets.contains_key(&TabletId(5)));
        assert!(!m.policies.contains_key(&TabletId(4)));
        assert!(!m.policies.contains_key(&TabletId(5)));
        // …while the other table's tablet + policy and the legacy tablet remain.
        assert!(m.tablets.contains_key(&TabletId(2)));
        assert!(m.policies.contains_key(&TabletId(2)));
        assert!(m.tablets.contains_key(&TabletId(3)));

        // Idempotent: dropping again is a no-op.
        assert_eq!(m.apply(&drop), ApplyOutcome::NoOp);

        // The allocator never rewinds: a later table gets a fresh id, above the
        // dropped ones.
        assert_eq!(m.next_free_tablet_id(), TabletId(6));
    }

    /// The monotonic tablet-id allocator (ADR 0023): every created tablet bumps the
    /// counter past its id, so the next allocation is unique; it never goes backward,
    /// and a pre-counter snapshot (counter `0`) still allocates above its tablets.
    #[test]
    fn next_tablet_id_is_monotonic_across_create_and_split() {
        let mut m = Metadata::default();
        let create = |id: u64, table: &str| MetaCommand::CreateTablet {
            tablet: TabletId(id),
            table: Some(table.to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2), nid(3)],
        };

        // Fresh metadata allocates id 1 (id 0 is reserved).
        assert_eq!(m.next_free_tablet_id(), TabletId(1));

        // Creating the allocated tablet advances the counter.
        assert_eq!(m.apply(&create(1, "users")), ApplyOutcome::Applied);
        assert_eq!(m.next_free_tablet_id(), TabletId(2));
        assert_eq!(m.apply(&create(2, "orders")), ApplyOutcome::Applied);
        assert_eq!(m.next_free_tablet_id(), TabletId(3));

        // A begin-split advances the counter past BOTH new children.
        split_tablet(
            &mut m,
            TabletId(1),
            0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            TabletId(3),
        );
        assert_eq!(m.next_free_tablet_id(), TabletId(5));

        // A pre-counter snapshot (counter field 0) still allocates above its
        // highest existing tablet, never colliding.
        let legacy = Metadata {
            tablets: [(
                TabletId(7),
                Tablet::new(TabletId(7), KeyRange::whole(), vec![nid(1)]),
            )]
            .into_iter()
            .collect(),
            next_tablet_id: 0,
            ..Metadata::default()
        };
        assert_eq!(legacy.next_free_tablet_id(), TabletId(8));
    }

    /// `SplitTablet` enforces the monotonic allocator at **apply** time: an id
    /// freed by `DropTableTablets` is never reused (a replica still holding the
    /// dropped tablet's on-disk files would re-host them as the new tablet), so a
    /// split carrying a below-allocator `new_id` — e.g. from a stale or divergent
    /// proposer computing `max(ids) + 1` — is rejected; the allocator's own id is
    /// accepted and the counter stays monotonic.
    /// ADR 0032 PR3: `RemoveMember` is rejected while a tablet still names the
    /// member as a replica — removing it would silently drop that tablet below
    /// its replication factor with the member gone from the placement candidate
    /// pool entirely.
    #[test]
    fn remove_member_rejects_while_referenced_by_a_tablet() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::UpsertMember {
            node: nid(301),
            labels: BTreeMap::new(),
            status: NodeStatus::Leaving,
        });
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(300), nid(301), nid(302)],
        });
        assert_eq!(m.tablets_referencing(&nid(301)), 1);
        assert_eq!(
            m.apply(&MetaCommand::RemoveMember { node: nid(301) }),
            ApplyOutcome::Rejected("still referenced by a tablet's replica set")
        );
        assert!(m.members.contains_key(&nid(301)));
    }

    /// `RemoveMember` is rejected while the member is still `Active`/`Joining` —
    /// removing a still-serving member could strand a tablet's replication
    /// factor with no warning.
    #[test]
    fn remove_member_rejects_while_active_or_joining() {
        for status in [NodeStatus::Active, NodeStatus::Joining] {
            let mut m = Metadata::default();
            m.apply(&MetaCommand::UpsertMember {
                node: nid(301),
                labels: BTreeMap::new(),
                status,
            });
            assert_eq!(
                m.apply(&MetaCommand::RemoveMember { node: nid(301) }),
                ApplyOutcome::Rejected("not drained: member is Active or Joining"),
                "status {status:?} should block removal"
            );
            assert!(m.members.contains_key(&nid(301)));
        }
    }

    /// A drained (`Leaving`/`Down`), unreferenced member is removed — and its
    /// address-book entries are pruned in the same apply — and a second removal
    /// of the same, now-absent id is an idempotent no-op (`Applied`), never a
    /// `Rejected`, so a proposer that retries after a timed-out confirm
    /// converges instead of erroring.
    #[test]
    fn remove_member_applies_after_drain_and_prunes_addrs_then_is_idempotent() {
        for status in [NodeStatus::Leaving, NodeStatus::Down] {
            let mut m = Metadata::default();
            m.apply(&MetaCommand::UpsertMember {
                node: nid(301),
                labels: BTreeMap::new(),
                status,
            });
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(301),
                addrs: NodeAddrs {
                    internal: "127.0.0.1:9301".to_owned(),
                    client: "127.0.0.1:9001".to_owned(),
                    admin: "127.0.0.1:9501".to_owned(),
                    intra: "127.0.0.1:9601".to_owned(),
                    role: "combined".to_string(),
                },
            });
            m.apply(&MetaCommand::RegisterCpAddr {
                id: nid(301),
                addr: "127.0.0.1:9301".to_owned(),
                tablet: None,
            });
            assert_eq!(m.tablets_referencing(&nid(301)), 0);

            assert_eq!(
                m.apply(&MetaCommand::RemoveMember { node: nid(301) }),
                ApplyOutcome::Applied,
                "status {status:?} should allow removal"
            );
            assert!(!m.members.contains_key(&nid(301)));
            assert!(!m.node_addrs.contains_key(&nid(301)));
            assert!(!m.cp_member_addrs.contains_key(&nid(301)));
            assert!(!m.cp_member_tablets.contains_key(&nid(301)));

            // Idempotent retry: already absent — `NoOp` (the file's convention
            // for nothing-changed applies), never `Rejected`.
            assert_eq!(
                m.apply(&MetaCommand::RemoveMember { node: nid(301) }),
                ApplyOutcome::NoOp
            );
        }
    }

    /// **ADR 0040 PR6**: `RemoveMember` also prunes a **claim-without-member**
    /// id — a `node_addrs` entry with no `members` row at all (the shape a
    /// control-role `RegisterNode` always produces) — instead of treating it
    /// as an already-absent no-op the way it did before this PR (which would
    /// have leaked the address-book claim forever, since nothing else ever
    /// removes a `node_addrs` entry with no member to gate on).
    #[test]
    fn remove_member_prunes_a_claim_without_a_members_row() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::RegisterNode {
            node: nid(301),
            addrs: NodeAddrs {
                internal: "127.0.0.1:9301".to_owned(),
                client: "127.0.0.1:9001".to_owned(),
                admin: "127.0.0.1:9501".to_owned(),
                intra: "127.0.0.1:9601".to_owned(),
                role: "control".to_string(),
            },
            labels: BTreeMap::new(),
        });
        assert!(m.node_addrs.contains_key(&nid(301)));
        assert!(
            !m.members.contains_key(&nid(301)),
            "test premise: a control-role registration never claims `members`"
        );

        assert_eq!(
            m.apply(&MetaCommand::RemoveMember { node: nid(301) }),
            ApplyOutcome::Applied
        );
        assert!(!m.node_addrs.contains_key(&nid(301)));

        // Idempotent retry: already fully absent now — `NoOp`, matching the
        // data-plane shape's own idempotent-retry contract.
        assert_eq!(
            m.apply(&MetaCommand::RemoveMember { node: nid(301) }),
            ApplyOutcome::NoOp
        );
    }

    /// **ADR 0040 PR6 safety argument.** The catastrophic case the orphan
    /// sweep must never cause: an `Active` member removed because its
    /// `RemoveMember` proposal was computed from a stale (pre-activation)
    /// view. Proven directly and exhaustively as a state-machine property —
    /// **regardless of which of the two commands a proposer computed first**,
    /// applying them in either order never leaves an `Active` member
    /// removed, because `RemoveMember`'s own apply-time guard re-checks
    /// status fresh against whatever already committed ahead of it in the
    /// log, not against whatever view the proposer that built it once saw.
    #[test]
    fn remove_member_never_removes_a_member_that_activated_first_regardless_of_proposal_order() {
        // Order 1: activation commits first (the realistic case — the
        // detector's promotion happened to land before the sweep's own
        // stale-view proposal). The stale removal is rejected outright; the
        // member stays exactly as activation left it.
        let mut m = Metadata::default();
        m.apply(&MetaCommand::UpsertMember {
            node: nid(301),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        });
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(301),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::RemoveMember { node: nid(301) }),
            ApplyOutcome::Rejected("not drained: member is Active or Joining"),
            "a stale removal proposal must never remove an already-Active member"
        );
        assert!(m.members.contains_key(&nid(301)));
        assert_eq!(m.members[&nid(301)].status, NodeStatus::Active);

        // Order 2: the removal genuinely commits first (the member really
        // never activated in time) — this is the intended sweep outcome, not
        // a bug. Removal succeeds; nothing here resurrects it (the actual
        // "no resurrection via the normal detector path" argument is proven
        // separately in `node::tests::
        // liveness_transitions_never_proposes_for_an_absent_member`, since it
        // exercises the crate-private decision function that is the only
        // realistic producer of an `UpsertMember{Active}` command in this
        // context).
        let mut m2 = Metadata::default();
        m2.apply(&MetaCommand::UpsertMember {
            node: nid(301),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        });
        assert_eq!(
            m2.apply(&MetaCommand::RemoveMember { node: nid(301) }),
            ApplyOutcome::Applied
        );
        assert!(!m2.members.contains_key(&nid(301)));
    }

    /// `Member::has_activated` is sticky: set the moment a member's status is
    /// ever recorded `Active` (regardless of the caller — a fresh
    /// `Down`→`Active` promotion or a direct `Active` insert alike, ADR 0040
    /// PR6), and never cleared by a later transition to any other status.
    #[test]
    fn has_activated_is_sticky_once_the_member_is_ever_active() {
        let mut m = Metadata::default();
        // A direct `Active` insert (mirroring `bootstrap`'s shape) sets it
        // immediately, with no prior `Down` state at all.
        m.apply(&MetaCommand::UpsertMember {
            node: nid(301),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
        assert!(m.members[&nid(301)].has_activated);

        // A later `Down` (a real crash) never clears it.
        m.apply(&MetaCommand::UpsertMember {
            node: nid(301),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        });
        assert!(m.members[&nid(301)].has_activated);

        // A fresh `Down` claim (never yet active) starts `false`.
        m.apply(&MetaCommand::UpsertMember {
            node: nid(302),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        });
        assert!(!m.members[&nid(302)].has_activated);
    }

    /// `Metadata::orphan_sweep_candidates` (ADR 0040 PR6): the pure
    /// candidate-set predicate, covering both shapes and every exclusion.
    #[test]
    fn orphan_sweep_candidates_covers_both_shapes_and_every_exclusion() {
        let mut m = Metadata::default();

        // Shape 1: a data-plane claim, never activated — a candidate.
        m.apply(&MetaCommand::RegisterNode {
            node: nid(900),
            addrs: NodeAddrs {
                internal: "127.0.0.1:9900".to_owned(),
                client: "127.0.0.1:9000".to_owned(),
                admin: "127.0.0.1:9950".to_owned(),
                intra: "127.0.0.1:9960".to_owned(),
                role: "combined".to_string(),
            },
            labels: BTreeMap::new(),
        });
        // Shape 2: a control-role claim-without-member — a candidate too.
        m.apply(&MetaCommand::RegisterNode {
            node: nid(901),
            addrs: NodeAddrs {
                internal: "127.0.0.1:9901".to_owned(),
                client: "127.0.0.1:9001".to_owned(),
                admin: "127.0.0.1:9951".to_owned(),
                intra: "127.0.0.1:9961".to_owned(),
                role: "control".to_string(),
            },
            labels: BTreeMap::new(),
        });
        // A member that activated, then went Down — NOT a candidate
        // (`has_activated` guard).
        m.apply(&MetaCommand::UpsertMember {
            node: nid(902),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
        m.apply(&MetaCommand::UpsertMember {
            node: nid(902),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        });
        m.apply(&MetaCommand::RegisterNodeAddrs {
            node: nid(902),
            addrs: NodeAddrs {
                internal: "127.0.0.1:9902".to_owned(),
                client: "127.0.0.1:9002".to_owned(),
                admin: "127.0.0.1:9952".to_owned(),
                intra: "127.0.0.1:9962".to_owned(),
                role: "combined".to_string(),
            },
        });
        // A never-activated claim, but still referenced by a tablet's
        // replica set — NOT a candidate (mirrors `RemoveMember`'s own guard).
        m.apply(&MetaCommand::RegisterNode {
            node: nid(903),
            addrs: NodeAddrs {
                internal: "127.0.0.1:9903".to_owned(),
                client: "127.0.0.1:9003".to_owned(),
                admin: "127.0.0.1:9953".to_owned(),
                intra: "127.0.0.1:9963".to_owned(),
                role: "combined".to_string(),
            },
            labels: BTreeMap::new(),
        });
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(903)],
        });
        // Shape 3: a `members`-row-only claim with **no** `node_addrs` entry
        // at all (`admin_add_member`'s bare growth registration — a node
        // declared `Down` ahead of its own later self-registration) — a
        // candidate too, proving the union covers this shape, not just the
        // `node_addrs`-keyed ones.
        m.apply(&MetaCommand::UpsertMember {
            node: nid(904),
            labels: BTreeMap::new(),
            status: NodeStatus::Down,
        });

        let candidates = m.orphan_sweep_candidates();
        assert!(candidates.contains(&nid(900)), "shape 1 missing");
        assert!(candidates.contains(&nid(901)), "shape 2 missing");
        assert!(
            !candidates.contains(&nid(902)),
            "activated-then-down member must never be a candidate"
        );
        assert!(
            !candidates.contains(&nid(903)),
            "tablet-referenced claim must never be a candidate"
        );
        assert!(candidates.contains(&nid(904)), "shape 3 missing");
    }

    /// A `Metadata` snapshot serialized before ADR 0032 PR3 (no `RemoveMember`
    /// variant in the enum at the time) still round-trips: `MetaCommand` gained
    /// an additive variant, and every pre-existing field/command already
    /// decodes unchanged (this doesn't touch any `#[serde(default)]` field, so a
    /// plain round trip is the whole proof).
    #[test]
    fn metadata_round_trips_with_the_remove_member_variant_in_scope() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::UpsertMember {
            node: nid(301),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
        let value = serde_json::to_value(&m).expect("metadata serializes");
        let decoded: Metadata = serde_json::from_value(value).expect("metadata round-trips");
        assert_eq!(decoded, m);
    }

    /// `Metadata::stream_shards` is keyed by `(TabletId, u64)` — serde_json's
    /// `MapKeySerializer` rejects any non-string map key, so once a real
    /// stream shard seals (the map becomes non-empty), a plain
    /// `serde_json::to_value(&metadata)` fails outright — silently returning
    /// `Value::Null` at every call site that swallows the error
    /// (`animusd`'s `GET /admin/status`) or panicking at every call site that
    /// unwraps it (`animusd`'s wire `write_frame`). This is the reproduction
    /// this crate owns: `Metadata` must round-trip through `serde_json`
    /// regardless of which collection is populated.
    #[test]
    fn metadata_round_trips_through_json_with_populated_stream_shards() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 0, 100)),
            ApplyOutcome::Applied
        );
        assert!(
            !m.stream_shards.is_empty(),
            "test premise: the catalog must actually be populated"
        );

        let value = serde_json::to_value(&m).expect("metadata serializes with stream_shards");
        let decoded: Metadata = serde_json::from_value(value).expect("metadata round-trips");
        assert_eq!(decoded, m);
    }

    /// The identical hazard as
    /// `metadata_round_trips_through_json_with_populated_stream_shards`
    /// above, for `Metadata::index_backfill` (ADR 0045 §4) — its key is also
    /// a non-string tuple (`(TabletId, String)`), so this must be proven
    /// against a genuinely populated map, not an empty one (per the
    /// engineering-lessons "an empty collection can't prove a map-key
    /// encoding rule" lesson). Written before any other backfill-catalog
    /// wiring, per that same lesson.
    #[test]
    fn metadata_round_trips_through_json_with_populated_index_backfill() {
        let mut m = Metadata::default();
        m.apply(&MetaCommand::CreateTableSchema {
            table: "users".to_owned(),
            schema: TableSchema::simple("id", ColumnType::String),
        });
        m.apply(&MetaCommand::CreateTableIndex {
            table: "users".to_owned(),
            index: IndexDef {
                name: "by-email".to_owned(),
                kind: crate::schema::IndexKind::Global,
                hash_attribute: "email".to_owned(),
                sort_attribute: None,
                projection: crate::schema::IndexProjection::All,
                status: IndexStatus::Creating,
                // A populated `Some` here (rather than the usual `None`
                // fixture value) is deliberate: an empty/`None` field can't
                // prove a JSON round trip actually preserves it (the same
                // "an empty collection can't prove a map-key encoding rule"
                // lesson this module's own doc already names for
                // `stream_shards`) — see the round-trip assertion below.
                hash_attribute_type: Some(ColumnType::String),
                sort_attribute_type: None,
            },
        });
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("users".to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        });
        assert_eq!(
            m.apply(&MetaCommand::MarkIndexBackfilled {
                table: "users".to_owned(),
                index: "by-email".to_owned(),
                tablet: TabletId(1),
            }),
            ApplyOutcome::Applied
        );
        assert!(
            !m.index_backfill.is_empty(),
            "test premise: the catalog must actually be populated"
        );

        let value = serde_json::to_value(&m).expect("metadata serializes with index_backfill");
        let decoded: Metadata = serde_json::from_value(value).expect("metadata round-trips");
        assert_eq!(decoded, m);
    }

    /// A helper `NodeAddrs` builder for the `RegisterNode`/`RegisterNodeAddrs`
    /// CAS tests below — mirrors the existing `addrs(suffix)` idiom used by
    /// `register_node_addrs_records_updates_and_is_idempotent`.
    fn cas_addrs(suffix: u16) -> NodeAddrs {
        NodeAddrs {
            internal: format!("127.0.0.1:{}", 9300 + suffix),
            client: format!("127.0.0.1:{}", 9000 + suffix),
            admin: format!("127.0.0.1:{}", 9500 + suffix),
            intra: format!("127.0.0.1:{}", 9600 + suffix),
            role: "combined".to_string(),
        }
    }

    /// **ADR 0040 Decision C, the core claim case**: `RegisterNode` on a
    /// genuinely unclaimed id (absent from both `members` and `node_addrs`)
    /// inserts a `Down` member with the given labels **and** the address
    /// book entry, atomically, in one apply.
    #[test]
    fn register_node_claims_an_unclaimed_id_with_member_and_addrs_atomically() {
        let mut m = Metadata::default();
        let mut labels = BTreeMap::new();
        labels.insert("region".to_owned(), "eu-west".to_owned());

        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(900),
                addrs: cas_addrs(0),
                labels: labels.clone(),
            }),
            ApplyOutcome::Applied
        );
        let member = m.members.get(&nid(900)).expect("member registered");
        assert_eq!(member.status, NodeStatus::Down);
        assert_eq!(member.labels, labels);
        assert_eq!(m.node_addrs.get(&nid(900)), Some(&cas_addrs(0)));
    }

    /// A **minted-collision-then-retry-with-a-different-id** shape at the
    /// state-machine level (ADR 0040 Decision C): a `RegisterNode` for an id
    /// already claimed by someone else with a **different** registration is
    /// `Rejected` and mutates nothing — the caller's own re-mint-and-retry
    /// loop (`animusd`) then succeeds simply by proposing a *different* id,
    /// proven here by the second `RegisterNode` (a distinct id) applying
    /// cleanly. Also covers a **proposed**-id collision: the same rejection
    /// is what makes `animusd`'s explicit-`--id` join path fail loudly
    /// instead of silently overwriting someone else's claim.
    #[test]
    fn register_node_rejects_a_different_registration_for_a_claimed_id_then_a_distinct_id_succeeds()
    {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(901),
                addrs: cas_addrs(1),
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Applied
        );
        let before = m.clone();

        // Same id, different addrs: rejected, state untouched.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(901),
                addrs: cas_addrs(2),
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Rejected("node id already claimed by a different registration")
        );
        assert_eq!(m, before, "a rejected collision must mutate nothing");

        // Same id, same addrs but DIFFERENT labels: the CAS is keyed on
        // `node_addrs` alone (see `MetaCommand::RegisterNode`'s doc for why),
        // so this is *not* a collision — a `NoOp`, and the original labels
        // are left untouched (this call's differing labels never overwrite
        // them; whichever command first claimed membership owns them).
        let mut other_labels = BTreeMap::new();
        other_labels.insert("region".to_owned(), "eu-west".to_owned());
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(901),
                addrs: cas_addrs(1),
                labels: other_labels,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(
            m, before,
            "a labels-only mismatch must mutate nothing either"
        );

        // The "re-mint and retry" case: a distinct id registers cleanly.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(902),
                addrs: cas_addrs(2),
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Applied
        );
        assert!(m.members.contains_key(&nid(901)) && m.members.contains_key(&nid(902)));
    }

    /// **Idempotent retry** (ADR 0040 Decision C): replaying the exact same
    /// `RegisterNode` (identical `addrs` + `labels`) — whether a proposer's
    /// retry after an accepted-but-unconfirmed propose, or a restarted
    /// process re-registering its own unchanged identity — is a `NoOp`, not
    /// a second insert or a rejection, and mutates nothing further.
    #[test]
    fn register_node_identical_replay_is_idempotent() {
        let mut m = Metadata::default();
        let mut labels = BTreeMap::new();
        labels.insert("az".to_owned(), "eu-west-1a".to_owned());
        let cmd = MetaCommand::RegisterNode {
            node: nid(903),
            addrs: cas_addrs(3),
            labels: labels.clone(),
        };
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        let after_first = m.clone();

        assert_eq!(m.apply(&cmd), ApplyOutcome::NoOp);
        assert_eq!(m, after_first, "an identical replay must mutate nothing");
    }

    /// **ADR 0032 same-identity rejoin**: a `RegisterNode` for an id already
    /// claimed **via a different command** (`UpsertMember`, e.g. a
    /// config-bootstrapped original member) with byte-identical `addrs` +
    /// `labels` is still a `NoOp`, not a rejection — `RegisterNode`'s
    /// "claimed" check reads `members`/`node_addrs` directly, not "did I
    /// insert this," so it agrees regardless of which command established
    /// the claim.
    #[test]
    fn register_node_agrees_with_a_claim_established_by_upsert_member_and_register_node_addrs() {
        let mut m = Metadata::default();
        let mut labels = BTreeMap::new();
        labels.insert("region".to_owned(), "eu-west".to_owned());
        m.apply(&MetaCommand::UpsertMember {
            node: nid(904),
            labels: labels.clone(),
            status: NodeStatus::Active,
        });
        m.apply(&MetaCommand::RegisterNodeAddrs {
            node: nid(904),
            addrs: cas_addrs(4),
        });

        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(904),
                addrs: cas_addrs(4),
                labels,
            }),
            ApplyOutcome::NoOp
        );
    }

    /// **The bug this design point exists to prevent** (found via
    /// `animusd`'s own `runtime_added_voter_survives_leadership_change_to_a_
    /// different_original_voter` integration test going bimodal): a member
    /// claimed via a wholly decoupled command (`UpsertMember`'s bootstrap
    /// insert, or `admin_add_member`'s operator-labeled `Down` row) with
    /// **no address yet** must still let `RegisterNode` claim the address
    /// slot — if the CAS were keyed on `members` (not `node_addrs`), this
    /// would misfire as "already claimed by a different registration"
    /// forever (there is no address to compare against, so any proposed one
    /// would look "different"), permanently starving the node's own
    /// self-registration. This is exactly the shape a permanently-non-voter
    /// control-only growth node hits: nothing else ever proposes
    /// `UpsertMember` for it, so its own `RegisterNode` self-registration is
    /// the *only* thing that ever creates its `members` row at all.
    #[test]
    fn register_node_claims_an_address_for_a_member_already_claimed_without_one() {
        let mut m = Metadata::default();
        let mut labels = BTreeMap::new();
        labels.insert("region".to_owned(), "eu-west".to_owned());
        // Membership claimed first (e.g. `admin_add_member`), no address yet.
        m.apply(&MetaCommand::UpsertMember {
            node: nid(905),
            labels: labels.clone(),
            status: NodeStatus::Down,
        });
        assert!(!m.node_addrs.contains_key(&nid(905)));

        // The node's own self-registration must still succeed...
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(905),
                addrs: cas_addrs(5),
                // Deliberately different/empty labels — proving they never
                // clobber the labels `UpsertMember` already set.
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.node_addrs.get(&nid(905)), Some(&cas_addrs(5)));
        // ...and the pre-existing labels/status are untouched.
        let member = m.members.get(&nid(905)).expect("member still present");
        assert_eq!(
            member.labels, labels,
            "RegisterNode must never overwrite labels a different command already set"
        );
        assert_eq!(member.status, NodeStatus::Down);
    }

    /// **The bug an integration test caught** (`animusd`'s `control_only_
    /// cluster_elects_leader_and_serves_status` and `mixed_cluster_put_via_
    /// control_node_forwards_to_data_node` both went bimodal): a
    /// control-only node's own self-registration must claim its
    /// `node_addrs` entry but **never** a `Metadata::members` row — a
    /// control-only node has no `raftkv` role and can never host a tablet,
    /// so appearing in `members` at all (even `Down`, since the failure
    /// detector promotes any heartbeating `Down` entry on its own) would
    /// make it a placement candidate and silently corrupt tablet placement.
    #[test]
    fn register_node_never_claims_membership_for_a_control_role_registration() {
        let mut m = Metadata::default();
        let control_addrs = NodeAddrs {
            internal: "127.0.0.1:9906".to_string(),
            client: "127.0.0.1:9006".to_string(),
            admin: "127.0.0.1:9506".to_string(),
            intra: "127.0.0.1:9606".to_string(),
            role: "control".to_string(),
        };
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(906),
                addrs: control_addrs.clone(),
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.node_addrs.get(&nid(906)), Some(&control_addrs));
        assert!(
            !m.members.contains_key(&nid(906)),
            "a control-role registration must never create a members row"
        );

        // Idempotent replay: still no members row, still a clean NoOp.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(906),
                addrs: control_addrs,
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::NoOp
        );
        assert!(!m.members.contains_key(&nid(906)));
    }

    /// **`animusd::ClientCtx::admin_add_control_member` regression companion
    /// (issue #406/#450)**: proves, at the state-machine level, the
    /// guarantee that fix relies on — once a control-only id's own
    /// `RegisterNode` self-registration has claimed `node_addrs` (with no
    /// `members` row, per the control-only carve-out above), a caller that
    /// re-observed the claim from a lagging local cache and only knows one
    /// field changed (here, `internal` — the admin action's whole purpose)
    /// can repair it via `RegisterNodeAddrs`, merging onto the entry it did
    /// see, and this **never** collides — only `RegisterNode`'s own
    /// claim-path CAS can ever produce the "already claimed by a different
    /// registration" rejection (see
    /// `register_node_rejects_a_different_registration_for_a_claimed_id_
    /// then_a_distinct_id_succeeds` above for that still-correct,
    /// contrasting case: a caller that mistakes an already-claimed id for a
    /// fresh one and goes through `RegisterNode` again with a *different*
    /// `NodeAddrs` is, correctly, rejected).
    #[test]
    fn register_node_addrs_repairs_a_control_only_registration_without_ever_colliding() {
        let mut m = Metadata::default();
        let original = NodeAddrs {
            internal: "127.0.0.1:9907".to_string(),
            client: "127.0.0.1:9007".to_string(),
            admin: "127.0.0.1:9507".to_string(),
            intra: "127.0.0.1:9607".to_string(),
            role: "control".to_string(),
        };
        assert_eq!(
            m.apply(&MetaCommand::RegisterNode {
                node: nid(907),
                addrs: original.clone(),
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Applied
        );
        assert!(
            !m.members.contains_key(&nid(907)),
            "test premise: a control-only registration must never claim membership"
        );

        // The repair: merge the one field the caller actually knows changed
        // onto the full entry it read (never a fresh/empty `NodeAddrs`) —
        // exactly `admin_add_control_member`'s fixed already-registered
        // branch.
        let mut repaired = original.clone();
        repaired.internal = "127.0.0.1:9999".to_string();
        assert_eq!(
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(907),
                addrs: repaired.clone(),
            }),
            ApplyOutcome::Applied,
            "RegisterNodeAddrs must repair an existing control-only entry, never collide"
        );
        assert_eq!(m.node_addrs.get(&nid(907)), Some(&repaired));
        assert!(!m.members.contains_key(&nid(907)));

        // Re-applying the identical repaired addrs is a clean NoOp — no
        // second "collision" surface exists on this path at all.
        assert_eq!(
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(907),
                addrs: repaired,
            }),
            ApplyOutcome::NoOp
        );
    }

    /// **ADR 0040 PR4 tightening**: `RegisterNodeAddrs` is update-only — a
    /// totally unclaimed id (absent from both `members` and `node_addrs`)
    /// is rejected, not silently registered. `RegisterNode` is the sole
    /// claim path now.
    #[test]
    fn register_node_addrs_rejects_an_unclaimed_id() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::RegisterNodeAddrs {
                node: nid(905),
                addrs: cas_addrs(5),
            }),
            ApplyOutcome::Rejected(
                "node id not yet a registered member — register it via \
                 MetaCommand::RegisterNode first"
            )
        );
        assert!(!m.node_addrs.contains_key(&nid(905)));
    }

    // --- ADR 0051 TTL catalog ------------------------------------------

    fn ttl_spec(attribute_name: &str) -> TtlSpec {
        TtlSpec {
            attribute_name: attribute_name.to_owned(),
        }
    }

    /// Enabling TTL on a table with a schema records the spec, `Applied`.
    #[test]
    fn set_table_ttl_enables_and_records_the_spec() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: Some(ttl_spec("expiresAt")),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().ttl,
            Some(ttl_spec("expiresAt"))
        );
    }

    /// Re-enabling with the identical attribute name is idempotent — unlike
    /// `SetTableStream`, TTL mints no label, so there is nothing that goes
    /// stale on a repeat enable.
    #[test]
    fn set_table_ttl_re_enable_with_same_attribute_is_noop() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: Some(ttl_spec("expiresAt")),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: Some(ttl_spec("expiresAt")),
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().ttl,
            Some(ttl_spec("expiresAt"))
        );
    }

    /// Changing the attribute name in place — no disable/re-enable round
    /// trip required — is a legal live `UpdateTimeToLive` and is `Applied`,
    /// recording the new name.
    #[test]
    fn set_table_ttl_change_attribute_in_place_applies() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: Some(ttl_spec("expiresAt")),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: Some(ttl_spec("ttlSeconds")),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().ttl,
            Some(ttl_spec("ttlSeconds"))
        );
    }

    /// Disabling clears the spec (`Applied`); disabling again is a no-op.
    #[test]
    fn set_table_ttl_disable_then_disable_again_is_noop() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: Some(ttl_spec("expiresAt")),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: None,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.schemas.get("orders").unwrap().ttl, None);
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "orders".to_owned(),
                spec: None,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(m.schemas.get("orders").unwrap().ttl, None);
    }

    /// `SetTableTtl` against a table with no schema is `Rejected`.
    #[test]
    fn set_table_ttl_rejects_unknown_table() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::SetTableTtl {
                table: "no-such-table".to_owned(),
                spec: Some(ttl_spec("expiresAt")),
            }),
            ApplyOutcome::Rejected("no such table schema")
        );
    }

    // --- ADR 0065 §5(b): per-table provisioned throughput ---------------

    fn throughput_spec(read_units: u64, write_units: u64) -> ProvisionedThroughput {
        ProvisionedThroughput {
            read_units,
            write_units,
        }
    }

    /// Setting throughput on a table with a schema records the spec,
    /// `Applied` — the `SetTableTtl` idiom.
    #[test]
    fn set_table_throughput_enables_and_records_the_spec() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: Some(throughput_spec(5, 5)),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().throughput,
            Some(throughput_spec(5, 5))
        );
        assert_eq!(m.table_throughput("orders"), Some(&throughput_spec(5, 5)));
    }

    /// Re-asserting the identical spec is idempotent — `ProvisionedThroughput`
    /// mints no label, so there is nothing that goes stale on a repeat set.
    #[test]
    fn set_table_throughput_re_set_with_same_spec_is_noop() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: Some(throughput_spec(5, 5)),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: Some(throughput_spec(5, 5)),
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().throughput,
            Some(throughput_spec(5, 5))
        );
    }

    /// Changing the units in place — no disable/re-enable round trip
    /// required — is a legal live `UpdateTable ProvisionedThroughput` and is
    /// `Applied`, recording the new units.
    #[test]
    fn set_table_throughput_change_units_in_place_applies() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: Some(throughput_spec(5, 5)),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: Some(throughput_spec(10, 20)),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().throughput,
            Some(throughput_spec(10, 20))
        );
    }

    /// Reverting to `PAY_PER_REQUEST` (`spec: None`) clears the spec
    /// (`Applied`); reverting again is a no-op.
    #[test]
    fn set_table_throughput_revert_to_pay_per_request_then_again_is_noop() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: Some(throughput_spec(5, 5)),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: None,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.schemas.get("orders").unwrap().throughput, None);
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "orders".to_owned(),
                spec: None,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(m.schemas.get("orders").unwrap().throughput, None);
    }

    /// `SetTableThroughput` against a table with no schema is `Rejected`.
    #[test]
    fn set_table_throughput_rejects_unknown_table() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::SetTableThroughput {
                table: "no-such-table".to_owned(),
                spec: Some(throughput_spec(5, 5)),
            }),
            ApplyOutcome::Rejected("no such table schema")
        );
    }

    /// A table with no `throughput` set falls back to `None` (the cluster
    /// default, resolved by `animusd::ClientCtx::throttle_limits_for`, not
    /// by this crate).
    #[test]
    fn table_throughput_is_none_for_a_table_with_no_spec() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.table_throughput("orders"), None);
        assert_eq!(m.table_throughput("no-such-table"), None);
    }

    // --- W-06: resource tagging -----------------------------------------

    /// `TagResource` on a table with a schema records the tags, `Applied`.
    #[test]
    fn tag_resource_adds_tags() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::TagResource {
                table: "orders".to_owned(),
                tags: BTreeMap::from([
                    ("env".to_owned(), "prod".to_owned()),
                    ("team".to_owned(), "payments".to_owned()),
                ]),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().tags,
            BTreeMap::from([
                ("env".to_owned(), "prod".to_owned()),
                ("team".to_owned(), "payments".to_owned()),
            ])
        );
    }

    /// An existing key is overwritten (last writer wins), matching
    /// DynamoDB's own `TagResource` semantics.
    #[test]
    fn tag_resource_overwrites_an_existing_key() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::TagResource {
                table: "orders".to_owned(),
                tags: BTreeMap::from([("env".to_owned(), "prod".to_owned())]),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::TagResource {
                table: "orders".to_owned(),
                tags: BTreeMap::from([("env".to_owned(), "staging".to_owned())]),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().tags,
            BTreeMap::from([("env".to_owned(), "staging".to_owned())])
        );
    }

    /// Re-`TagResource`-ing with exactly the already-recorded `(key, value)`
    /// pairs is a no-op — the same idempotent-retry shape `SetTableTtl`'s
    /// identical-spec case gets.
    #[test]
    fn tag_resource_identical_repeat_is_noop() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::TagResource {
                table: "orders".to_owned(),
                tags: BTreeMap::from([("env".to_owned(), "prod".to_owned())]),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::TagResource {
                table: "orders".to_owned(),
                tags: BTreeMap::from([("env".to_owned(), "prod".to_owned())]),
            }),
            ApplyOutcome::NoOp
        );
    }

    /// `TagResource` against a table with no schema is `Rejected`.
    #[test]
    fn tag_resource_rejects_unknown_table() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::TagResource {
                table: "no-such-table".to_owned(),
                tags: BTreeMap::from([("env".to_owned(), "prod".to_owned())]),
            }),
            ApplyOutcome::Rejected("no such table schema")
        );
    }

    /// `UntagResource` removes the named keys, leaving the rest.
    #[test]
    fn untag_resource_removes_named_keys() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::TagResource {
                table: "orders".to_owned(),
                tags: BTreeMap::from([
                    ("env".to_owned(), "prod".to_owned()),
                    ("team".to_owned(), "payments".to_owned()),
                ]),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::UntagResource {
                table: "orders".to_owned(),
                tag_keys: vec!["env".to_owned()],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.schemas.get("orders").unwrap().tags,
            BTreeMap::from([("team".to_owned(), "payments".to_owned())])
        );
    }

    /// `UntagResource` naming a key that isn't present is a no-op — a
    /// missing key is silently ignored, not an error, matching DynamoDB.
    #[test]
    fn untag_resource_missing_key_is_noop() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: "orders".to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::UntagResource {
                table: "orders".to_owned(),
                tag_keys: vec!["env".to_owned()],
            }),
            ApplyOutcome::NoOp
        );
    }

    /// `UntagResource` against a table with no schema is `Rejected`.
    #[test]
    fn untag_resource_rejects_unknown_table() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::UntagResource {
                table: "no-such-table".to_owned(),
                tag_keys: vec!["env".to_owned()],
            }),
            ApplyOutcome::Rejected("no such table schema")
        );
    }

    // --- ADR 0059 §9: PITR catalog (Train 3) ---------------------------

    fn table_with_schema(m: &mut Metadata, table: &str) {
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
    }

    /// Enabling PITR on a table with a schema mints generation 1 and records
    /// the enable timestamp.
    #[test]
    fn update_continuous_backups_enable_mints_generation_one() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 1_000,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.table_pitr("orders"),
            Some(&PitrSpec {
                generation: 1,
                enabled_wall_ms: 1_000,
            })
        );
    }

    /// Re-enabling an already-enabled table is a no-op — no fresh generation,
    /// no window reset (mirrors real DynamoDB's own idempotent-call contract).
    #[test]
    fn update_continuous_backups_re_enable_is_noop() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 1_000,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 2_000,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(m.table_pitr("orders").unwrap().generation, 1);
        assert_eq!(m.table_pitr("orders").unwrap().enabled_wall_ms, 1_000);
    }

    /// Disable clears the schema's own spec but leaves the generation floor
    /// untouched; disabling again is a no-op. A later re-enable mints
    /// generation 2, never reusing 1 — the ADR's "fresh window, no fake
    /// continuity" rule.
    #[test]
    fn update_continuous_backups_disable_then_re_enable_mints_a_fresh_generation() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 1_000,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: false,
                wall_ms: 1_500,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.table_pitr("orders"), None);
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: false,
                wall_ms: 1_600,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 2_000,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.table_pitr("orders"),
            Some(&PitrSpec {
                generation: 2,
                enabled_wall_ms: 2_000,
            })
        );
    }

    /// `UpdateContinuousBackups` against a table with no schema is `Rejected`.
    #[test]
    fn update_continuous_backups_rejects_unknown_table() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "no-such-table".to_owned(),
                enabled: true,
                wall_ms: 1_000,
            }),
            ApplyOutcome::Rejected("no such table schema")
        );
    }

    /// A dropped-and-recreated table under the same name never reuses a
    /// generation its earlier incarnation already minted — the identical
    /// non-reuse guarantee `next_tablet_id`'s allocator floor gives tablet
    /// ids, now over `Metadata::pitr_generation`.
    #[test]
    fn pitr_generation_floor_survives_a_drop_and_recreate_of_the_same_table_name() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 1_000,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::DropTableSchema {
                table: "orders".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        table_with_schema(&mut m, "orders");
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 5_000,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.table_pitr("orders").unwrap().generation, 2);
    }

    fn pitr_seal(table: &str, generation: u64, tablet: TabletId, epoch: u64) -> MetaCommand {
        MetaCommand::SealPitrSegment {
            table: table.to_owned(),
            generation,
            tablet,
            epoch,
            hlc_range: (epoch * 100, epoch * 100 + 50),
            count: 5,
            seal_wall_ms: 1_000 + epoch,
            replicas: Vec::new(),
            object_id: format!("backup/pitr/{table}/{}/{epoch}/attempt-{epoch}", tablet.0),
        }
    }

    /// A basic PITR segment seal against a currently-enabled generation
    /// applies and is readable back through the watermark accessors.
    #[test]
    fn seal_pitr_segment_basic_seal_applies() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 1_000,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&pitr_seal("orders", 1, TabletId(1), 0)),
            ApplyOutcome::Applied
        );
        assert_eq!(m.pitr_segment_watermark(TabletId(1)), Some(50));
        assert_eq!(m.last_pitr_seal_wall_ms(TabletId(1)), Some(1_000));
        assert_eq!(m.pitr_generations_with_rows("orders"), BTreeSet::from([1]));
    }

    /// A repeat proposal of the identical seal (the sealer's own crash-retry
    /// racing itself) is a genuine no-op.
    #[test]
    fn seal_pitr_segment_identical_retry_is_noop() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        assert_eq!(
            m.apply(&pitr_seal("orders", 1, TabletId(1), 0)),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&pitr_seal("orders", 1, TabletId(1), 0)),
            ApplyOutcome::NoOp
        );
    }

    /// A replicas-only re-proposal (the janitor's own repair sweep shape) is
    /// `Applied`, updating only `replicas`.
    #[test]
    fn seal_pitr_segment_replicas_only_update_applies() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        let mut repaired = pitr_seal("orders", 1, TabletId(1), 0);
        if let MetaCommand::SealPitrSegment { replicas, .. } = &mut repaired {
            *replicas = vec![nid(2)];
        }
        assert_eq!(m.apply(&repaired), ApplyOutcome::Applied);
        assert_eq!(m.pitr_segments[&(TabletId(1), 0)].replicas, vec![nid(2)]);
    }

    /// A genuinely conflicting re-proposal for the same `(tablet, epoch)` —
    /// different content, not just `replicas` — is rejected as a no-op.
    #[test]
    fn seal_pitr_segment_conflicting_content_is_noop() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        let mut conflicting = pitr_seal("orders", 1, TabletId(1), 0);
        if let MetaCommand::SealPitrSegment { count, .. } = &mut conflicting {
            *count = 999;
        }
        assert_eq!(m.apply(&conflicting), ApplyOutcome::NoOp);
        assert_eq!(m.pitr_segments[&(TabletId(1), 0)].count, 5);
    }

    /// A generation matching neither the table's current spec nor any
    /// existing catalog row is rejected outright.
    #[test]
    fn seal_pitr_segment_rejects_an_unlicensed_generation() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        assert_eq!(
            m.apply(&pitr_seal("orders", 99, TabletId(1), 0)),
            ApplyOutcome::Rejected(
                "PITR generation has no current schema entry and no existing catalog rows to \
                 extend"
            )
        );
    }

    /// A disabled table's un-reaped rows still license a further seal of the
    /// SAME generation — the disable-triggered final seal, mirroring
    /// `SealStreamShard`'s F12-b rule exactly.
    #[test]
    fn seal_pitr_segment_disable_triggered_final_seal_is_licensed() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: false,
            wall_ms: 2_000,
        });
        // The final seal after disable: epoch 1, still generation 1.
        assert_eq!(
            m.apply(&pitr_seal("orders", 1, TabletId(1), 1)),
            ApplyOutcome::Applied
        );
    }

    /// Epoch-chain sanity: epoch 0 always accepted; epoch > 0 requires the
    /// tablet's own prior epoch row to exist first.
    #[test]
    fn seal_pitr_segment_rejects_an_epoch_chain_gap() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        assert_eq!(
            m.apply(&pitr_seal("orders", 1, TabletId(1), 3)),
            ApplyOutcome::Rejected("epoch chain gap: no prior epoch row for this tablet")
        );
    }

    /// `ExpirePitrSegments`'s two-phase mark/remove shape mirrors
    /// `ExpireStreamShards` exactly.
    #[test]
    fn expire_pitr_segments_mark_then_remove() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        assert_eq!(
            m.apply(&MetaCommand::ExpirePitrSegments {
                rows: vec![(TabletId(1), 0)],
                remove: false,
            }),
            ApplyOutcome::Applied
        );
        assert!(m.pitr_segments[&(TabletId(1), 0)].expired);
        // Re-marking an already-marked row is a no-op.
        assert_eq!(
            m.apply(&MetaCommand::ExpirePitrSegments {
                rows: vec![(TabletId(1), 0)],
                remove: false,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(
            m.apply(&MetaCommand::ExpirePitrSegments {
                rows: vec![(TabletId(1), 0)],
                remove: true,
            }),
            ApplyOutcome::Applied
        );
        assert!(!m.pitr_segments.contains_key(&(TabletId(1), 0)));
        // Removing an already-absent row is a no-op.
        assert_eq!(
            m.apply(&MetaCommand::ExpirePitrSegments {
                rows: vec![(TabletId(1), 0)],
                remove: true,
            }),
            ApplyOutcome::NoOp
        );
    }

    /// ADR 0059 §10 (Train 3 PR②): before any base snapshot or segment has
    /// ever landed, a freshly-enabled generation still reports a trivially
    /// valid (zero-width) window at its own enable moment — mirroring
    /// `animusd::dynamo::pitr_description`'s identical `.unwrap_or(spec.
    /// enabled_wall_ms)` fallback, not `None` (which would misreport "PITR
    /// was never enabled" the instant `RestoreTableToPointInTime` is asked
    /// about it in this narrow window).
    #[test]
    fn pitr_restore_window_before_any_seal_is_a_zero_width_window_at_enable() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 5_000,
        });
        let window = m
            .pitr_restore_window("orders")
            .expect("a live generation has a window");
        assert_eq!(window.generation, 1);
        assert_eq!(window.earliest_ms, 5_000);
        assert_eq!(window.latest_ms, 5_000);
    }

    /// `Latest` advances as segments seal, tracking the SLOWEST tablet ever
    /// to have sealed a segment of the current generation — the identical
    /// minimum-over-tablets shape `pitr_description` uses for a live
    /// table's *current* tablets, generalized here to "every tablet this
    /// generation's own history ever touched" (see `PitrRestoreWindow`'s
    /// own doc for why that generalization is what still makes sense once
    /// a table can be dropped).
    #[test]
    fn pitr_restore_window_advances_as_segments_seal() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        m.apply(&pitr_seal("orders", 1, TabletId(2), 0));
        // The minimum across BOTH tablets that have ever sealed — tablet 2
        // hasn't sealed its own epoch 1 yet, so `Latest` stays pinned at
        // tablet 1's own first seal until it does.
        let mut with_gap = m.clone();
        with_gap.apply(&pitr_seal("orders", 1, TabletId(1), 1));
        let window = with_gap.pitr_restore_window("orders").unwrap();
        assert_eq!(
            window.latest_ms, 1_000,
            "the slower tablet (2) still bounds Latest"
        );
        m.apply(&pitr_seal("orders", 1, TabletId(1), 1));
        m.apply(&pitr_seal("orders", 1, TabletId(2), 1));
        let window = m.pitr_restore_window("orders").unwrap();
        assert_eq!(window.latest_ms, 1_000 + 1);
    }

    /// A `T` before the CURRENT generation's own enable moment — including
    /// one that falls inside an earlier disable/re-enable's own now-
    /// superseded coverage — is never consulted: `pitr_restore_window`
    /// scopes to the table's own LATEST generation alone, so this settles
    /// the ADR's own Train 3 PR① "not yet decided" generation-gap question.
    #[test]
    fn pitr_restore_window_scopes_to_the_latest_generation_only() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: false,
            wall_ms: 5_000,
        });
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 9_000,
        });
        let window = m.pitr_restore_window("orders").unwrap();
        assert_eq!(window.generation, 2);
        assert_eq!(
            window.earliest_ms, 9_000,
            "generation 1's own earlier coverage (starting at 1_000) must never surface \
             once generation 2 is current"
        );
    }

    /// `pitr_restore_window` works after `DropTableSchema` — the catalog's
    /// own outlives-the-source-table rule applied to restore's own
    /// validation gate, not just to the raw catalog rows.
    #[test]
    fn pitr_restore_window_survives_drop_table_schema() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        m.apply(&MetaCommand::DropTableSchema {
            table: "orders".to_owned(),
        });
        assert!(!m.has_table_schema("orders"));
        let window = m
            .pitr_restore_window("orders")
            .expect("a dropped table's PITR window must still resolve");
        assert_eq!(window.generation, 1);
        assert_eq!(window.latest_ms, 1_000);
    }

    /// A table name that has never enabled PITR at all (no live spec, no
    /// generation floor) has no window — the genuine "unknown source"
    /// case `RestoreTableToPointInTime`'s own handler maps to either
    /// `TableNotFoundException` or `PointInTimeRecoveryUnavailableException`
    /// depending on whether the name is a live table.
    #[test]
    fn pitr_restore_window_is_none_for_a_table_that_never_enabled_pitr() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        assert_eq!(m.pitr_restore_window("orders"), None);
        assert_eq!(m.pitr_restore_window("no-such-table"), None);
    }

    /// `pitr_replay_segments` on a tablet that never split just floors its
    /// own chain at the base snapshot's own cut version and stops at the
    /// cutoff — the common case.
    #[test]
    fn pitr_replay_segments_floors_at_the_base_cut_version_and_respects_the_cutoff() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_owned()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        // Three sealed segments at seal_wall_ms 1_000/1_001/1_002 with
        // hlc_range (0,50)/(100,150)/(200,250) (see `pitr_seal`'s own
        // formula: `(epoch*100, epoch*100+50)`).
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        m.apply(&pitr_seal("orders", 1, TabletId(1), 1));
        m.apply(&pitr_seal("orders", 1, TabletId(1), 2));

        // A base snapshot captured at cut_version 25 (mid-way through
        // epoch 0's own range) — epoch 0 must still be included (it has
        // records past 25), sliced from 25 rather than 0.
        let base = vec![(TabletId(1), 25)];
        let refs = m.pitr_replay_segments(&base, 1_001); // cutoff excludes epoch 2
        assert_eq!(refs.len(), 2, "{refs:?}");
        assert_eq!(refs[0].epoch, 0);
        assert_eq!(refs[0].replay_range, (25, 50));
        assert_eq!(refs[1].epoch, 1);
        assert_eq!(refs[1].replay_range, (100, 150));
    }

    /// The regression for the real bug this function's own doc names: a
    /// table drop (`DropTableTablets`) retires a tablet WITHOUT ever
    /// writing a `split_lineage` entry for it (unlike a split) — a naive
    /// `live_split_descendants`-based re-planning would see "no live
    /// descendant" and silently replay NOTHING for a dropped table's own
    /// never-split tablet, even though its segments are sitting right
    /// there in the catalog. `pitr_replay_segments` must keep including a
    /// base tablet's own segments regardless of whether it is currently
    /// live.
    #[test]
    fn pitr_replay_segments_still_finds_a_dropped_never_split_tablets_own_segments() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_owned()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        m.apply(&MetaCommand::DropTableSchema {
            table: "orders".to_owned(),
        });
        m.apply(&MetaCommand::DropTableTablets {
            table: "orders".to_owned(),
        });
        assert!(!m.tablets.contains_key(&TabletId(1)), "the tablet is gone");
        assert!(
            m.live_split_descendants(TabletId(1)).is_empty(),
            "a dropped, never-split tablet genuinely has no live descendant — this is the \
             exact case `pitr_replay_segments` must not delegate to that accessor for"
        );

        let base = vec![(TabletId(1), 0)];
        let refs = m.pitr_replay_segments(&base, 10_000);
        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].tablet, TabletId(1));
        assert_eq!(refs[0].replay_range, (0, 50));
    }

    /// `pitr_replay_segments` a base tablet's own share (§6-style, ADR
    /// 0059's re-planning technique applied to PITR): a split retires the
    /// base tablet after the snapshot but before the requested second, so
    /// the plan must include the parent's own remaining segment PLUS both
    /// children's own full chains.
    #[test]
    fn pitr_replay_segments_re_plans_onto_live_split_descendants() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_owned()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        // The base snapshot pinned tablet 1 at cut_version 10, then it
        // sealed one more segment (epoch 0, hlc 0..50) before splitting.
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        split_tablet(&mut m, TabletId(1), b"m".to_vec(), TabletId(2));
        assert!(!m.tablets.contains_key(&TabletId(1)));
        // Each child seals its own fresh epoch-0 chain independently.
        m.apply(&pitr_seal("orders", 1, TabletId(2), 0));
        m.apply(&pitr_seal("orders", 1, TabletId(3), 0));

        let base = vec![(TabletId(1), 10)];
        let refs = m.pitr_replay_segments(&base, 10_000); // generous cutoff
        let tablets: Vec<u64> = refs.iter().map(|r| r.tablet.0).collect();
        assert!(tablets.contains(&1), "{refs:?}");
        assert!(tablets.contains(&2), "{refs:?}");
        assert!(tablets.contains(&3), "{refs:?}");
        let parent_ref = refs.iter().find(|r| r.tablet == TabletId(1)).unwrap();
        assert_eq!(parent_ref.replay_range, (10, 50));
        for child in [TabletId(2), TabletId(3)] {
            let child_ref = refs.iter().find(|r| r.tablet == child).unwrap();
            assert_eq!(
                child_ref.replay_range,
                (0, 50),
                "a split child's own chain starts fresh, not floored at the parent's cut"
            );
        }
    }

    /// PITR segments/generation survive `DropTableSchema` — the catalog's
    /// deliberate outlives-the-table rule (ADR 0059 §3/§10), the override of
    /// the streams retention-zero rule.
    #[test]
    fn pitr_segments_survive_drop_table_schema() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::UpdateContinuousBackups {
            table: "orders".to_owned(),
            enabled: true,
            wall_ms: 1_000,
        });
        m.apply(&pitr_seal("orders", 1, TabletId(1), 0));
        assert_eq!(
            m.apply(&MetaCommand::DropTableSchema {
                table: "orders".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert!(m.pitr_segments.contains_key(&(TabletId(1), 0)));
        assert_eq!(m.pitr_generation.get("orders"), Some(&1));
    }

    /// `BeginBackup { pitr_base: true }` tags the row in the SAME apply that
    /// mints it (issue #593) — never a separate command, so there is no
    /// committed state in which the row exists but the tag doesn't. Covers
    /// every reachable `BackupStatus` (`Creating` at mint, then `Available`
    /// via `CompleteBackup`) to prove the tag survives every transition, not
    /// just the instant of creation.
    #[test]
    fn begin_backup_pitr_base_tags_atomically_with_the_mint() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        assert_eq!(
            m.apply(&MetaCommand::UpdateContinuousBackups {
                table: "orders".to_owned(),
                enabled: true,
                wall_ms: 500,
            }),
            ApplyOutcome::Applied
        );
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_owned()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "b1".to_owned(),
                table: "orders".to_owned(),
                created_wall_ms: 1_000,
                backup_name: "__pitr_base__orders".to_owned(),
                pitr_base: true,
            }),
            ApplyOutcome::Applied
        );
        // Tagged immediately — no separate command ever ran, and the row is
        // `Creating` at this point: the exact state the pre-fix two-command
        // sequence used to leave briefly untagged.
        assert_eq!(m.backups["b1"].status, BackupStatus::Creating);
        assert!(m.pitr_base_backups.contains("b1"));

        // Still tagged once the backup completes.
        m.apply(&MetaCommand::RecordBackupTabletComplete {
            backup_id: "b1".to_owned(),
            tablet: TabletId(1),
            cut_version: 1,
            bytes: 0,
        });
        assert_eq!(
            m.apply(&MetaCommand::CompleteBackup {
                backup_id: "b1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.backups["b1"].status, BackupStatus::Available);
        assert!(m.pitr_base_backups.contains("b1"));
    }

    /// An ordinary on-demand backup (`pitr_base: false`, every non-PITR
    /// proposer) is never tagged — the flag is not a blanket default-true.
    #[test]
    fn begin_backup_without_pitr_base_is_never_tagged() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_owned()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        assert_eq!(
            m.apply(&MetaCommand::BeginBackup {
                backup_id: "b1".to_owned(),
                table: "orders".to_owned(),
                created_wall_ms: 1_000,
                backup_name: "my-backup".to_owned(),
                pitr_base: false,
            }),
            ApplyOutcome::Applied
        );
        assert!(!m.pitr_base_backups.contains("b1"));
    }

    /// `DeleteBackup` prunes a PITR base tag alongside the row it tags.
    #[test]
    fn delete_backup_prunes_its_own_pitr_base_tag() {
        let mut m = Metadata::default();
        table_with_schema(&mut m, "orders");
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_owned()),
            range: KeyRange::whole(),
            replicas: Vec::new(),
        });
        m.apply(&MetaCommand::BeginBackup {
            backup_id: "b1".to_owned(),
            table: "orders".to_owned(),
            created_wall_ms: 1_000,
            backup_name: "pitr-base".to_owned(),
            pitr_base: true,
        });
        assert_eq!(
            m.apply(&MetaCommand::DeleteBackup {
                backup_id: "b1".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert!(!m.pitr_base_backups.contains("b1"));
    }

    // --- ADR 0042/0043 stream-shard catalog ---------------------------

    use crate::schema::StreamViewType;

    fn stream_spec(label: &str) -> StreamSpec {
        StreamSpec {
            view_type: StreamViewType::NewAndOldImages,
            label: label.to_owned(),
        }
    }

    fn enable_stream(m: &mut Metadata, table: &str, label: &str) {
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: table.to_owned(),
                schema: TableSchema::simple("pk", ColumnType::String),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTableStream {
                table: table.to_owned(),
                spec: Some(stream_spec(label)),
            }),
            ApplyOutcome::Applied
        );
    }

    /// A test-only object id, deterministic in exactly `(table, label,
    /// tablet, epoch)` — NOT the real per-attempt scheme
    /// (`animus_cp_data::segment::segment_object_id`, unreachable from this
    /// crate's own dependency direction), but stable enough that two calls
    /// to [`seal`] with the same identity produce the same id (so the
    /// existing "byte-identical re-propose" tests below still see a true
    /// no-op) while two calls with a different identity never collide.
    fn test_object_id(table: &str, label: &str, tablet: u64, epoch: u64) -> String {
        format!("{table}/{label}/{tablet}/{epoch}/test")
    }

    /// `m`'s CURRENT range for `tablet` becomes this command's own
    /// `expected_range` stamp — mirroring production's `seal_now`, which
    /// always fences against whatever the tablet's live metadata range is
    /// *at the moment it builds the proposal*. This is why `m` is a
    /// parameter here at all: a caller testing a real split (`split_tablet`
    /// below) gets an automatically-correct stamp with no per-call
    /// override, and a caller that never registers a tablet at all gets
    /// `whole()` — inert either way, since an absent tablet makes the CAS
    /// permissive regardless of what was stamped (see the apply arm's own
    /// doc).
    fn seal(
        _m: &Metadata,
        table: &str,
        label: &str,
        tablet: u64,
        epoch: u64,
        end: u64,
    ) -> MetaCommand {
        MetaCommand::SealStreamShard {
            table: table.to_owned(),
            label: label.to_owned(),
            tablet: TabletId(tablet),
            epoch,
            view_type: StreamViewType::NewAndOldImages,
            hlc_range: (end.saturating_sub(100), end),
            count: 1,
            seal_wall_ms: 1_700_000_000_000,
            replicas: vec![nid(1), nid(2), nid(3)],
            object_id: test_object_id(table, label, tablet, epoch),
        }
    }

    /// First-committer-wins on `(tablet, epoch)`'s **content** (ADR 0043
    /// §A8, round-3 PR7 amendment): the first proposal for an identity
    /// lands; a byte-identical re-propose (the sealer's own crash-retry) is
    /// a no-op; a proposal whose non-`replicas` content genuinely differs
    /// (a stale/duelling leader) is also a no-op that never overwrites the
    /// winning row's own facts. The replicas-only-update case (the segment
    /// janitor's repair sweep) is covered by its own test below.
    #[test]
    fn seal_stream_shard_first_committer_wins_on_tablet_epoch() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");

        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 0, 100)),
            ApplyOutcome::Applied
        );
        let first = m.stream_shards[&(TabletId(1), 0)].clone();
        assert_eq!(first.hlc_range, (0, 100));

        // Byte-identical re-propose (the sealer's own crash-retry).
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 0, 100)),
            ApplyOutcome::NoOp
        );
        assert_eq!(m.stream_shards[&(TabletId(1), 0)], first);

        // A genuinely differing proposal for the SAME (tablet, epoch) — a
        // duelling/stale leader — must not overwrite the winner either.
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 0, 999)),
            ApplyOutcome::NoOp
        );
        assert_eq!(
            m.stream_shards[&(TabletId(1), 0)],
            first,
            "the first committer's row must survive unchanged"
        );
    }

    /// F11 (ADR 0042 §14, Fork D) apply-time seatbelt, over `BeginSplitInPlace`
    /// (its own arm carries the identical check, see that command's doc):
    /// against a **streamed** table's tablet, a split key that isn't exactly
    /// `TOKEN_BYTES` long is rejected — the structural check against a
    /// future caller that bypasses `animusd::ClientCtx::trigger_split`'s own
    /// rounding (the primary enforcement, tested at that layer). A properly
    /// token-aligned (8-byte) key still applies normally. Supersedes the
    /// deleted copy-based `BeginSplit` version of this test (copy-split
    /// deletion stack, layer 1) — the F11 seatbelt is byte-for-byte
    /// identical on both commands' apply arms.
    #[test]
    fn split_in_place_rejects_a_non_token_aligned_key_on_a_streamed_table() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );

        // A 5-byte key: strictly inside the whole range, but shorter than
        // one token (`TOKEN_BYTES == 8`) — rejected before `KeyRange::
        // split_at` is even consulted.
        let homes = vec![nid(1), nid(2), nid(3)];
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: b"mmmmm".to_vec(),
                children: [(TabletId(2), homes.clone()), (TabletId(3), homes.clone())],
            }),
            ApplyOutcome::Rejected("split key not token-aligned for a streamed table")
        );
        assert_eq!(m.tablets.len(), 1, "the rejected split changed nothing");
        assert!(
            m.tablets[&TabletId(1)].inplace_split.is_none(),
            "the rejected split recorded no intent either"
        );

        // The same tablet, same epoch, a properly token-aligned key: applies
        // — the parent records the intent (no tablet-map row for the
        // children yet, unlike the deleted copy-based arm).
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
                children: [(TabletId(2), homes.clone()), (TabletId(3), homes)],
            }),
            ApplyOutcome::Applied
        );
        assert!(m.tablets[&TabletId(1)].inplace_split.is_some());

        // An unstreamed table's tablet is completely unaffected by the
        // fence — any strictly-interior key, of any length, still applies.
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(10),
                table: Some("plain".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: TabletId(10),
                expected_epoch: Epoch::INITIAL,
                split_key: b"mmmmm".to_vec(),
                children: [
                    (TabletId(11), vec![nid(1), nid(2), nid(3)]),
                    (TabletId(12), vec![nid(1), nid(2), nid(3)]),
                ],
            }),
            ApplyOutcome::Applied
        );
    }

    /// F11 Fork E (ADR 0042 §14), over `BeginSplitInPlace`: the accepted
    /// single-token hot-partition limit at the apply layer — a
    /// token-aligned split key that happens to equal the *target* tablet's
    /// own `range.start` (a single very hot partition token owning the
    /// tablet's entire range) is rejected by the pre-existing `KeyRange::
    /// split_at` "strictly inside" guard, not silently accepted. `ClientCtx::
    /// trigger_split` (`animusd`) is the layer that turns this into a
    /// metered skip before ever proposing; this proves the fence holds even
    /// if a future caller reaches apply directly. Supersedes the deleted
    /// copy-based `BeginSplit` version of this test (copy-split deletion
    /// stack, layer 1).
    #[test]
    fn split_in_place_rejects_a_token_aligned_key_equal_to_range_start() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        let boundary = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        let homes = vec![nid(1), nid(2), nid(3)];
        split_tablet(&mut m, TabletId(1), boundary.clone(), TabletId(2));
        assert_eq!(m.tablets[&TabletId(3)].range.start, boundary);

        // Splitting the right child at a key equal to its own `range.start`
        // — the single-hot-token degenerate case — is rejected, not
        // accepted into a zero-width tablet.
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: TabletId(3),
                expected_epoch: m.tablets[&TabletId(3)].epoch,
                split_key: boundary,
                children: [(TabletId(4), homes.clone()), (TabletId(5), homes)],
            }),
            ApplyOutcome::Rejected("split key not strictly inside range")
        );
    }

    /// **The ledger-named-object amendment's own regression**: two
    /// attempts that agree on every field EXCEPT `object_id` — exactly what
    /// two independently-computed seal attempts for the same epoch produce,
    /// the dueling-seal race this amendment closes (each attempt mints its
    /// own unique per-attempt id even when it happens to compute the
    /// identical `hlc_range`/`count`, e.g. a lost-ack retry of the exact
    /// same underlying computation) — must be treated as a genuine content
    /// mismatch, a `NoOp` that leaves the first committer's row (and its
    /// own `object_id`) untouched. Before this field existed, this exact
    /// shape was misclassified as the identical-content case.
    #[test]
    fn seal_stream_shard_treats_a_differing_object_id_as_a_content_mismatch() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");

        let mut first_attempt = seal(&m, "orders", "L1", 1, 0, 100);
        if let MetaCommand::SealStreamShard { object_id, .. } = &mut first_attempt {
            *object_id = "orders/L1/1/0/attempt-a".to_owned();
        }
        assert_eq!(m.apply(&first_attempt), ApplyOutcome::Applied);
        let winner = m.stream_shards[&(TabletId(1), 0)].clone();
        assert_eq!(winner.object_id, "orders/L1/1/0/attempt-a");

        // A second attempt, identical `hlc_range`/`count`/etc but its OWN
        // freshly-minted object_id — the lost-ack-retry / dueling-seal
        // shape.
        let mut second_attempt = seal(&m, "orders", "L1", 1, 0, 100);
        if let MetaCommand::SealStreamShard { object_id, .. } = &mut second_attempt {
            *object_id = "orders/L1/1/0/attempt-b".to_owned();
        }
        assert_eq!(m.apply(&second_attempt), ApplyOutcome::NoOp);
        assert_eq!(
            m.stream_shards[&(TabletId(1), 0)],
            winner,
            "the first committer's row, object_id included, must survive unchanged"
        );
    }

    /// The segment janitor's replica-repair shape (ADR 0043 §A9, round-3
    /// PR7): a second proposal for an already-committed `(tablet, epoch)`
    /// whose content matches exactly but whose `replicas` differs is
    /// `Applied` — a genuine in-place update — never a `NoOp`. A
    /// content-conflicting proposal (a different `hlc_range`) with the
    /// identical new `replicas` is still rejected as a `NoOp`, proving the
    /// content check runs independently of whether `replicas` also
    /// happens to differ.
    #[test]
    fn seal_stream_shard_replicas_only_update_applies() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 0, 100)),
            ApplyOutcome::Applied
        );
        let original = m.stream_shards[&(TabletId(1), 0)].clone();
        assert_eq!(original.replicas, vec![nid(1), nid(2), nid(3)]);

        // Repair: node 2 was lost and replaced by node 4 — identical
        // content, a different replica set.
        let mut repaired = seal(&m, "orders", "L1", 1, 0, 100);
        if let MetaCommand::SealStreamShard { replicas, .. } = &mut repaired {
            *replicas = vec![nid(1), nid(3), nid(4)];
        }
        assert_eq!(m.apply(&repaired), ApplyOutcome::Applied);
        let row = &m.stream_shards[&(TabletId(1), 0)];
        assert_eq!(row.replicas, vec![nid(1), nid(3), nid(4)]);
        // Every other field is untouched.
        assert_eq!(row.hlc_range, original.hlc_range);
        assert_eq!(row.count, original.count);
        assert_eq!(row.seal_wall_ms, original.seal_wall_ms);
        assert_eq!(row.view_type, original.view_type);
        assert!(!row.expired, "a replicas update never touches `expired`");

        // Re-proposing the now-current (repaired) replicas is a genuine
        // no-op — nothing left to change.
        assert_eq!(m.apply(&repaired), ApplyOutcome::NoOp);

        // But a proposal that ALSO changes real content (a different
        // `hlc_range`) alongside a new replica set is still rejected,
        // never applied — the content check is independent of whether
        // `replicas` happens to differ too.
        let mut conflicting = seal(&m, "orders", "L1", 1, 0, 999);
        if let MetaCommand::SealStreamShard { replicas, .. } = &mut conflicting {
            *replicas = vec![nid(5)];
        }
        assert_eq!(m.apply(&conflicting), ApplyOutcome::NoOp);
        assert_eq!(
            m.stream_shards[&(TabletId(1), 0)].replicas,
            vec![nid(1), nid(3), nid(4)],
            "a content-conflicting proposal must not sneak a replicas update through"
        );
    }

    /// **Split-seal range-fence CAS (2026-08-15, ADR 0043 §A3/§A4)**: a
    /// proposal for a genuinely NEW `(tablet, epoch)` row is rejected if its
    /// declared `expected_range` no longer matches the tablet's CURRENT
    /// range — this is the authoritative backstop for the case a
    /// proposal-side metadata read (`animusd::index_drain::
    /// in_declared_range`) cannot close on its own: the range moved on
    /// (here, via a real `SplitTablet` apply) between when the proposer
    /// Label validation (F12-b): a label with a matching *current* schema
    /// stream spec is accepted; a label matching neither the current spec
    /// nor any existing row is rejected; and — the draining case — a label
    /// with no current schema entry (disabled) but at least one existing
    /// catalog row still licenses a further seal under it (e.g. the
    /// disable-triggered final seal, proposed after `SetTableStream{None}`
    /// already cleared the schema).
    #[test]
    fn seal_stream_shard_validates_label_against_schema_or_existing_rows() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");

        // A label nobody has ever licensed: rejected.
        assert_eq!(
            m.apply(&seal(&m, "orders", "bogus", 1, 0, 100)),
            ApplyOutcome::Rejected(
                "stream label has no current schema entry and no existing catalog rows \
                 to extend"
            )
        );
        assert!(m.stream_shards.is_empty());

        // The current schema's own label: accepted.
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 0, 100)),
            ApplyOutcome::Applied
        );

        // Disable the stream — the schema no longer names "L1" at all —
        // then seal a further epoch under the SAME label (the
        // disable-triggered final seal): still accepted, because an
        // existing row for (orders, L1) already licenses it.
        assert_eq!(
            m.apply(&MetaCommand::SetTableStream {
                table: "orders".to_owned(),
                spec: None,
            }),
            ApplyOutcome::Applied
        );
        assert!(m.table_stream("orders").is_none(), "test premise");
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 1, 200)),
            ApplyOutcome::Applied,
            "a disabled stream's un-reaped rows must still license a further seal \
             of the same generation"
        );

        // A DIFFERENT label, still with no schema entry and no rows of its
        // own, remains rejected even though the table has *some* rows.
        assert_eq!(
            m.apply(&seal(&m, "orders", "L2", 1, 2, 300)),
            ApplyOutcome::Rejected(
                "stream label has no current schema entry and no existing catalog rows \
                 to extend"
            )
        );
    }

    /// Epoch-chain sanity: epoch 0 is always accepted; epoch > 0 needs
    /// either a local predecessor row or `split_parents` provenance to
    /// explain the tablet's own chain start (permissive-but-sane).
    #[test]
    fn seal_stream_shard_epoch_chain_guard() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");

        // epoch 0 with no history at all: always fine.
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 0, 100)),
            ApplyOutcome::Applied
        );

        // epoch 2 with no epoch-1 row: rejected — a genuine gap this state
        // machine can't explain (the zero-copy provenance escape hatch is
        // gone, Train B rung 7).
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 2, 300)),
            ApplyOutcome::Rejected("epoch chain gap: no prior epoch row for this tablet")
        );

        // Filling in epoch 1 makes epoch 2 acceptable.
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 1, 200)),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 1, 2, 300)),
            ApplyOutcome::Applied
        );

        // A fresh tablet with NO local history at all may seal epoch 0 —
        // the ordinary case, and a copy-based split child's own first seal
        // (ADR 0050: children start their own chains at 0, always).
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 2, 0, 350)),
            ApplyOutcome::Applied
        );
        // The zero-copy provenance escape hatch is GONE (Train B rung 7):
        // a non-zero epoch with no local predecessor row is a chain gap,
        // full stop.
        assert_eq!(
            m.apply(&seal(&m, "orders", "L1", 3, 1, 400)),
            ApplyOutcome::Rejected("epoch chain gap: no prior epoch row for this tablet"),
            "no provenance escape hatch remains for a non-zero epoch with no local history"
        );
    }

    /// `ExpireStreamShards`'s two-phase shape (ADR 0043 §A9): `remove:
    /// false` marks a row `expired` in place (idempotent, never removes
    /// it); `remove: true` physically removes it (idempotent, works
    /// whether or not the row was ever marked). Absent rows are a no-op
    /// either way.
    #[test]
    fn expire_stream_shards_mark_then_remove_is_idempotent() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");
        m.apply(&seal(&m, "orders", "L1", 1, 0, 100));
        m.apply(&seal(&m, "orders", "L1", 1, 1, 200));

        // Mark: row(s) present, not yet expired -> Applied, now expired.
        assert_eq!(
            m.apply(&MetaCommand::ExpireStreamShards {
                rows: vec![(TabletId(1), 0)],
                remove: false,
            }),
            ApplyOutcome::Applied
        );
        assert!(m.stream_shards[&(TabletId(1), 0)].expired);
        assert!(
            !m.stream_shards[&(TabletId(1), 1)].expired,
            "only the named row is marked"
        );

        // Re-marking the same row is idempotent (a no-op, not a re-Applied).
        assert_eq!(
            m.apply(&MetaCommand::ExpireStreamShards {
                rows: vec![(TabletId(1), 0)],
                remove: false,
            }),
            ApplyOutcome::NoOp
        );

        // Marking an absent row is a no-op.
        assert_eq!(
            m.apply(&MetaCommand::ExpireStreamShards {
                rows: vec![(TabletId(99), 0)],
                remove: false,
            }),
            ApplyOutcome::NoOp
        );

        // The marked-but-not-removed row is STILL present (never a
        // visibility gate) until the janitor's remove phase.
        assert!(m.stream_shards.contains_key(&(TabletId(1), 0)));

        // Remove: physically deletes the row.
        assert_eq!(
            m.apply(&MetaCommand::ExpireStreamShards {
                rows: vec![(TabletId(1), 0)],
                remove: true,
            }),
            ApplyOutcome::Applied
        );
        assert!(!m.stream_shards.contains_key(&(TabletId(1), 0)));

        // Removing an already-removed (or never-marked, drop-table-cascade
        // shape) row is idempotent.
        assert_eq!(
            m.apply(&MetaCommand::ExpireStreamShards {
                rows: vec![(TabletId(1), 0)],
                remove: true,
            }),
            ApplyOutcome::NoOp
        );
        assert_eq!(
            m.apply(&MetaCommand::ExpireStreamShards {
                rows: vec![(TabletId(1), 1)],
                remove: true,
            }),
            ApplyOutcome::Applied,
            "remove works directly on a never-marked row (the drop-table cascade shape)"
        );
    }

    // --- accessors ------------------------------------------------------

    /// Run a full split of `source` at `split_key` (`new_id`/`new_id + 1`
    /// end up `Active`, `source` removed, `split_lineage` frozen) as a
    /// shared fixture step for tests below whose actual subject is
    /// downstream of the split (backup re-planning, PITR replay, table
    /// drop, the allocator floor) rather than the split apply-gates
    /// themselves — those get their own dedicated `begin_split_in_place_*`/
    /// `cutover_split_in_place_*` tests. Drives `BeginSplitInPlace` (ADR
    /// 0062: both children fork onto `source`'s own current replicas,
    /// verbatim and identical) then `CutoverSplit`'s in-place branch, rather
    /// than the deprecated copy-based `BeginSplit` this helper used before
    /// the copy-split deletion stack's layer 1 — no caller here sets a
    /// policy on `source` before splitting, so `CutoverSplit` never has
    /// anything to place and writes no `split_placing` entry either.
    fn split_tablet(m: &mut Metadata, source: TabletId, split_key: Vec<u8>, new_id: TabletId) {
        let expected_epoch = m.tablets.get(&source).map_or(Epoch::INITIAL, |t| t.epoch);
        let replicas = m
            .tablets
            .get(&source)
            .map(|t| t.replicas.clone())
            .unwrap_or_default();
        assert_eq!(
            m.apply(&MetaCommand::BeginSplitInPlace {
                parent: source,
                expected_epoch,
                split_key,
                children: [
                    (new_id, replicas.clone()),
                    (TabletId(new_id.0 + 1), replicas)
                ],
            }),
            ApplyOutcome::Applied,
            "test setup: begin-split-in-place must apply"
        );
        let bumped = m.tablets.get(&source).map_or(Epoch::INITIAL, |t| t.epoch);
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: source,
                expected_epoch: bumped,
                cutover_wall_ms: 1_000,
            }),
            ApplyOutcome::Applied,
            "test setup: cutover must apply"
        );
    }

    /// `stream_shard_chain`/`stream_shard_watermark`/
    /// `stream_shard_rows_for_label`/`stream_labels_with_rows` over a
    /// multi-epoch, multi-tablet fixture (one tablet with a split child).
    #[test]
    fn stream_shard_accessors_over_a_multi_epoch_multi_tablet_fixture() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "orders", "L1");
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("orders".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        m.apply(&seal(&m, "orders", "L1", 1, 0, 100));
        m.apply(&seal(&m, "orders", "L1", 1, 1, 200));
        m.apply(&seal(&m, "orders", "L1", 1, 2, 300));
        // A real copy-based split — Begin+Cutover — freezes tablet 2's
        // `split_lineage` naming tablet 1's final epoch (2) at cutover.
        split_tablet(
            &mut m,
            TabletId(1),
            0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            TabletId(2),
        );
        m.apply(&seal(&m, "orders", "L1", 2, 0, 350));

        // Chain: ascending epoch order, scoped to this tablet + label.
        let chain: Vec<u64> = m
            .stream_shard_chain("orders", "L1", TabletId(1))
            .map(|(epoch, _)| epoch)
            .collect();
        assert_eq!(chain, vec![0, 1, 2]);

        // Watermark: the tablet's own last-sealed end-HLC, per tablet.
        assert_eq!(m.stream_shard_watermark(TabletId(1)), Some(300));
        assert_eq!(m.stream_shard_watermark(TabletId(2)), Some(350));
        assert_eq!(m.stream_shard_watermark(TabletId(99)), None);

        // Rows for a label: every tablet, ascending (tablet, epoch).
        let all: Vec<(u64, u64)> = m
            .stream_shard_rows_for_label("orders", "L1")
            .map(|(t, e, _)| (t.0, e))
            .collect();
        assert_eq!(all, vec![(1, 0), (1, 1), (1, 2), (2, 0)]);

        // Labels with rows: just "L1" here; disabling doesn't remove rows.
        assert_eq!(
            m.stream_labels_with_rows("orders"),
            BTreeSet::from(["L1".to_owned()])
        );
        assert!(m.stream_labels_with_rows("nonexistent").is_empty());

        // Parent shard id: epoch>0 names the same tablet's own previous
        // epoch; a split child's epoch-0 names its retired parent's FINAL
        // shard via `Metadata::split_lineage` (ADR 0050 fork F9).
        assert_eq!(
            m.stream_shard_parent_id(TabletId(1), 2),
            Some("shardId-1-1".to_owned())
        );
        assert_eq!(m.stream_shard_parent_id(TabletId(1), 0), None);
        assert_eq!(
            m.stream_shard_parent_id(TabletId(2), 0),
            Some("shardId-1-2".to_owned()),
            "the split child's epoch-0 parent is retired tablet 1's final shard"
        );
    }

    /// Issue #588: a cascading split can legitimately produce an
    /// intermediate tablet that takes zero direct writes of its own before
    /// splitting further (ADR 0043 §A3's "never seal an empty segment" —
    /// not a race, the documented case `docs/engineering-lessons.md`'s
    /// #580 entry names as a known-but-unaddressed edge case). Its own
    /// `SplitLineage::parents_final_epoch` is legitimately `None` forever,
    /// but its grandchildren must still resolve a real `ParentShardId` —
    /// the nearest ancestor's own final sealed shard, walking past however
    /// many never-sealed intermediate hops lie in between (ADR 0043's
    /// 2026-09-04 amendment).
    ///
    /// Tree built here: 1 seals once, then splits into {2, 3}. 2 splits
    /// again into {4, 5} **without ever sealing anything of its own** —
    /// `split_lineage[2].parents_final_epoch == Some(_)` (tablet 1's real
    /// final epoch), but tablet 2's OWN chain in `stream_shards` stays
    /// empty. 4 and 5 must still resolve back to tablet 1's final shard,
    /// not `None`. 3 (which DOES seal before its own further split into
    /// {6, 7}) proves the ordinary one-hop case is unaffected by the walk.
    #[test]
    fn stream_shard_parent_id_walks_past_an_ancestor_that_never_sealed() {
        let mut m = Metadata::default();
        enable_stream(&mut m, "events", "L1");
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: Some("events".to_owned()),
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        // Tablet 1's own real, final sealed shard.
        m.apply(&seal(&m, "events", "L1", 1, 0, 100));

        // 1 -> {2, 3}: ordinary first-generation split.
        split_tablet(
            &mut m,
            TabletId(1),
            0x4000_0000_0000_0000u64.to_be_bytes().to_vec(),
            TabletId(2),
        );
        assert_eq!(
            m.split_lineage[&TabletId(2)].parents_final_epoch,
            Some(0),
            "test setup: tablet 2's own lineage names tablet 1's real final epoch"
        );

        // 2 -> {4, 5}: tablet 2 splits again having sealed NOTHING of its
        // own (no `seal()` call for tablet 2 anywhere in this test) — the
        // exact issue #588 shape.
        split_tablet(
            &mut m,
            TabletId(2),
            0x2000_0000_0000_0000u64.to_be_bytes().to_vec(),
            TabletId(4),
        );
        assert_eq!(
            m.split_lineage[&TabletId(4)].parents_final_epoch,
            None,
            "test setup: tablet 4's immediate parent (2) never sealed anything"
        );
        assert!(
            m.stream_shard_chain("events", "L1", TabletId(2))
                .next()
                .is_none(),
            "test setup: tablet 2 truly has zero sealed shards of its own"
        );

        // 3 seals once before ITS OWN further split into {6, 7} — the
        // ordinary, unaffected one-hop case, proven alongside the walked
        // case so a regression that broke the common path would show up
        // here too.
        m.apply(&seal(&m, "events", "L1", 3, 0, 150));
        split_tablet(
            &mut m,
            TabletId(3),
            0x6000_0000_0000_0000u64.to_be_bytes().to_vec(),
            TabletId(6),
        );

        // The walked case: 4 and 5's epoch-0 `ParentShardId` must name
        // tablet 1's real final shard (shardId-1-0), NOT `None` — walking
        // straight past tablet 2's own empty chain.
        assert_eq!(
            m.stream_shard_parent_id(TabletId(4), 0),
            Some("shardId-1-0".to_owned()),
            "must walk past tablet 2's never-sealed lineage to tablet 1's real final shard"
        );
        assert_eq!(
            m.stream_shard_parent_id(TabletId(5), 0),
            Some("shardId-1-0".to_owned())
        );

        // The unaffected one-hop case: 6's parent link stays exactly
        // tablet 3's own final shard.
        assert_eq!(
            m.stream_shard_parent_id(TabletId(6), 0),
            Some("shardId-3-0".to_owned())
        );
    }
}
