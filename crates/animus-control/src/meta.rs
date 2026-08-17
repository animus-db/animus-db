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
use animus_placement::{Candidate, PlacementPolicy, rebalance_step, replan};
use animus_tablet::{Epoch, KeyRange, TOKEN_BYTES, Tablet, TabletId, TabletState};
use serde::{Deserialize, Serialize};

use crate::schema::{
    IndexDef, IndexStatus, SchemaCatalog, StreamSpec, StreamViewType, TableName, TableSchema,
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
    /// Replicated **keyspace** names (ADR 0013 / v1 A3): the CQL keyspace namespace,
    /// mutated only through [`MetaCommand::CreateKeyspace`] /
    /// [`MetaCommand::DropKeyspace`] so it is Raft-replicated and recovered from the
    /// WAL/snapshot — a `CREATE KEYSPACE` survives restart and is agreed
    /// cluster-wide, instead of living in per-process edge state. Names are stored
    /// as given (the CQL edge lowercases before proposing). `#[serde(default)]`
    /// keeps snapshots written before this field existed loading (empty set).
    #[serde(default)]
    pub keyspaces: BTreeSet<String>,
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
    /// Begin a **copy-based split** (ADR 0050 stage 1): the parent moves to
    /// [`TabletState::Splitting`] (still fully serving — reads AND writes —
    /// until the workflow's freeze), and two children are minted
    /// [`TabletState::Building`] over the parent's two half-ranges
    /// (`split_at(split_key)`), each with its own proposer-chosen replica
    /// set (fork F5: the placement engine picks final homes at mint) and
    /// the parent's placement policy. Children are hosted (their groups
    /// form, their engines open empty) but **unroutable** until
    /// [`MetaCommand::CutoverSplit`]. Epoch-CAS on the parent (the
    /// `CasTabletReplicas` discipline); rejected unless the parent is `Active`
    /// (no re-split of a `Splitting` parent, no split of a `Building`
    /// child); child ids obey the monotonic allocator floor; the F11
    /// token-alignment seatbelt applies to a streamed table's split key
    /// (token-alignment) seatbelt holds for streamed tables.
    BeginSplit {
        parent: TabletId,
        expected_epoch: Epoch,
        split_key: Vec<u8>,
        /// Exactly two `(child id, replica set)` pairs, left half first.
        children: [(TabletId, Vec<NodeId>); 2],
    },
    /// Complete a copy-based split (ADR 0050 stage 4): atomically flip both
    /// children [`TabletState::Active`], **remove the parent from the
    /// tablet map** (fork F6 — every hosting node's reconciler then
    /// reclaims it as an ordinary hosted-but-absent tablet), and record
    /// each child's [`SplitLineage`] row (fork F9) — written here, at the
    /// one moment the parent's shard chain is complete and immutable.
    /// Epoch-CAS on the parent; rejected unless the parent is `Splitting`
    /// and both children of this parent exist in `Building`. The caller
    /// (the B4/B5 split driver) is responsible for having seeded the
    /// children and frozen the parent first — this command is the
    /// metadata flip only.
    CutoverSplit {
        parent: TabletId,
        expected_epoch: Epoch,
        /// Proposer-stamped wall-clock ms for the lineage row (the pure
        /// state machine has no clock — `SealStreamShard::seal_wall_ms`'s
        /// discipline). Diagnostic only.
        cutover_wall_ms: u64,
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
    /// schema mutation behind CQL `ALTER TABLE … ADD` (which appends columns to
    /// the current schema and replaces it wholesale). One command, one apply: no
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
    /// `Creating`/`Active`/`Deleting`. Mirrors `SetTableMode`'s minimal shape.
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
    /// Set a table's **replication mode** (ADR 0016 / ADR 0017): `Ap` (leaderless
    /// data plane) or `Cp` (leaderful per-tablet Raft). Rejected if the table has
    /// no schema; a no-op if the mode is already set. Replicated like the rest of
    /// the catalog, so the choice is durable + cluster-agreed and the wire edges
    /// route reads/writes accordingly.
    SetTableMode {
        table: TableName,
        mode: crate::ReplicationMode,
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
    /// Register a **keyspace** name (ADR 0013 / v1 A3). Idempotent: a no-op if the
    /// keyspace already exists. Replicated so a CQL `CREATE KEYSPACE` is durable +
    /// cluster-agreed.
    CreateKeyspace { keyspace: String },
    /// Remove a keyspace name. Idempotent: a no-op if absent. (Tables in the
    /// keyspace are dropped separately; this removes only the namespace entry.)
    DropKeyspace { keyspace: String },
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
}

impl PlacementView {
    /// The pure placement decision over this view — identical to
    /// [`Metadata::reconcile`] (both delegate to the same body).
    #[must_use]
    pub fn reconcile(&self) -> Vec<MetaCommand> {
        reconcile_placement(&self.members, &self.tablets, &self.policies)
    }

    /// The pure load-rebalancing decision over this view — identical to
    /// [`Metadata::rebalance`] (both delegate to the same body).
    #[must_use]
    pub fn rebalance(&self) -> Option<MetaCommand> {
        rebalance_placement(&self.members, &self.tablets, &self.policies)
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
fn rebalance_placement(
    members: &BTreeMap<NodeId, Member>,
    tablets: &BTreeMap<TabletId, Tablet>,
    policies: &BTreeMap<TabletId, PlacementPolicy>,
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

impl Metadata {
    /// The narrow placement view ([`PlacementView`]) — clones only the
    /// placement-relevant maps, never the schema catalog / CP address book.
    #[must_use]
    pub fn placement_view(&self) -> PlacementView {
        PlacementView {
            members: self.members.clone(),
            tablets: self.tablets.clone(),
            policies: self.policies.clone(),
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
    /// set.
    #[must_use]
    pub fn reconcile(&self) -> Vec<MetaCommand> {
        reconcile_placement(&self.members, &self.tablets, &self.policies)
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
        rebalance_placement(&self.members, &self.tablets, &self.policies)
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
            MetaCommand::BeginSplit {
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
                    // Same monotonic-allocator floor as `SplitTablet` (and for
                    // the same dropped-data-resurrection reason).
                    if id.0 < self.next_free_tablet_id().0 {
                        return ApplyOutcome::Rejected(
                            "child tablet id below the monotonic allocator",
                        );
                    }
                }
                let Some(source) = self.tablets.get(parent) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                // Epoch-CAS (the `SplitTablet` discipline) plus the ADR 0050
                // state gate: only an `Active` tablet may begin a split — a
                // `Splitting` parent is already mid-workflow (one split at a
                // time) and a `Building` child has no committed contents to
                // split.
                if source.epoch != *expected_epoch {
                    return ApplyOutcome::Rejected("epoch mismatch");
                }
                if source.state != TabletState::Active {
                    return ApplyOutcome::Rejected("tablet is not Active");
                }
                // F11 seatbelt, identical to `SplitTablet`'s arm above.
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
                let Some((left, right)) = source.range.split_at(split_key) else {
                    return ApplyOutcome::Rejected("split key not strictly inside range");
                };
                let table = source.table.clone();
                let policy = self.policies.get(parent).cloned();
                let mut mint = |id: &TabletId, range: KeyRange, replicas: &[NodeId]| {
                    let mut child =
                        Tablet::with_table(*id, table.clone(), range, replicas.to_vec());
                    child.state = TabletState::Building;
                    self.tablets.insert(*id, child);
                    self.next_tablet_id = self.next_tablet_id.max(id.0 + 1);
                    if let Some(policy) = policy.clone() {
                        self.policies.insert(*id, policy);
                    }
                };
                mint(left_id, left, left_replicas);
                mint(right_id, right, right_replicas);
                let source = self.tablets.get_mut(parent).expect("tablet present");
                source.state = TabletState::Splitting;
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
                // The parent's two `Building` children are exactly the
                // `Building` tablets whose ranges partition the parent's own —
                // recomputed here from the map rather than carried in the
                // command, so a replayed/duplicate cutover can't name stale
                // ids. `BeginSplit` is the only minter of `Building` tablets
                // and gates on `Active`, so a parent has either exactly two
                // (mid-workflow) or zero (already cut over → the state gate
                // above already rejected).
                let parent_range = source.range.clone();
                let children: Vec<TabletId> = self
                    .tablets
                    .values()
                    .filter(|t| {
                        t.state == TabletState::Building
                            && parent_range.contains_range(&t.range)
                            && t.table == source.table
                    })
                    .map(|t| t.id)
                    .collect();
                let [left, right] = children.as_slice() else {
                    return ApplyOutcome::Rejected(
                        "parent does not have exactly two Building children",
                    );
                };
                let (left, right) = (*left, *right);
                // The parent's final stream epoch: its chain's highest sealed
                // epoch, immutable after this apply (the parent is removed
                // below; `SealStreamShard` requires an existing tablet).
                let parents_final_epoch = self
                    .stream_shards
                    .range((*parent, 0)..=(*parent, u64::MAX))
                    .next_back()
                    .map(|(&(_, epoch), _)| epoch);
                for child in [left, right] {
                    let t = self.tablets.get_mut(&child).expect("child present");
                    t.state = TabletState::Active;
                    t.epoch = t.epoch.next();
                    self.split_lineage.insert(
                        child,
                        SplitLineage {
                            parent: *parent,
                            parents_final_epoch,
                            cutover_wall_ms: *cutover_wall_ms,
                        },
                    );
                }
                self.tablets.remove(parent);
                self.policies.remove(parent);
                ApplyOutcome::Applied
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
            MetaCommand::SetTableMode { table, mode } => {
                let Some(schema) = self.schemas.get_mut(table) else {
                    return ApplyOutcome::Rejected("no such table schema");
                };
                if schema.mode == *mode {
                    return ApplyOutcome::NoOp;
                }
                schema.mode = *mode;
                ApplyOutcome::Applied
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
            MetaCommand::CreateKeyspace { keyspace } => {
                if crate::syskv::is_reserved_name(keyspace) {
                    return ApplyOutcome::Rejected(
                        "keyspace name collides with the reserved system namespace",
                    );
                }
                if self.keyspaces.insert(keyspace.clone()) {
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
            }
            MetaCommand::DropKeyspace { keyspace } => {
                if self.keyspaces.remove(keyspace) {
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

    /// Whether `keyspace` is registered (ADR 0013 / v1 A3). Read by the CQL edge
    /// for `USE` / qualifier validation, in place of per-process edge state.
    #[must_use]
    pub fn has_keyspace(&self, keyspace: &str) -> bool {
        self.keyspaces.contains(keyspace)
    }

    /// The table's replication mode (ADR 0016 / ADR 0017). Defaults to `Cp` —
    /// including for an unknown table — since the leaderful per-tablet Raft plane
    /// is the only v1 data plane (ADR 0019; the AP plane is deferred and its
    /// crate deleted). Read by the wire edges to route a table's reads/writes.
    #[must_use]
    pub fn table_mode(&self, table: &str) -> crate::ReplicationMode {
        self.schemas
            .get(table)
            .map_or(crate::ReplicationMode::default(), |s| s.mode)
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
    /// names the parent tablet's own FINAL sealed shard as frozen in
    /// [`Metadata::split_lineage`] at cutover (ADR 0050, fork F9). `None`
    /// for a genuine root (an epoch-0 shard whose tablet was never a split
    /// child, or whose parent never sealed).
    #[must_use]
    pub fn stream_shard_parent_id(&self, tablet: TabletId, epoch: u64) -> Option<String> {
        if epoch > 0 {
            return Some(shard_id_string(tablet, epoch - 1));
        }
        // ADR 0050 rung 5: a copy-based split child's epoch-0 shard names
        // its parent's FINAL sealed shard via `split_lineage` — written at
        // `CutoverSplit`'s own apply, the one moment the parent's chain is
        // complete and immutable, so this needs no freezing defense layers
        // at all (fork F9).
        let lineage = self.split_lineage.get(&tablet)?;
        let parent_epoch = lineage.parents_final_epoch?;
        Some(shard_id_string(lineage.parent, parent_epoch))
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

    /// `CreateKeyspace` rejects the same reserved-namespace collision as
    /// `CreateTableSchema` (ADR 0038).
    #[test]
    fn create_keyspace_rejects_reserved_namespace() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateKeyspace {
                keyspace: crate::syskv::RESERVED_NAMESPACE.to_owned(),
            }),
            ApplyOutcome::Rejected("keyspace name collides with the reserved system namespace")
        );
        assert!(!m.has_keyspace(crate::syskv::RESERVED_NAMESPACE));

        // An ordinary keyspace name is unaffected.
        assert_eq!(
            m.apply(&MetaCommand::CreateKeyspace {
                keyspace: "orders_ks".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert!(m.has_keyspace("orders_ks"));
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

    /// ADR 0018 PR2 (range-seal design, replacing the retired `version_floor`
    /// Shared harness for the ADR 0050 copy-based-split tests below: one
    /// `Active` parent tablet (id 1, whole range, RF 3) with a recorded
    /// policy, plus the `BeginSplit` command splitting it at the ring
    /// midpoint into children 2 and 3 at hand-picked (distinct) homes.
    fn begin_split_fixture() -> (Metadata, MetaCommand) {
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
        let cmd = MetaCommand::BeginSplit {
            parent: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            children: [
                (TabletId(2), vec![nid(4), nid(5), nid(6)]),
                (TabletId(3), vec![nid(7), nid(8), nid(9)]),
            ],
        };
        (m, cmd)
    }

    /// ADR 0050 stage 1: `BeginSplit` marks the parent `Splitting` (range
    /// untouched — it serves until cutover), mints two `Building` children
    /// over the half-ranges at the command's own replica homes (fork F5),
    /// inherits the policy, advances the allocator, and writes NO
    /// zero-copy-split provenance (`split_parents`/`stream_split_basis` are
    /// the old mechanism's maps; copy-based lineage is CutoverSplit's
    /// `split_lineage`, written only at cutover).
    #[test]
    fn begin_split_mints_two_building_children_and_marks_the_parent_splitting() {
        let (mut m, cmd) = begin_split_fixture();
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);

        let parent = &m.tablets[&TabletId(1)];
        assert_eq!(parent.state, TabletState::Splitting);
        assert_eq!(parent.range, KeyRange::whole(), "parent range untouched");
        assert_eq!(parent.epoch, Epoch::INITIAL.next());

        let left = &m.tablets[&TabletId(2)];
        let right = &m.tablets[&TabletId(3)];
        assert_eq!(left.state, TabletState::Building);
        assert_eq!(right.state, TabletState::Building);
        assert_eq!(left.table.as_deref(), Some("users"));
        assert_eq!(right.table.as_deref(), Some("users"));
        assert_eq!(left.replicas, vec![nid(4), nid(5), nid(6)]);
        assert_eq!(right.replicas, vec![nid(7), nid(8), nid(9)]);
        let mid = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        assert_eq!(left.range.end.as_deref(), Some(mid.as_slice()));
        assert_eq!(right.range.start, mid);
        assert!(m.policies.contains_key(&TabletId(2)));
        assert!(m.policies.contains_key(&TabletId(3)));
        assert!(m.next_free_tablet_id().0 >= 4);

        // No premature lineage (the zero-copy provenance/basis maps are
        // gone entirely — nothing to assert absent, Train B rung 7).
        assert!(m.split_lineage.is_empty());
    }

    /// ADR 0050 state/CAS gates on `BeginSplit`: epoch mismatch, a
    /// non-`Active` parent (no re-split of a `Splitting` parent, no split of
    /// a `Building` child), duplicate/colliding child ids, and ids below the
    /// allocator floor are all rejected.
    #[test]
    fn begin_split_rejects_bad_epoch_state_and_child_ids() {
        let (mut m, cmd) = begin_split_fixture();

        // Epoch mismatch.
        let MetaCommand::BeginSplit {
            parent,
            split_key,
            children,
            ..
        } = cmd.clone()
        else {
            unreachable!()
        };
        assert_eq!(
            m.apply(&MetaCommand::BeginSplit {
                parent,
                expected_epoch: Epoch::INITIAL.next(),
                split_key: split_key.clone(),
                children: children.clone(),
            }),
            ApplyOutcome::Rejected("epoch mismatch")
        );

        // Identical child ids.
        assert_eq!(
            m.apply(&MetaCommand::BeginSplit {
                parent,
                expected_epoch: Epoch::INITIAL,
                split_key: split_key.clone(),
                children: [(TabletId(2), vec![nid(4)]), (TabletId(2), vec![nid(5)])],
            }),
            ApplyOutcome::Rejected("child ids must be distinct")
        );

        // Child id below the monotonic allocator floor (id 1 is spoken for).
        assert_eq!(
            m.apply(&MetaCommand::BeginSplit {
                parent,
                expected_epoch: Epoch::INITIAL,
                split_key: split_key.clone(),
                children: [(TabletId(1), vec![nid(4)]), (TabletId(9), vec![nid(5)])],
            }),
            ApplyOutcome::Rejected("child tablet id already exists")
        );

        // A real begin succeeds…
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        // …after which the parent is `Splitting`: a re-split is rejected on
        // state (with the freshly bumped epoch, so the CAS passes).
        assert_eq!(
            m.apply(&MetaCommand::BeginSplit {
                parent,
                expected_epoch: Epoch::INITIAL.next(),
                split_key: split_key.clone(),
                children: [(TabletId(10), vec![nid(4)]), (TabletId(11), vec![nid(5)])],
            }),
            ApplyOutcome::Rejected("tablet is not Active")
        );
        // …and a `Building` child is not splittable either.
        assert_eq!(
            m.apply(&MetaCommand::BeginSplit {
                parent: TabletId(3),
                expected_epoch: Epoch::INITIAL,
                split_key: 0xC000_0000_0000_0000u64.to_be_bytes().to_vec(),
                children: [(TabletId(10), vec![nid(4)]), (TabletId(11), vec![nid(5)])],
            }),
            ApplyOutcome::Rejected("tablet is not Active")
        );
    }

    /// ADR 0050 stage 4: `CutoverSplit` atomically activates both children
    /// (epoch-bumped), removes the parent (tablet + policy), and freezes a
    /// `split_lineage` row per child naming the parent and its final stream
    /// epoch — `None` here (never streamed/sealed). Also: rejected unless
    /// the parent is `Splitting`, and rejected after it has already run
    /// (the parent is gone).
    #[test]
    fn cutover_split_activates_children_removes_parent_and_freezes_lineage() {
        let (mut m, cmd) = begin_split_fixture();

        // Cutover before any begin: the parent is `Active`, not `Splitting`.
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                cutover_wall_ms: 42,
            }),
            ApplyOutcome::Rejected("tablet is not Splitting")
        );

        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        let parent_epoch = m.tablets[&TabletId(1)].epoch;
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: parent_epoch,
                cutover_wall_ms: 42,
            }),
            ApplyOutcome::Applied
        );

        assert!(!m.tablets.contains_key(&TabletId(1)), "parent removed");
        assert!(
            !m.policies.contains_key(&TabletId(1)),
            "parent policy removed"
        );
        for child in [TabletId(2), TabletId(3)] {
            let t = &m.tablets[&child];
            assert_eq!(t.state, TabletState::Active);
            assert_eq!(t.epoch, Epoch::INITIAL.next());
            let lineage = &m.split_lineage[&child];
            assert_eq!(lineage.parent, TabletId(1));
            assert_eq!(lineage.parents_final_epoch, None);
            assert_eq!(lineage.cutover_wall_ms, 42);
        }

        // A duplicate cutover finds no parent left.
        assert_eq!(
            m.apply(&MetaCommand::CutoverSplit {
                parent: TabletId(1),
                expected_epoch: parent_epoch,
                cutover_wall_ms: 43,
            }),
            ApplyOutcome::Rejected("no such tablet")
        );
    }

    /// A streamed-and-sealed parent's final epoch rides the lineage row: the
    /// chain's highest sealed epoch at cutover (here 7, via two catalog
    /// rows), immutable forever after since the parent is gone.
    #[test]
    fn cutover_split_records_the_parents_final_stream_epoch() {
        let (mut m, cmd) = begin_split_fixture();
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
        assert_eq!(
            m.split_lineage[&TabletId(2)].parents_final_epoch,
            Some(7),
            "the chain's highest sealed epoch, not its first"
        );
    }

    /// ADR 0050 (fork F5 rider): placement is frozen mid-split — neither the
    /// repair reconciler nor the rebalancer proposes a move for a
    /// `Splitting` parent or a `Building` child, even when the replica set
    /// violates policy (here: RF 3 policy, 1-replica sets, which repair
    /// would otherwise fix immediately); an ordinary `Active` tablet in the
    /// same view still gets repaired.
    #[test]
    fn placement_is_frozen_for_splitting_parents_and_building_children() {
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
        // A second table, mid-split: equally under-replicated parent+children.
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
            m.apply(&MetaCommand::BeginSplit {
                parent: TabletId(2),
                expected_epoch: Epoch::INITIAL,
                split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
                children: [(TabletId(3), vec![nid(3)]), (TabletId(4), vec![nid(4)])],
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
            "repair touches only the Active tablet; parent (Splitting) and \
             children (Building) are frozen"
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
        // Split `users` (copy-based, Begin+Cutover) so the table owns two
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

    /// F11 (ADR 0042 §14, Fork D) apply-time seatbelt: `SplitTablet`
    /// against a **streamed** table's tablet rejects a split key that
    /// isn't exactly `TOKEN_BYTES` long, the structural check against a
    /// future caller that bypasses `animusd::ClientCtx::trigger_split`'s
    /// own rounding (the primary enforcement, tested at that layer). A
    /// properly token-aligned (8-byte) key still applies normally.
    #[test]
    fn split_rejects_a_non_token_aligned_key_on_a_streamed_table() {
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
            m.apply(&MetaCommand::BeginSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: b"mmmmm".to_vec(),
                children: [(TabletId(2), homes.clone()), (TabletId(3), homes.clone())],
            }),
            ApplyOutcome::Rejected("split key not token-aligned for a streamed table")
        );
        assert_eq!(m.tablets.len(), 1, "the rejected split changed nothing");

        // The same tablet, same epoch, a properly token-aligned key: applies.
        assert_eq!(
            m.apply(&MetaCommand::BeginSplit {
                parent: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
                children: [(TabletId(2), homes.clone()), (TabletId(3), homes)],
            }),
            ApplyOutcome::Applied
        );
        assert!(m.tablets.contains_key(&TabletId(2)));

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
            m.apply(&MetaCommand::BeginSplit {
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

    /// F11 Fork E (ADR 0042 §14): the accepted single-token hot-partition
    /// limit at the apply layer — a token-aligned split key that happens
    /// to equal the *target* tablet's own `range.start` (a single very hot
    /// partition token owning the tablet's entire range) is rejected by
    /// the pre-existing `KeyRange::split_at` "strictly inside" guard, not
    /// silently accepted into a zero-width sibling. `ClientCtx::
    /// trigger_split` (`animusd`) is the layer that turns this into a
    /// metered skip before ever proposing; this proves the fence holds
    /// even if a future caller reaches apply directly.
    #[test]
    fn split_rejects_a_token_aligned_key_equal_to_range_start() {
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
            m.apply(&MetaCommand::BeginSplit {
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

    /// A real `SplitTablet` apply of `source` into `new_id` (a `CreateTablet`
    /// for `source` must already have applied) — used by the stream-basis
    /// tests below instead of hand-poking `split_parents`, so
    /// `Metadata::stream_split_basis` gets frozen exactly the way production
    /// freezes it.
    /// Run a full copy-based split of `source` at `split_key` (ADR 0050):
    /// `BeginSplit` minting children `new_id`/`new_id + 1` on the parent's
    /// own replicas, then `CutoverSplit` — children `Active`, parent
    /// removed, `split_lineage` frozen. The metadata-only equivalent of
    /// the workflow (sound as a fixture: `Metadata::apply` carries no
    /// build state; on an empty/unhosted fixture there is nothing to copy).
    fn split_tablet(m: &mut Metadata, source: TabletId, split_key: Vec<u8>, new_id: TabletId) {
        let expected_epoch = m.tablets.get(&source).map_or(Epoch::INITIAL, |t| t.epoch);
        let replicas = m
            .tablets
            .get(&source)
            .map(|t| t.replicas.clone())
            .unwrap_or_default();
        assert_eq!(
            m.apply(&MetaCommand::BeginSplit {
                parent: source,
                expected_epoch,
                split_key,
                children: [
                    (new_id, replicas.clone()),
                    (TabletId(new_id.0 + 1), replicas)
                ],
            }),
            ApplyOutcome::Applied,
            "test setup: begin-split must apply"
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
}
