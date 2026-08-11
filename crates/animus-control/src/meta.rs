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
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};
use serde::{Deserialize, Serialize};

use crate::schema::{IndexDef, SchemaCatalog, TableName, TableSchema};

/// The floor of the **cluster-allocated member id** counter (ADR 0036).
///
/// **ADR 0040 PR3 shim, removed in ADR 0040 PR4.** PR4 retires this whole
/// allocator (`AllocateNodeId`, this counter, `node_id_allocations`) in favor
/// of self-minted random ids + a registration CAS (ADR 0040 Decision C) — but
/// deleting it a PR early would be out of this PR's scope (string
/// representation + explicit config ids), so it stays wired exactly as
/// before, just minting a *string* id instead of a raw `u64`:
/// [`alloc_node_id`] formats the counter as `"alloc-{n}"` — the `"alloc-"`
/// prefix (disjoint from `nid`'s `"n{n}"` test ids and from any operator- or
/// config-proposed id, since [`NodeId::propose`](animus_env::NodeId::propose)
/// accepts `-` but a config author has no reason to start an id with this
/// exact reserved word) is what now keeps a minted id from colliding with an
/// operator-chosen one — no more numeric floor comparison, since ids are no
/// longer ordered by magnitude. This crate has no dependency on `animusd`, so
/// the disjointness is still only documented in prose, same as before.
pub const ALLOC_ID_BASE: u64 = 1_000_000;

/// The reserved prefix every allocator-minted id carries (see
/// [`ALLOC_ID_BASE`]'s doc — this whole mechanism is a PR3 shim, removed in
/// ADR 0040 PR4).
const ALLOC_ID_PREFIX: &str = "alloc-";

/// Mint the allocator's `n`th id as `"alloc-{n}"` (PR3 shim, removed in PR4).
pub fn alloc_node_id(n: u64) -> NodeId {
    NodeId::new_unchecked(format!("{ALLOC_ID_PREFIX}{n}"))
}

/// Parse an allocator-minted id back to its counter value, if `id` carries
/// the `"alloc-"` prefix (PR3 shim, removed in PR4). `pub` so a caller
/// (`animusd`'s `admin_add_control_member`) can tell an operator-supplied id
/// apart from the allocator's own reserved range without a numeric floor
/// comparison (ids are no longer ordered by magnitude).
pub fn parse_alloc_id(id: &NodeId) -> Option<u64> {
    id.as_str().strip_prefix(ALLOC_ID_PREFIX)?.parse().ok()
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
    pub internal: String,
    /// The plain client-protocol listen address.
    pub client: String,
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
    /// address set (raftkv/client/admin), keyed by its raftkv id. Mutated only
    /// through [`MetaCommand::RegisterNodeAddrs`]. Unlike [`Metadata::cp_member_addrs`]
    /// (internal raftkv addresses only, including transient split-sibling/CP-group
    /// member ids that are never full cluster members), this is populated once per
    /// **node** at startup, closing the ADR 0030 gap where a pre-growth node's
    /// `client_route`/admin peer list was a static, process-start-only snapshot
    /// that could never learn about a node grown in afterward. `#[serde(default)]`
    /// keeps pre-ADR-0032 snapshots loading (empty map).
    #[serde(default)]
    pub node_addrs: BTreeMap<NodeId, NodeAddrs>,
    /// Tablet ids that have been **merged away** (ADR 0033): a
    /// [`MetaCommand::MergeTablets`] apply inserts `right` here (never
    /// pruned — tablet ids are never reused, by the same monotonic-allocator
    /// invariant [`Metadata::next_tablet_id`] already enforces, so an entry
    /// can never resurrect a wrong decision for a later, unrelated tablet
    /// reusing the id). This is what lets a per-node tablet-host reconciler
    /// tell "this hosted tablet vanished from `tablets` because it was
    /// **merged into a sibling** — tear its group down but never touch its
    /// data, a survivor now owns that range on the same shared engine" apart
    /// from "vanished because its **whole table was dropped**
    /// ([`MetaCommand::DropTableTablets`]) — tear down **and** erase."
    /// Inferring this purely from the tablet map (e.g. "does some other
    /// tablet's range now cover mine") is unsound: two different tables'
    /// still-unsplit tablets can have byte-identical default ranges
    /// ([`animus_tablet::KeyRange::whole`]), so a range-containment check
    /// with no table identity to disambiguate would misattribute an
    /// unrelated table's tablet as "the merge survivor" and silently skip a
    /// real drop's erase. A tiny, permanently-retained marker per merge ever
    /// performed (bounded by the total number of splits ever performed,
    /// since a tablet cannot be merged unless it was first split off from
    /// something) is far cheaper than getting that inference wrong. See ADR
    /// 0033. `#[serde(default)]` keeps pre-ADR-0033 snapshots loading (empty
    /// set).
    #[serde(default)]
    pub merged_tablets: BTreeSet<TabletId>,
    /// The next **cluster-allocated member id** to hand out (ADR 0036) — a
    /// monotonic allocator over the [`ALLOC_ID_BASE`]-disjoint range,
    /// mirroring [`Metadata::next_tablet_id`]'s discipline exactly: bumped
    /// past every id [`MetaCommand::AllocateNodeId`] mints, so two
    /// concurrent allocations can't derive the same id, and a never-reused
    /// id can't alias a stale, still-`Down`, address-less member entry left
    /// behind by an abandoned join attempt (see that command's doc).
    /// `#[serde(default = "default_next_alloc_id")]` keeps a pre-ADR-0036
    /// snapshot loading at the base rather than `0`, which would otherwise
    /// collide with (and be rejected below) every operator-chosen id —
    /// [`Metadata::next_free_alloc_id`] also folds in the highest existing
    /// allocation, so this is a floor, not the sole source of truth.
    #[serde(default = "default_next_alloc_id")]
    pub next_alloc_id: u64,
    /// The **idempotency ledger** for [`MetaCommand::AllocateNodeId`] (ADR
    /// 0036): every nonce a join attempt has ever proposed, mapped to the id
    /// it was (or would have been, on a retry) allocated. Bounded by "one
    /// entry per join attempt ever made" — the same accepted, unbounded-but-
    /// slow growth already accepted for [`Metadata::merged_tablets`].
    /// `#[serde(default)]` keeps a pre-ADR-0036 snapshot loading (empty map).
    #[serde(default)]
    pub node_id_allocations: BTreeMap<String, NodeId>,
}

/// [`Metadata::next_alloc_id`]'s missing-field default: [`ALLOC_ID_BASE`],
/// not `0` — a decoded pre-ADR-0036 snapshot has no allocations yet, so
/// starting the counter at the base (rather than `0`, which would immediately
/// collide with real, small operator-chosen ids) is both correct and the
/// obviously-intended value, mirroring `NodeAddrs::role`'s
/// historically-accurate-default reasoning.
fn default_next_alloc_id() -> u64 {
    ALLOC_ID_BASE
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
    /// Split `tablet` at `split_key` into `[start, split_key)` (the original,
    /// with a bumped epoch) and `[split_key, end)` (a new tablet `new_id`,
    /// inheriting the replica set at [`Epoch::INITIAL`]). The split key must lie
    /// strictly inside the tablet's range. **Compare-and-swap on `expected_epoch`**
    /// (mirroring `CasTabletReplicas`): rejected if `tablet`'s epoch has moved
    /// since the caller read it, so two proposers racing to split the same
    /// tablet at the same epoch with different keys can't both commit — only the
    /// first lands, the second is cleanly rejected instead of minting a second
    /// child tablet id that the per-tablet CP-data Raft group (which applies at
    /// most one real `Split`, ever) can never actually host.
    SplitTablet {
        tablet: TabletId,
        expected_epoch: Epoch,
        split_key: Vec<u8>,
        new_id: TabletId,
    },
    /// Merge adjacent tablets `left` and `right` (where `left.end == right.start`,
    /// they share a replica set, and they are scoped to the same table) into
    /// `left`, extended to cover both ranges with a bumped epoch; `right` is
    /// removed and recorded in [`Metadata::merged_tablets`] (ADR 0033) so a
    /// per-node reconciler can tell this apart from a table drop. **Compare-and-swap
    /// on both `expected_left_epoch` and `expected_right_epoch`** (mirroring
    /// `SplitTablet`/`CasTabletReplicas`): rejected if either tablet's epoch has
    /// moved since the caller read it, so a merge proposal computed from a stale
    /// view (e.g. racing a concurrent rebalance/repair CAS or another split/merge
    /// touching either tablet) is cleanly rejected instead of applying against
    /// state the proposer never actually observed.
    MergeTablets {
        left: TabletId,
        expected_left_epoch: Epoch,
        right: TabletId,
        expected_right_epoch: Epoch,
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
    /// Set a table's **replication mode** (ADR 0016 / ADR 0017): `Ap` (leaderless
    /// data plane) or `Cp` (leaderful per-tablet Raft). Rejected if the table has
    /// no schema; a no-op if the mode is already set. Replicated like the rest of
    /// the catalog, so the choice is durable + cluster-agreed and the wire edges
    /// route reads/writes accordingly.
    SetTableMode {
        table: TableName,
        mode: crate::ReplicationMode,
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
    /// Register (or update) a **node's full address book** (ADR 0032 PR1): the
    /// client/admin/raftkv listen addresses of member `id`, stored in
    /// [`Metadata::node_addrs`]. Idempotent: a no-op if `id` already maps to an
    /// identical [`NodeAddrs`]. Superset of [`MetaCommand::RegisterCpAddr`] for
    /// the client/admin axes — every node proposes this once at startup so any
    /// other node (including one that joined earlier and never restarted) can
    /// resolve it as a forward/relay target.
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
    /// **Cluster-allocated member id** (ADR 0036): atomically mint a fresh,
    /// never-reused member id from the [`ALLOC_ID_BASE`]-disjoint monotonic
    /// allocator and register it [`Down`](NodeStatus::Down) with `labels` and
    /// no address yet — the address arrives later via the joiner's own,
    /// unchanged [`MetaCommand::RegisterNodeAddrs`] self-registration. This
    /// is the *hard* alternative to an operator picking a `--node I` index by
    /// hand: uniqueness comes from the same monotonic-floor-plus-presence-
    /// check discipline [`MetaCommand::SplitTablet`]'s allocator guard
    /// already uses for tablet ids, evaluated identically on every replica,
    /// so no epoch-CAS or pre-bind collision check is needed — two proposers
    /// racing through this command can never both mint the same id.
    ///
    /// `nonce` is a **joiner-generated idempotency key**: replaying the same
    /// nonce (a proposer retry after an `Accepted`-but-unconfirmed propose —
    /// the durable-before-visible discipline every proposer here must respect,
    /// root `CLAUDE.md`) applies as a no-op that returns the identical,
    /// already-allocated id, recorded once in
    /// [`Metadata::node_id_allocations`] — so a retried join attempt can never
    /// mint a second id for itself, and a genuinely distinct join attempt
    /// (a different nonce) always gets a fresh one.
    ///
    /// An abandoned join attempt (the process crashes before ever
    /// self-registering an address) leaves its allocated id `Down` and
    /// address-less **forever** — this is accepted, not a leak to fix: ids
    /// are never reused (mirroring tablet ids), and the entry is prunable
    /// through the existing [`MetaCommand::RemoveMember`] path exactly like
    /// any other drained, unreferenced member, once an operator notices it.
    AllocateNodeId {
        nonce: String,
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
                self.members.insert(
                    node.clone(),
                    Member {
                        labels: labels.clone(),
                        status: *status,
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
            MetaCommand::SplitTablet {
                tablet,
                expected_epoch,
                split_key,
                new_id,
            } => {
                if self.tablets.contains_key(new_id) {
                    return ApplyOutcome::Rejected("new tablet id already exists");
                }
                // Enforce the monotonic allocator (ADR 0023) at apply time, not just
                // at the proposer: a `new_id` below [`Metadata::next_free_tablet_id`]
                // could *reuse* an id freed by `DropTableTablets` — and a replica
                // still holding the dropped tablet's `db-t{id}-*` files (GC
                // incomplete, or down during the drop) would re-host them AS the new
                // tablet, resurrecting dropped data the absence-keyed GC can then
                // never reclaim. The present-tablet check above cannot catch a
                // *freed* id, so reject anything below the allocator floor.
                if new_id.0 < self.next_free_tablet_id().0 {
                    return ApplyOutcome::Rejected("new tablet id below the monotonic allocator");
                }
                let Some(source) = self.tablets.get(tablet) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                // CAS on the source's epoch (mirroring `CasTabletReplicas`): a second
                // proposer racing to split the same tablet at the same epoch — with a
                // different `split_key`, computed from an equally-stale view of the
                // pre-split range — must not also commit. The tablet's own per-group
                // CP-data Raft can only ever apply one real `Split` (an at-most-once
                // apply-time guard there), so a second metadata-level split of the
                // same epoch would mint a `new_id` that can never get a CP group: a
                // permanent, leaderless, unreachable orphan tablet. Checking epoch
                // equality here — before recomputing the range split below — rejects
                // the loser's proposal outright instead of silently accepting it.
                if source.epoch != *expected_epoch {
                    return ApplyOutcome::Rejected("epoch mismatch");
                }
                let Some((left, right)) = source.range.split_at(split_key) else {
                    return ApplyOutcome::Rejected("split key not strictly inside range");
                };
                // The split child inherits the parent's table scope (ADR 0023): a
                // split never crosses a table boundary, so both halves stay scoped
                // to the same table.
                let mut new_tablet = Tablet::with_table(
                    *new_id,
                    source.table.clone(),
                    right,
                    source.replicas.clone(),
                );
                // Cross-group LWW version-floor fix: the new sibling is a brand-new
                // data-plane Raft group whose own log index restarts low, but it
                // immediately serves keys the *source* group already wrote at
                // whatever (possibly much higher) index it had reached — on the
                // same node-shared `StorageEngine` (ADR 0026/0028). Without a floor
                // strictly past the source's, a subsequent write through the
                // sibling could carry a version no higher than what's already
                // stored, and per-key LWW would silently drop it (a write-confirm
                // timeout, not corruption — but the write never lands). Bumping
                // past `source.version_floor` (not `new_id`, which is a *different*
                // monotonic sequence — see `Tablet::version_floor`'s doc) is both
                // necessary and sufficient: it exceeds every version the source
                // could ever have stamped, as long as the source's own local index
                // never reaches `VERSION_FLOOR_SCALE` (`animus-cp-data`) between
                // rescopes — astronomically generous given auto-split already caps
                // a tablet's key/byte count long before that. The source's own
                // floor is untouched (it never absorbs foreign data, only narrows).
                new_tablet.version_floor = source.version_floor + 1;
                let source = self.tablets.get_mut(tablet).expect("tablet present");
                source.range = left;
                source.epoch = source.epoch.next();
                self.tablets.insert(*new_id, new_tablet);
                self.next_tablet_id = self.next_tablet_id.max(new_id.0 + 1);
                // The split child inherits the source's placement policy (ADR 0029):
                // without it the new sibling has no policy and is invisible to both
                // the repair reconciler and the load rebalancer, so it would never
                // be re-placed or balanced onto new members.
                if let Some(policy) = self.policies.get(tablet).cloned() {
                    self.policies.insert(*new_id, policy);
                }
                ApplyOutcome::Applied
            }
            MetaCommand::MergeTablets {
                left,
                expected_left_epoch,
                right,
                expected_right_epoch,
            } => {
                let (Some(l), Some(r)) = (self.tablets.get(left), self.tablets.get(right)) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                // CAS on both epochs (mirroring `SplitTablet`/`CasTabletReplicas`,
                // ADR 0033): a merge proposal is computed from one metadata
                // snapshot of both tablets, so either one drifting since — a
                // racing rebalance/repair CAS, or another split/merge touching
                // either side — must reject cleanly rather than apply against
                // stale assumptions the proposer never actually observed.
                if l.epoch != *expected_left_epoch || r.epoch != *expected_right_epoch {
                    return ApplyOutcome::Rejected("epoch mismatch");
                }
                if !l.range.abuts(&r.range) {
                    return ApplyOutcome::Rejected("tablets are not adjacent");
                }
                if l.replicas != r.replicas {
                    return ApplyOutcome::Rejected("tablets have different replica sets");
                }
                // A merge never crosses a table boundary: both halves' physical
                // keys live under the same table's `StorageScope` prefix on the
                // node-shared engine (ADR 0026/0028), which only makes sense if
                // they were always the same table to begin with.
                if l.table != r.table {
                    return ApplyOutcome::Rejected("tablets belong to different tables");
                }
                let new_end = r.range.end.clone();
                // Cross-group LWW version-floor fix (the merge dual of
                // `SplitTablet`'s, see `Tablet::version_floor`'s doc): `left`'s
                // group keeps running unchanged, but is about to start serving
                // keys `right`'s group already wrote under its own, unrelated
                // index sequence on the same node-shared engine. Bump `left`'s
                // floor past *both* current floors so every future write through
                // `left` outranks anything either side ever stamped — read
                // `r.version_floor` before `r` is dropped below.
                let right_floor = r.version_floor;
                let l = self.tablets.get_mut(left).expect("tablet present");
                l.range.end = new_end;
                l.epoch = l.epoch.next();
                l.version_floor = l.version_floor.max(right_floor).saturating_add(1);
                self.tablets.remove(right);
                // The merged-away tablet can no longer be reconciled.
                self.policies.remove(right);
                // Recorded so a per-node reconciler can tell "merged into a
                // sibling" apart from "table dropped" (ADR 0033) — see
                // `Metadata::merged_tablets`'s doc. Never pruned.
                self.merged_tablets.insert(*right);
                // …and its CP members' addresses are dead (ADR 0024 GC).
                self.prune_cp_member_addrs();
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
                for id in dropped {
                    self.tablets.remove(&id);
                    // A dropped tablet can no longer be reconciled (mirrors the
                    // `MergeTablets` cleanup).
                    self.policies.remove(&id);
                }
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
                    ApplyOutcome::Applied
                } else {
                    ApplyOutcome::NoOp
                }
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
                if self.node_addrs.get(node) == Some(addrs) {
                    ApplyOutcome::NoOp
                } else {
                    self.node_addrs.insert(node.clone(), addrs.clone());
                    ApplyOutcome::Applied
                }
            }
            MetaCommand::RemoveMember { node } => {
                let Some(member) = self.members.get(node) else {
                    // Already absent: an idempotent retry (e.g. a proposer whose
                    // confirm timed out after the command actually committed).
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
            MetaCommand::AllocateNodeId { nonce, labels } => {
                if self.node_id_allocations.contains_key(nonce) {
                    // Idempotent replay of an already-served join attempt
                    // (same house style as `RegisterNodeAddrs`'s identical-
                    // input no-op) — the caller re-reads the id from
                    // `node_id_allocations`, never from this outcome alone.
                    return ApplyOutcome::NoOp;
                }
                let n = self.next_free_alloc_id_n();
                let node_id = alloc_node_id(n);
                self.next_alloc_id = n + 1;
                self.node_id_allocations
                    .insert(nonce.clone(), node_id.clone());
                self.members.insert(
                    node_id,
                    Member {
                        labels: labels.clone(),
                        status: NodeStatus::Down,
                    },
                );
                ApplyOutcome::Applied
            }
        }
    }

    /// Drop every CP member-addr entry recorded against a tablet that is **no
    /// longer in the map** (ADR 0024 — the address-book half of drop-table GC,
    /// closing the designed `cp_member_addrs` leak). Called from the apply arms
    /// that remove tablets (`DropTableTablets`, `MergeTablets`); keyed purely on
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

    /// The next cluster-allocated member id a proposer should mint (ADR
    /// 0036) — the [`ALLOC_ID_BASE`]-range dual of
    /// [`next_free_tablet_id`](Self::next_free_tablet_id): folds the
    /// monotonic `next_alloc_id` counter together with the highest id
    /// already seen in either `members` or `node_id_allocations` (so a
    /// pre-counter snapshot, or one whose counter somehow lagged a recorded
    /// allocation, still allocates strictly above every id it has ever
    /// handed out) and a floor of [`ALLOC_ID_BASE`] itself. Race-safe by
    /// construction, same as the tablet allocator: [`MetaCommand::
    /// AllocateNodeId`]'s apply always calls this immediately before
    /// minting, under the single-threaded state-machine apply, so two
    /// concurrent proposals are simply applied in some log order — the
    /// second sees the first's bumped counter.
    #[must_use]
    pub fn next_free_alloc_id(&self) -> NodeId {
        alloc_node_id(self.next_free_alloc_id_n())
    }

    /// The numeric counter value backing [`Metadata::next_free_alloc_id`]
    /// (PR3 shim, removed in PR4 along with the rest of the allocator).
    fn next_free_alloc_id_n(&self) -> u64 {
        let highest_member = self
            .members
            .keys()
            .filter_map(parse_alloc_id)
            .max()
            .unwrap_or(0);
        let highest_allocation = self
            .node_id_allocations
            .values()
            .filter_map(parse_alloc_id)
            .max()
            .unwrap_or(0);
        self.next_alloc_id
            .max(highest_member + 1)
            .max(highest_allocation + 1)
            .max(ALLOC_ID_BASE)
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
    /// change — mirroring `RegisterCpAddr`'s own contract.
    #[test]
    fn register_node_addrs_records_updates_and_is_idempotent() {
        let mut m = Metadata::default();
        let addrs = |suffix: u16| NodeAddrs {
            internal: format!("127.0.0.1:{}", 9300 + suffix),
            client: format!("127.0.0.1:{}", 9000 + suffix),
            admin: format!("127.0.0.1:{}", 9500 + suffix),
            role: "combined".to_string(),
        };

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
            assert_eq!(
                m.apply(&MetaCommand::RegisterNodeAddrs {
                    node: nid(node),
                    addrs: NodeAddrs {
                        internal: format!("127.0.0.1:{}", 9300 + node),
                        client: format!("127.0.0.1:{}", 9000 + node),
                        admin: format!("127.0.0.1:{}", 9500 + node),
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

    /// ADR 0024 address GC: a tablet-scoped `RegisterCpAddr` entry is pruned from
    /// both maps when its tablet leaves the map (`DropTableTablets` /
    /// `MergeTablets`); a registration for an absent tablet is rejected (the
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

    /// The `MergeTablets` removal path prunes the merged-away tablet's CP member
    /// addresses exactly like a drop (ADR 0024 GC).
    #[test]
    fn merge_prunes_the_removed_tablets_cp_addrs() {
        let mut m = Metadata::default();
        let mid = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: None,
                range: KeyRange::new(Vec::new(), Some(mid.clone())),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(2),
                table: None,
                range: KeyRange::new(mid, None),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        for (id, tablet) in [(1301, 1u64), (2301, 2u64)] {
            assert_eq!(
                m.apply(&MetaCommand::RegisterCpAddr {
                    id: nid(id),
                    addr: format!("127.0.0.1:{id}"),
                    tablet: Some(TabletId(tablet)),
                }),
                ApplyOutcome::Applied
            );
        }

        assert_eq!(
            m.apply(&MetaCommand::MergeTablets {
                left: TabletId(1),
                expected_left_epoch: Epoch::INITIAL,
                right: TabletId(2),
                expected_right_epoch: Epoch::INITIAL,
            }),
            ApplyOutcome::Applied
        );
        // The merged-away right tablet's member is reclaimed; the survivor's stays.
        assert!(!m.cp_member_addrs.contains_key(&nid(2301)));
        assert!(!m.cp_member_tablets.contains_key(&nid(2301)));
        assert!(m.cp_member_addrs.contains_key(&nid(1301)));
        assert_eq!(m.cp_member_tablets.get(&nid(1301)), Some(&TabletId(1)));
        assert!(
            m.merged_tablets.contains(&TabletId(2)),
            "the merged-away tablet must be recorded (ADR 0033)"
        );
    }

    /// Cross-group LWW version-floor fix (flagged in a PR #90 review comment,
    /// root `CLAUDE.md`'s cross-group-LWW entry): `SplitTablet` seeds the new
    /// sibling's `version_floor` strictly past the source's own — every
    /// tablet's data-plane group stamps its own local Raft log index as the
    /// MVCC version (`animus-cp-data`), so a fresh sibling group serving keys
    /// the source already wrote at a (possibly much higher) index must never
    /// restart at a version low enough to collide. The source's own floor is
    /// untouched — it never absorbs foreign data, only narrows.
    #[test]
    fn split_tablet_seeds_the_new_siblings_version_floor_past_the_sources() {
        let mut m = Metadata::default();
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: None,
                range: KeyRange::whole(),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.tablets[&TabletId(1)].version_floor, 0);

        assert_eq!(
            m.apply(&MetaCommand::SplitTablet {
                tablet: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
                new_id: TabletId(2),
            }),
            ApplyOutcome::Applied
        );
        // The source's own floor is unchanged (it never absorbs foreign data)…
        assert_eq!(m.tablets[&TabletId(1)].version_floor, 0);
        // …while the sibling's is strictly past it.
        assert_eq!(m.tablets[&TabletId(2)].version_floor, 1);

        // A second split (of the sibling, which now itself has a nonzero
        // floor) must seed its own new child past ITS floor, not just past 0
        // or the tablet id — proving the formula reads `version_floor`, not
        // some other monotonic counter.
        assert_eq!(
            m.apply(&MetaCommand::SplitTablet {
                tablet: TabletId(2),
                expected_epoch: Epoch::INITIAL,
                split_key: 0xC000_0000_0000_0000u64.to_be_bytes().to_vec(),
                new_id: TabletId(3),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.tablets[&TabletId(2)].version_floor, 1);
        assert_eq!(m.tablets[&TabletId(3)].version_floor, 2);
    }

    /// `MergeTablets` bumps the surviving `left`'s `version_floor` to
    /// `max(left, right) + 1` — the merge dual of the split fix above. Built
    /// with `right`'s floor *higher* than `left`'s (via a prior split) so the
    /// test cannot pass by accident from a naive `left + 1` formula that
    /// ignores `right` entirely.
    #[test]
    fn merge_tablets_bumps_the_survivors_version_floor_past_both_sides() {
        let mut m = Metadata::default();
        let mid = 0x8000_0000_0000_0000u64.to_be_bytes().to_vec();
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(1),
                table: None,
                range: KeyRange::new(Vec::new(), Some(mid.clone())),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(2),
                table: None,
                range: KeyRange::new(mid.clone(), None),
                replicas: vec![nid(1), nid(2), nid(3)],
            }),
            ApplyOutcome::Applied
        );
        // Give `right` (tablet 2) a HIGHER floor than `left` (tablet 1) via a
        // split-and-merge-back-in dance is overkill — directly split tablet 2
        // once (against a third, throwaway sibling) so its own floor becomes
        // 1, strictly above tablet 1's untouched 0.
        assert_eq!(
            m.apply(&MetaCommand::SplitTablet {
                tablet: TabletId(2),
                expected_epoch: Epoch::INITIAL,
                split_key: 0xC000_0000_0000_0000u64.to_be_bytes().to_vec(),
                new_id: TabletId(3),
            }),
            ApplyOutcome::Applied
        );
        // Merge the throwaway sibling straight back into tablet 2, so tablet
        // 2's range is whole again (abutting tablet 1) but its floor is now 2
        // (bumped past both its own prior 0 and the throwaway's 1).
        assert_eq!(
            m.apply(&MetaCommand::MergeTablets {
                left: TabletId(2),
                expected_left_epoch: m.tablets[&TabletId(2)].epoch,
                right: TabletId(3),
                expected_right_epoch: m.tablets[&TabletId(3)].epoch,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.tablets[&TabletId(2)].version_floor, 2);
        assert_eq!(m.tablets[&TabletId(1)].version_floor, 0);

        // Now merge tablet 1 (`left`, floor 0) with tablet 2 (`right`, floor
        // 2) — the survivor's floor must become `max(0, 2) + 1 = 3`, proving
        // the formula reads BOTH sides, not just `left`.
        assert_eq!(
            m.apply(&MetaCommand::MergeTablets {
                left: TabletId(1),
                expected_left_epoch: m.tablets[&TabletId(1)].epoch,
                right: TabletId(2),
                expected_right_epoch: m.tablets[&TabletId(2)].epoch,
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.tablets[&TabletId(1)].version_floor, 3);
        assert!(!m.tablets.contains_key(&TabletId(2)));
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
        // Split `users` so the table owns two tablets (the child inherits scope).
        let split = MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            new_id: TabletId(4),
        };
        assert_eq!(m.apply(&split), ApplyOutcome::Applied);
        for id in [1u64, 2, 4] {
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
        assert!(!m.tablets.contains_key(&TabletId(1)));
        assert!(!m.tablets.contains_key(&TabletId(4)));
        assert!(!m.policies.contains_key(&TabletId(1)));
        assert!(!m.policies.contains_key(&TabletId(4)));
        // …while the other table's tablet + policy and the legacy tablet remain.
        assert!(m.tablets.contains_key(&TabletId(2)));
        assert!(m.policies.contains_key(&TabletId(2)));
        assert!(m.tablets.contains_key(&TabletId(3)));

        // Idempotent: dropping again is a no-op.
        assert_eq!(m.apply(&drop), ApplyOutcome::NoOp);

        // The allocator never rewinds: a later table gets a fresh id, above the
        // dropped ones.
        assert_eq!(m.next_free_tablet_id(), TabletId(5));
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

        // A split advances the counter past the new child too.
        let split = MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: 0x8000_0000_0000_0000u64.to_be_bytes().to_vec(),
            new_id: TabletId(3),
        };
        assert_eq!(m.apply(&split), ApplyOutcome::Applied);
        assert_eq!(m.next_free_tablet_id(), TabletId(4));

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
    #[test]
    fn split_rejects_a_reused_tablet_id_below_the_allocator() {
        let mut m = Metadata::default();
        let create = |id: u64, table: &str| MetaCommand::CreateTablet {
            tablet: TabletId(id),
            table: Some(table.to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2), nid(3)],
        };
        assert_eq!(m.apply(&create(1, "users")), ApplyOutcome::Applied);
        assert_eq!(m.apply(&create(2, "orders")), ApplyOutcome::Applied);

        // Drop the table owning the **highest** id; the freed id must not come back.
        assert_eq!(
            m.apply(&MetaCommand::DropTableTablets {
                table: "orders".to_owned(),
            }),
            ApplyOutcome::Applied
        );
        assert!(!m.tablets.contains_key(&TabletId(2)));
        assert_eq!(m.next_free_tablet_id(), TabletId(3));

        // A split re-minting the freed id (what `max(ids) + 1` would derive here)
        // is rejected — it does not collide with a *present* tablet, so only the
        // allocator floor catches it.
        let split_at = |new_id: u64| MetaCommand::SplitTablet {
            tablet: TabletId(1),
            expected_epoch: Epoch::INITIAL,
            split_key: b"m".to_vec(),
            new_id: TabletId(new_id),
        };
        assert_eq!(
            m.apply(&split_at(2)),
            ApplyOutcome::Rejected("new tablet id below the monotonic allocator")
        );
        // The rejected split changed nothing.
        assert_eq!(m.tablets.len(), 1);
        assert_eq!(m.tablets[&TabletId(1)].epoch, Epoch::INITIAL);

        // The allocator's own id is accepted, and the counter stays monotonic.
        assert_eq!(m.apply(&split_at(3)), ApplyOutcome::Applied);
        assert!(m.tablets.contains_key(&TabletId(3)));
        assert_eq!(m.next_free_tablet_id(), TabletId(4));
    }

    /// `SplitTablet` is a compare-and-swap on the source tablet's epoch, exactly
    /// like `CasTabletReplicas`: two proposers racing to split the same tablet at
    /// the same epoch — each computing a different median from an equally-stale
    /// view of the pre-split range — must not both commit. Without this guard the
    /// second, losing proposal would still apply (its `split_key` is strictly
    /// inside the *original* range), minting a second child tablet id that the
    /// per-tablet CP-data Raft group (which applies at most one real `Split`,
    /// ever) can never actually host — a permanent, leaderless orphan tablet
    /// (observed live under sustained `--auto-split` bulk-seed load).
    #[test]
    fn split_rejects_a_stale_epoch_racing_a_concurrent_split() {
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

        // The winner: split at "m", still at the tablet's original epoch.
        assert_eq!(
            m.apply(&MetaCommand::SplitTablet {
                tablet: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: b"m".to_vec(),
                new_id: TabletId(2),
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(m.tablets[&TabletId(1)].epoch, Epoch::INITIAL.next());

        // The loser: a different median, proposed against the *same* stale
        // epoch (it read the tablet before the winner's split committed) — even
        // though "q" is still strictly inside the tablet's original range, this
        // must be rejected now that the epoch has moved, not silently accepted
        // into a second, never-hostable child tablet.
        assert_eq!(
            m.apply(&MetaCommand::SplitTablet {
                tablet: TabletId(1),
                expected_epoch: Epoch::INITIAL,
                split_key: b"q".to_vec(),
                new_id: TabletId(3),
            }),
            ApplyOutcome::Rejected("epoch mismatch")
        );
        // No orphan was minted, and the winning split is untouched.
        assert!(!m.tablets.contains_key(&TabletId(3)));
        assert_eq!(m.tablets.len(), 2);
        assert_eq!(m.tablets[&TabletId(1)].epoch, Epoch::INITIAL.next());

        // A retry against the *current* epoch succeeds normally.
        assert_eq!(
            m.apply(&MetaCommand::SplitTablet {
                tablet: TabletId(1),
                expected_epoch: Epoch::INITIAL.next(),
                split_key: b"c".to_vec(),
                new_id: TabletId(3),
            }),
            ApplyOutcome::Applied
        );
        assert!(m.tablets.contains_key(&TabletId(3)));
    }

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

    /// The cluster-allocated member id allocator (ADR 0036) is monotonic and
    /// its range is disjoint from every small, manually-configured id (a
    /// handful of ordinary `UpsertMember`s, standing in for any realistic
    /// `--node I` node count) — mirroring
    /// `next_tablet_id_is_monotonic_across_create_and_split`'s coverage for
    /// the tablet allocator this one is deliberately shaped after.
    #[test]
    fn allocate_node_id_is_monotonic_and_disjoint_from_small_manual_ids() {
        let mut m = Metadata::default();
        // A handful of ordinary, small manually-configured members —
        // comfortably below `ALLOC_ID_BASE` for any realistic node count.
        for node in [0u64, 1, 300, 301, 302] {
            m.apply(&MetaCommand::UpsertMember {
                node: nid(node),
                labels: BTreeMap::new(),
                status: NodeStatus::Active,
            });
        }

        assert_eq!(m.next_free_alloc_id(), alloc_node_id(ALLOC_ID_BASE));
        assert_eq!(
            m.apply(&MetaCommand::AllocateNodeId {
                nonce: "join-1".to_owned(),
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Applied
        );
        let first = m
            .node_id_allocations
            .get("join-1")
            .expect("recorded")
            .clone();
        assert_eq!(
            first,
            alloc_node_id(ALLOC_ID_BASE),
            "first allocation lands at the base"
        );
        // Disjointness is now by *namespace* (the reserved `"alloc-"` prefix),
        // not by magnitude — string ids aren't ordered by "size" the way the
        // old raw-`u64` scheme was (`"alloc-1000000"` sorts *before* `"n302"`
        // lexicographically, so an `>` comparison here would be a false
        // negative for the very property this test means to prove).
        assert!(
            parse_alloc_id(&first).is_some(),
            "an allocator-minted id must carry the reserved \"alloc-\" prefix"
        );
        for node in [0u64, 1, 300, 301, 302] {
            assert_ne!(
                first,
                nid(node),
                "allocated id must never collide with a small manual id"
            );
        }

        // A second, distinct join attempt allocates strictly past the first —
        // the counter never goes backward.
        assert_eq!(
            m.apply(&MetaCommand::AllocateNodeId {
                nonce: "join-2".to_owned(),
                labels: BTreeMap::new(),
            }),
            ApplyOutcome::Applied
        );
        let second = m
            .node_id_allocations
            .get("join-2")
            .expect("recorded")
            .clone();
        assert_eq!(
            parse_alloc_id(&second).unwrap(),
            parse_alloc_id(&first).unwrap() + 1
        );
        assert_eq!(
            parse_alloc_id(&m.next_free_alloc_id()).unwrap(),
            parse_alloc_id(&second).unwrap() + 1
        );
    }

    /// Replaying the **same nonce** (a proposer retry after an `Accepted`-
    /// but-unconfirmed propose) is a no-op that returns the identical,
    /// already-allocated id — never a second one — and mutates nothing else
    /// (no bump of `next_alloc_id`, no second `members` entry). A genuinely
    /// **different** nonce always gets a fresh id.
    #[test]
    fn allocate_node_id_same_nonce_is_idempotent_distinct_nonces_get_distinct_ids() {
        let mut m = Metadata::default();
        let alloc = |nonce: &str| MetaCommand::AllocateNodeId {
            nonce: nonce.to_owned(),
            labels: BTreeMap::new(),
        };

        assert_eq!(m.apply(&alloc("retry-me")), ApplyOutcome::Applied);
        let id = m
            .node_id_allocations
            .get("retry-me")
            .expect("recorded")
            .clone();
        let after_first = m.clone();

        // A retry with the same nonce: no-op, identical id, no further
        // mutation of `Metadata` at all (proposer-observable state is
        // unchanged, not just the returned id).
        assert_eq!(m.apply(&alloc("retry-me")), ApplyOutcome::NoOp);
        assert_eq!(*m.node_id_allocations.get("retry-me").unwrap(), id);
        assert_eq!(m, after_first, "a same-nonce replay must mutate nothing");

        // A third, distinct nonce still gets a fresh id.
        assert_eq!(m.apply(&alloc("different-attempt")), ApplyOutcome::Applied);
        let other = m
            .node_id_allocations
            .get("different-attempt")
            .expect("recorded")
            .clone();
        assert_ne!(other, id);
    }

    /// A successful allocation registers the id in `members` as `Down` with
    /// the given labels and no address — the address arrives later via the
    /// joiner's own `RegisterNodeAddrs` self-registration, unchanged.
    #[test]
    fn allocate_node_id_registers_the_member_down_with_labels() {
        let mut m = Metadata::default();
        let mut labels = BTreeMap::new();
        labels.insert("region".to_owned(), "eu-west".to_owned());

        assert_eq!(
            m.apply(&MetaCommand::AllocateNodeId {
                nonce: "join-1".to_owned(),
                labels: labels.clone(),
            }),
            ApplyOutcome::Applied
        );
        let id = m
            .node_id_allocations
            .get("join-1")
            .expect("recorded")
            .clone();
        let member = m.members.get(&id).expect("member registered");
        assert_eq!(member.status, NodeStatus::Down);
        assert_eq!(member.labels, labels);
        assert!(
            !m.node_addrs.contains_key(&id),
            "no address yet — that's a separate, later self-registration"
        );
    }

    /// A `Metadata` snapshot serialized before ADR 0036 (no `next_alloc_id`/
    /// `node_id_allocations` fields in the JSON) still decodes: the counter
    /// defaults to `ALLOC_ID_BASE` (not `0`, which would immediately collide
    /// with real small ids) and the ledger defaults to empty — the same
    /// `#[serde(default)]` back-compat contract every other additive field on
    /// `Metadata` already carries.
    #[test]
    fn metadata_without_alloc_fields_still_decodes_at_the_base() {
        let m = Metadata::default();
        let mut value = serde_json::to_value(&m).expect("metadata serializes");
        let obj = value.as_object_mut().expect("metadata is a JSON object");
        obj.remove("next_alloc_id");
        obj.remove("node_id_allocations");
        let decoded: Metadata =
            serde_json::from_value(value).expect("metadata without alloc fields still decodes");
        assert_eq!(decoded.next_alloc_id, ALLOC_ID_BASE);
        assert!(decoded.node_id_allocations.is_empty());
        assert_eq!(decoded.next_free_alloc_id(), alloc_node_id(ALLOC_ID_BASE));
    }
}
