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
use animus_placement::{Candidate, PlacementPolicy, replan};
use animus_tablet::{Epoch, KeyRange, Tablet, TabletId};
use serde::{Deserialize, Serialize};

use crate::schema::{IndexDef, SchemaCatalog, TableName, TableSchema};

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
    /// Merge adjacent tablets `left` and `right` (where `left.end == right.start`
    /// and they share a replica set) into `left`, extended to cover both ranges
    /// with a bumped epoch; `right` is removed.
    MergeTablets { left: TabletId, right: TabletId },
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
}

/// Build placement candidates from the `Active` members and their labels.
/// Liveness is the control plane's job (ADR 0005): only `Active` members are
/// offered to the placement engine, which then enforces *policy* (residency
/// + spread). Iteration is over a `BTreeMap`, so the order is deterministic.
fn active_candidates(members: &BTreeMap<NodeId, Member>) -> Vec<Candidate> {
    members
        .iter()
        .filter(|(_, m)| m.status == NodeStatus::Active)
        .map(|(id, m)| Candidate::new(*id, m.labels.clone()))
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
                    *node,
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
                let new_tablet = Tablet::with_table(
                    *new_id,
                    source.table.clone(),
                    right,
                    source.replicas.clone(),
                );
                let source = self.tablets.get_mut(tablet).expect("tablet present");
                source.range = left;
                source.epoch = source.epoch.next();
                self.tablets.insert(*new_id, new_tablet);
                self.next_tablet_id = self.next_tablet_id.max(new_id.0 + 1);
                ApplyOutcome::Applied
            }
            MetaCommand::MergeTablets { left, right } => {
                let (Some(l), Some(r)) = (self.tablets.get(left), self.tablets.get(right)) else {
                    return ApplyOutcome::Rejected("no such tablet");
                };
                if !l.range.abuts(&r.range) {
                    return ApplyOutcome::Rejected("tablets are not adjacent");
                }
                if l.replicas != r.replicas {
                    return ApplyOutcome::Rejected("tablets have different replica sets");
                }
                let new_end = r.range.end.clone();
                let l = self.tablets.get_mut(left).expect("tablet present");
                l.range.end = new_end;
                l.epoch = l.epoch.next();
                self.tablets.remove(right);
                // The merged-away tablet can no longer be reconciled.
                self.policies.remove(right);
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
                if let Some(t) = tablet {
                    if !self.tablets.contains_key(t) {
                        return ApplyOutcome::Rejected("no such tablet for cp addr");
                    }
                }
                if self.cp_member_addrs.get(id) == Some(addr)
                    && self.cp_member_tablets.get(id) == tablet.as_ref()
                {
                    ApplyOutcome::NoOp
                } else {
                    self.cp_member_addrs.insert(*id, addr.clone());
                    match tablet {
                        Some(t) => {
                            self.cp_member_tablets.insert(*id, *t);
                        }
                        None => {
                            self.cp_member_tablets.remove(id);
                        }
                    }
                    ApplyOutcome::Applied
                }
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
            .map(|(&id, _)| id)
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

/// `Metadata` is the control plane's replicated state machine: the [`RaftCore`]
/// agrees the order of [`MetaCommand`]s and applies them here. (The inherent
/// [`Metadata::apply`] returns an [`ApplyOutcome`] for callers that care; the
/// consensus core only needs the order, so the trait impl discards it.)
///
/// [`RaftCore`]: crate::raft::RaftCore
impl crate::raft::StateMachine<MetaCommand> for Metadata {
    fn apply(&mut self, command: &MetaCommand) {
        let _ = Metadata::apply(self, command);
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
        assert_eq!(m.apply(&reg(301, "127.0.0.1:9001")), ApplyOutcome::Applied);
        assert_eq!(
            m.cp_member_addrs.get(&301).map(String::as_str),
            Some("127.0.0.1:9001")
        );

        // Re-registering the same address is a no-op (so a periodic re-register
        // does not churn the Raft log).
        assert_eq!(m.apply(&reg(301, "127.0.0.1:9001")), ApplyOutcome::NoOp);

        // A changed address updates the entry.
        assert_eq!(m.apply(&reg(301, "127.0.0.1:9002")), ApplyOutcome::Applied);
        assert_eq!(
            m.cp_member_addrs.get(&301).map(String::as_str),
            Some("127.0.0.1:9002")
        );

        // A distinct member coexists.
        assert_eq!(m.apply(&reg(401, "127.0.0.1:9101")), ApplyOutcome::Applied);
        assert_eq!(m.cp_member_addrs.len(), 2);
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
                replicas: vec![1, 2, 3],
            },
            // Tablet-scoped members of tablet 1.
            MetaCommand::RegisterCpAddr {
                id: 1301,
                addr: "127.0.0.1:9301".to_owned(),
                tablet: Some(TabletId(1)),
            },
            MetaCommand::RegisterCpAddr {
                id: 1302,
                addr: "127.0.0.1:9302".to_owned(),
                tablet: Some(TabletId(1)),
            },
            // A legacy (tablet-less) member: never pruned.
            MetaCommand::RegisterCpAddr {
                id: 301,
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
        assert!(!m.cp_member_addrs.contains_key(&1301));
        assert!(!m.cp_member_addrs.contains_key(&1302));
        assert!(m.cp_member_tablets.is_empty());
        // …the legacy entry survives.
        assert_eq!(
            m.cp_member_addrs.get(&301).map(String::as_str),
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
                id: 1301,
                addr: "127.0.0.1:9301".to_owned(),
                tablet: Some(TabletId(1)),
            }),
            ApplyOutcome::Rejected("no such tablet for cp addr")
        );
        assert!(!m.cp_member_addrs.contains_key(&1301));
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
                replicas: vec![1, 2, 3],
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(2),
                table: None,
                range: KeyRange::new(mid, None),
                replicas: vec![1, 2, 3],
            }),
            ApplyOutcome::Applied
        );
        for (id, tablet) in [(1301, 1u64), (2301, 2u64)] {
            assert_eq!(
                m.apply(&MetaCommand::RegisterCpAddr {
                    id,
                    addr: format!("127.0.0.1:{id}"),
                    tablet: Some(TabletId(tablet)),
                }),
                ApplyOutcome::Applied
            );
        }

        assert_eq!(
            m.apply(&MetaCommand::MergeTablets {
                left: TabletId(1),
                right: TabletId(2),
            }),
            ApplyOutcome::Applied
        );
        // The merged-away right tablet's member is reclaimed; the survivor's stays.
        assert!(!m.cp_member_addrs.contains_key(&2301));
        assert!(!m.cp_member_tablets.contains_key(&2301));
        assert!(m.cp_member_addrs.contains_key(&1301));
        assert_eq!(m.cp_member_tablets.get(&1301), Some(&TabletId(1)));
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
            replicas: vec![1, 2, 3],
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
            replicas: vec![1, 2, 3],
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
                Tablet::new(TabletId(7), KeyRange::whole(), vec![1]),
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
            replicas: vec![1, 2, 3],
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
                replicas: vec![1, 2, 3],
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
}
