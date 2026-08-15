//! The replicated **table-schema catalog** (ADR 0013): the cluster-wide,
//! Raft-replicated record of which tables exist and what their keys and columns
//! are.
//!
//! Both wire adapters (`animus-dynamo`, `animus-cql`) previously kept table
//! schemas in per-process, in-memory catalogs, so a `CreateTable` / `CREATE
//! TABLE` neither survived a restart nor replicated to other nodes. This module
//! is the control-plane substrate that fixes that: a [`TableSchema`] lives in
//! [`Metadata`](crate::Metadata) and is mutated by replicated
//! [`MetaCommand`](crate::MetaCommand)s, so it is agreed cluster-wide, durable
//! (recovered from the WAL/snapshot like all metadata), and consistent on every
//! replica. Wiring the adapters to *consume* this catalog is a deliberate
//! follow-up; this slice is the substrate plus the read accessors.
//!
//! ## A shape that fits both adapters
//!
//! DynamoDB declares a **partition key** and an optional **sort key** (its key
//! attributes are typed; non-key attributes are schemaless). CQL declares an
//! ordered list of typed **columns**, one of which is the partition key, plus —
//! eventually — clustering columns. The union modelled here is:
//!
//! - a [`partition_key`](TableSchema::partition_key): the single required key
//!   column, by name;
//! - any number of ordered [`clustering_keys`](TableSchema::clustering_keys):
//!   DynamoDB's optional sort key is the one-element case, CQL's clustering
//!   columns the general case;
//! - a set of typed [`columns`](TableSchema::columns) (keyed and non-keyed
//!   alike), each carrying a [`ColumnType`].
//!
//! [`ColumnType`] is the union of the CQL scalar types and the DynamoDB
//! key-attribute families, so an adapter can map its own type onto it losslessly
//! enough to round-trip key handling. Everything here is **pure and
//! deterministic** (ADR 0003): plain data, `BTreeMap`/`Vec`, no I/O, no clock,
//! no RNG.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// A table name (the catalog key). For CQL this is the keyspace-qualified table
/// name the adapter chooses to use (e.g. `ks.table`); for DynamoDB it is the
/// bare table name. The control plane treats it as an opaque, case-sensitive
/// identifier — namespacing is the adapter's responsibility.
pub type TableName = String;

/// The column type vocabulary shared by both wire adapters.
///
/// It is the union of the CQL scalar type system (`text`/`int`/`bigint`/
/// `boolean`/`blob`/`uuid`) and the DynamoDB key-attribute families
/// (`String`/`Number`/`Binary`/`Bool`). The two overlap (`String`≈CQL `text`,
/// `Binary`≈CQL `blob`, `Bool`≈CQL `boolean`); both names are kept so each
/// adapter can record its declared type faithfully and recover it on read. The
/// control plane never interprets a value — it only stores the declared type —
/// so the breadth here is about *fidelity for the adapters*, not validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ColumnType {
    /// UTF-8 string (CQL `text`/`varchar`, DynamoDB `S`).
    String,
    /// A number (DynamoDB `N`; CQL callers should prefer [`Int`](ColumnType::Int)
    /// / [`BigInt`](ColumnType::BigInt) where the width is known).
    Number,
    /// 32-bit signed integer (CQL `int`).
    Int,
    /// 64-bit signed integer (CQL `bigint`).
    BigInt,
    /// Boolean (CQL `boolean`, DynamoDB `BOOL`).
    Bool,
    /// Arbitrary bytes (CQL `blob`, DynamoDB `B`).
    Binary,
    /// A 16-byte UUID (CQL `uuid`).
    Uuid,
}

/// What attributes a secondary index projects — the replicated counterpart of a
/// DynamoDB `CreateTable` index `Projection` (ADR 0013). The control plane stores
/// only the *declaration*; an adapter maps it onto its own projection type when it
/// reads the catalog. Plain data (`Vec`), deterministic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexProjection {
    /// Every attribute (`ALL`).
    All,
    /// Only the base-table key + index key attributes (`KEYS_ONLY`).
    KeysOnly,
    /// The keys plus an explicit list of non-key attributes (`INCLUDE`).
    Include(Vec<String>),
}

/// Whether a secondary index is **global** (its own hash keyspace, independent of
/// the base partition) or **local** (shares the base partition key, alternate sort
/// only). Mirrors DynamoDB's GSI / LSI distinction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    /// Global secondary index: hashes by `hash_attribute` (an independent
    /// keyspace), optionally plus a range attribute.
    Global,
    /// Local secondary index: hashes by the base table's partition key, sorts by
    /// `sort_attribute` (always present for an LSI).
    Local,
}

/// A DynamoDB Streams **view type** (ADR 0042 §3): which image(s) a
/// `GetRecords` response projects for this table's stream. This is a
/// **read-time projection only** — a shard record always stores both the old
/// and new item images regardless of the declared view type (ADR 0043's
/// `KIND_STREAM` record), so changing it (disable + re-enable, since a live
/// view-type change is not a real DynamoDB operation either) never needs a
/// backfill or a different storage format.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamViewType {
    /// Both the old and new item images.
    NewAndOldImages,
    /// Only the new item image.
    NewImage,
    /// Only the old item image.
    OldImage,
    /// Only the modified item's key attributes.
    KeysOnly,
}

/// A table's replicated DynamoDB Streams configuration (ADR 0042 §2/§4), when
/// enabled. `label` is minted **once**, at enable time (including a
/// re-enable after a disable), by the proposer — never reused — and is what
/// makes a stream's identity `(table, label)`: a stale ARN from a
/// disabled-then-re-enabled stream carries the *old* label, which a
/// `DescribeStream`/`GetRecords`/`GetShardIterator` request against the
/// *current* `StreamSpec.label` fails to match, surfacing as
/// `ResourceNotFoundException` rather than silently serving the new stream
/// (ADR 0042 §4/§9).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSpec {
    /// Which image(s) a read projects (read-time only, see [`StreamViewType`]).
    pub view_type: StreamViewType,
    /// This stream's identity component, minted fresh on every enable.
    pub label: String,
}

/// The lifecycle status of a secondary index (ADR 0045): whether it is still
/// being backfilled, fully materialized and queryable, or being torn down.
///
/// A just-created table's indexes start `Active` directly (they are empty by
/// construction, ADR 0041 §5) — only `UpdateTable`-added indexes on an already
/// populated table pass through `Creating` first. `#[serde(default =
/// "IndexStatus::active")]` on [`IndexDef::status`] means this only matters for
/// deserializing a status-less fixture/pre-existing record (no live deployments
/// exist to migrate, root `CLAUDE.md`); a status-less record is never actually
/// mid-backfill, so `Active` is the correct default, not merely a convenient one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexStatus {
    /// Declared but not yet fully backfilled — writes since declaration are
    /// already covered (`table_takes_kind_write_path` gates on presence, not
    /// status), but rows that predate declaration may not be materialized yet.
    /// The drain still maintains it (see `IndexKind`'s consumers) so it is not
    /// left further behind while backfill catches up.
    Creating,
    /// Fully backfilled and queryable.
    Active,
    /// Being torn down — the drain/backfill stop touching it; its hidden table
    /// is being reclaimed. Never observed by a query (rejected at the wire edge).
    Deleting,
}

impl IndexStatus {
    /// The default for a status-less (pre-ADR-0045) `IndexDef` — see the type's
    /// own doc for why `Active`, not `Creating`, is correct here.
    #[must_use]
    pub fn active() -> Self {
        IndexStatus::Active
    }
}

/// A secondary-index **definition** as replicated in the schema catalog (ADR
/// 0013): its name, kind, key attributes, and projection. This is the *shape* of
/// the index — the cluster-wide, durable agreement on which indexes exist — not
/// its entry data (the actual indexed rows), whose maintenance stays at the wire
/// edge (see ADR 0013 Consequences). Plain data, deterministic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDef {
    /// The index name (unique within its table; the `IndexName` a query targets).
    pub name: String,
    /// Global vs local.
    pub kind: IndexKind,
    /// The item attribute the index hashes by. For a [`Global`](IndexKind::Global)
    /// index this is its own hash key; for a [`Local`](IndexKind::Local) index it
    /// is, by convention, the base table's partition key (the adapter sets it so).
    pub hash_attribute: String,
    /// The optional range/sort attribute. Always present for an LSI; present for a
    /// composite GSI; `None` for a hash-only GSI.
    pub sort_attribute: Option<String>,
    /// What attributes a query against this index returns.
    pub projection: IndexProjection,
    /// This index's lifecycle status (ADR 0045). Mutated only through
    /// `MetaCommand::SetIndexStatus` (so it replicates); see [`IndexStatus`].
    #[serde(default = "IndexStatus::active")]
    pub status: IndexStatus,
}

/// One column's declared name and type. The name is stored as written
/// (case-preserved); an adapter that wants case-insensitivity normalizes before
/// it gets here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    /// The column name.
    pub name: String,
    /// The column's declared type.
    pub ty: ColumnType,
}

impl ColumnDef {
    /// A column with the given name and type.
    #[must_use]
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// How a table's data is replicated (ADR 0016 / ADR 0017). The default is **CP**
/// — the leaderful per-tablet Raft plane (`animus-cp-data`) with linearizable
/// single-tablet reads/writes, the only data plane in v1 (ADR 0019). **AP** is
/// retained as a forward-compat hook for the deferred leaderless plane (its crate
/// was deleted; ADR 0019); nothing routes to it in v1. Replicated in the schema
/// catalog so the choice is durable, cluster-agreed, and recovered from Raft like
/// the rest of the schema; the wire edges read it to route a table's
/// reads/writes to the right plane.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// Leaderless AP data plane (tunable quorum). Deferred in v1 (ADR 0019) —
    /// kept only as a forward-compat hook; no v1 table uses it.
    Ap,
    /// Leaderful per-tablet Raft (linearizable single-tablet KV). The default,
    /// and the only v1 plane (ADR 0019).
    #[default]
    Cp,
}

/// A table's replicated schema: its key structure plus its typed columns.
///
/// Invariants enforced by [`TableSchema::validate`] (and thus by
/// [`Metadata::apply`](crate::Metadata::apply), which rejects a malformed
/// schema):
/// - the schema has at least one column;
/// - `partition_key` names a column present in `columns`;
/// - every name in `clustering_keys` names a column present in `columns`;
/// - column names are unique;
/// - the partition key does not also appear in `clustering_keys`;
/// - no clustering key is repeated.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// The partition-key column, by name. Always present (every table has one).
    pub partition_key: String,
    /// The clustering / sort-key columns, in order. Empty for a hash-only table
    /// (DynamoDB simple table); one element for a DynamoDB composite table's
    /// sort key; many for CQL clustering columns.
    pub clustering_keys: Vec<String>,
    /// Every column (keys included), in declaration order.
    pub columns: Vec<ColumnDef>,
    /// Declared **secondary indexes** (GSI/LSI), by definition (ADR 0013). Empty
    /// for a table with none. This carries the index *shape* cluster-wide and
    /// durably; the index *entry data* (the actual indexed rows) is maintained at
    /// the wire edge. Mutated only through `MetaCommand::{CreateTableIndex,
    /// DropTableIndex}` (so they replicate), kept sorted by name for a
    /// deterministic order. Validated by [`TableSchema::validate`].
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    /// The table's replication mode (ADR 0016 / ADR 0017). `#[serde(default)]` →
    /// [`ReplicationMode::Cp`] (the only v1 plane, ADR 0019), so a schema
    /// persisted before this field existed deserializes as CP — the correct v1
    /// semantic, since the AP plane it predated no longer exists. Additive like
    /// `indexes`.
    #[serde(default)]
    pub mode: ReplicationMode,
    /// This table's DynamoDB Streams configuration (ADR 0042), if enabled.
    /// `None` for a table with no stream (the common case, and every schema
    /// persisted before this field existed — `#[serde(default)]`, additive
    /// like `indexes`/`mode`). Mutated only through
    /// `MetaCommand::SetTableStream` (so it replicates).
    #[serde(default)]
    pub stream: Option<StreamSpec>,
}

/// Why a [`TableSchema`] was rejected as malformed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaError {
    /// A schema declared no columns at all.
    NoColumns,
    /// The partition key names a column that is not in `columns`.
    UnknownPartitionKey,
    /// A clustering key names a column that is not in `columns`.
    UnknownClusteringKey,
    /// Two columns share a name.
    DuplicateColumn,
    /// The partition key is also listed as a clustering key.
    PartitionKeyIsClustering,
    /// The same clustering key appears more than once.
    DuplicateClusteringKey,
    /// Two secondary indexes share a name.
    DuplicateIndex,
    /// A local secondary index declared no sort attribute (an LSI must have one).
    LocalIndexMissingSort,
}

impl TableSchema {
    /// A hash-only table: a single partition key column, no clustering keys.
    /// (DynamoDB "simple" table.)
    #[must_use]
    pub fn simple(partition_key: impl Into<String>, ty: ColumnType) -> Self {
        let pk = partition_key.into();
        Self {
            columns: vec![ColumnDef::new(pk.clone(), ty)],
            partition_key: pk,
            clustering_keys: Vec::new(),
            indexes: Vec::new(),
            mode: ReplicationMode::default(),
            stream: None,
        }
    }

    /// A table with a partition key and one clustering (sort) key.
    /// (DynamoDB "composite" table.)
    #[must_use]
    pub fn composite(
        partition_key: impl Into<String>,
        pk_ty: ColumnType,
        sort_key: impl Into<String>,
        sk_ty: ColumnType,
    ) -> Self {
        let pk = partition_key.into();
        let sk = sort_key.into();
        Self {
            columns: vec![
                ColumnDef::new(pk.clone(), pk_ty),
                ColumnDef::new(sk.clone(), sk_ty),
            ],
            partition_key: pk,
            clustering_keys: vec![sk],
            indexes: Vec::new(),
            mode: ReplicationMode::default(),
            stream: None,
        }
    }

    /// Build a schema from an explicit column list, a partition key, and an
    /// ordered list of clustering keys. The columns may be in any order; the
    /// keys must name columns present in the list (checked by
    /// [`validate`](TableSchema::validate)).
    #[must_use]
    pub fn with_columns(
        partition_key: impl Into<String>,
        clustering_keys: Vec<String>,
        columns: Vec<ColumnDef>,
    ) -> Self {
        Self {
            partition_key: partition_key.into(),
            clustering_keys,
            columns,
            indexes: Vec::new(),
            mode: ReplicationMode::default(),
            stream: None,
        }
    }

    /// Set the replication mode (builder; default [`ReplicationMode::Cp`]).
    #[must_use]
    pub fn with_mode(mut self, mode: ReplicationMode) -> Self {
        self.mode = mode;
        self
    }

    /// Look up a column by name (exact match).
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// The partition-key column definition (present in a validated schema).
    #[must_use]
    pub fn partition_key_column(&self) -> Option<&ColumnDef> {
        self.column(&self.partition_key)
    }

    /// Whether this is a hash-only table (no clustering/sort keys).
    #[must_use]
    pub fn is_simple(&self) -> bool {
        self.clustering_keys.is_empty()
    }

    /// Validate the schema's internal consistency. Called by
    /// [`Metadata::apply`](crate::Metadata::apply) before recording the schema,
    /// so a malformed `CreateTableSchema` is rejected deterministically on every
    /// replica.
    ///
    /// # Errors
    /// A [`SchemaError`] describing the first violated invariant.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.columns.is_empty() {
            return Err(SchemaError::NoColumns);
        }
        // Unique column names.
        let mut names: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        names.sort_unstable();
        for pair in names.windows(2) {
            if pair[0] == pair[1] {
                return Err(SchemaError::DuplicateColumn);
            }
        }
        // Partition key must name a real column.
        if self.column(&self.partition_key).is_none() {
            return Err(SchemaError::UnknownPartitionKey);
        }
        // Clustering keys must name real columns, must not repeat, and must not
        // be the partition key.
        let mut seen = BTreeSet::new();
        for ck in &self.clustering_keys {
            if *ck == self.partition_key {
                return Err(SchemaError::PartitionKeyIsClustering);
            }
            if self.column(ck).is_none() {
                return Err(SchemaError::UnknownClusteringKey);
            }
            if !seen.insert(ck.as_str()) {
                return Err(SchemaError::DuplicateClusteringKey);
            }
        }
        // Secondary indexes: unique names; an LSI must carry a sort attribute.
        let mut index_names = BTreeSet::new();
        for idx in &self.indexes {
            if !index_names.insert(idx.name.as_str()) {
                return Err(SchemaError::DuplicateIndex);
            }
            if idx.kind == IndexKind::Local && idx.sort_attribute.is_none() {
                return Err(SchemaError::LocalIndexMissingSort);
            }
        }
        Ok(())
    }

    /// Look up a secondary index by name.
    #[must_use]
    pub fn index(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|i| i.name == name)
    }

    /// Add or replace a secondary index by name, keeping `indexes` sorted by name
    /// (deterministic order). Returns whether an index of that name already
    /// existed. Used by the state machine; callers go through `MetaCommand`.
    pub(crate) fn upsert_index(&mut self, index: IndexDef) -> bool {
        match self.indexes.iter_mut().find(|i| i.name == index.name) {
            Some(slot) => {
                *slot = index;
                true
            }
            None => {
                self.indexes.push(index);
                self.indexes.sort_by(|a, b| a.name.cmp(&b.name));
                false
            }
        }
    }

    /// Remove a secondary index by name, returning whether it existed. Used by the
    /// state machine; callers go through `MetaCommand`.
    pub(crate) fn remove_index(&mut self, name: &str) -> bool {
        let before = self.indexes.len();
        self.indexes.retain(|i| i.name != name);
        self.indexes.len() != before
    }

    /// Set a secondary index's status in place, leaving every other field
    /// untouched (deliberately **not** `upsert_index`'s whole-struct replace —
    /// a status transition must not resurrect a stale copy of the rest of the
    /// definition a racing proposer read before this one committed). Returns
    /// whether the index exists at all; a no-op (but still `true`) if it is
    /// already at `status`. Used by the state machine; callers go through
    /// `MetaCommand::SetIndexStatus`.
    pub(crate) fn set_index_status(&mut self, name: &str, status: IndexStatus) -> bool {
        match self.indexes.iter_mut().find(|i| i.name == name) {
            Some(idx) => {
                idx.status = status;
                true
            }
            None => false,
        }
    }
}

/// The replicated catalog of table schemas, keyed by table name.
///
/// A thin wrapper around a `BTreeMap` so the iteration order is deterministic
/// (ADR 0003) and so accessors can grow without widening
/// [`Metadata`](crate::Metadata)'s surface. Lives inside `Metadata`, mutated
/// only through `MetaCommand::{CreateTableSchema, DropTableSchema}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCatalog {
    tables: BTreeMap<TableName, TableSchema>,
}

impl SchemaCatalog {
    /// An empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `table` is registered.
    #[must_use]
    pub fn contains(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// The schema registered for `table`, if any.
    #[must_use]
    pub fn get(&self, table: &str) -> Option<&TableSchema> {
        self.tables.get(table)
    }

    /// A mutable handle to `table`'s schema (used by the state machine to mutate
    /// secondary indexes; callers go through `MetaCommand`).
    pub(crate) fn get_mut(&mut self, table: &str) -> Option<&mut TableSchema> {
        self.tables.get_mut(table)
    }

    /// The number of registered tables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Whether the catalog is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// All registered table names, in ascending (deterministic) order.
    pub fn table_names(&self) -> impl Iterator<Item = &TableName> {
        self.tables.keys()
    }

    /// All `(name, schema)` pairs, in ascending name order.
    pub fn iter(&self) -> impl Iterator<Item = (&TableName, &TableSchema)> {
        self.tables.iter()
    }

    /// Insert a schema (used by the state machine; callers go through
    /// `MetaCommand`).
    pub(crate) fn insert(&mut self, table: TableName, schema: TableSchema) {
        self.tables.insert(table, schema);
    }

    /// Remove a schema, returning whether it existed.
    pub(crate) fn remove(&mut self, table: &str) -> bool {
        self.tables.remove(table).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_and_composite_validate() {
        assert!(
            TableSchema::simple("id", ColumnType::Uuid)
                .validate()
                .is_ok()
        );
        assert!(
            TableSchema::composite("pk", ColumnType::String, "sk", ColumnType::String)
                .validate()
                .is_ok()
        );
    }

    #[test]
    fn simple_is_simple_composite_is_not() {
        assert!(TableSchema::simple("id", ColumnType::String).is_simple());
        assert!(
            !TableSchema::composite("pk", ColumnType::String, "sk", ColumnType::Int).is_simple()
        );
    }

    #[test]
    fn rejects_empty_columns() {
        let s = TableSchema::with_columns("pk", Vec::new(), Vec::new());
        assert_eq!(s.validate(), Err(SchemaError::NoColumns));
    }

    #[test]
    fn rejects_unknown_partition_key() {
        let s = TableSchema::with_columns(
            "missing",
            Vec::new(),
            vec![ColumnDef::new("a", ColumnType::Int)],
        );
        assert_eq!(s.validate(), Err(SchemaError::UnknownPartitionKey));
    }

    #[test]
    fn rejects_unknown_clustering_key() {
        let s = TableSchema::with_columns(
            "a",
            vec!["nope".into()],
            vec![ColumnDef::new("a", ColumnType::Int)],
        );
        assert_eq!(s.validate(), Err(SchemaError::UnknownClusteringKey));
    }

    #[test]
    fn rejects_duplicate_column() {
        let s = TableSchema::with_columns(
            "a",
            Vec::new(),
            vec![
                ColumnDef::new("a", ColumnType::Int),
                ColumnDef::new("a", ColumnType::String),
            ],
        );
        assert_eq!(s.validate(), Err(SchemaError::DuplicateColumn));
    }

    #[test]
    fn rejects_partition_key_as_clustering() {
        let s = TableSchema::with_columns(
            "a",
            vec!["a".into()],
            vec![ColumnDef::new("a", ColumnType::Int)],
        );
        assert_eq!(s.validate(), Err(SchemaError::PartitionKeyIsClustering));
    }

    #[test]
    fn rejects_duplicate_clustering_key() {
        let s = TableSchema::with_columns(
            "a",
            vec!["b".into(), "b".into()],
            vec![
                ColumnDef::new("a", ColumnType::Int),
                ColumnDef::new("b", ColumnType::Int),
            ],
        );
        assert_eq!(s.validate(), Err(SchemaError::DuplicateClusteringKey));
    }

    fn gsi(name: &str, hash: &str) -> IndexDef {
        IndexDef {
            name: name.into(),
            kind: IndexKind::Global,
            hash_attribute: hash.into(),
            sort_attribute: None,
            projection: IndexProjection::All,
            status: IndexStatus::Active,
        }
    }

    #[test]
    fn upsert_index_keeps_sorted_and_replaces_by_name() {
        let mut s = TableSchema::simple("id", ColumnType::String);
        assert!(!s.upsert_index(gsi("by-b", "b")));
        assert!(!s.upsert_index(gsi("by-a", "a")));
        // Sorted by name regardless of insertion order.
        assert_eq!(
            s.indexes
                .iter()
                .map(|i| i.name.as_str())
                .collect::<Vec<_>>(),
            vec!["by-a", "by-b"]
        );
        // Re-upsert replaces in place and reports it existed.
        assert!(s.upsert_index(gsi("by-a", "a2")));
        assert_eq!(s.index("by-a").unwrap().hash_attribute, "a2");
        assert_eq!(s.indexes.len(), 2);
    }

    #[test]
    fn remove_index_is_idempotent() {
        let mut s = TableSchema::simple("id", ColumnType::String);
        s.upsert_index(gsi("by-a", "a"));
        assert!(s.remove_index("by-a"));
        assert!(!s.remove_index("by-a"));
        assert!(s.index("by-a").is_none());
    }

    #[test]
    fn set_index_status_on_an_unknown_index_returns_false() {
        let mut s = TableSchema::simple("id", ColumnType::String);
        assert!(!s.set_index_status("ghost", IndexStatus::Active));
    }

    #[test]
    fn set_index_status_transitions_a_real_index_leaving_other_fields_untouched() {
        let mut s = TableSchema::simple("id", ColumnType::String);
        s.upsert_index(gsi("by-email", "email"));
        assert_eq!(s.index("by-email").unwrap().status, IndexStatus::Active);

        assert!(s.set_index_status("by-email", IndexStatus::Creating));
        let idx = s.index("by-email").unwrap();
        assert_eq!(idx.status, IndexStatus::Creating);
        // Every other field is exactly what `gsi()` built — only `status` moved.
        assert_eq!(idx.name, "by-email");
        assert_eq!(idx.kind, IndexKind::Global);
        assert_eq!(idx.hash_attribute, "email");
        assert_eq!(idx.sort_attribute, None);
        assert_eq!(idx.projection, IndexProjection::All);
    }

    #[test]
    fn set_index_status_to_the_same_status_is_reported_as_found_and_is_a_no_op() {
        let mut s = TableSchema::simple("id", ColumnType::String);
        s.upsert_index(gsi("by-email", "email"));
        // Already `Active` (the constructed default) — setting it again still
        // reports "found" (`true`); the apply-arm's own no-op detection (a
        // separate concern, tested at the `MetaCommand::SetIndexStatus` level)
        // is what turns this into `ApplyOutcome::NoOp`, not this method.
        assert!(s.set_index_status("by-email", IndexStatus::Active));
        assert_eq!(s.index("by-email").unwrap().status, IndexStatus::Active);
    }

    #[test]
    fn rejects_duplicate_index_name() {
        let mut s = TableSchema::simple("id", ColumnType::String);
        // Bypass `upsert_index`'s dedup to construct a malformed schema directly.
        s.indexes = vec![gsi("dup", "a"), gsi("dup", "b")];
        assert_eq!(s.validate(), Err(SchemaError::DuplicateIndex));
    }

    #[test]
    fn rejects_lsi_without_sort_attribute() {
        let mut s = TableSchema::composite("pk", ColumnType::String, "sk", ColumnType::String);
        s.indexes = vec![IndexDef {
            name: "lsi".into(),
            kind: IndexKind::Local,
            hash_attribute: "pk".into(),
            sort_attribute: None,
            projection: IndexProjection::All,
            status: IndexStatus::Active,
        }];
        assert_eq!(s.validate(), Err(SchemaError::LocalIndexMissingSort));
    }

    #[test]
    fn valid_indexes_pass_validation() {
        let mut s = TableSchema::simple("id", ColumnType::String);
        s.upsert_index(gsi("by-email", "email"));
        s.upsert_index(IndexDef {
            name: "by-ts".into(),
            kind: IndexKind::Local,
            hash_attribute: "id".into(),
            sort_attribute: Some("ts".into()),
            projection: IndexProjection::KeysOnly,
            status: IndexStatus::Active,
        });
        assert!(s.validate().is_ok());
    }

    #[test]
    fn catalog_insert_get_remove() {
        let mut cat = SchemaCatalog::new();
        assert!(cat.is_empty());
        cat.insert("t".into(), TableSchema::simple("id", ColumnType::String));
        assert!(cat.contains("t"));
        assert_eq!(cat.len(), 1);
        assert_eq!(cat.get("t").unwrap().partition_key, "id");
        assert_eq!(cat.table_names().collect::<Vec<_>>(), vec!["t"]);
        assert!(cat.remove("t"));
        assert!(!cat.remove("t"));
        assert!(cat.is_empty());
    }

    /// A status-less `IndexDef` (the JSON shape every pre-ADR-0045 fixture/
    /// persisted record has) deserializes with `status: Active` via
    /// `#[serde(default = "IndexStatus::active")]` — never `Creating`, since a
    /// record predating the field can never genuinely be mid-backfill (see
    /// `IndexStatus`'s own doc). A round-trip through a *populated* status
    /// also proves the field rides the wire at all once present.
    #[test]
    fn index_def_without_a_status_field_deserializes_as_active() {
        let json = r#"{
            "name": "by-email",
            "kind": "Global",
            "hash_attribute": "email",
            "sort_attribute": null,
            "projection": "All"
        }"#;
        let def: IndexDef = serde_json::from_str(json).expect("status-less IndexDef decodes");
        assert_eq!(def.status, IndexStatus::Active);
        assert_eq!(def, gsi("by-email", "email"));

        // And a populated status rides the wire unchanged, round-tripping.
        let mut creating = gsi("by-a", "a");
        creating.status = IndexStatus::Creating;
        let encoded = serde_json::to_string(&creating).unwrap();
        let decoded: IndexDef = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.status, IndexStatus::Creating);
        assert_eq!(decoded, creating);
    }
}
