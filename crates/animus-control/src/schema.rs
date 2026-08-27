//! The replicated **table-schema catalog** (ADR 0013): the cluster-wide,
//! Raft-replicated record of which tables exist and what their keys and columns
//! are.
//!
//! `animus-dynamo` previously kept table schemas in a per-process, in-memory
//! catalog, so a `CreateTable` neither survived a restart nor replicated to
//! other nodes. This module is the control-plane substrate that fixes that: a
//! [`TableSchema`] lives in [`Metadata`](crate::Metadata) and is mutated by
//! replicated [`MetaCommand`](crate::MetaCommand)s, so it is agreed
//! cluster-wide, durable (recovered from the WAL/snapshot like all metadata),
//! and consistent on every replica.
//!
//! ## A shape wider than DynamoDB alone needs
//!
//! DynamoDB declares a **partition key** and an optional **sort key** (its key
//! attributes are typed; non-key attributes are schemaless). This module's
//! shape (ADR 0006) was originally designed to also fit a CQL wire adapter
//! (`animus-cql`, since dropped — v1 is DynamoDB-only, ADR 0053) — a table
//! declared as an ordered list of typed
//! **columns**, one of which is the partition key, plus any number of
//! clustering columns. The union kept here, unused past its DynamoDB subset
//! today, is:
//!
//! - a [`partition_key`](TableSchema::partition_key): the single required key
//!   column, by name;
//! - any number of ordered [`clustering_keys`](TableSchema::clustering_keys):
//!   DynamoDB's optional sort key is the one-element case;
//! - a set of typed [`columns`](TableSchema::columns) (keyed and non-keyed
//!   alike), each carrying a [`ColumnType`].
//!
//! [`ColumnType`] carries a few scalar variants beyond DynamoDB's own key
//! types, kept for the same reason. Everything here is **pure and
//! deterministic** (ADR 0003): plain data, `BTreeMap`/`Vec`, no I/O, no clock,
//! no RNG.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// A table name (the catalog key). For DynamoDB it is the bare table name.
/// The control plane treats it as an opaque, case-sensitive identifier —
/// namespacing is the adapter's responsibility.
pub type TableName = String;

/// The column type vocabulary, wider than DynamoDB's own key-attribute
/// families alone (`String`/`Number`/`Binary`/`Bool`) — the extra variants
/// were originally the CQL scalar type system's own vocabulary (`text`/`int`/
/// `bigint`/`boolean`/`blob`/`uuid`, ADR 0006), kept even after CQL's removal
/// since the catalog stores only a declared type and never interprets a
/// value, so the breadth costs nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ColumnType {
    /// UTF-8 string (DynamoDB `S`).
    String,
    /// A number (DynamoDB `N`; prefer [`Int`](ColumnType::Int)
    /// / [`BigInt`](ColumnType::BigInt) where the width is known).
    Number,
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    BigInt,
    /// Boolean (DynamoDB `BOOL`).
    Bool,
    /// Arbitrary bytes (DynamoDB `B`).
    Binary,
    /// A 16-byte UUID.
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

/// A table's replicated **DynamoDB-style TTL** configuration (ADR 0051), when
/// enabled.
///
/// [`attribute_name`](TtlSpec::attribute_name) names an item attribute that,
/// when present, holds an **absolute Unix epoch second** as a DynamoDB `N`
/// (never milliseconds, never a relative duration — the same convention
/// DynamoDB itself uses). The control plane records **only this
/// declaration** — which attribute a table's items *may* carry an
/// expiration timestamp in — and never inspects an item itself; whatever
/// component actually deletes expired items (the wire edge / a background
/// sweep, outside this crate's scope) is the one that reads the attribute
/// and compares it against wall-clock time.
///
/// Consequently an item is "not expired" by simple absence of a positive
/// determination, not by an explicit check here: an item whose
/// `attribute_name` attribute is missing, is present but not a number (the
/// wrong DynamoDB type), or names a future instant is never treated as
/// expired. Only an item whose named attribute is a number in the past is a
/// candidate for expiry. This mirrors DynamoDB's own documented behavior and
/// keeps the control plane's job purely declarative — identical in spirit to
/// [`StreamSpec`]: the catalog stores the *shape* of the feature, never
/// interprets data.
///
/// Unlike [`StreamSpec`], a `TtlSpec` mints no identity label — there is
/// nothing here analogous to a stream's `(table, label)` pair, since TTL has
/// no downstream consumer that needs to distinguish "generations" of a
/// table's TTL configuration. That is also why re-enabling TTL with the same
/// attribute name is idempotent (a `NoOp`) rather than rejected the way
/// re-enabling an already-enabled stream is — see
/// [`MetaCommand::SetTableTtl`](crate::meta::MetaCommand::SetTableTtl)'s own
/// doc for the full apply semantics, including that changing the attribute
/// name in place (no disable/re-enable round trip required) is a legal
/// DynamoDB operation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlSpec {
    /// The item attribute name that, when present as a numeric (`N`)
    /// attribute, holds an absolute Unix epoch second past which the item is
    /// eligible for expiry.
    pub attribute_name: String,
}

/// A table's replicated **point-in-time recovery (PITR)** configuration (ADR
/// 0059 §9), when enabled via `UpdateContinuousBackups { Enabled: true }`.
///
/// Unlike [`StreamSpec`]'s `label` (a fresh string minted per enable) this
/// carries a monotonic [`generation`](Self::generation) — a small integer,
/// never reused even across a disable/re-enable cycle or a drop-and-recreate
/// of the table under the same name (`Metadata::pitr_generation`'s own
/// never-rewound counter is the allocator; see that field's doc). A PITR
/// sealing consumer licenses its `SealPitrSegment` proposals against the
/// table's *current* generation (or an existing catalog row's own
/// generation, for a disable-triggered final seal — mirroring
/// `SealStreamShard`'s label-licensing rule exactly), so two non-overlapping
/// "coverage epochs" of one table's PITR history can never be confused with
/// each other the way a bare boolean flag would risk.
///
/// `enabled_wall_ms` is this generation's own start of window — the ADR's
/// "enable starts the clock at now" rule — stamped at propose time by the
/// wire-serving node (`env.wall_now()`, the same ADR 0051 discipline every
/// other wall-clock-stamped command field in this catalog already follows).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitrSpec {
    /// This enable's own generation number (ADR 0059 §9's "disable then
    /// re-enable resets the window" — a fresh generation, never reused).
    pub generation: u64,
    /// Wall-clock time this generation was enabled, `env.wall_now()`-stamped
    /// at propose time — the basis `DescribeContinuousBackups`'
    /// `EarliestRestorableDateTime` floors against (never earlier than this).
    pub enabled_wall_ms: u64,
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
    /// sort key. More than one is a shape only a CQL-style multi-column
    /// clustering key ever used (ADR 0006, since dropped by ADR 0053).
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
    /// This table's DynamoDB Streams configuration (ADR 0042), if enabled.
    /// `None` for a table with no stream (the common case, and every schema
    /// persisted before this field existed — `#[serde(default)]`, additive
    /// like `indexes`). Mutated only through
    /// `MetaCommand::SetTableStream` (so it replicates).
    #[serde(default)]
    pub stream: Option<StreamSpec>,
    /// This table's DynamoDB-style TTL configuration (ADR 0051), if enabled.
    /// `None` for a table with no TTL (the common case, and every schema
    /// persisted before this field existed — `#[serde(default)]`, additive
    /// like `indexes`/`stream`). Mutated only through
    /// `MetaCommand::SetTableTtl` (so it replicates); see [`TtlSpec`] for
    /// what the control plane does and does not do with it.
    #[serde(default)]
    pub ttl: Option<TtlSpec>,
    /// This table's **point-in-time recovery (PITR)** configuration (ADR
    /// 0059 §9), if enabled. `None` for a table with no PITR (the common
    /// case, and every schema persisted before this field existed —
    /// `#[serde(default)]`, additive like `stream`/`ttl`). Mutated only
    /// through `MetaCommand::UpdateContinuousBackups` (so it replicates).
    #[serde(default)]
    pub pitr: Option<PitrSpec>,
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
            stream: None,
            ttl: None,
            pitr: None,
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
            stream: None,
            ttl: None,
            pitr: None,
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
            stream: None,
            ttl: None,
            pitr: None,
        }
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

    /// A `TableSchema` JSON blob predating the `ttl` field (no `ttl` key at
    /// all — the shape of every schema record written before ADR 0051)
    /// still deserializes, via `#[serde(default)]`, as `ttl: None` — the
    /// same additive contract `indexes`/`mode`/`stream` already carry. A
    /// round-trip through a *populated* `ttl` also proves the field rides
    /// the wire at all once present.
    #[test]
    fn table_schema_without_a_ttl_field_deserializes_as_none() {
        let json = r#"{
            "partition_key": "id",
            "clustering_keys": [],
            "columns": [{"name": "id", "ty": "String"}]
        }"#;
        let schema: TableSchema = serde_json::from_str(json).expect("ttl-less TableSchema decodes");
        assert_eq!(schema.ttl, None);
        assert_eq!(schema, TableSchema::simple("id", ColumnType::String));

        // And a populated ttl rides the wire unchanged, round-tripping.
        let mut with_ttl = TableSchema::simple("id", ColumnType::String);
        with_ttl.ttl = Some(TtlSpec {
            attribute_name: "expiresAt".to_string(),
        });
        let encoded = serde_json::to_string(&with_ttl).unwrap();
        let decoded: TableSchema = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.ttl, with_ttl.ttl);
        assert_eq!(decoded, with_ttl);
    }
}
