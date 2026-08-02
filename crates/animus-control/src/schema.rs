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
    /// sort key; many for CQL clustering columns.
    pub clustering_keys: Vec<String>,
    /// Every column (keys included), in declaration order.
    pub columns: Vec<ColumnDef>,
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
        Ok(())
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
}
