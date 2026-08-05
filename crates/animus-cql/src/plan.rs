//! Schema resolution + partition (de)serialization: the bridge from a parsed
//! [`Statement`](crate::query::Statement) to concrete data-plane operations.
//!
//! Pure and deterministic (ADR 0003). Given the [`Catalog`] and a connection's
//! `USE`d keyspace, this resolves an `INSERT`/`SELECT`/`UPDATE`/`DELETE` against
//! the table schema, type-checks (and parses) its literal/bound values, and
//! yields a plan the wire edge executes against the quorum coordinator.
//!
//! ## Why a *partition* is the unit of storage
//!
//! The data plane offers only point read/write/delete (no quorum range scan), so
//! a single data-plane key must hold everything a `SELECT pk = ?` must return.
//! With clustering columns one partition key maps to **many** rows (one per
//! distinct clustering-key tuple), so we store the **whole partition** as one
//! data-plane value keyed by `data_key(table, pk_key_bytes)` — a clustering-key
//! → row map. `INSERT`/`UPDATE`/`DELETE` are therefore read-modify-write: the
//! edge quorum-reads the current partition, applies the mutation here (pure),
//! and quorum-writes (or tombstones, when the partition becomes empty) the
//! result. A `SELECT` decodes the partition and returns the matching rows in
//! clustering order (a `BTreeMap` keyed by the order-preserving clustering
//! bytes), filtering to one row when the clustering key is fully specified.
//!
//! ## Partition storage format
//!
//! ```text
//! u8   format byte (ROW_FORMAT_V2)
//! u16  row count
//! repeat row count:
//!   u32  clustering-blob length, then that many bytes (the ordered, length-
//!        prefixed `to_key_bytes` of each clustering value; empty when the table
//!        has no clustering columns — then there is exactly one row)
//!   u16  non-key cell count, then per cell: u16 schema column index,
//!        u32 cell length, cell bytes
//! ```
//!
//! Neither the partition key nor the clustering keys are stored in a row's cells
//! (the pk round-trips through the data-plane key; the clustering values are the
//! row's map key and decode back from the clustering blob). A `SELECT`
//! reconstructs primary-key cells from the resolved predicate / clustering blob.

use std::collections::BTreeMap;

use crate::catalog::{Catalog, CatalogError, TableSchema};
use crate::query::{CreateTable, Delete, Insert, Predicate, Select, Term, Update};
use crate::types::{CqlType, CqlValue, ValueError};

/// The format byte prefixing a stored partition value (v2 = clustering-aware).
const ROW_FORMAT_V2: u8 = 2;

/// A resolved column for a result set: its name, type, and its index in the
/// table schema (so a `SELECT` can look the column's stored cell up in a row,
/// which is keyed by schema index).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    /// The column name.
    pub name: String,
    /// The column type.
    pub ty: CqlType,
    /// The column's index in the table schema's column list.
    pub schema_index: usize,
}

/// A single decoded row: its clustering values (in clustering order, empty for a
/// clustering-free table) plus its non-key cells by schema index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    /// The clustering-key values, in clustering order.
    pub clustering: Vec<CqlValue>,
    /// Non-key column cells, keyed by schema column index.
    pub cells: BTreeMap<usize, Vec<u8>>,
}

/// Why planning failed (distinct from a frame/parse error).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    /// A catalog lookup failed.
    Catalog(CatalogError),
    /// A value did not type-check / parse.
    Value(ValueError),
    /// A referenced column is not in the table.
    NoSuchColumn(String),
    /// The partition key was missing from an `INSERT`.
    MissingPartitionKey(String),
    /// A clustering-key column was missing where it is required (full primary
    /// key needed — e.g. on `INSERT` or `UPDATE`).
    MissingClusteringKey(String),
    /// The `WHERE`/assignment tried to set or filter a key column wrongly.
    NotPartitionKey {
        /// The column used in the predicate.
        used: String,
        /// The table's actual partition-key column.
        expected: String,
    },
    /// A `WHERE` predicate referenced a column out of primary-key order, or a
    /// non-key column (only `pk` then clustering keys in order are allowed).
    BadPredicate(String),
    /// An `UPDATE` `SET` tried to assign a primary-key column.
    AssignsPrimaryKey(String),
    /// The number of supplied bind values did not match the markers.
    BindCountMismatch {
        /// Markers in the statement.
        expected: usize,
        /// Values supplied.
        got: usize,
    },
    /// A stored partition value was malformed.
    CorruptRow,
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::Catalog(e) => write!(f, "{e}"),
            PlanError::Value(e) => write!(f, "{e}"),
            PlanError::NoSuchColumn(c) => write!(f, "undefined column `{c}`"),
            PlanError::MissingPartitionKey(c) => {
                write!(f, "missing partition key column `{c}` in INSERT")
            }
            PlanError::MissingClusteringKey(c) => {
                write!(f, "missing clustering key column `{c}`")
            }
            PlanError::NotPartitionKey { used, expected } => write!(
                f,
                "WHERE must filter on partition key `{expected}`, got `{used}`"
            ),
            PlanError::BadPredicate(why) => write!(f, "unsupported WHERE predicate: {why}"),
            PlanError::AssignsPrimaryKey(c) => {
                write!(f, "cannot assign primary-key column `{c}` in UPDATE")
            }
            PlanError::BindCountMismatch { expected, got } => {
                write!(f, "expected {expected} bound values, got {got}")
            }
            PlanError::CorruptRow => write!(f, "stored partition is corrupt"),
        }
    }
}

impl std::error::Error for PlanError {}

impl From<CatalogError> for PlanError {
    fn from(e: CatalogError) -> Self {
        PlanError::Catalog(e)
    }
}
impl From<ValueError> for PlanError {
    fn from(e: ValueError) -> Self {
        PlanError::Value(e)
    }
}

// --- resolved plans ---------------------------------------------------------

/// A resolved `INSERT`: the partition key + the single row to upsert into it.
/// `INSERT` is a read-modify-write: the edge reads the partition at [`key`],
/// merges this row (by its clustering bytes), and writes the partition back.
///
/// [`key`]: InsertPlan::key
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InsertPlan {
    /// The resolved (qualified) table name, for result metadata.
    pub table: String,
    /// The data-plane key for the whole partition.
    pub key: Vec<u8>,
    /// The clustering-key bytes (the row's map key within the partition).
    pub clustering: Vec<u8>,
    /// The row to upsert.
    pub row: Row,
}

/// A resolved `UPDATE`: like an [`InsertPlan`] but the merged row's cells are
/// applied over whatever already exists for the clustering key (a missing row is
/// created — CQL `UPDATE` is an upsert).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdatePlan {
    /// The resolved table name.
    pub table: String,
    /// The data-plane key for the whole partition.
    pub key: Vec<u8>,
    /// The clustering-key bytes of the row to update.
    pub clustering: Vec<u8>,
    /// The non-key cell assignments, by schema index.
    pub assignments: BTreeMap<usize, Vec<u8>>,
}

/// A resolved `DELETE`. `clustering` is `Some(bytes)` to remove one row (a full
/// primary key), or `None` to remove the whole partition. The edge reads the
/// partition, removes the row(s), and writes the remainder back — tombstoning
/// the data-plane key when the partition becomes empty.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeletePlan {
    /// The resolved table name.
    pub table: String,
    /// The data-plane key for the whole partition.
    pub key: Vec<u8>,
    /// The clustering-key bytes of the single row to delete, or `None` for the
    /// whole partition.
    pub clustering: Option<Vec<u8>>,
}

/// A resolved `SELECT`: the partition key plus the projection and the resolved
/// primary-key predicate (so the edge can reconstruct primary-key cells and, if
/// a clustering key was fully specified, filter to that one row).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadPlan {
    /// The resolved table name, for result metadata.
    pub table: String,
    /// The data-plane key to quorum-read (the whole partition).
    pub key: Vec<u8>,
    /// The columns to return, in projection order (`*` expands to all columns).
    pub projection: Vec<ColumnSpec>,
    /// The partition-key value (echoed back; not stored in a row).
    pub pk_value: CqlValue,
    /// The partition-key column name.
    pub pk_name: String,
    /// The resolved clustering-key prefix from the `WHERE` clause, in clustering
    /// order. When it covers every clustering column the `SELECT` returns at most
    /// one row; a shorter (possibly empty) prefix returns all matching rows in
    /// clustering order.
    pub clustering_prefix: Vec<CqlValue>,
}

// --- bind-type resolution (for PREPARE) -------------------------------------

/// Resolve a value term to a typed value of `ty`. A `Term::Bind` pulls the next
/// value from `binds` (advancing `next_bind`); a literal is parsed for `ty`.
fn resolve_term(
    ty: CqlType,
    term: &Term,
    binds: &[CqlValue],
    next_bind: &mut usize,
) -> Result<CqlValue, PlanError> {
    match term {
        Term::Bind => {
            let v = binds.get(*next_bind).ok_or(PlanError::BindCountMismatch {
                expected: *next_bind + 1,
                got: binds.len(),
            })?;
            *next_bind += 1;
            if v.cql_type() != ty {
                return Err(PlanError::Value(ValueError::TypeMismatch {
                    expected: ty,
                    got: v.clone(),
                }));
            }
            Ok(v.clone())
        }
        Term::Literal { text, quoted } => Ok(ty.parse_literal(text, *quoted)?),
    }
}

fn spec_of(schema: &TableSchema, idx: usize) -> ColumnSpec {
    ColumnSpec {
        name: schema.columns[idx].name.clone(),
        ty: schema.columns[idx].ty,
        schema_index: idx,
    }
}

/// Resolve the column types (in order) for the bind markers of an `INSERT`, so a
/// `PREPARE` can advertise the correct `[col spec]` for each `?`.
pub fn insert_bind_types(
    catalog: &Catalog,
    selected: Option<&str>,
    ins: &Insert,
) -> Result<Vec<ColumnSpec>, PlanError> {
    let schema = catalog.resolve(ins.keyspace.as_deref(), selected, &ins.table)?;
    let mut specs = Vec::new();
    for (col, term) in ins.columns.iter().zip(&ins.values) {
        if matches!(term, Term::Bind) {
            let idx = schema
                .column_index(col)
                .ok_or_else(|| PlanError::NoSuchColumn(col.clone()))?;
            specs.push(spec_of(schema, idx));
        }
    }
    Ok(specs)
}

/// Resolve the bind-marker types for a `SELECT`'s `WHERE` predicates, in order.
pub fn select_bind_types(
    catalog: &Catalog,
    selected: Option<&str>,
    sel: &Select,
) -> Result<Vec<ColumnSpec>, PlanError> {
    let schema = catalog.resolve(sel.keyspace.as_deref(), selected, &sel.table)?;
    predicate_bind_types(schema, &sel.predicates)
}

/// Resolve bind-marker types for an `UPDATE` — its `SET` assignments first
/// (left to right), then its `WHERE` predicates.
pub fn update_bind_types(
    catalog: &Catalog,
    selected: Option<&str>,
    upd: &Update,
) -> Result<Vec<ColumnSpec>, PlanError> {
    let schema = catalog.resolve(upd.keyspace.as_deref(), selected, &upd.table)?;
    let mut specs = Vec::new();
    for (col, term) in &upd.assignments {
        if matches!(term, Term::Bind) {
            let idx = schema
                .column_index(col)
                .ok_or_else(|| PlanError::NoSuchColumn(col.clone()))?;
            specs.push(spec_of(schema, idx));
        }
    }
    specs.extend(predicate_bind_types(schema, &upd.predicates)?);
    Ok(specs)
}

/// Resolve bind-marker types for a `DELETE`'s `WHERE` predicates.
pub fn delete_bind_types(
    catalog: &Catalog,
    selected: Option<&str>,
    del: &Delete,
) -> Result<Vec<ColumnSpec>, PlanError> {
    let schema = catalog.resolve(del.keyspace.as_deref(), selected, &del.table)?;
    predicate_bind_types(schema, &del.predicates)
}

fn predicate_bind_types(
    schema: &TableSchema,
    predicates: &[Predicate],
) -> Result<Vec<ColumnSpec>, PlanError> {
    let mut specs = Vec::new();
    for pred in predicates {
        if matches!(pred.value, Term::Bind) {
            let idx = schema
                .column_index(&pred.column)
                .ok_or_else(|| PlanError::NoSuchColumn(pred.column.clone()))?;
            specs.push(spec_of(schema, idx));
        }
    }
    Ok(specs)
}

/// Resolve a `CREATE TABLE` into a [`TableSchema`].
#[must_use]
pub fn schema_of(ct: &CreateTable) -> TableSchema {
    let columns: Vec<crate::catalog::Column> = ct
        .columns
        .iter()
        .map(|(name, ty)| crate::catalog::Column {
            name: name.clone(),
            ty: *ty,
        })
        .collect();
    let partition_key = columns
        .iter()
        .position(|c| c.name.eq_ignore_ascii_case(&ct.partition_key))
        .unwrap_or(0);
    let clustering_keys = ct
        .clustering_keys
        .iter()
        .filter_map(|name| {
            columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
        })
        .collect();
    TableSchema {
        name: ct.table.clone(),
        columns,
        partition_key,
        clustering_keys,
    }
}

// --- WHERE / clustering-key resolution --------------------------------------

/// Resolve the `WHERE` predicates of a `SELECT`/`UPDATE`/`DELETE` against the
/// schema. Returns `(pk_value, clustering_prefix)` where the prefix is the
/// clustering values supplied, in clustering order. Enforces that the first
/// predicate is the partition key and any further predicates are the clustering
/// columns in order (equality only).
fn resolve_where(
    schema: &TableSchema,
    predicates: &[Predicate],
    binds: &[CqlValue],
    next_bind: &mut usize,
) -> Result<(CqlValue, Vec<CqlValue>), PlanError> {
    let pk_name = &schema.pk_column().name;
    let Some(first) = predicates.first() else {
        return Err(PlanError::BadPredicate("WHERE clause is required".into()));
    };
    if !first.column.eq_ignore_ascii_case(pk_name) {
        return Err(PlanError::NotPartitionKey {
            used: first.column.clone(),
            expected: pk_name.clone(),
        });
    }
    let pk_value = resolve_term(schema.pk_column().ty, &first.value, binds, next_bind)?;

    let mut clustering = Vec::new();
    for (i, pred) in predicates[1..].iter().enumerate() {
        let Some(&ck_idx) = schema.clustering_keys.get(i) else {
            return Err(PlanError::BadPredicate(format!(
                "`{}` is not a clustering-key column (or is out of order)",
                pred.column
            )));
        };
        let ck_col = &schema.columns[ck_idx];
        if !pred.column.eq_ignore_ascii_case(&ck_col.name) {
            return Err(PlanError::BadPredicate(format!(
                "clustering key #{} must be `{}`, got `{}`",
                i + 1,
                ck_col.name,
                pred.column
            )));
        }
        clustering.push(resolve_term(ck_col.ty, &pred.value, binds, next_bind)?);
    }
    Ok((pk_value, clustering))
}

/// Encode an ordered, decodable clustering blob from clustering values: each
/// value's `to_key_bytes` length-prefixed (`u32`) and concatenated. Empty for a
/// clustering-free table. Lexicographic byte order over the blob matches
/// clustering order because each component is order-preserving and
/// length-delimited at a fixed-width prefix.
#[must_use]
pub fn encode_clustering(values: &[CqlValue]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        let bytes = v.to_key_bytes();
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(&bytes);
    }
    out
}

// --- INSERT / UPDATE / DELETE / SELECT planning -----------------------------

/// Plan an `INSERT`: resolve columns/values into a single [`Row`] keyed by its
/// clustering bytes, plus the partition data-plane key.
///
/// # Errors
/// A [`PlanError`] for any schema/type/bind mismatch, or a missing pk/clustering.
pub fn plan_insert(
    catalog: &Catalog,
    selected: Option<&str>,
    ins: &Insert,
    binds: &[CqlValue],
) -> Result<InsertPlan, PlanError> {
    let schema = catalog.resolve(ins.keyspace.as_deref(), selected, &ins.table)?;
    let mut next_bind = 0;

    let mut by_index: BTreeMap<usize, CqlValue> = BTreeMap::new();
    for (col, term) in ins.columns.iter().zip(&ins.values) {
        let idx = schema
            .column_index(col)
            .ok_or_else(|| PlanError::NoSuchColumn(col.clone()))?;
        let value = resolve_term(schema.columns[idx].ty, term, binds, &mut next_bind)?;
        by_index.insert(idx, value);
    }
    if next_bind != binds.len() {
        return Err(PlanError::BindCountMismatch {
            expected: next_bind,
            got: binds.len(),
        });
    }

    // The partition key must be present.
    let pk_idx = schema.partition_key;
    let pk_value = by_index
        .get(&pk_idx)
        .ok_or_else(|| PlanError::MissingPartitionKey(schema.pk_column().name.clone()))?;
    let key = crate::query::data_key(&pk_value.to_key_bytes());

    // Every clustering key must be present (a full primary key on INSERT).
    let mut clustering_values = Vec::new();
    for &ck_idx in &schema.clustering_keys {
        let v = by_index
            .get(&ck_idx)
            .ok_or_else(|| PlanError::MissingClusteringKey(schema.columns[ck_idx].name.clone()))?;
        clustering_values.push(v.clone());
    }
    let clustering = encode_clustering(&clustering_values);

    // The row's non-key cells (skip the pk + all clustering keys).
    let mut cells = BTreeMap::new();
    for (idx, val) in &by_index {
        if schema.is_primary_key(*idx) {
            continue;
        }
        cells.insert(*idx, schema.columns[*idx].ty.encode(val)?);
    }

    Ok(InsertPlan {
        table: schema.name.clone(),
        key,
        clustering,
        row: Row {
            clustering: clustering_values,
            cells,
        },
    })
}

/// Plan an `UPDATE`: resolve the `SET` assignments + a full primary-key `WHERE`.
///
/// # Errors
/// A [`PlanError`] if the `WHERE` does not fully specify the primary key, a `SET`
/// assigns a key column, or any value mismatches.
pub fn plan_update(
    catalog: &Catalog,
    selected: Option<&str>,
    upd: &Update,
    binds: &[CqlValue],
) -> Result<UpdatePlan, PlanError> {
    let schema = catalog.resolve(upd.keyspace.as_deref(), selected, &upd.table)?;
    let mut next_bind = 0;

    let mut assignments: BTreeMap<usize, Vec<u8>> = BTreeMap::new();
    for (col, term) in &upd.assignments {
        let idx = schema
            .column_index(col)
            .ok_or_else(|| PlanError::NoSuchColumn(col.clone()))?;
        if schema.is_primary_key(idx) {
            return Err(PlanError::AssignsPrimaryKey(
                schema.columns[idx].name.clone(),
            ));
        }
        let value = resolve_term(schema.columns[idx].ty, term, binds, &mut next_bind)?;
        assignments.insert(idx, schema.columns[idx].ty.encode(&value)?);
    }

    let (pk_value, clustering_values) =
        resolve_where(schema, &upd.predicates, binds, &mut next_bind)?;
    if clustering_values.len() != schema.clustering_keys.len() {
        return Err(PlanError::MissingClusteringKey(
            "UPDATE requires every clustering key in WHERE".into(),
        ));
    }
    if next_bind != binds.len() {
        return Err(PlanError::BindCountMismatch {
            expected: next_bind,
            got: binds.len(),
        });
    }

    let key = crate::query::data_key(&pk_value.to_key_bytes());
    Ok(UpdatePlan {
        table: schema.name.clone(),
        key,
        clustering: encode_clustering(&clustering_values),
        assignments,
    })
}

/// Plan a `DELETE`: resolve the `WHERE`. A full primary key deletes one row; a
/// partition-key-only `WHERE` deletes the whole partition.
///
/// # Errors
/// A [`PlanError`] for any schema/type/bind mismatch.
pub fn plan_delete(
    catalog: &Catalog,
    selected: Option<&str>,
    del: &Delete,
    binds: &[CqlValue],
) -> Result<DeletePlan, PlanError> {
    let schema = catalog.resolve(del.keyspace.as_deref(), selected, &del.table)?;
    let mut next_bind = 0;
    let (pk_value, clustering_values) =
        resolve_where(schema, &del.predicates, binds, &mut next_bind)?;
    if next_bind != binds.len() {
        return Err(PlanError::BindCountMismatch {
            expected: next_bind,
            got: binds.len(),
        });
    }
    let key = crate::query::data_key(&pk_value.to_key_bytes());
    // A full clustering key targets one row; anything shorter targets the whole
    // partition (CQL allows `DELETE FROM t WHERE pk = ?` to drop a partition).
    let clustering = if clustering_values.len() == schema.clustering_keys.len()
        && !schema.clustering_keys.is_empty()
    {
        Some(encode_clustering(&clustering_values))
    } else {
        None
    };
    Ok(DeletePlan {
        table: schema.name.clone(),
        key,
        clustering,
    })
}

/// Plan a `SELECT`: resolve the projection + the partition/clustering predicate.
///
/// # Errors
/// A [`PlanError`] for any schema/type/bind mismatch, or a predicate not led by
/// the partition key.
pub fn plan_select(
    catalog: &Catalog,
    selected: Option<&str>,
    sel: &Select,
    binds: &[CqlValue],
) -> Result<ReadPlan, PlanError> {
    let schema = catalog.resolve(sel.keyspace.as_deref(), selected, &sel.table)?;
    let mut next_bind = 0;
    let (pk_value, clustering_prefix) =
        resolve_where(schema, &sel.predicates, binds, &mut next_bind)?;
    if next_bind != binds.len() {
        return Err(PlanError::BindCountMismatch {
            expected: next_bind,
            got: binds.len(),
        });
    }
    let key = crate::query::data_key(&pk_value.to_key_bytes());

    let projection: Vec<ColumnSpec> = if sel.projection.is_empty() {
        (0..schema.columns.len())
            .map(|i| spec_of(schema, i))
            .collect()
    } else {
        let mut specs = Vec::new();
        for col in &sel.projection {
            let idx = schema
                .column_index(col)
                .ok_or_else(|| PlanError::NoSuchColumn(col.clone()))?;
            specs.push(spec_of(schema, idx));
        }
        specs
    };

    Ok(ReadPlan {
        table: schema.name.clone(),
        key,
        projection,
        pk_value,
        pk_name: schema.pk_column().name.clone(),
        clustering_prefix,
    })
}

// --- partition (de)serialization --------------------------------------------

/// A decoded partition: an ordered map of clustering-blob → [`Row`]. The map's
/// `BTreeMap` key is the order-preserving clustering blob, so iteration yields
/// rows in clustering order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Partition {
    /// clustering blob → row.
    pub rows: BTreeMap<Vec<u8>, Row>,
}

impl Partition {
    /// An empty partition.
    #[must_use]
    pub fn new() -> Partition {
        Partition::default()
    }

    /// Whether the partition holds no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Decode a stored partition value. An empty/absent value decodes to an empty
    /// partition (so a read-modify-write of a missing key starts fresh).
    ///
    /// # Errors
    /// [`PlanError::CorruptRow`] if non-empty bytes do not match the format.
    pub fn decode(value: &[u8], schema: &TableSchema) -> Result<Partition, PlanError> {
        if value.is_empty() {
            return Ok(Partition::new());
        }
        if value[0] != ROW_FORMAT_V2 {
            return Err(PlanError::CorruptRow);
        }
        let mut pos = 1;
        let row_count = read_u16(value, &mut pos)? as usize;
        let mut rows = BTreeMap::new();
        for _ in 0..row_count {
            let clen = read_u32(value, &mut pos)? as usize;
            let cend = pos.checked_add(clen).ok_or(PlanError::CorruptRow)?;
            if cend > value.len() {
                return Err(PlanError::CorruptRow);
            }
            let clustering_blob = value[pos..cend].to_vec();
            pos = cend;
            let cell_count = read_u16(value, &mut pos)? as usize;
            let mut cells = BTreeMap::new();
            for _ in 0..cell_count {
                let idx = read_u16(value, &mut pos)? as usize;
                let len = read_u32(value, &mut pos)? as usize;
                let end = pos.checked_add(len).ok_or(PlanError::CorruptRow)?;
                if end > value.len() {
                    return Err(PlanError::CorruptRow);
                }
                cells.insert(idx, value[pos..end].to_vec());
                pos = end;
            }
            let clustering = decode_clustering(&clustering_blob, schema)?;
            rows.insert(clustering_blob, Row { clustering, cells });
        }
        Ok(Partition { rows })
    }

    /// Encode the partition to a stored value (in clustering order).
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![ROW_FORMAT_V2];
        out.extend_from_slice(&(self.rows.len() as u16).to_be_bytes());
        for (clustering, row) in &self.rows {
            out.extend_from_slice(&(clustering.len() as u32).to_be_bytes());
            out.extend_from_slice(clustering);
            out.extend_from_slice(&(row.cells.len() as u16).to_be_bytes());
            for (idx, cell) in &row.cells {
                out.extend_from_slice(&(*idx as u16).to_be_bytes());
                out.extend_from_slice(&(cell.len() as u32).to_be_bytes());
                out.extend_from_slice(cell);
            }
        }
        out
    }

    /// The rows whose clustering values start with `prefix` (in clustering
    /// order). An empty prefix returns every row.
    #[must_use]
    pub fn rows_matching(&self, prefix: &[CqlValue]) -> Vec<&Row> {
        self.rows
            .values()
            .filter(|row| {
                prefix.len() <= row.clustering.len()
                    && prefix.iter().zip(&row.clustering).all(|(a, b)| a == b)
            })
            .collect()
    }
}

/// Decode a clustering blob back into its clustering values, typed per the
/// schema's clustering columns.
fn decode_clustering(blob: &[u8], schema: &TableSchema) -> Result<Vec<CqlValue>, PlanError> {
    let mut out = Vec::new();
    let mut pos = 0;
    for &ck_idx in &schema.clustering_keys {
        if pos >= blob.len() {
            break;
        }
        let len = read_u32(blob, &mut pos)? as usize;
        let end = pos.checked_add(len).ok_or(PlanError::CorruptRow)?;
        if end > blob.len() {
            return Err(PlanError::CorruptRow);
        }
        let ty = schema.columns[ck_idx].ty;
        out.push(decode_key_value(ty, &blob[pos..end])?);
        pos = end;
    }
    Ok(out)
}

/// Decode the order-preserving key bytes of a value (the inverse of
/// [`CqlValue::to_key_bytes`]).
fn decode_key_value(ty: CqlType, bytes: &[u8]) -> Result<CqlValue, PlanError> {
    match ty {
        CqlType::Int => {
            let arr: [u8; 4] = bytes.try_into().map_err(|_| PlanError::CorruptRow)?;
            Ok(CqlValue::Int(
                (u32::from_be_bytes(arr) ^ 0x8000_0000) as i32,
            ))
        }
        CqlType::BigInt => {
            let arr: [u8; 8] = bytes.try_into().map_err(|_| PlanError::CorruptRow)?;
            Ok(CqlValue::BigInt(
                (u64::from_be_bytes(arr) ^ 0x8000_0000_0000_0000) as i64,
            ))
        }
        // For these, key bytes are the cell bytes.
        other => other.decode(bytes).map_err(PlanError::Value),
    }
}

fn read_u16(buf: &[u8], pos: &mut usize) -> Result<u16, PlanError> {
    let end = pos.checked_add(2).ok_or(PlanError::CorruptRow)?;
    if end > buf.len() {
        return Err(PlanError::CorruptRow);
    }
    let v = u16::from_be_bytes([buf[*pos], buf[*pos + 1]]);
    *pos = end;
    Ok(v)
}

fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32, PlanError> {
    let end = pos.checked_add(4).ok_or(PlanError::CorruptRow)?;
    if end > buf.len() {
        return Err(PlanError::CorruptRow);
    }
    let v = u32::from_be_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos = end;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, TableSchema};
    use crate::query::{Statement, parse_statement};

    fn catalog() -> Catalog {
        let mut cat = Catalog::new();
        cat.create_keyspace("app");
        cat.create_table(
            "app",
            TableSchema {
                name: "users".into(),
                columns: vec![
                    Column {
                        name: "id".into(),
                        ty: CqlType::Int,
                    },
                    Column {
                        name: "name".into(),
                        ty: CqlType::Text,
                    },
                    Column {
                        name: "active".into(),
                        ty: CqlType::Boolean,
                    },
                ],
                partition_key: 0,
                clustering_keys: vec![],
            },
            false,
        )
        .unwrap();
        cat
    }

    /// A table with a clustering key: `events(room text, seq int, msg text,
    /// PRIMARY KEY (room, seq))`.
    fn clustered_catalog() -> Catalog {
        let mut cat = Catalog::new();
        cat.create_keyspace("app");
        cat.create_table(
            "app",
            TableSchema {
                name: "events".into(),
                columns: vec![
                    Column {
                        name: "room".into(),
                        ty: CqlType::Text,
                    },
                    Column {
                        name: "seq".into(),
                        ty: CqlType::Int,
                    },
                    Column {
                        name: "msg".into(),
                        ty: CqlType::Text,
                    },
                ],
                partition_key: 0,
                clustering_keys: vec![1],
            },
            false,
        )
        .unwrap();
        cat
    }

    fn insert(cat: &Catalog, cql: &str) -> InsertPlan {
        let Statement::Insert(ins) = parse_statement(cql).unwrap() else {
            panic!("expected insert")
        };
        plan_insert(cat, Some("app"), &ins, &[]).unwrap()
    }

    #[test]
    fn insert_then_select_round_trips_typed() {
        let cat = catalog();
        let write = insert(
            &cat,
            "INSERT INTO users (id, name, active) VALUES (7, 'Ada', true)",
        );

        let Statement::Select(sel) = parse_statement("SELECT * FROM users WHERE id = 7").unwrap()
        else {
            panic!()
        };
        let read = plan_select(&cat, Some("app"), &sel, &[]).unwrap();
        assert_eq!(read.key, write.key, "same key for same pk");
        assert_eq!(read.pk_value, CqlValue::Int(7));

        // Materialize a partition with the inserted row and read it back.
        let schema = cat.resolve(None, Some("app"), "users").unwrap();
        let mut part = Partition::new();
        part.rows
            .insert(write.clustering.clone(), write.row.clone());
        let bytes = part.encode();
        let decoded = Partition::decode(&bytes, schema).unwrap();
        let rows = decoded.rows_matching(&read.clustering_prefix);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            CqlType::Text.decode(&rows[0].cells[&1]).unwrap(),
            CqlValue::Text("Ada".into())
        );
        assert!(!rows[0].cells.contains_key(&0), "pk not stored");
    }

    #[test]
    fn clustered_partition_orders_rows_by_clustering_key() {
        let cat = clustered_catalog();
        let schema = cat.resolve(None, Some("app"), "events").unwrap();
        let mut part = Partition::new();
        // Insert out of order: seq 3, 1, 2.
        for (seq, msg) in [(3, "c"), (1, "a"), (2, "b")] {
            let p = insert(
                &cat,
                &format!("INSERT INTO events (room, seq, msg) VALUES ('r1', {seq}, '{msg}')"),
            );
            part.rows.insert(p.clustering, p.row);
        }
        let bytes = part.encode();
        let decoded = Partition::decode(&bytes, schema).unwrap();
        // SELECT * WHERE room = 'r1' → all three rows in seq order.
        let rows = decoded.rows_matching(&[]);
        assert_eq!(rows.len(), 3);
        let seqs: Vec<i32> = rows
            .iter()
            .map(|r| match r.clustering[0] {
                CqlValue::Int(n) => n,
                _ => panic!(),
            })
            .collect();
        assert_eq!(seqs, vec![1, 2, 3], "rows come back clustering-ordered");

        // SELECT one row by full primary key.
        let Statement::Select(sel) =
            parse_statement("SELECT msg FROM events WHERE room = 'r1' AND seq = 2").unwrap()
        else {
            panic!()
        };
        let read = plan_select(&cat, Some("app"), &sel, &[]).unwrap();
        let one = decoded.rows_matching(&read.clustering_prefix);
        assert_eq!(one.len(), 1);
        assert_eq!(
            CqlType::Text.decode(&one[0].cells[&2]).unwrap(),
            CqlValue::Text("b".into())
        );
    }

    #[test]
    fn update_assigns_non_key_cells() {
        let cat = clustered_catalog();
        let Statement::Update(upd) =
            parse_statement("UPDATE events SET msg = 'edited' WHERE room = 'r1' AND seq = 2")
                .unwrap()
        else {
            panic!()
        };
        let plan = plan_update(&cat, Some("app"), &upd, &[]).unwrap();
        assert_eq!(plan.assignments.len(), 1);
        assert_eq!(
            CqlType::Text.decode(&plan.assignments[&2]).unwrap(),
            CqlValue::Text("edited".into())
        );
    }

    #[test]
    fn update_rejects_assigning_primary_key() {
        let cat = clustered_catalog();
        let Statement::Update(upd) =
            parse_statement("UPDATE events SET seq = 9 WHERE room = 'r1' AND seq = 2").unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            plan_update(&cat, Some("app"), &upd, &[]),
            Err(PlanError::AssignsPrimaryKey(_))
        ));
    }

    #[test]
    fn delete_one_row_vs_whole_partition() {
        let cat = clustered_catalog();
        // Full primary key → single-row delete.
        let Statement::Delete(d) =
            parse_statement("DELETE FROM events WHERE room = 'r1' AND seq = 2").unwrap()
        else {
            panic!()
        };
        let plan = plan_delete(&cat, Some("app"), &d, &[]).unwrap();
        assert!(plan.clustering.is_some());

        // Partition-key only → whole-partition delete.
        let Statement::Delete(d2) =
            parse_statement("DELETE FROM events WHERE room = 'r1'").unwrap()
        else {
            panic!()
        };
        let plan2 = plan_delete(&cat, Some("app"), &d2, &[]).unwrap();
        assert!(plan2.clustering.is_none());
    }

    #[test]
    fn binds_resolve_by_position_and_type() {
        let cat = catalog();
        let Statement::Insert(ins) =
            parse_statement("INSERT INTO users (id, name) VALUES (?, ?)").unwrap()
        else {
            panic!()
        };
        let binds = vec![CqlValue::Int(1), CqlValue::Text("Grace".into())];
        let plan = plan_insert(&cat, Some("app"), &ins, &binds).unwrap();
        assert_eq!(
            CqlType::Text.decode(&plan.row.cells[&1]).unwrap(),
            CqlValue::Text("Grace".into())
        );
    }

    #[test]
    fn bind_type_mismatch_rejected() {
        let cat = catalog();
        let Statement::Insert(ins) =
            parse_statement("INSERT INTO users (id, name) VALUES (?, ?)").unwrap()
        else {
            panic!()
        };
        let binds = vec![CqlValue::Text("x".into()), CqlValue::Text("y".into())];
        assert!(matches!(
            plan_insert(&cat, Some("app"), &ins, &binds),
            Err(PlanError::Value(ValueError::TypeMismatch { .. }))
        ));
    }

    #[test]
    fn select_on_non_pk_rejected() {
        let cat = catalog();
        let Statement::Select(sel) =
            parse_statement("SELECT * FROM users WHERE name = 'x'").unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            plan_select(&cat, Some("app"), &sel, &[]),
            Err(PlanError::NotPartitionKey { .. })
        ));
    }

    #[test]
    fn missing_partition_key_rejected() {
        let cat = catalog();
        let Statement::Insert(ins) =
            parse_statement("INSERT INTO users (name) VALUES ('x')").unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            plan_insert(&cat, Some("app"), &ins, &[]),
            Err(PlanError::MissingPartitionKey(_))
        ));
    }

    #[test]
    fn insert_missing_clustering_key_rejected() {
        let cat = clustered_catalog();
        let Statement::Insert(ins) =
            parse_statement("INSERT INTO events (room, msg) VALUES ('r1', 'x')").unwrap()
        else {
            panic!()
        };
        assert!(matches!(
            plan_insert(&cat, Some("app"), &ins, &[]),
            Err(PlanError::MissingClusteringKey(_))
        ));
    }

    #[test]
    fn prepared_bind_types_are_resolved() {
        let cat = catalog();
        let Statement::Insert(ins) =
            parse_statement("INSERT INTO users (id, name) VALUES (?, ?)").unwrap()
        else {
            panic!()
        };
        let specs = insert_bind_types(&cat, Some("app"), &ins).unwrap();
        assert_eq!(specs[0].ty, CqlType::Int);
        assert_eq!(specs[1].ty, CqlType::Text);

        // UPDATE bind types: SET assignment then WHERE predicate.
        let cat2 = clustered_catalog();
        let Statement::Update(upd) =
            parse_statement("UPDATE events SET msg = ? WHERE room = ? AND seq = ?").unwrap()
        else {
            panic!()
        };
        let specs = update_bind_types(&cat2, Some("app"), &upd).unwrap();
        assert_eq!(specs.len(), 3);
        assert_eq!(specs[0].ty, CqlType::Text); // msg
        assert_eq!(specs[1].ty, CqlType::Text); // room
        assert_eq!(specs[2].ty, CqlType::Int); // seq
    }
}
