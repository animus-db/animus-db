//! Schema resolution + row (de)serialization: the bridge from a parsed
//! [`Statement`](crate::query::Statement) to concrete data-plane operations.
//!
//! Pure and deterministic (ADR 0003). Given the [`Catalog`] and a connection's
//! `USE`d keyspace, this resolves an `INSERT`/`SELECT` against the table schema,
//! type-checks (and parses) its literal/bound values, and yields a [`WritePlan`]
//! or [`ReadPlan`] the wire edge executes against the quorum coordinator.
//!
//! ## Row storage format
//!
//! A row is stored as a single data-plane value under the key
//! `data_key(table, pk_key_bytes)`. The value is a self-describing blob: a
//! `u16` column count, then for each present non-key column a `(u16 column
//! index, [bytes] cell)` pair. The partition-key column is not stored in the
//! value (it is recoverable from the request / round-trips through the key); a
//! `SELECT` reconstructs typed cells by looking each stored index up in the
//! schema. The format is versioned by a leading byte so it can evolve.

use crate::catalog::{Catalog, CatalogError};
use crate::query::{CreateTable, Insert, Select, Term};
use crate::types::{CqlType, CqlValue, ValueError};

/// The format byte prefixing a stored row value.
const ROW_FORMAT_V1: u8 = 1;

/// A resolved column for a result set: its name, type, and its index in the
/// table schema (so a `SELECT` can look the column's stored cell up in the row
/// blob, which is keyed by schema index).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    /// The column name.
    pub name: String,
    /// The column type.
    pub ty: CqlType,
    /// The column's index in the table schema's column list.
    pub schema_index: usize,
}

/// A resolved write: the data-plane key plus the encoded row value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WritePlan {
    /// The resolved (qualified) table name, for result metadata.
    pub table: String,
    /// The data-plane key.
    pub key: Vec<u8>,
    /// The encoded row value bytes.
    pub value: Vec<u8>,
}

/// A resolved read: the data-plane key plus the columns to return (in order).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadPlan {
    /// The resolved table name, for result metadata.
    pub table: String,
    /// The data-plane key to quorum-read.
    pub key: Vec<u8>,
    /// The columns to return, in projection order (`*` expands to all columns).
    pub projection: Vec<ColumnSpec>,
    /// The partition-key value (so a `*`/key projection can echo it back even
    /// though it is not stored in the row value).
    pub pk_value: CqlValue,
    /// The partition-key column name.
    pub pk_name: String,
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
    /// The `WHERE` column was not the partition key.
    NotPartitionKey {
        /// The column used in the predicate.
        used: String,
        /// The table's actual partition-key column.
        expected: String,
    },
    /// The number of supplied bind values did not match the markers.
    BindCountMismatch {
        /// Markers in the statement.
        expected: usize,
        /// Values supplied.
        got: usize,
    },
    /// A stored row value was malformed.
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
            PlanError::NotPartitionKey { used, expected } => write!(
                f,
                "WHERE must filter on partition key `{expected}`, got `{used}`"
            ),
            PlanError::BindCountMismatch { expected, got } => {
                write!(f, "expected {expected} bound values, got {got}")
            }
            PlanError::CorruptRow => write!(f, "stored row is corrupt"),
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
            // A bound value must already match the column type.
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

/// Resolve the column types (in order) for the bind markers of an `INSERT`, so a
/// `PREPARE` can advertise the correct `[col spec]` for each `?`. Returns the
/// type of each marker, left to right.
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
            specs.push(ColumnSpec {
                name: schema.columns[idx].name.clone(),
                ty: schema.columns[idx].ty,
                schema_index: idx,
            });
        }
    }
    Ok(specs)
}

/// Resolve the bind-marker type for a `SELECT`'s `WHERE` value (at most one).
pub fn select_bind_types(
    catalog: &Catalog,
    selected: Option<&str>,
    sel: &Select,
) -> Result<Vec<ColumnSpec>, PlanError> {
    let schema = catalog.resolve(sel.keyspace.as_deref(), selected, &sel.table)?;
    if matches!(sel.where_value, Term::Bind) {
        let idx = schema
            .column_index(&sel.where_column)
            .ok_or_else(|| PlanError::NoSuchColumn(sel.where_column.clone()))?;
        Ok(vec![ColumnSpec {
            name: schema.columns[idx].name.clone(),
            ty: schema.columns[idx].ty,
            schema_index: idx,
        }])
    } else {
        Ok(Vec::new())
    }
}

/// Resolve a `CREATE TABLE` into a [`crate::catalog::TableSchema`].
#[must_use]
pub fn schema_of(ct: &CreateTable) -> crate::catalog::TableSchema {
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
    crate::catalog::TableSchema {
        name: ct.table.clone(),
        columns,
        partition_key,
    }
}

/// Plan an `INSERT`: resolve columns/values, encode the row, and build the key.
///
/// # Errors
/// A [`PlanError`] for any schema/type/bind mismatch.
pub fn plan_insert(
    catalog: &Catalog,
    selected: Option<&str>,
    ins: &Insert,
    binds: &[CqlValue],
) -> Result<WritePlan, PlanError> {
    let schema = catalog.resolve(ins.keyspace.as_deref(), selected, &ins.table)?;
    let mut next_bind = 0;

    // Map each named column to its resolved value, by schema index.
    let mut by_index: std::collections::BTreeMap<usize, CqlValue> =
        std::collections::BTreeMap::new();
    for (col, term) in ins.columns.iter().zip(&ins.values) {
        let idx = schema
            .column_index(col)
            .ok_or_else(|| PlanError::NoSuchColumn(col.clone()))?;
        let ty = schema.columns[idx].ty;
        let value = resolve_term(ty, term, binds, &mut next_bind)?;
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
    let key = crate::query::data_key(&schema.name, &pk_value.to_key_bytes());

    // Encode the row value: every non-key present column, by index.
    let mut value = vec![ROW_FORMAT_V1];
    let stored: Vec<(usize, &CqlValue)> = by_index
        .iter()
        .filter(|(i, _)| **i != pk_idx)
        .map(|(i, v)| (*i, v))
        .collect();
    value.extend_from_slice(&(stored.len() as u16).to_be_bytes());
    for (idx, val) in stored {
        value.extend_from_slice(&(idx as u16).to_be_bytes());
        let cell = schema.columns[idx].ty.encode(val)?;
        value.extend_from_slice(&(cell.len() as u32).to_be_bytes());
        value.extend_from_slice(&cell);
    }

    Ok(WritePlan {
        table: schema.name.clone(),
        key,
        value,
    })
}

/// Plan a `SELECT`: resolve the projection + the partition-key predicate.
///
/// # Errors
/// A [`PlanError`] for any schema/type/bind mismatch, or if the predicate is
/// not on the partition key.
pub fn plan_select(
    catalog: &Catalog,
    selected: Option<&str>,
    sel: &Select,
    binds: &[CqlValue],
) -> Result<ReadPlan, PlanError> {
    let schema = catalog.resolve(sel.keyspace.as_deref(), selected, &sel.table)?;

    // Only a partition-key equality predicate is supported.
    let pk_name = &schema.pk_column().name;
    if !sel.where_column.eq_ignore_ascii_case(pk_name) {
        return Err(PlanError::NotPartitionKey {
            used: sel.where_column.clone(),
            expected: pk_name.clone(),
        });
    }
    let mut next_bind = 0;
    let pk_value = resolve_term(
        schema.pk_column().ty,
        &sel.where_value,
        binds,
        &mut next_bind,
    )?;
    if next_bind != binds.len() {
        return Err(PlanError::BindCountMismatch {
            expected: next_bind,
            got: binds.len(),
        });
    }
    let key = crate::query::data_key(&schema.name, &pk_value.to_key_bytes());

    // Resolve the projection (empty `*` → all columns, in schema order).
    let projection: Vec<ColumnSpec> = if sel.projection.is_empty() {
        schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnSpec {
                name: c.name.clone(),
                ty: c.ty,
                schema_index: i,
            })
            .collect()
    } else {
        let mut specs = Vec::new();
        for col in &sel.projection {
            let idx = schema
                .column_index(col)
                .ok_or_else(|| PlanError::NoSuchColumn(col.clone()))?;
            specs.push(ColumnSpec {
                name: schema.columns[idx].name.clone(),
                ty: schema.columns[idx].ty,
                schema_index: idx,
            });
        }
        specs
    };

    Ok(ReadPlan {
        table: schema.name.clone(),
        key,
        projection,
        pk_value,
        pk_name: pk_name.clone(),
    })
}

/// Decode a stored row value into a map of `schema column index → cell bytes`.
///
/// # Errors
/// [`PlanError::CorruptRow`] if the bytes do not match the row format.
pub fn decode_row(value: &[u8]) -> Result<std::collections::BTreeMap<usize, Vec<u8>>, PlanError> {
    let mut out = std::collections::BTreeMap::new();
    if value.is_empty() || value[0] != ROW_FORMAT_V1 {
        return Err(PlanError::CorruptRow);
    }
    let mut pos = 1;
    let count = read_u16(value, &mut pos)? as usize;
    for _ in 0..count {
        let idx = read_u16(value, &mut pos)? as usize;
        let len = read_u32(value, &mut pos)? as usize;
        let end = pos.checked_add(len).ok_or(PlanError::CorruptRow)?;
        if end > value.len() {
            return Err(PlanError::CorruptRow);
        }
        out.insert(idx, value[pos..end].to_vec());
        pos = end;
    }
    Ok(out)
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
            },
            false,
        )
        .unwrap();
        cat
    }

    #[test]
    fn insert_then_select_round_trips_typed() {
        let cat = catalog();
        let Statement::Insert(ins) =
            parse_statement("INSERT INTO users (id, name, active) VALUES (7, 'Ada', true)")
                .unwrap()
        else {
            panic!()
        };
        let write = plan_insert(&cat, Some("app"), &ins, &[]).unwrap();

        let Statement::Select(sel) = parse_statement("SELECT * FROM users WHERE id = 7").unwrap()
        else {
            panic!()
        };
        let read = plan_select(&cat, Some("app"), &sel, &[]).unwrap();
        assert_eq!(read.key, write.key, "same key for same pk");

        let cells = decode_row(&write.value).unwrap();
        // index 1 = name, index 2 = active (index 0 = pk, not stored).
        assert_eq!(
            CqlType::Text.decode(&cells[&1]).unwrap(),
            CqlValue::Text("Ada".into())
        );
        assert_eq!(
            CqlType::Boolean.decode(&cells[&2]).unwrap(),
            CqlValue::Boolean(true)
        );
        assert!(!cells.contains_key(&0), "pk is not stored in the row value");

        // Projection echoes the pk and decodes stored cells.
        assert_eq!(read.projection.len(), 3);
        assert_eq!(read.pk_value, CqlValue::Int(7));
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
        let write = plan_insert(&cat, Some("app"), &ins, &binds).unwrap();
        let cells = decode_row(&write.value).unwrap();
        assert_eq!(
            CqlType::Text.decode(&cells[&1]).unwrap(),
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
        // id is int; supply text.
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
    }
}
