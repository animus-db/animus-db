//! An in-memory schema catalog: keyspaces and table column definitions.
//!
//! Pure and deterministic (ADR 0003) — a `BTreeMap`-backed registry with no
//! I/O. The socket edge in `animusd` owns one shared `Catalog` per process and
//! consults it to resolve `INSERT`/`SELECT` columns against the declared schema
//! (rather than the legacy fixed `(pk, v)` convention).
//!
//! **Limitation (documented):** the catalog is in-memory and **not durable** —
//! schemas are lost on restart and, in single-process `--cluster N` dev mode,
//! one catalog is shared across all in-process nodes. Replicating schemas
//! through the control plane (so they survive restart and are agreed
//! cluster-wide) is future work, exactly as on the DynamoDB side (ADR 0006).

use std::collections::BTreeMap;
use std::fmt;

use crate::types::CqlType;

/// One column's name and declared type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    /// The column name (as written, case preserved).
    pub name: String,
    /// The column's CQL type.
    pub ty: CqlType,
}

/// A table's schema: ordered columns plus which one is the partition key and,
/// optionally, an ordered list of clustering columns.
///
/// This subset supports a **single partition-key column** plus any number of
/// **clustering columns** (a compound primary key `PRIMARY KEY (pk, c1, c2)`)
/// and any number of non-key (regular) columns. Composite (multi-column)
/// partition keys are still future work; a `CREATE TABLE` that asks for one is
/// rejected by the parser, not silently truncated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableSchema {
    /// The table name.
    pub name: String,
    /// All columns in declaration order (the partition + clustering keys included).
    pub columns: Vec<Column>,
    /// The index into [`TableSchema::columns`] of the partition-key column.
    pub partition_key: usize,
    /// The indices (into [`TableSchema::columns`]) of the clustering columns, in
    /// clustering order. Empty for a partition-key-only table.
    pub clustering_keys: Vec<usize>,
}

impl TableSchema {
    /// The partition-key column.
    #[must_use]
    pub fn pk_column(&self) -> &Column {
        &self.columns[self.partition_key]
    }

    /// The clustering columns, in clustering order.
    #[must_use]
    pub fn clustering_columns(&self) -> Vec<&Column> {
        self.clustering_keys
            .iter()
            .map(|i| &self.columns[*i])
            .collect()
    }

    /// Whether `index` is part of the primary key (partition or clustering).
    #[must_use]
    pub fn is_primary_key(&self, index: usize) -> bool {
        index == self.partition_key || self.clustering_keys.contains(&index)
    }

    /// Look up a column by name (case-insensitive), returning its index.
    #[must_use]
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Look up a column by name (case-insensitive).
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.column_index(name).map(|i| &self.columns[i])
    }
}

/// The process-wide schema catalog: keyspaces, each holding tables by name.
///
/// Names are stored lowercased as map keys (CQL identifiers are
/// case-insensitive unless quoted, which this subset does not model), while the
/// stored `TableSchema`/`Column` keep the originally written casing for display.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    /// keyspace (lowercased) → (table (lowercased) → schema).
    keyspaces: BTreeMap<String, BTreeMap<String, TableSchema>>,
}

/// Why a catalog operation failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogError {
    /// The referenced keyspace does not exist.
    NoSuchKeyspace(String),
    /// The referenced table does not exist in the keyspace.
    NoSuchTable {
        /// The keyspace looked in.
        keyspace: String,
        /// The missing table.
        table: String,
    },
    /// A `CREATE TABLE` named a table that already exists (without `IF NOT
    /// EXISTS`).
    TableExists(String),
    /// No keyspace is in use and the statement did not qualify the table.
    NoKeyspaceSelected,
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::NoSuchKeyspace(k) => write!(f, "keyspace `{k}` does not exist"),
            CatalogError::NoSuchTable { keyspace, table } => {
                write!(f, "table `{keyspace}.{table}` does not exist")
            }
            CatalogError::TableExists(t) => write!(f, "table `{t}` already exists"),
            CatalogError::NoKeyspaceSelected => {
                write!(f, "no keyspace selected; USE one or qualify the table name")
            }
        }
    }
}

impl std::error::Error for CatalogError {}

impl Catalog {
    /// A fresh, empty catalog.
    #[must_use]
    pub fn new() -> Catalog {
        Catalog::default()
    }

    /// Create a keyspace (idempotent — creating an existing one is a no-op, as
    /// `CREATE KEYSPACE IF NOT EXISTS` would be).
    pub fn create_keyspace(&mut self, name: &str) {
        self.keyspaces.entry(name.to_ascii_lowercase()).or_default();
    }

    /// Whether a keyspace exists.
    #[must_use]
    pub fn has_keyspace(&self, name: &str) -> bool {
        self.keyspaces.contains_key(&name.to_ascii_lowercase())
    }

    /// Create a table in `keyspace`. `if_not_exists` makes a duplicate a no-op
    /// rather than an error.
    ///
    /// # Errors
    /// [`CatalogError::NoSuchKeyspace`] if the keyspace is missing;
    /// [`CatalogError::TableExists`] if the table exists and `!if_not_exists`.
    pub fn create_table(
        &mut self,
        keyspace: &str,
        schema: TableSchema,
        if_not_exists: bool,
    ) -> Result<(), CatalogError> {
        let ks_key = keyspace.to_ascii_lowercase();
        let tables = self
            .keyspaces
            .get_mut(&ks_key)
            .ok_or_else(|| CatalogError::NoSuchKeyspace(keyspace.to_owned()))?;
        let table_key = schema.name.to_ascii_lowercase();
        if tables.contains_key(&table_key) {
            if if_not_exists {
                return Ok(());
            }
            return Err(CatalogError::TableExists(schema.name.clone()));
        }
        tables.insert(table_key, schema);
        Ok(())
    }

    /// Resolve a table schema. `qualified` is an optional `keyspace` prefix
    /// (from a `ks.table` reference); `selected` is the connection's current
    /// `USE`d keyspace.
    ///
    /// # Errors
    /// [`CatalogError::NoKeyspaceSelected`] when neither a qualifier nor a
    /// selection is present, or the keyspace/table is missing.
    pub fn resolve<'a>(
        &'a self,
        qualified: Option<&str>,
        selected: Option<&str>,
        table: &str,
    ) -> Result<&'a TableSchema, CatalogError> {
        let keyspace = qualified
            .or(selected)
            .ok_or(CatalogError::NoKeyspaceSelected)?;
        let tables = self
            .keyspaces
            .get(&keyspace.to_ascii_lowercase())
            .ok_or_else(|| CatalogError::NoSuchKeyspace(keyspace.to_owned()))?;
        tables
            .get(&table.to_ascii_lowercase())
            .ok_or_else(|| CatalogError::NoSuchTable {
                keyspace: keyspace.to_owned(),
                table: table.to_owned(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users_schema() -> TableSchema {
        TableSchema {
            name: "users".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    ty: CqlType::Uuid,
                },
                Column {
                    name: "name".into(),
                    ty: CqlType::Text,
                },
            ],
            partition_key: 0,
            clustering_keys: vec![],
        }
    }

    #[test]
    fn create_and_resolve() {
        let mut cat = Catalog::new();
        cat.create_keyspace("app");
        cat.create_table("app", users_schema(), false).unwrap();

        let schema = cat.resolve(None, Some("app"), "users").unwrap();
        assert_eq!(schema.pk_column().name, "id");
        assert_eq!(schema.column("name").unwrap().ty, CqlType::Text);

        // Qualified reference works without a selection.
        let schema = cat.resolve(Some("app"), None, "USERS").unwrap();
        assert_eq!(schema.name, "users");
    }

    #[test]
    fn missing_keyspace_or_table_errors() {
        let cat = Catalog::new();
        assert!(matches!(
            cat.resolve(None, Some("nope"), "users"),
            Err(CatalogError::NoSuchKeyspace(_))
        ));
        assert!(matches!(
            cat.resolve(None, None, "users"),
            Err(CatalogError::NoKeyspaceSelected)
        ));
    }

    #[test]
    fn duplicate_table_rules() {
        let mut cat = Catalog::new();
        cat.create_keyspace("app");
        cat.create_table("app", users_schema(), false).unwrap();
        assert!(matches!(
            cat.create_table("app", users_schema(), false),
            Err(CatalogError::TableExists(_))
        ));
        // IF NOT EXISTS makes it a no-op.
        cat.create_table("app", users_schema(), true).unwrap();
    }
}
