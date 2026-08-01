//! A per-table **schema registry** (ADR 0006), so the DynamoDB key convention is
//! no longer hard-coded: `CreateTable` records a table's key attribute names and
//! types, and later `PutItem`/`GetItem`/`Query` resolve their keys against it.
//!
//! The registry is a **pure, deterministic** in-memory map (`BTreeMap` only — no
//! `HashMap`, per ADR 0003). `custosd` holds one instance behind a lock at its
//! HTTP edge; **it is not durable** — schemas (and the key index below) are lost
//! on restart. Persisting them through the control plane is future work.
//!
//! ## Why a key index lives here too
//!
//! The data plane (`custos-data`) exposes only point read/write/delete — it has
//! no quorum range scan. To serve `Query` over the distributed plane, the
//! registry also tracks, per table, the set of item **storage keys** written so
//! far (`note_put` / `note_delete`). `Query` resolves the partition's contiguous
//! key sub-range from this ordered index, and the caller quorum-reads each key.
//! This is an honest range scan over a *tracked* keyspace; the index being
//! in-memory (rebuilt only by observed writes) is the same non-durability caveat
//! as the schema map.

use std::collections::{BTreeMap, BTreeSet};

use crate::condition::SortKeyCondition;
use crate::{AttributeValue, TableSchema, escape};

/// Errors from registry-mediated key resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    /// The named table has not been created.
    NoSuchTable(String),
    /// A table with this name already exists.
    TableExists(String),
    /// A required key attribute was absent from the item/key.
    MissingKey(String),
    /// The table has no sort key, but the query/key supplied one (or vice versa).
    SortKeyMismatch(String),
}

/// A table's registered schema plus the storage keys of its known items.
#[derive(Clone, Debug)]
struct TableState {
    schema: TableSchema,
    /// When set, the schema's sort key is treated as **optional** during key
    /// extraction (the legacy `pk`/`sk` convention, auto-registered for tables a
    /// pre-`CreateTable` client uses without declaring a schema).
    sort_key_optional: bool,
    /// Storage keys (`escape(pk) || sk`) of every live item observed via
    /// `note_put`, minus those `note_delete`d. Ordered, so a partition's keys
    /// form a contiguous sub-range.
    keys: BTreeSet<Vec<u8>>,
}

/// An in-memory registry of table schemas + per-table item-key indexes.
#[derive(Clone, Debug, Default)]
pub struct SchemaRegistry {
    tables: BTreeMap<String, TableState>,
}

impl SchemaRegistry {
    /// An empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// `CreateTable`: register `schema` under `table`.
    ///
    /// # Errors
    /// [`RegistryError::TableExists`] if the table is already registered.
    pub fn create_table(&mut self, table: &str, schema: TableSchema) -> Result<(), RegistryError> {
        self.create_table_inner(table, schema, false)
    }

    /// Register `table` under the **legacy convention** (partition key `pk`,
    /// optional sort key `sk`) so a pre-`CreateTable` client's writes can be
    /// tracked for `Query` without rejecting a key that omits the sort key.
    /// A no-op if the table already exists.
    pub fn create_table_legacy(&mut self, table: &str) {
        let _ = self.create_table_inner(table, TableSchema::composite("pk", "sk"), true);
    }

    fn create_table_inner(
        &mut self,
        table: &str,
        schema: TableSchema,
        sort_key_optional: bool,
    ) -> Result<(), RegistryError> {
        if self.tables.contains_key(table) {
            return Err(RegistryError::TableExists(table.to_owned()));
        }
        self.tables.insert(
            table.to_owned(),
            TableState {
                schema,
                sort_key_optional,
                keys: BTreeSet::new(),
            },
        );
        Ok(())
    }

    /// Whether `table` has been created.
    #[must_use]
    pub fn has_table(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }

    /// The schema registered for `table`, if any.
    #[must_use]
    pub fn schema(&self, table: &str) -> Option<&TableSchema> {
        self.tables.get(table).map(|t| &t.schema)
    }

    fn state(&self, table: &str) -> Result<&TableState, RegistryError> {
        self.tables
            .get(table)
            .ok_or_else(|| RegistryError::NoSuchTable(table.to_owned()))
    }

    /// Resolve the `(partition_key, optional sort_key)` attribute values from an
    /// item/key map, per `table`'s schema.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`] / [`RegistryError::MissingKey`] /
    /// [`RegistryError::SortKeyMismatch`].
    pub fn extract_key(
        &self,
        table: &str,
        item: &crate::Item,
    ) -> Result<(AttributeValue, Option<AttributeValue>), RegistryError> {
        let state = self.state(table)?;
        let schema = &state.schema;
        let pk = item
            .get(&schema.partition_key)
            .cloned()
            .ok_or_else(|| RegistryError::MissingKey(schema.partition_key.clone()))?;
        let sk = match &schema.sort_key {
            Some(name) => match item.get(name).cloned() {
                Some(sk) => Some(sk),
                // A legacy table treats a missing sort key as absent; a declared
                // composite table requires it.
                None if state.sort_key_optional => None,
                None => return Err(RegistryError::MissingKey(name.clone())),
            },
            None => None,
        };
        Ok((pk, sk))
    }

    /// Record that an item at `key` (its storage key) now exists.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`].
    pub fn note_put(&mut self, table: &str, key: &[u8]) -> Result<(), RegistryError> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| RegistryError::NoSuchTable(table.to_owned()))?
            .keys
            .insert(key.to_vec());
        Ok(())
    }

    /// Record that the item at `key` was deleted (drop it from the index).
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`].
    pub fn note_delete(&mut self, table: &str, key: &[u8]) -> Result<(), RegistryError> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| RegistryError::NoSuchTable(table.to_owned()))?
            .keys
            .remove(key);
        Ok(())
    }

    /// The storage keys of items in `table`'s partition `pk` that satisfy an
    /// optional sort-key `condition`, in sort order. The caller quorum-reads each
    /// key to assemble the `Query` result.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`], or [`RegistryError::SortKeyMismatch`] if a
    /// sort condition is given for a table without a sort key.
    pub fn query_keys(
        &self,
        table: &str,
        pk: &AttributeValue,
        condition: Option<&SortKeyCondition>,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        let state = self.state(table)?;
        if condition.is_some() && state.schema.sort_key.is_none() {
            return Err(RegistryError::SortKeyMismatch(table.to_owned()));
        }
        // A partition's keys all start with `escape(pk)` (which ends in
        // `0x00 0x00`); the first key past the partition replaces that final
        // terminator byte with `0x01`.
        let prefix = escape(&pk.key_bytes());
        let mut end = prefix.clone();
        *end.last_mut().expect("escape is non-empty") = 0x01;

        let mut keys = Vec::new();
        for key in state.keys.range(prefix.clone()..end) {
            if let Some(cond) = condition {
                // Recover the sort-key bytes (everything after the escaped pk)
                // and test the condition without decoding the stored value.
                let sk_bytes = &key[prefix.len()..];
                if !cond.matches(&AttributeValue::B(sk_bytes.to_vec())) {
                    continue;
                }
            }
            keys.push(key.clone());
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Item, storage_key};

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    fn item(pairs: &[(&str, AttributeValue)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn create_then_extract_key() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::composite("pk", "sk"))
            .unwrap();
        assert!(reg.has_table("t"));
        let (pk, sk) = reg
            .extract_key("t", &item(&[("pk", s("p")), ("sk", s("a")), ("x", s("y"))]))
            .unwrap();
        assert_eq!(pk, s("p"));
        assert_eq!(sk, Some(s("a")));
    }

    #[test]
    fn duplicate_create_is_rejected() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::simple("id")).unwrap();
        assert_eq!(
            reg.create_table("t", TableSchema::simple("id")),
            Err(RegistryError::TableExists("t".into()))
        );
    }

    #[test]
    fn extract_key_on_unknown_table() {
        let reg = SchemaRegistry::new();
        assert_eq!(
            reg.extract_key("nope", &item(&[("id", s("k"))])),
            Err(RegistryError::NoSuchTable("nope".into()))
        );
    }

    #[test]
    fn missing_key_attribute() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::composite("pk", "sk"))
            .unwrap();
        assert_eq!(
            reg.extract_key("t", &item(&[("pk", s("p"))])),
            Err(RegistryError::MissingKey("sk".into()))
        );
    }

    #[test]
    fn query_keys_partition_isolated_and_ordered() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::composite("pk", "sk"))
            .unwrap();
        for (p, sk) in [("p1", "c"), ("p1", "a"), ("p1", "b"), ("p2", "z")] {
            let key = storage_key(&s(p), Some(&s(sk)));
            reg.note_put("t", &key).unwrap();
        }
        // p1 yields its three keys in sort order (a, b, c).
        let got = reg.query_keys("t", &s("p1"), None).unwrap();
        let expect: Vec<_> = ["a", "b", "c"]
            .iter()
            .map(|sk| storage_key(&s("p1"), Some(&s(sk))))
            .collect();
        assert_eq!(got, expect);
        // p2 is isolated.
        assert_eq!(reg.query_keys("t", &s("p2"), None).unwrap().len(), 1);
    }

    #[test]
    fn query_keys_with_sort_conditions() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::composite("pk", "sk"))
            .unwrap();
        for sk in ["a", "ab", "abc", "b", "c"] {
            reg.note_put("t", &storage_key(&s("p"), Some(&s(sk))))
                .unwrap();
        }
        let eq = reg
            .query_keys("t", &s("p"), Some(&SortKeyCondition::Equals(s("b"))))
            .unwrap();
        assert_eq!(eq, vec![storage_key(&s("p"), Some(&s("b")))]);

        let between = reg
            .query_keys(
                "t",
                &s("p"),
                Some(&SortKeyCondition::Between(s("ab"), s("b"))),
            )
            .unwrap();
        assert_eq!(between.len(), 3); // ab, abc, b

        let begins = reg
            .query_keys("t", &s("p"), Some(&SortKeyCondition::BeginsWith(s("ab"))))
            .unwrap();
        assert_eq!(begins.len(), 2); // ab, abc
    }

    #[test]
    fn note_delete_removes_from_index() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::composite("pk", "sk"))
            .unwrap();
        let key = storage_key(&s("p"), Some(&s("a")));
        reg.note_put("t", &key).unwrap();
        reg.note_delete("t", &key).unwrap();
        assert!(reg.query_keys("t", &s("p"), None).unwrap().is_empty());
    }
}
