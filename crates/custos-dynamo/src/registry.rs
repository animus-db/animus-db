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
//! no quorum range scan. To serve `Query` / `Scan` over the distributed plane,
//! the registry also tracks, per table, the set of item **storage keys** written
//! so far (`note_put` / `note_delete`). `Query` resolves the partition's
//! contiguous key sub-range from this ordered index, `scan_keys` walks the whole
//! ordered index (with a cursor for pagination), and the caller quorum-reads each
//! key. This is an honest range scan over a *tracked* keyspace; the index being
//! in-memory (rebuilt only by observed writes) is the same non-durability caveat
//! as the schema map.
//!
//! ## Global secondary indexes (GSI)
//!
//! A `CreateTable` may declare a single-attribute (hash-only) **global secondary
//! index**: an alternate way to look an item up by a non-key attribute. The
//! registry stores the GSI's key-attribute name and, alongside the base key
//! index, a per-GSI ordered set of `escape(gsi_value) || base_storage_key`
//! entries. `note_put` extracts the indexed attribute from the item and records
//! one such entry (only base storage keys are stored, not item copies, so the
//! base item stays the single source of truth); `note_delete` removes it. A
//! `Query` against an `IndexName` resolves the contiguous sub-range for a GSI
//! value back to its base storage keys, which the caller quorum-reads.
//! **Deferred:** projections, composite (hash+range) GSIs, multiple GSIs,
//! local secondary indexes.

use std::collections::{BTreeMap, BTreeSet};

use crate::condition::SortKeyCondition;
use crate::{AttributeValue, Item, TableSchema, escape};

/// One page of a [`SchemaRegistry::scan_keys`] walk: the base storage keys on
/// the page, plus the pagination cursor (the last key) when a `limit` truncated
/// the page, else `None`.
pub type ScanPage = (Vec<Vec<u8>>, Option<Vec<u8>>);

/// A single-attribute (hash-only) global secondary index declaration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalSecondaryIndex {
    /// The index name (the `IndexName` a `Query` targets).
    pub name: String,
    /// The item attribute the index is keyed by (its hash key).
    pub key_attribute: String,
}

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
    /// The named index does not exist on the table.
    NoSuchIndex(String),
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
    /// Declared global secondary indexes, by index name.
    indexes: BTreeMap<String, IndexState>,
}

/// A single GSI's declaration plus its `escape(gsi_value) || base_key` index.
#[derive(Clone, Debug)]
struct IndexState {
    /// The item attribute this index is keyed by.
    key_attribute: String,
    /// `escape(gsi_value) || base_storage_key` for each indexed live item, so a
    /// GSI value's items form a contiguous sub-range and resolve to base keys.
    entries: BTreeSet<Vec<u8>>,
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

    /// `CreateTable`: register `schema` under `table` (no secondary indexes).
    ///
    /// # Errors
    /// [`RegistryError::TableExists`] if the table is already registered.
    pub fn create_table(&mut self, table: &str, schema: TableSchema) -> Result<(), RegistryError> {
        self.create_table_with_indexes(table, schema, Vec::new())
    }

    /// `CreateTable` with global secondary indexes: register `schema` under
    /// `table`, declaring each [`GlobalSecondaryIndex`] in `indexes`.
    ///
    /// # Errors
    /// [`RegistryError::TableExists`] if the table is already registered.
    pub fn create_table_with_indexes(
        &mut self,
        table: &str,
        schema: TableSchema,
        indexes: Vec<GlobalSecondaryIndex>,
    ) -> Result<(), RegistryError> {
        self.create_table_inner(table, schema, indexes, false)
    }

    /// Register `table` under the **legacy convention** (partition key `pk`,
    /// optional sort key `sk`) so a pre-`CreateTable` client's writes can be
    /// tracked for `Query` without rejecting a key that omits the sort key.
    /// A no-op if the table already exists.
    pub fn create_table_legacy(&mut self, table: &str) {
        let _ =
            self.create_table_inner(table, TableSchema::composite("pk", "sk"), Vec::new(), true);
    }

    fn create_table_inner(
        &mut self,
        table: &str,
        schema: TableSchema,
        indexes: Vec<GlobalSecondaryIndex>,
        sort_key_optional: bool,
    ) -> Result<(), RegistryError> {
        if self.tables.contains_key(table) {
            return Err(RegistryError::TableExists(table.to_owned()));
        }
        let indexes = indexes
            .into_iter()
            .map(|gsi| {
                (
                    gsi.name,
                    IndexState {
                        key_attribute: gsi.key_attribute,
                        entries: BTreeSet::new(),
                    },
                )
            })
            .collect();
        self.tables.insert(
            table.to_owned(),
            TableState {
                schema,
                sort_key_optional,
                keys: BTreeSet::new(),
                indexes,
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

    /// Record that an item at `key` (its base storage key) now exists, given the
    /// full `item` so any declared GSI entries can be maintained.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`].
    pub fn note_put(&mut self, table: &str, key: &[u8], item: &Item) -> Result<(), RegistryError> {
        let state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| RegistryError::NoSuchTable(table.to_owned()))?;
        state.keys.insert(key.to_vec());
        for index in state.indexes.values_mut() {
            // Drop any stale entry for this base key first (the indexed
            // attribute may have changed on an overwrite), then re-index if the
            // item still carries the GSI key attribute.
            index.entries.retain(|e| base_key_of(e) != key);
            if let Some(value) = item.get(&index.key_attribute) {
                index.entries.insert(index_entry(value, key));
            }
        }
        Ok(())
    }

    /// Record that the item at `key` was deleted (drop it from the base index
    /// and from every GSI index).
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`].
    pub fn note_delete(&mut self, table: &str, key: &[u8]) -> Result<(), RegistryError> {
        let state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| RegistryError::NoSuchTable(table.to_owned()))?;
        state.keys.remove(key);
        for index in state.indexes.values_mut() {
            index.entries.retain(|e| base_key_of(e) != key);
        }
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

    /// The base storage keys of items whose GSI `index` key attribute equals
    /// `value`, in the GSI's stored order. The caller quorum-reads each base key
    /// (the GSI stores no item copies — the base item is the source of truth).
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`] or [`RegistryError::NoSuchIndex`].
    pub fn index_query_keys(
        &self,
        table: &str,
        index: &str,
        value: &AttributeValue,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        let state = self.state(table)?;
        let idx = state
            .indexes
            .get(index)
            .ok_or_else(|| RegistryError::NoSuchIndex(index.to_owned()))?;
        // Entries are `escape(value) || base_key`; one value's entries form a
        // contiguous sub-range (the escape is prefix-free, ending `0x00 0x00`).
        let prefix = escape(&value.key_bytes());
        let mut end = prefix.clone();
        *end.last_mut().expect("escape is non-empty") = 0x01;
        Ok(idx
            .entries
            .range(prefix.clone()..end)
            .map(|e| e[prefix.len()..].to_vec())
            .collect())
    }

    /// All live base storage keys of `table` in key order, starting *after*
    /// `start_after` (exclusive) when given, and capped at `limit` when given.
    /// Returns the keys plus the last key emitted (the pagination cursor) when
    /// `limit` truncated the result, else `None`. Backs the distributed `Scan`.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`].
    pub fn scan_keys(
        &self,
        table: &str,
        start_after: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<ScanPage, RegistryError> {
        let state = self.state(table)?;
        // Range strictly after the cursor: keys are unique, so `cursor || 0x00`
        // is the first key past it.
        let lower = start_after.map(|c| {
            let mut bound = c.to_vec();
            bound.push(0x00);
            bound
        });
        let iter = match &lower {
            Some(lower) => state.keys.range(lower.clone()..),
            None => state.keys.range::<Vec<u8>, _>(..),
        };
        let mut keys = Vec::new();
        let mut truncated = false;
        for key in iter {
            if let Some(limit) = limit {
                if keys.len() == limit {
                    truncated = true;
                    break;
                }
            }
            keys.push(key.clone());
        }
        let cursor = if truncated {
            keys.last().cloned()
        } else {
            None
        };
        Ok((keys, cursor))
    }
}

/// A GSI index entry: `escape(gsi_value) || base_storage_key`. The escape is
/// prefix-free, so the base key is recoverable ([`base_key_of`]) and one GSI
/// value's entries form a contiguous range.
fn index_entry(value: &AttributeValue, base_key: &[u8]) -> Vec<u8> {
    let mut entry = escape(&value.key_bytes());
    entry.extend_from_slice(base_key);
    entry
}

/// Recover the base storage key from a GSI index entry by skipping the
/// prefix-free `escape(gsi_value)` prefix (terminated by `0x00 0x00`).
fn base_key_of(entry: &[u8]) -> &[u8] {
    // Find the `0x00 0x00` terminator that is not part of an escaped `0x00`
    // (escaped as `0x00 0x01`). Scan for `0x00` and look at the next byte.
    let mut i = 0;
    while i + 1 < entry.len() {
        if entry[i] == 0x00 {
            match entry[i + 1] {
                0x00 => return &entry[i + 2..], // terminator
                0x01 => i += 2,                 // escaped zero byte
                _ => i += 1,
            }
        } else {
            i += 1;
        }
    }
    &[]
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
            reg.note_put("t", &key, &item(&[("pk", s(p)), ("sk", s(sk))]))
                .unwrap();
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
            reg.note_put(
                "t",
                &storage_key(&s("p"), Some(&s(sk))),
                &item(&[("pk", s("p")), ("sk", s(sk))]),
            )
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
        reg.note_put("t", &key, &item(&[("pk", s("p")), ("sk", s("a"))]))
            .unwrap();
        reg.note_delete("t", &key).unwrap();
        assert!(reg.query_keys("t", &s("p"), None).unwrap().is_empty());
    }

    #[test]
    fn scan_keys_paginates_across_partitions() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::composite("pk", "sk"))
            .unwrap();
        // Three partitions, two items each (inserted out of order).
        for (p, sk) in [
            ("p2", "a"),
            ("p1", "b"),
            ("p3", "a"),
            ("p1", "a"),
            ("p2", "b"),
            ("p3", "b"),
        ] {
            reg.note_put(
                "t",
                &storage_key(&s(p), Some(&s(sk))),
                &item(&[("pk", s(p)), ("sk", s(sk))]),
            )
            .unwrap();
        }
        // First page of 2 from the start, with a cursor back.
        let (page1, cursor) = reg.scan_keys("t", None, Some(2)).unwrap();
        assert_eq!(page1.len(), 2);
        let cursor = cursor.expect("page truncated, so a cursor is returned");
        assert_eq!(&cursor, page1.last().unwrap());
        // Continuing from the cursor yields the rest with no overlap.
        let (page2, cursor2) = reg.scan_keys("t", Some(&cursor), None).unwrap();
        assert_eq!(page2.len(), 4);
        assert_eq!(cursor2, None);
        // The two pages together are the full ordered key set, deduplicated.
        let (all, _) = reg.scan_keys("t", None, None).unwrap();
        let mut joined = page1;
        joined.extend(page2);
        assert_eq!(joined, all);
    }

    #[test]
    fn gsi_write_then_index_query() {
        let mut reg = SchemaRegistry::new();
        reg.create_table_with_indexes(
            "users",
            TableSchema::simple("id"),
            vec![GlobalSecondaryIndex {
                name: "by-email".into(),
                key_attribute: "email".into(),
            }],
        )
        .unwrap();
        let put = |reg: &mut SchemaRegistry, id: &str, email: &str| {
            let key = storage_key(&s(id), None);
            reg.note_put("users", &key, &item(&[("id", s(id)), ("email", s(email))]))
                .unwrap();
            key
        };
        let k1 = put(&mut reg, "u1", "a@x");
        let _k2 = put(&mut reg, "u2", "b@x");
        let k3 = put(&mut reg, "u3", "a@x");

        // Two users share email a@x; the index returns both their base keys.
        let mut got = reg
            .index_query_keys("users", "by-email", &s("a@x"))
            .unwrap();
        got.sort();
        let mut expect = vec![k1.clone(), k3.clone()];
        expect.sort();
        assert_eq!(got, expect);

        // Re-indexing on overwrite: u1 changes email; it leaves a@x, joins c@x.
        put(&mut reg, "u1", "c@x");
        assert_eq!(
            reg.index_query_keys("users", "by-email", &s("a@x"))
                .unwrap(),
            vec![k3.clone()]
        );
        assert_eq!(
            reg.index_query_keys("users", "by-email", &s("c@x"))
                .unwrap(),
            vec![k1.clone()]
        );

        // Delete drops the item from the index.
        reg.note_delete("users", &k3).unwrap();
        assert!(
            reg.index_query_keys("users", "by-email", &s("a@x"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn index_query_on_unknown_index() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::simple("id")).unwrap();
        assert_eq!(
            reg.index_query_keys("t", "nope", &s("v")),
            Err(RegistryError::NoSuchIndex("nope".into()))
        );
    }
}
