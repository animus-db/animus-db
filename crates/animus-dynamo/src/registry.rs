//! A per-table **schema registry** (ADR 0006), so the DynamoDB key convention is
//! no longer hard-coded: `CreateTable` records a table's key attribute names and
//! types, and later `PutItem`/`GetItem`/`Query` resolve their keys against it.
//!
//! The registry is a **pure, deterministic** in-memory map (`BTreeMap` only — no
//! `HashMap`, per ADR 0003). `animusd` holds one instance behind a lock at its
//! HTTP edge; **it is not durable** — schemas (and the key index below) are lost
//! on restart. Persisting them through the control plane is future work.
//!
//! ## Why a key index lives here too
//!
//! The data plane (`animus-data`) exposes only point read/write/delete — it has
//! no quorum range scan. To serve `Query` / `Scan` over the distributed plane,
//! the registry also tracks, per table, the set of item **storage keys** written
//! so far (`note_put` / `note_delete`). `Query` resolves the partition's
//! contiguous key sub-range from this ordered index, `scan_keys` walks the whole
//! ordered index (with a cursor for pagination), and the caller quorum-reads each
//! key. This is an honest range scan over a *tracked* keyspace; the index being
//! in-memory (rebuilt only by observed writes) is the same non-durability caveat
//! as the schema map.
//!
//! ## Secondary indexes (GSI + LSI)
//!
//! A `CreateTable` may declare **secondary indexes**: alternate ways to look an
//! item up. The registry supports any number of them, keyed by name:
//!
//! - **Global secondary indexes (GSI)** are keyed by a hash attribute, optionally
//!   plus a range attribute (a composite GSI). Their keyspace is independent of
//!   the base table's partition.
//! - **Local secondary indexes (LSI)** share the base table's *partition* key but
//!   project an alternate **sort** attribute, so they narrow within a partition by
//!   a different sort key.
//!
//! Each index keeps, alongside the base key index, an ordered set of
//! `escape(hash_value) [|| escape(range_value)] || base_storage_key` entries.
//! `note_put` extracts the indexed attribute(s) from the item and records one
//! such entry (only base storage keys are stored, not item copies, so the base
//! item stays the single source of truth); `note_delete` removes it. A `Query`
//! against an `IndexName` resolves the contiguous sub-range for an index value —
//! plus an optional sort-key condition on a composite GSI / LSI — back to its
//! base storage keys, which the caller quorum-reads. **Deferred:** projection
//! attribute lists per index (every index here projects `ALL`), document-path
//! projections.

use std::collections::{BTreeMap, BTreeSet};

use crate::condition::SortKeyCondition;
use crate::{AttributeValue, Item, TableSchema, escape};

/// One page of a [`SchemaRegistry::scan_keys`] walk: the base storage keys on
/// the page, plus the pagination cursor (the last key) when a `limit` truncated
/// the page, else `None`.
pub type ScanPage = (Vec<Vec<u8>>, Option<Vec<u8>>);

/// What attributes a secondary index projects (the `Projection` of a
/// `CreateTable` index declaration). Because this registry stores only base keys
/// (never item copies — the base item is the single source of truth), the
/// projection does not change what is *stored*; it bounds what a `Query` against
/// the index is allowed to **return**, applied at the edge after the base item is
/// read. (Real DynamoDB also bounds what a non-projected attribute fetch costs;
/// here the edge always has the whole base item, so the projection is purely a
/// returned-attribute filter.)
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum IndexProjection {
    /// `ALL` — every attribute (the default).
    #[default]
    All,
    /// `KEYS_ONLY` — only the base table key + index key attributes.
    KeysOnly,
    /// `INCLUDE` — the keys plus an explicit list of non-key attributes.
    Include(Vec<String>),
}

/// A global secondary index declaration: a hash key attribute, optionally plus a
/// range (sort) attribute for a composite GSI. Its keyspace is independent of the
/// base table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlobalSecondaryIndex {
    /// The index name (the `IndexName` a `Query` targets).
    pub name: String,
    /// The item attribute the index is keyed by (its hash key).
    pub key_attribute: String,
    /// The optional range attribute (a composite GSI). `None` ⇒ hash-only.
    pub sort_attribute: Option<String>,
    /// What attributes a query against this index returns.
    pub projection: IndexProjection,
}

/// A local secondary index declaration: it shares the base table's partition key
/// and projects an alternate **sort** attribute, narrowing within a partition by
/// a different sort key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalSecondaryIndex {
    /// The index name (the `IndexName` a `Query` targets).
    pub name: String,
    /// The item attribute used as this index's sort key (within the base
    /// partition).
    pub sort_attribute: String,
    /// What attributes a query against this index returns.
    pub projection: IndexProjection,
}

/// A secondary index of either kind, the unit `CreateTable` declares and the
/// registry stores.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecondaryIndex {
    /// A global secondary index (independent keyspace).
    Global(GlobalSecondaryIndex),
    /// A local secondary index (shares the base partition key).
    Local(LocalSecondaryIndex),
}

impl SecondaryIndex {
    /// The index's name.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            SecondaryIndex::Global(g) => &g.name,
            SecondaryIndex::Local(l) => &l.name,
        }
    }
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
    /// A sort-key condition was supplied for an index that has no sort key
    /// (a hash-only GSI).
    IndexSortMismatch(String),
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
    /// Declared secondary indexes, by index name.
    indexes: BTreeMap<String, IndexState>,
}

/// A single secondary index's declaration plus its index entries.
///
/// For a GSI the hash attribute is the item attribute named by `hash_attribute`;
/// for an LSI the hash value is the item's *base partition key* (so the index is
/// "local" to a partition). `sort_attribute` is the optional range attribute
/// (always set for an LSI; set for a composite GSI; `None` for a hash-only GSI).
///
/// Each entry is `escape(hash_value) [|| escape(sort_value)] || base_storage_key`,
/// so one hash value's entries form a contiguous sub-range, ordered by sort value
/// then base key, and the base key is recoverable past the prefix-free escapes.
#[derive(Clone, Debug)]
struct IndexState {
    /// For a GSI: the item attribute holding the index's hash value. For an LSI:
    /// the base table's partition-key attribute (the index hashes by partition).
    hash_attribute: String,
    /// The optional range/sort attribute (LSI: always; composite GSI: yes;
    /// hash-only GSI: `None`).
    sort_attribute: Option<String>,
    /// What attributes a query against this index returns (ADR 0006).
    projection: IndexProjection,
    /// `escape(hash) [|| escape(sort)] || base_storage_key` per indexed live item.
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

    /// `CreateTable` with secondary indexes: register `schema` under `table`,
    /// declaring each [`SecondaryIndex`] in `indexes` (GSIs and LSIs).
    ///
    /// # Errors
    /// [`RegistryError::TableExists`] if the table is already registered.
    pub fn create_table_with_indexes(
        &mut self,
        table: &str,
        schema: TableSchema,
        indexes: Vec<SecondaryIndex>,
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
        indexes: Vec<SecondaryIndex>,
        sort_key_optional: bool,
    ) -> Result<(), RegistryError> {
        if self.tables.contains_key(table) {
            return Err(RegistryError::TableExists(table.to_owned()));
        }
        let indexes = indexes
            .into_iter()
            .map(|index| match index {
                SecondaryIndex::Global(g) => (
                    g.name,
                    IndexState {
                        hash_attribute: g.key_attribute,
                        sort_attribute: g.sort_attribute,
                        projection: g.projection,
                        entries: BTreeSet::new(),
                    },
                ),
                // An LSI hashes by the base table's partition key and sorts by
                // its alternate sort attribute.
                SecondaryIndex::Local(l) => (
                    l.name,
                    IndexState {
                        hash_attribute: schema.partition_key.clone(),
                        sort_attribute: Some(l.sort_attribute),
                        projection: l.projection,
                        entries: BTreeSet::new(),
                    },
                ),
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
            // Drop any stale entry for this base key first (an indexed attribute
            // may have changed on an overwrite), then re-index if the item still
            // carries the index's hash attribute (and, for a composite index, its
            // sort attribute).
            let segments = if index.sort_attribute.is_some() { 2 } else { 1 };
            index.entries.retain(|e| base_key_of(e, segments) != key);
            let Some(hash) = item.get(&index.hash_attribute) else {
                continue;
            };
            let sort = match &index.sort_attribute {
                // A composite index requires the item to carry the sort
                // attribute; without it the item is simply not indexed.
                Some(name) => match item.get(name) {
                    Some(v) => Some(v),
                    None => continue,
                },
                None => None,
            };
            index.entries.insert(index_entry(hash, sort, key));
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
            let segments = if index.sort_attribute.is_some() { 2 } else { 1 };
            index.entries.retain(|e| base_key_of(e, segments) != key);
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

    /// The base storage keys of items whose secondary `index` hash value equals
    /// `value`, optionally narrowed by a sort-key `condition` on the index's
    /// range attribute, in the index's stored order. The caller quorum-reads each
    /// base key (the index stores no item copies — the base item is the source of
    /// truth).
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`], [`RegistryError::NoSuchIndex`], or
    /// [`RegistryError::IndexSortMismatch`] if a sort condition is given for a
    /// hash-only index.
    pub fn index_query_keys(
        &self,
        table: &str,
        index: &str,
        value: &AttributeValue,
        condition: Option<&SortKeyCondition>,
    ) -> Result<Vec<Vec<u8>>, RegistryError> {
        let state = self.state(table)?;
        let idx = state
            .indexes
            .get(index)
            .ok_or_else(|| RegistryError::NoSuchIndex(index.to_owned()))?;
        if condition.is_some() && idx.sort_attribute.is_none() {
            return Err(RegistryError::IndexSortMismatch(index.to_owned()));
        }
        // Entries are `escape(hash) [|| escape(sort)] || base_key`; one hash
        // value's entries form a contiguous sub-range (the escape is prefix-free,
        // ending `0x00 0x00`). `hash_len` is where the sort/base suffix begins.
        let hash_prefix = escape(&value.key_bytes());
        let mut end = hash_prefix.clone();
        *end.last_mut().expect("escape is non-empty") = 0x01;
        let composite = idx.sort_attribute.is_some();
        let mut keys = Vec::new();
        for entry in idx.entries.range(hash_prefix.clone()..end) {
            let suffix = &entry[hash_prefix.len()..];
            if composite {
                // The sort value is `escape(sort)` (prefix-free), then the base
                // key. Recover it to test the condition.
                let (sort_bytes, base) = split_escaped_prefix(suffix);
                if let Some(cond) = condition {
                    if !cond.matches(&AttributeValue::B(sort_bytes.to_vec())) {
                        continue;
                    }
                }
                keys.push(base.to_vec());
            } else {
                keys.push(suffix.to_vec());
            }
        }
        Ok(keys)
    }

    /// Whether `index` on `table` is composite (has a sort/range attribute).
    /// Used by the caller to decide whether a sort-key condition is allowed.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`] or [`RegistryError::NoSuchIndex`].
    pub fn index_is_composite(&self, table: &str, index: &str) -> Result<bool, RegistryError> {
        let state = self.state(table)?;
        let idx = state
            .indexes
            .get(index)
            .ok_or_else(|| RegistryError::NoSuchIndex(index.to_owned()))?;
        Ok(idx.sort_attribute.is_some())
    }

    /// The set of top-level attribute names a `Query` against `index` returns,
    /// per the index's declared [`IndexProjection`]. `None` means "all attributes"
    /// (`ALL`); `Some(names)` is the projected set: for `KEYS_ONLY` the base
    /// table's key attributes plus the index's own key attributes, and for
    /// `INCLUDE` those keys plus the explicitly included non-key attributes. The
    /// caller applies this set the same way a `ProjectionExpression` would, after
    /// reading the base item.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`] or [`RegistryError::NoSuchIndex`].
    pub fn index_projected_attributes(
        &self,
        table: &str,
        index: &str,
    ) -> Result<Option<Vec<String>>, RegistryError> {
        let state = self.state(table)?;
        let idx = state
            .indexes
            .get(index)
            .ok_or_else(|| RegistryError::NoSuchIndex(index.to_owned()))?;
        match &idx.projection {
            IndexProjection::All => Ok(None),
            IndexProjection::KeysOnly => Ok(Some(self.key_attributes(state, idx))),
            IndexProjection::Include(extra) => {
                let mut names = self.key_attributes(state, idx);
                for name in extra {
                    if !names.contains(name) {
                        names.push(name.clone());
                    }
                }
                Ok(Some(names))
            }
        }
    }

    /// The base-table + index key attribute names of an index (the always-present
    /// projected attributes for `KEYS_ONLY` / `INCLUDE`), de-duplicated, in a
    /// stable order: base partition key, base sort key, index hash attribute,
    /// index sort attribute.
    fn key_attributes(&self, state: &TableState, idx: &IndexState) -> Vec<String> {
        let mut names = Vec::new();
        let mut push = |name: &str| {
            if !names.iter().any(|n: &String| n == name) {
                names.push(name.to_owned());
            }
        };
        push(&state.schema.partition_key);
        if let Some(sk) = &state.schema.sort_key {
            push(sk);
        }
        push(&idx.hash_attribute);
        if let Some(sort) = &idx.sort_attribute {
            push(sort);
        }
        names
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

/// A secondary-index entry: `escape(hash) [|| escape(sort)] || base_storage_key`.
/// Each escape is prefix-free, so one hash value's entries form a contiguous
/// range (ordered by sort value then base key for a composite index), and both
/// the sort value and the base key are recoverable.
fn index_entry(hash: &AttributeValue, sort: Option<&AttributeValue>, base_key: &[u8]) -> Vec<u8> {
    let mut entry = escape(&hash.key_bytes());
    if let Some(sort) = sort {
        entry.extend_from_slice(&escape(&sort.key_bytes()));
    }
    entry.extend_from_slice(base_key);
    entry
}

/// Split a buffer that begins with one prefix-free `escape(..)` value into that
/// value's *raw* bytes (the escape removed) and the remaining suffix. Used to
/// peel an index entry's `escape(sort)` off its base key.
fn split_escaped_prefix(buf: &[u8]) -> (Vec<u8>, &[u8]) {
    let mut raw = Vec::new();
    let mut i = 0;
    while i + 1 < buf.len() {
        if buf[i] == 0x00 {
            match buf[i + 1] {
                0x00 => return (raw, &buf[i + 2..]), // terminator
                0x01 => {
                    raw.push(0x00); // escaped zero byte
                    i += 2;
                }
                _ => {
                    raw.push(buf[i]);
                    i += 1;
                }
            }
        } else {
            raw.push(buf[i]);
            i += 1;
        }
    }
    (raw, &[])
}

/// Recover the base storage key from an index entry by skipping `segments`
/// prefix-free `escape(..)` segments (each terminated by `0x00 0x00`). A
/// hash-only entry has one segment; a composite entry has two (hash, then sort).
fn base_key_of(entry: &[u8], segments: usize) -> &[u8] {
    // Find each `0x00 0x00` terminator that is not part of an escaped `0x00`
    // (escaped as `0x00 0x01`). Scan for `0x00` and look at the next byte.
    let mut rest = entry;
    for _ in 0..segments {
        let mut i = 0;
        let mut found = false;
        while i + 1 < rest.len() {
            if rest[i] == 0x00 {
                match rest[i + 1] {
                    0x00 => {
                        rest = &rest[i + 2..]; // past this terminator
                        found = true;
                        break;
                    }
                    0x01 => i += 2, // escaped zero byte
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }
        if !found {
            return &[];
        }
    }
    rest
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
            vec![SecondaryIndex::Global(GlobalSecondaryIndex {
                name: "by-email".into(),
                key_attribute: "email".into(),
                sort_attribute: None,
                projection: IndexProjection::All,
            })],
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
            .index_query_keys("users", "by-email", &s("a@x"), None)
            .unwrap();
        got.sort();
        let mut expect = vec![k1.clone(), k3.clone()];
        expect.sort();
        assert_eq!(got, expect);

        // Re-indexing on overwrite: u1 changes email; it leaves a@x, joins c@x.
        put(&mut reg, "u1", "c@x");
        assert_eq!(
            reg.index_query_keys("users", "by-email", &s("a@x"), None)
                .unwrap(),
            vec![k3.clone()]
        );
        assert_eq!(
            reg.index_query_keys("users", "by-email", &s("c@x"), None)
                .unwrap(),
            vec![k1.clone()]
        );

        // Delete drops the item from the index.
        reg.note_delete("users", &k3).unwrap();
        assert!(
            reg.index_query_keys("users", "by-email", &s("a@x"), None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn index_query_on_unknown_index() {
        let mut reg = SchemaRegistry::new();
        reg.create_table("t", TableSchema::simple("id")).unwrap();
        assert_eq!(
            reg.index_query_keys("t", "nope", &s("v"), None),
            Err(RegistryError::NoSuchIndex("nope".into()))
        );
    }

    #[test]
    fn multiple_gsis_are_independent() {
        let mut reg = SchemaRegistry::new();
        reg.create_table_with_indexes(
            "users",
            TableSchema::simple("id"),
            vec![
                SecondaryIndex::Global(GlobalSecondaryIndex {
                    name: "by-email".into(),
                    key_attribute: "email".into(),
                    sort_attribute: None,
                    projection: IndexProjection::All,
                }),
                SecondaryIndex::Global(GlobalSecondaryIndex {
                    name: "by-org".into(),
                    key_attribute: "org".into(),
                    sort_attribute: None,
                    projection: IndexProjection::All,
                }),
            ],
        )
        .unwrap();
        let key = storage_key(&s("u1"), None);
        reg.note_put(
            "users",
            &key,
            &item(&[("id", s("u1")), ("email", s("a@x")), ("org", s("acme"))]),
        )
        .unwrap();
        assert_eq!(
            reg.index_query_keys("users", "by-email", &s("a@x"), None)
                .unwrap(),
            vec![key.clone()]
        );
        assert_eq!(
            reg.index_query_keys("users", "by-org", &s("acme"), None)
                .unwrap(),
            vec![key.clone()]
        );
    }

    #[test]
    fn lsi_narrows_by_alternate_sort_within_partition() {
        let mut reg = SchemaRegistry::new();
        reg.create_table_with_indexes(
            "events",
            TableSchema::composite("pk", "sk"),
            vec![SecondaryIndex::Local(LocalSecondaryIndex {
                name: "by-ts".into(),
                sort_attribute: "ts".into(),
                projection: IndexProjection::All,
            })],
        )
        .unwrap();
        // Same partition p1, different (sk, ts).
        for (sk, ts) in [("a", "30"), ("b", "10"), ("c", "20")] {
            let key = storage_key(&s("p1"), Some(&s(sk)));
            reg.note_put(
                "events",
                &key,
                &item(&[("pk", s("p1")), ("sk", s(sk)), ("ts", s(ts))]),
            )
            .unwrap();
        }
        // A different partition's item must not appear in p1's LSI query.
        let other = storage_key(&s("p2"), Some(&s("a")));
        reg.note_put(
            "events",
            &other,
            &item(&[("pk", s("p2")), ("sk", s("a")), ("ts", s("10"))]),
        )
        .unwrap();

        // LSI hashes by the base partition key (p1) and sorts by ts: order is
        // ts 10 (b), 20 (c), 30 (a).
        let got = reg
            .index_query_keys("events", "by-ts", &s("p1"), None)
            .unwrap();
        assert_eq!(
            got,
            vec![
                storage_key(&s("p1"), Some(&s("b"))),
                storage_key(&s("p1"), Some(&s("c"))),
                storage_key(&s("p1"), Some(&s("a"))),
            ]
        );
        // Narrow by a sort condition on ts.
        let between = reg
            .index_query_keys(
                "events",
                "by-ts",
                &s("p1"),
                Some(&SortKeyCondition::Between(s("10"), s("20"))),
            )
            .unwrap();
        assert_eq!(
            between,
            vec![
                storage_key(&s("p1"), Some(&s("b"))),
                storage_key(&s("p1"), Some(&s("c"))),
            ]
        );
    }

    #[test]
    fn sort_condition_on_hash_only_index_is_rejected() {
        let mut reg = SchemaRegistry::new();
        reg.create_table_with_indexes(
            "users",
            TableSchema::simple("id"),
            vec![SecondaryIndex::Global(GlobalSecondaryIndex {
                name: "by-email".into(),
                key_attribute: "email".into(),
                sort_attribute: None,
                projection: IndexProjection::All,
            })],
        )
        .unwrap();
        assert_eq!(
            reg.index_query_keys(
                "users",
                "by-email",
                &s("a@x"),
                Some(&SortKeyCondition::Equals(s("z")))
            ),
            Err(RegistryError::IndexSortMismatch("by-email".into()))
        );
    }
}
