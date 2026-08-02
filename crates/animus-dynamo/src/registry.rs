//! A per-table **schema registry** (ADR 0006), so the DynamoDB key convention is
//! no longer hard-coded: `CreateTable` records a table's key attribute names and
//! types, and later `PutItem`/`GetItem`/`Query` resolve their keys against it.
//!
//! The registry is a **pure, deterministic** in-memory map (`BTreeMap` only — no
//! `HashMap`, per ADR 0003). `animusd` holds one instance behind a lock at its
//! HTTP edge; **it is not durable** — schemas (and the key index below) are lost
//! on restart. Persisting them through the control plane is future work.
//!
//! ## What `note_put` / `note_delete` maintain
//!
//! The base table's items are **not** tracked here any more: a base-table `Query`
//! and a `Scan` are served by the data plane's **native quorum range scan**
//! ([`crate`] is pure, so the scan itself lives in `animus-data` / the `animusd`
//! edge), reading live storage in key order. The registry only maintains the
//! **secondary-index** entries below: `note_put` adds each declared index's entry
//! for an item and `note_delete` drops it. A table with no secondary indexes makes
//! both a no-op. The index being in-memory (rebuilt only by observed writes) is
//! the same non-durability caveat as the schema map; durable/replicated index
//! state is future work.
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
//! Each index keeps an ordered set of
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

/// A table's registered schema plus its declared secondary-index entries. The
/// base table's item keys are **not** tracked here — a base `Query`/`Scan` uses
/// the data plane's native range scan over live storage.
#[derive(Clone, Debug)]
struct TableState {
    schema: TableSchema,
    /// When set, the schema's sort key is treated as **optional** during key
    /// extraction (the legacy `pk`/`sk` convention, auto-registered for tables a
    /// pre-`CreateTable` client uses without declaring a schema).
    sort_key_optional: bool,
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

impl IndexState {
    /// Whether two index states declare the *same shape* (hash/sort attributes +
    /// projection), ignoring accumulated entry data. Used by
    /// [`SchemaRegistry::sync_indexes`] to decide whether a catalog definition can
    /// reuse an existing index's entries or must clear and re-declare them.
    fn same_shape(&self, other: &IndexState) -> bool {
        self.hash_attribute == other.hash_attribute
            && self.sort_attribute == other.sort_attribute
            && self.projection == other.projection
    }
}

/// Build the `(index_name, IndexState)` for a [`SecondaryIndex`] declaration,
/// given the base table's partition key (an LSI hashes by it). The state starts
/// with no entries; writes populate them.
fn index_state(index: &SecondaryIndex, base_partition_key: &str) -> (String, IndexState) {
    match index {
        SecondaryIndex::Global(g) => (
            g.name.clone(),
            IndexState {
                hash_attribute: g.key_attribute.clone(),
                sort_attribute: g.sort_attribute.clone(),
                projection: g.projection.clone(),
                entries: BTreeSet::new(),
            },
        ),
        // An LSI hashes by the base table's partition key and sorts by its
        // alternate sort attribute.
        SecondaryIndex::Local(l) => (
            l.name.clone(),
            IndexState {
                hash_attribute: base_partition_key.to_owned(),
                sort_attribute: Some(l.sort_attribute.clone()),
                projection: l.projection.clone(),
                entries: BTreeSet::new(),
            },
        ),
    }
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
            .map(|index| index_state(&index, &schema.partition_key))
            .collect();
        self.tables.insert(
            table.to_owned(),
            TableState {
                schema,
                sort_key_optional,
                indexes,
            },
        );
        Ok(())
    }

    /// Reconcile `table`'s declared secondary indexes to match `indexes` — the
    /// **definitions read from the control plane's replicated catalog** (ADR 0013).
    /// This is how the wire edge rebuilds its index-maintenance machinery from the
    /// cluster-agreed, durable index *definitions* (surviving a restart) rather
    /// than from process-local `create_table_with_indexes` state.
    ///
    /// The table is registered with `schema` if absent (no indexes yet), then its
    /// index set is reconciled: an index whose definition is **unchanged** keeps
    /// its accumulated entry data; a **new** index is added empty (later writes
    /// populate it); an index whose definition **changed shape** has its (now
    /// stale) entries cleared and is re-declared; and an index no longer in
    /// `indexes` is dropped. The entry *data* itself is still rebuilt by observed
    /// `note_put`/`note_delete` writes — only the *definitions* are now authoritative
    /// from the catalog.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`] never occurs (the table is created if
    /// absent); the signature returns `Result` for symmetry and future use.
    pub fn sync_indexes(
        &mut self,
        table: &str,
        schema: TableSchema,
        indexes: &[SecondaryIndex],
    ) -> Result<(), RegistryError> {
        if !self.tables.contains_key(table) {
            // Register fresh with the desired indexes in one shot.
            return self.create_table_with_indexes(table, schema, indexes.to_vec());
        }
        let partition_key = self
            .tables
            .get(table)
            .map(|t| t.schema.partition_key.clone())
            .unwrap_or_default();
        let state = self.tables.get_mut(table).expect("table present");
        // Desired index name -> its freshly-built (empty) IndexState shape.
        let desired: BTreeMap<String, IndexState> = indexes
            .iter()
            .map(|index| {
                let (name, st) = index_state(index, &partition_key);
                (name, st)
            })
            .collect();
        // Drop indexes that are no longer declared.
        state.indexes.retain(|name, _| desired.contains_key(name));
        // Add new indexes and replace changed-shape ones (preserving entries when
        // the shape is identical).
        for (name, fresh) in desired {
            match state.indexes.get(&name) {
                Some(existing) if existing.same_shape(&fresh) => {} // keep entries
                _ => {
                    state.indexes.insert(name, fresh);
                }
            }
        }
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
    /// full `item` so any declared GSI/LSI entries can be maintained. A table with
    /// no secondary indexes is a no-op (the base table is no longer tracked — its
    /// `Query`/`Scan` reads live storage via the data plane's native range scan).
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`].
    pub fn note_put(&mut self, table: &str, key: &[u8], item: &Item) -> Result<(), RegistryError> {
        let state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| RegistryError::NoSuchTable(table.to_owned()))?;
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

    /// Record that the item at `key` was deleted (drop it from every secondary
    /// index). A no-op for a table with no secondary indexes.
    ///
    /// # Errors
    /// [`RegistryError::NoSuchTable`].
    pub fn note_delete(&mut self, table: &str, key: &[u8]) -> Result<(), RegistryError> {
        let state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| RegistryError::NoSuchTable(table.to_owned()))?;
        for index in state.indexes.values_mut() {
            let segments = if index.sort_attribute.is_some() { 2 } else { 1 };
            index.entries.retain(|e| base_key_of(e, segments) != key);
        }
        Ok(())
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
    fn note_delete_removes_from_secondary_index() {
        // The base table is no longer tracked, so observe `note_delete` through a
        // GSI: a put indexes the item, a delete drops the index entry.
        let mut reg = SchemaRegistry::new();
        reg.create_table_with_indexes(
            "t",
            TableSchema::composite("pk", "sk"),
            vec![SecondaryIndex::Global(GlobalSecondaryIndex {
                name: "by-g".into(),
                key_attribute: "g".into(),
                sort_attribute: None,
                projection: IndexProjection::All,
            })],
        )
        .unwrap();
        let key = storage_key(&s("p"), Some(&s("a")));
        reg.note_put(
            "t",
            &key,
            &item(&[("pk", s("p")), ("sk", s("a")), ("g", s("gv"))]),
        )
        .unwrap();
        assert_eq!(
            reg.index_query_keys("t", "by-g", &s("gv"), None).unwrap(),
            vec![key.clone()]
        );
        reg.note_delete("t", &key).unwrap();
        assert!(
            reg.index_query_keys("t", "by-g", &s("gv"), None)
                .unwrap()
                .is_empty()
        );
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
    fn sync_indexes_creates_table_with_indexes_when_absent() {
        let mut reg = SchemaRegistry::new();
        let defs = vec![SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-email".into(),
            key_attribute: "email".into(),
            sort_attribute: None,
            projection: IndexProjection::All,
        })];
        reg.sync_indexes("users", TableSchema::simple("id"), &defs)
            .unwrap();
        assert!(reg.has_table("users"));
        // The index exists; a write then indexes through it.
        let key = storage_key(&s("u1"), None);
        reg.note_put(
            "users",
            &key,
            &item(&[("id", s("u1")), ("email", s("a@x"))]),
        )
        .unwrap();
        assert_eq!(
            reg.index_query_keys("users", "by-email", &s("a@x"), None)
                .unwrap(),
            vec![key]
        );
    }

    #[test]
    fn sync_indexes_preserves_entries_on_unchanged_shape_and_drops_removed() {
        let mut reg = SchemaRegistry::new();
        let email = SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-email".into(),
            key_attribute: "email".into(),
            sort_attribute: None,
            projection: IndexProjection::All,
        });
        let org = SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-org".into(),
            key_attribute: "org".into(),
            sort_attribute: None,
            projection: IndexProjection::All,
        });
        reg.create_table_with_indexes(
            "users",
            TableSchema::simple("id"),
            vec![email.clone(), org.clone()],
        )
        .unwrap();
        let key = storage_key(&s("u1"), None);
        reg.note_put(
            "users",
            &key,
            &item(&[("id", s("u1")), ("email", s("a@x")), ("org", s("acme"))]),
        )
        .unwrap();

        // Re-sync with `by-org` dropped and `by-email` unchanged.
        reg.sync_indexes("users", TableSchema::simple("id"), &[email])
            .unwrap();
        // `by-email`'s entry survives (shape unchanged, entries preserved).
        assert_eq!(
            reg.index_query_keys("users", "by-email", &s("a@x"), None)
                .unwrap(),
            vec![key]
        );
        // `by-org` is gone.
        assert_eq!(
            reg.index_query_keys("users", "by-org", &s("acme"), None),
            Err(RegistryError::NoSuchIndex("by-org".into()))
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
