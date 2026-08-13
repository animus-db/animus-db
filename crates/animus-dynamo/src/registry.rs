//! A per-table **schema registry** (ADR 0006), so the DynamoDB key convention is
//! no longer hard-coded: `CreateTable` records a table's key attribute names and
//! types, and later `PutItem`/`GetItem`/`Query` resolve their keys against it.
//!
//! The registry is a **pure, deterministic** in-memory map (`BTreeMap` only — no
//! `HashMap`, per ADR 0003). `animusd` holds one instance behind a lock at its
//! HTTP edge; **this in-memory copy is not itself durable** — but the *table
//! key schema* and each secondary index's *definition* are (ADR 0013): they
//! live in the control plane's replicated `Metadata` catalog, and `animusd`
//! rebuilds this registry's schema/index-definition mirror from that catalog
//! on every read/write path (`sync_indexes`), so a restart or a node that never
//! saw the `CreateTable` still knows the shape.
//!
//! ## What this registry does NOT track (ADR 0041)
//!
//! Neither the base table's items nor a secondary index's **entries** live
//! here. A base-table `Query`/`Scan` is served by the data plane's **native
//! quorum range scan** ([`crate`] is pure, so the scan itself lives in the
//! `animusd` edge), reading live storage in key order. A secondary-index
//! `Query` is, likewise, a **second native range scan** — over a GSI's own
//! hidden table or an LSI's colocated `KIND_LSI` scope — decoding each row's
//! already-projected stored value directly; see `animusd::dynamo`'s
//! `run_gsi_query`/`run_lsi_query`. Index rows are ordinary replicated
//! data-plane rows now, materialized by an atomic write (LSI) or an
//! asynchronous drain over a change log (GSI, `animusd::index_drain`) — not by
//! this registry. What this registry keeps is purely the **definition**
//! bookkeeping a table needs regardless: its key schema (for key extraction)
//! and each index's declared shape (hash/sort attribute names + projection),
//! reconciled from the replicated catalog by [`SchemaRegistry::sync_indexes`].
//!
//! ## Secondary indexes (GSI + LSI)
//!
//! A `CreateTable` may declare **secondary indexes**: alternate ways to look an
//! item up. The registry tracks any number of them, keyed by name, purely as
//! **shape**:
//!
//! - **Global secondary indexes (GSI)** are keyed by a hash attribute, optionally
//!   plus a range attribute (a composite GSI). Their keyspace is independent of
//!   the base table's partition.
//! - **Local secondary indexes (LSI)** share the base table's *partition* key but
//!   project an alternate **sort** attribute, so they narrow within a partition by
//!   a different sort key.

use std::collections::BTreeMap;

use crate::TableSchema;

/// What attributes a secondary index projects (the `Projection` of a
/// `CreateTable` index declaration). The *shape* recorded here bounds what an
/// index `Query` is allowed to **return** — applied at the edge (or, for a
/// GSI, already applied by the drain when it materializes each row).
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

/// A table's registered schema plus its declared secondary-index **shapes**.
/// Neither the base table's items nor a secondary index's entries are tracked
/// here (ADR 0041) — see the module doc.
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

/// A single secondary index's declared **shape** — hash/sort attribute names
/// plus its projection. No entry data (ADR 0041): an index `Query` reads
/// replicated data-plane rows directly (`animusd::dynamo`'s
/// `run_gsi_query`/`run_lsi_query`), not anything tracked here.
///
/// For a GSI the hash attribute is the item attribute named by `hash_attribute`;
/// for an LSI the hash value is the item's *base partition key* (so the index is
/// "local" to a partition). `sort_attribute` is the optional range attribute
/// (always set for an LSI; set for a composite GSI; `None` for a hash-only GSI).
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
}

/// Build the `(index_name, IndexState)` for a [`SecondaryIndex`] declaration,
/// given the base table's partition key (an LSI hashes by it).
fn index_state(index: &SecondaryIndex, base_partition_key: &str) -> (String, IndexState) {
    match index {
        SecondaryIndex::Global(g) => (
            g.name.clone(),
            IndexState {
                hash_attribute: g.key_attribute.clone(),
                sort_attribute: g.sort_attribute.clone(),
                projection: g.projection.clone(),
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
            },
        ),
    }
}

/// An in-memory registry of table schemas + per-table secondary-index shapes.
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

    /// Reconcile `table`'s declared secondary-index **shapes** to match
    /// `indexes` — the **definitions read from the control plane's replicated
    /// catalog** (ADR 0013). This is how the wire edge rebuilds its
    /// key/index-definition bookkeeping from the cluster-agreed, durable index
    /// *definitions* (surviving a restart) rather than from process-local
    /// `create_table_with_indexes` state.
    ///
    /// The table is registered with `schema` if absent, then its index set is
    /// replaced wholesale to match `indexes` — there is no per-index entry data
    /// to preserve or discard across a shape change any more (ADR 0041), so
    /// this is a plain resync, not a merge.
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
        state.indexes = indexes
            .iter()
            .map(|index| index_state(index, &partition_key))
            .collect();
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
    ) -> Result<(crate::AttributeValue, Option<crate::AttributeValue>), RegistryError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AttributeValue, Item};

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
        // The index's shape is known: hash-only, ALL projection.
        assert_eq!(reg.index_is_composite("users", "by-email"), Ok(false));
        assert_eq!(
            reg.index_projected_attributes("users", "by-email"),
            Ok(None)
        );
    }

    #[test]
    fn sync_indexes_adds_and_drops_indexes() {
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
        reg.create_table_with_indexes("users", TableSchema::simple("id"), vec![email.clone(), org])
            .unwrap();
        assert!(reg.index_is_composite("users", "by-org").is_ok());

        // Re-sync with `by-org` dropped and `by-email` unchanged.
        reg.sync_indexes("users", TableSchema::simple("id"), &[email])
            .unwrap();
        assert_eq!(reg.index_is_composite("users", "by-email"), Ok(false));
        assert_eq!(
            reg.index_is_composite("users", "by-org"),
            Err(RegistryError::NoSuchIndex("by-org".into()))
        );

        // A later sync can re-add a dropped index (and add a genuinely new one).
        let ts = SecondaryIndex::Global(GlobalSecondaryIndex {
            name: "by-ts".into(),
            key_attribute: "ts".into(),
            sort_attribute: Some("id".into()),
            projection: IndexProjection::KeysOnly,
        });
        reg.sync_indexes("users", TableSchema::simple("id"), &[ts])
            .unwrap();
        assert_eq!(
            reg.index_is_composite("users", "by-email"),
            Err(RegistryError::NoSuchIndex("by-email".into())),
            "by-email dropped by the latest sync"
        );
        assert_eq!(reg.index_is_composite("users", "by-ts"), Ok(true));
    }

    #[test]
    fn index_lookup_on_unknown_table_or_index() {
        let mut reg = SchemaRegistry::new();
        assert_eq!(
            reg.index_is_composite("nope", "by-email"),
            Err(RegistryError::NoSuchTable("nope".into()))
        );
        reg.create_table("t", TableSchema::simple("id")).unwrap();
        assert_eq!(
            reg.index_is_composite("t", "nope"),
            Err(RegistryError::NoSuchIndex("nope".into()))
        );
        assert_eq!(
            reg.index_projected_attributes("t", "nope"),
            Err(RegistryError::NoSuchIndex("nope".into()))
        );
    }

    #[test]
    fn index_projected_attributes_reflects_declared_projection() {
        let mut reg = SchemaRegistry::new();
        reg.create_table_with_indexes(
            "users",
            TableSchema::simple("id"),
            vec![
                SecondaryIndex::Global(GlobalSecondaryIndex {
                    name: "by-email".into(),
                    key_attribute: "email".into(),
                    sort_attribute: None,
                    projection: IndexProjection::KeysOnly,
                }),
                SecondaryIndex::Global(GlobalSecondaryIndex {
                    name: "by-org-ts".into(),
                    key_attribute: "org".into(),
                    sort_attribute: Some("ts".into()),
                    projection: IndexProjection::Include(vec!["extra".into()]),
                }),
                SecondaryIndex::Global(GlobalSecondaryIndex {
                    name: "by-all".into(),
                    key_attribute: "x".into(),
                    sort_attribute: None,
                    projection: IndexProjection::All,
                }),
            ],
        )
        .unwrap();

        // KEYS_ONLY: the base table's key ("id") plus the index's own hash key.
        let mut got = reg
            .index_projected_attributes("users", "by-email")
            .unwrap()
            .unwrap();
        got.sort();
        assert_eq!(got, vec!["email".to_owned(), "id".to_owned()]);

        // INCLUDE: keys plus the extra attribute, including a composite index's
        // sort attribute.
        let mut got = reg
            .index_projected_attributes("users", "by-org-ts")
            .unwrap()
            .unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                "extra".to_owned(),
                "id".to_owned(),
                "org".to_owned(),
                "ts".to_owned(),
            ]
        );

        // ALL: no filtering at all.
        assert_eq!(reg.index_projected_attributes("users", "by-all"), Ok(None));
    }
}
