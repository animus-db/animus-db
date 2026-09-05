//! A DynamoDB-style **item API** over the common AnimusDB storage core
//! (ADR 0006). This is the data-model half of the adapter: it maps the
//! Dynamo-lineage item model (a partition key, an optional sort key, and an
//! attribute map — ADR 0004) onto the [`StorageEngine`] trait, so the same
//! engine that backs the data plane also backs the DynamoDB surface.
//!
//! **The pure item model lives in `animus-item` now (ADR 0054 step 1).**
//! `AttributeValue`/`Item`/`TableSchema`, the key-encoding primitives
//! (`escape`/`storage_key`/[`numkey`]), `ConditionExpression`/
//! `SortKeyCondition` ([`condition`]), the `UpdateExpression` model and its
//! evaluator ([`wire::apply_update`] et al.), the stored-item codec, and the
//! secondary-index key/footprint/change-record derivation ([`index`]) were
//! extracted into that crate, which sits **below** both this crate and
//! `animus-cp-data` — a protocol-agnostic KV state machine that cannot
//! depend on a wire crate. Everything below is re-exported here unchanged,
//! so every existing `animus_dynamo::X` path keeps compiling; see
//! `crates/animus-item/CLAUDE.md` for what actually lives there now.
//!
//! **Scope.** This implements `PutItem` / `GetItem` / `DeleteItem` / `Query`
//! against a [`StorageEngine`]. The DynamoDB HTTP/JSON *wire protocol* lives
//! in [`wire`]; the distributed request path is `animusd`'s.
//!
//! ## Key encoding
//!
//! A storage key is `escape(partition_key) || sort_key`, where `escape` is
//! order-preserving and prefix-free (`0x00 -> 0x00 0x01`, `0x00 0x00`
//! terminator). All items in a partition are therefore contiguous and ordered
//! by sort key, so a `Query` is a single range scan over the partition's prefix.

use animus_storage::{StorageEngine, Version};

pub mod capacity;
pub mod internal_tables;
pub mod registry;
pub mod schema;
pub mod sigv4;
pub mod streams_wire;
pub mod ttl;
pub mod wire;

pub use animus_item::{
    AttributeValue, ChangeRecord, Comparator, ConditionError, ConditionExpression, FootprintEntry,
    GsiRowRef, IndexFootprint, Item, ItemFootprint, LsiRowRef, SortKeyCondition, TableSchema,
    condition, index, index_table_name, is_index_table_name, numkey, split_index_table_name,
    storage_key,
};
pub use internal_tables::{TXN_IDEMPOTENCY_TABLE, is_internal_table_name};
pub use registry::{
    GlobalSecondaryIndex, IndexProjection, LocalSecondaryIndex, RegistryError, SchemaRegistry,
    SecondaryIndex,
};
pub use ttl::{MAX_PAST_EXPIRY_SECS, expires_at, is_expired};

/// Errors from the item API.
#[derive(Debug, thiserror::Error)]
pub enum DynamoError {
    /// A required key attribute was absent from the item.
    #[error("item is missing key attribute `{0}`")]
    MissingKey(String),
    /// The underlying storage engine failed.
    #[error("storage error: {0}")]
    Storage(#[from] animus_storage::StorageError),
    /// A stored item could not be decoded.
    #[error("corrupt stored item: {0}")]
    Corrupt(String),
}

type Result<T> = std::result::Result<T, DynamoError>;

/// A table backed by a [`StorageEngine`]. Writes use a monotonic version
/// counter seeded from the engine's latest version.
pub struct Table<S: StorageEngine> {
    engine: S,
    schema: TableSchema,
    next_version: std::sync::Mutex<Version>,
}

impl<S: StorageEngine> Table<S> {
    /// Open a table with `schema` over `engine`.
    pub fn new(engine: S, schema: TableSchema) -> Self {
        let next = engine.latest_version() + 1;
        Self {
            engine,
            schema,
            next_version: std::sync::Mutex::new(next),
        }
    }

    fn next_version(&self) -> Version {
        let mut v = self.next_version.lock().expect("version counter poisoned");
        let version = *v;
        *v += 1;
        version
    }

    /// The storage key for the item's key attributes.
    fn key_for_item(&self, item: &Item) -> Result<Vec<u8>> {
        let pk = item
            .get(&self.schema.partition_key)
            .ok_or_else(|| DynamoError::MissingKey(self.schema.partition_key.clone()))?;
        let sk = match &self.schema.sort_key {
            Some(name) => Some(
                item.get(name)
                    .ok_or_else(|| DynamoError::MissingKey(name.clone()))?,
            ),
            None => None,
        };
        Ok(self.storage_key(pk, sk))
    }

    fn storage_key(&self, pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
        storage_key(pk, sk)
    }

    /// `PutItem`: insert or replace an item (keyed by its key attributes).
    pub async fn put_item(&self, item: Item) -> Result<()> {
        let key = self.key_for_item(&item)?;
        let value = serde_json::to_vec(&item).expect("item serializes");
        self.engine.put(&key, &value, self.next_version()).await?;
        Ok(())
    }

    /// `GetItem`: fetch the item with the given key, if present.
    pub async fn get_item(
        &self,
        pk: &AttributeValue,
        sk: Option<&AttributeValue>,
    ) -> Result<Option<Item>> {
        let key = self.storage_key(pk, sk);
        match self.engine.get(&key).await? {
            Some(vv) => Ok(Some(decode_item(&vv.value)?)),
            None => Ok(None),
        }
    }

    /// `DeleteItem`: remove the item with the given key (a tombstone).
    pub async fn delete_item(
        &self,
        pk: &AttributeValue,
        sk: Option<&AttributeValue>,
    ) -> Result<()> {
        let key = self.storage_key(pk, sk);
        self.engine.delete(&key, self.next_version()).await?;
        Ok(())
    }

    /// `Query`: all live items in a partition, ordered by sort key.
    pub async fn query(&self, pk: &AttributeValue) -> Result<Vec<Item>> {
        self.query_with(pk, None).await
    }

    /// `Query` with an optional sort-key `condition` (`=`, `BETWEEN`,
    /// `begins_with`): the live items in `pk`'s partition that satisfy it,
    /// ordered by sort key. With `None` this is the whole partition.
    pub async fn query_with(
        &self,
        pk: &AttributeValue,
        condition: Option<&crate::condition::SortKeyCondition>,
    ) -> Result<Vec<Item>> {
        // `storage_key(pk, None)` is exactly `escape(pk.key_bytes())` — no
        // sort key to append — which keeps this crate from needing
        // `animus-item`'s private `escape`/`key_bytes` outside `storage_key`
        // itself (ADR 0054 step 1).
        let prefix = storage_key(pk, None);
        // The partition's keys all start with `prefix` (which ends in
        // `0x00 0x00`); bumping the final byte to `0x01` is the first key past
        // the partition.
        let mut end = prefix.clone();
        *end.last_mut().expect("escape is non-empty") = 0x01;
        let mut items = Vec::new();
        for (key, vv) in self.engine.scan(&prefix, &end).await? {
            if let Some(cond) = condition {
                // The sort-key bytes are everything after the escaped pk;
                // `matches_raw` reinterprets them per the condition's own
                // declared type (numeric for `N`, raw bytes otherwise) rather
                // than comparing them as opaque bytes — see its own doc for
                // why that distinction matters for `N` sort keys.
                if !cond.matches_raw(&key[prefix.len()..]) {
                    continue;
                }
            }
            items.push(decode_item(&vv.value)?);
        }
        Ok(items)
    }
}

fn decode_item(bytes: &[u8]) -> Result<Item> {
    serde_json::from_slice(bytes).map_err(|e| DynamoError::Corrupt(e.to_string()))
}
