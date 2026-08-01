//! A DynamoDB-style **item API** over the common CustosDB storage core
//! (ADR 0006). This is the data-model half of the adapter: it maps the
//! Dynamo-lineage item model (a partition key, an optional sort key, and an
//! attribute map — ADR 0004) onto the [`StorageEngine`] trait, so the same
//! engine that backs the data plane also backs the DynamoDB surface.
//!
//! **Scope.** This implements `PutItem` / `GetItem` / `DeleteItem` / `Query`
//! against a [`StorageEngine`]. The DynamoDB HTTP/JSON *wire protocol*,
//! conditional writes, secondary indexes, and the distributed request path are
//! explicitly future work — as is the parallel CQL adapter (`custos-cql`), which
//! would map the CQL model onto the same core in the same way.
//!
//! ## Key encoding
//!
//! A storage key is `escape(partition_key) || sort_key`, where `escape` is
//! order-preserving and prefix-free (`0x00 -> 0x00 0x01`, `0x00 0x00`
//! terminator). All items in a partition are therefore contiguous and ordered
//! by sort key, so a `Query` is a single range scan over the partition's prefix.

use std::collections::BTreeMap;

use custos_storage::{StorageEngine, Version};
use serde::{Deserialize, Serialize};

pub mod wire;

/// A DynamoDB-style attribute value (a useful subset).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttributeValue {
    /// String (`S`).
    S(String),
    /// Number (`N`) — carried as text, as on the DynamoDB wire.
    N(String),
    /// Binary (`B`).
    B(Vec<u8>),
    /// Boolean (`BOOL`).
    Bool(bool),
    /// Null (`NULL`).
    Null,
}

impl AttributeValue {
    /// Byte encoding used when an attribute is part of a key. String/number/
    /// binary sort by these bytes (numbers therefore sort lexicographically — a
    /// documented simplification of DynamoDB's numeric ordering).
    pub(crate) fn key_bytes(&self) -> Vec<u8> {
        match self {
            AttributeValue::S(s) => s.clone().into_bytes(),
            AttributeValue::N(n) => n.clone().into_bytes(),
            AttributeValue::B(b) => b.clone(),
            AttributeValue::Bool(b) => vec![u8::from(*b)],
            AttributeValue::Null => Vec::new(),
        }
    }
}

/// An item: a map from attribute name to value.
pub type Item = BTreeMap<String, AttributeValue>;

/// A table's key schema.
#[derive(Clone, Debug)]
pub struct TableSchema {
    /// Partition (hash) key attribute name.
    pub partition_key: String,
    /// Optional sort (range) key attribute name.
    pub sort_key: Option<String>,
}

impl TableSchema {
    /// A table with only a partition key.
    #[must_use]
    pub fn simple(partition_key: impl Into<String>) -> Self {
        Self {
            partition_key: partition_key.into(),
            sort_key: None,
        }
    }

    /// A table with a partition key and a sort key.
    #[must_use]
    pub fn composite(partition_key: impl Into<String>, sort_key: impl Into<String>) -> Self {
        Self {
            partition_key: partition_key.into(),
            sort_key: Some(sort_key.into()),
        }
    }
}

/// Errors from the item API.
#[derive(Debug, thiserror::Error)]
pub enum DynamoError {
    /// A required key attribute was absent from the item.
    #[error("item is missing key attribute `{0}`")]
    MissingKey(String),
    /// The underlying storage engine failed.
    #[error("storage error: {0}")]
    Storage(#[from] custos_storage::StorageError),
    /// A stored item could not be decoded.
    #[error("corrupt stored item: {0}")]
    Corrupt(String),
}

type Result<T> = std::result::Result<T, DynamoError>;

/// Order-preserving, prefix-free escape (shared scheme with the `fjall` backend).
pub(crate) fn escape(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0x01);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
    out
}

/// The storage key for an item addressed by partition key `pk` and optional
/// sort key `sk`: `escape(pk) || sk`. This is the engine/data-plane key the
/// item maps onto — exposed so the wire layer can route an item through the
/// distributed data plane without instantiating a local-engine [`Table`].
#[must_use]
pub fn storage_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    let mut key = escape(&pk.key_bytes());
    if let Some(sk) = sk {
        key.extend_from_slice(&sk.key_bytes());
    }
    key
}

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
    pub fn put_item(&self, item: Item) -> Result<()> {
        let key = self.key_for_item(&item)?;
        let value = serde_json::to_vec(&item).expect("item serializes");
        self.engine.put(&key, &value, self.next_version())?;
        Ok(())
    }

    /// `GetItem`: fetch the item with the given key, if present.
    pub fn get_item(
        &self,
        pk: &AttributeValue,
        sk: Option<&AttributeValue>,
    ) -> Result<Option<Item>> {
        let key = self.storage_key(pk, sk);
        match self.engine.get(&key)? {
            Some(vv) => Ok(Some(decode_item(&vv.value)?)),
            None => Ok(None),
        }
    }

    /// `DeleteItem`: remove the item with the given key (a tombstone).
    pub fn delete_item(&self, pk: &AttributeValue, sk: Option<&AttributeValue>) -> Result<()> {
        let key = self.storage_key(pk, sk);
        self.engine.delete(&key, self.next_version())?;
        Ok(())
    }

    /// `Query`: all live items in a partition, ordered by sort key.
    pub fn query(&self, pk: &AttributeValue) -> Result<Vec<Item>> {
        let prefix = escape(&pk.key_bytes());
        // The partition's keys all start with `prefix` (which ends in
        // `0x00 0x00`); bumping the final byte to `0x01` is the first key past
        // the partition.
        let mut end = prefix.clone();
        *end.last_mut().expect("escape is non-empty") = 0x01;
        let mut items = Vec::new();
        for (_k, vv) in self.engine.scan(&prefix, &end)? {
            items.push(decode_item(&vv.value)?);
        }
        Ok(items)
    }
}

fn decode_item(bytes: &[u8]) -> Result<Item> {
    serde_json::from_slice(bytes).map_err(|e| DynamoError::Corrupt(e.to_string()))
}
