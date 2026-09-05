//! The stored-item codec: the serialized form of an item as the data plane
//! stores it at its key (ADR 0054 step 1 — moved here so a future apply-path
//! evaluator, which reads and writes this exact byte shape, does not need
//! `animus-dynamo`).
//!
//! A live item is `{"item": {..}}`; a deleted item is recorded as a
//! tombstone (`{"tombstone": true}`) because the data plane has no native
//! delete yet (ADR 0010). A read treats a tombstone as absent.

use serde::{Deserialize, Serialize};

use crate::Item;

/// The serialized form of an item as stored in the data plane. See the
/// module doc for the live/tombstone shape.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredItem {
    Item(Item),
    Tombstone,
}

/// Serialize a live item to the bytes the data plane stores at its key.
#[must_use]
pub fn encode_stored_item(item: &Item) -> Vec<u8> {
    serde_json::to_vec(&StoredItem::Item(item.clone())).expect("stored item serializes")
}

/// Serialize a delete tombstone (the data plane has no native delete).
#[must_use]
pub fn encode_tombstone() -> Vec<u8> {
    serde_json::to_vec(&StoredItem::Tombstone).expect("tombstone serializes")
}

/// Decode bytes read from the data plane back into an item, or `None` for an
/// absent key or a tombstone.
///
/// # Errors
/// Returns a message describing the decode failure if the stored bytes are
/// not a valid encoded item. The caller (`animus_dynamo::wire::
/// decode_stored_item`) wraps this into its own `WireError::serialization`.
pub fn decode_stored_item(bytes: &[u8]) -> Result<Option<Item>, String> {
    let stored: StoredItem = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    Ok(match stored {
        StoredItem::Item(item) => Some(item),
        StoredItem::Tombstone => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AttributeValue;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    #[test]
    fn stored_item_tombstone_reads_as_absent() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        let bytes = encode_stored_item(&item);
        assert_eq!(decode_stored_item(&bytes).unwrap(), Some(item));
        let tomb = encode_tombstone();
        assert_eq!(decode_stored_item(&tomb).unwrap(), None);
    }
}
