//! The stored-item codec: the serialized form of an item as the data plane
//! stores it at its key (ADR 0054 step 1 — moved here so a future apply-path
//! evaluator, which reads and writes this exact byte shape, does not need
//! `animus-dynamo`).
//!
//! A live item is `{"item": {..}}`; a deleted item is recorded as a
//! tombstone (the bare JSON string `"tombstone"`) because the data plane has no native
//! delete yet (ADR 0010). A read treats a tombstone as absent.

use serde::{Deserialize, Serialize};

use crate::Item;

/// The serialized form of an item as stored in the data plane. See the
/// module doc for the live/tombstone shape.
///
/// **Frozen format (ADR 0073).** This enum's serde shape — together with
/// [`Item`] and [`crate::AttributeValue`], which it embeds — is the durable
/// v1 row-value format of every base row, and it outlives the cluster
/// (backups, PITR segments, S3 export all carry it). It is never changed in
/// place: changes are additive-only (a new optional field or variant, each
/// with a new golden fixture under `tests/fixtures/formats/stored-item/`),
/// and anything else is a new tagged version (see [`stored_item_version`]).
/// Any `legacy` module that embeds `Item`/`AttributeValue` depends on this
/// shape being frozen (ADR 0073 decoder pattern, point 4).
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StoredItem {
    Item(Item),
    Tombstone,
}

/// The version of the stored-item encoding `bytes` is written in, sniffed
/// from its first byte, or `None` if it is not a recognised encoding.
///
/// **v1 is untagged JSON** of serde's externally-tagged `StoredItem`: a live
/// item is an object (`{"item": ..}`), a tombstone is the bare JSON string
/// `"tombstone"` (a unit variant serializes as a string, not as an object).
/// So v1's first non-whitespace byte is `{` or `"` (the v1 writer emits no
/// leading whitespace; it is skipped only so the sniff accepts everything the
/// v1 JSON parser always accepted). A later, tagged version must begin with a
/// magic whose first byte is **neither** `{` nor `"` (nor ASCII whitespace),
/// which makes the sniff unambiguous. See ADR 0073's "Phase 1 design" amendment.
#[must_use]
pub fn stored_item_version(bytes: &[u8]) -> Option<u32> {
    match bytes.iter().find(|b| !b.is_ascii_whitespace()) {
        Some(b'{' | b'"') => Some(1),
        _ => None,
    }
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
    let stored: StoredItem = match stored_item_version(bytes) {
        Some(1) => serde_json::from_slice(bytes).map_err(|e| e.to_string())?,
        _ => {
            return Err(format!(
                "unrecognised stored-item encoding (first byte {:?})",
                bytes.first()
            ));
        }
    };
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

    #[test]
    fn version_sniff_treats_the_untagged_json_form_as_v1() {
        assert_eq!(stored_item_version(&encode_tombstone()), Some(1));
        assert_eq!(
            stored_item_version(&encode_stored_item(&Item::new())),
            Some(1)
        );
        assert_eq!(stored_item_version(b"  \n\"tombstone\""), Some(1));
        assert_eq!(stored_item_version(b""), None);
        assert_eq!(stored_item_version(b"\x00ITM2"), None);
        assert!(decode_stored_item(b"\x00ITM2").is_err());
        assert!(decode_stored_item(b"").is_err());
    }
}
