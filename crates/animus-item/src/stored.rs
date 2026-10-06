//! The stored-item codec: the serialized form of an item as the data plane
//! stores it at its key (ADR 0054 step 1 — moved here so a future apply-path
//! evaluator, which reads and writes this exact byte shape, does not need
//! `animus-dynamo`).
//!
//! A live item is `{"item": {..}}`; a deleted item is recorded as a
//! tombstone (the bare JSON string `"tombstone"`) because the data plane has no native
//! delete yet (ADR 0010). A read treats a tombstone as absent.
//!
//! A row of a **multi-Region eventual-consistency (MREC) global table** (ADR
//! 0075, G-01 stage G-d) additionally carries the [`MrecVersion`] that decides
//! last-writer-wins across Regions: `{"versioned_item": {"item": {..}, "ver":
//! {..}}}` / `{"versioned_tombstone": {"ver": {..}}}`. These two variants are
//! additive within v1 (a new variant plus golden fixtures, as the frozen-format
//! note on [`StoredItem`] prescribes); an unversioned row decodes exactly as it
//! always has and compares as [`MrecVersion::ZERO`].

use serde::{Deserialize, Serialize};

use crate::{AttributeValue, Item};

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
    /// A live item of an MREC table with its last-writer-wins stamp (ADR
    /// 0075 G-d; additive within v1). Written only on MREC tables, which are
    /// gated on `Gate::MrecReplication`, so no pre-MREC binary ever reads one
    /// out of a cluster that did not opt in (it still outlives the cluster in
    /// backups/exports: class F).
    VersionedItem {
        item: Item,
        ver: MrecVersion,
    },
    /// A delete tombstone of an MREC table with its stamp: the stamp is what
    /// stops a stale replicated put from resurrecting the item.
    VersionedTombstone {
        ver: MrecVersion,
        /// The deleted item's partition key, carried so a tombstone can be
        /// *shipped* from a scan of the base rows alone (a base key is a
        /// one-way encoding; a tombstone has no image to recover it from).
        /// Absent on a tombstone written without it (additive within v1).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pk: Option<AttributeValue>,
        /// The deleted item's sort key (see `pk`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sk: Option<AttributeValue>,
    },
}

/// The cross-Region last-writer-wins stamp of one item of an MREC global table
/// (ADR 0075 section 4.4, G-01 stage G-d decision D1).
///
/// It is a **calendar** stamp, deliberately not the node HLC: the HLC's wall
/// part is relative to the `Env` clock's epoch (process start under
/// `ProdEnv`), so it is not comparable between clusters. The total order is
/// the tuple order of the fields as declared (`wall_ms`, then `logical`, then
/// `region_id`), which is what the derived `Ord` gives; `region_id` makes the
/// order total across Regions (same-millisecond ties break the same way
/// everywhere). An unversioned row compares as [`ZERO`](Self::ZERO).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MrecVersion {
    /// Calendar milliseconds (`Env::wall_now`) of the originating write.
    pub wall_ms: u64,
    /// Tie-break counter for writes within one millisecond / causally after a
    /// stored stamp.
    pub logical: u32,
    /// The originating Region (FNV-1a of its name, `animus_control::
    /// mrec_region_id`), the final tie-break.
    pub region_id: u32,
}

impl MrecVersion {
    /// The stamp an unversioned row compares as: below every real stamp.
    pub const ZERO: MrecVersion = MrecVersion {
        wall_ms: 0,
        logical: 0,
        region_id: 0,
    };

    /// The stamp a **local** write of Region `region_id` takes at apply, given
    /// the version the item currently stores (`None` = an unversioned row or
    /// an absent key, which compares as [`ZERO`](Self::ZERO)) and the write's
    /// own wall clock `wall_ms` (the leader's `Env::wall_now`, frozen into
    /// `WriteSchema::mrec` at propose time; a TTL delete uses the expiry
    /// instant).
    ///
    /// `wall = max(wall_ms, stored.wall_ms)`; `logical` is `0` when the write's
    /// clock is strictly ahead of the stored stamp and `stored.logical + 1`
    /// otherwise (a clock behind or tied with the stored stamp, including a
    /// remote Region's stamp this Region has already applied). The result is
    /// therefore **strictly greater than the stored stamp, whatever its
    /// `region_id`**: a local write made after observing a remote one always
    /// beats it (causality per item). It is a pure function of its arguments
    /// (no clock state, no witness), so every replica applying the same entry
    /// to the same stored row computes identical bytes. On `logical`
    /// overflow the wall part is bumped by one (still strictly greater).
    #[must_use]
    pub fn next_local(stored: Option<MrecVersion>, wall_ms: u64, region_id: u32) -> MrecVersion {
        let s = stored.unwrap_or(Self::ZERO);
        if wall_ms > s.wall_ms {
            return MrecVersion {
                wall_ms,
                logical: 0,
                region_id,
            };
        }
        match s.logical.checked_add(1) {
            Some(logical) => MrecVersion {
                wall_ms: s.wall_ms,
                logical,
                region_id,
            },
            None => MrecVersion {
                wall_ms: s.wall_ms.saturating_add(1),
                logical: 0,
                region_id,
            },
        }
    }

    /// The last-writer-wins rule for a **replicated** write: an incoming stamp
    /// is applied only if it is strictly greater than the stored one
    /// (`None` = [`ZERO`](Self::ZERO)). Equal means "already applied" (an
    /// idempotent re-delivery) and is not applied.
    #[must_use]
    pub fn supersedes(self, stored: Option<MrecVersion>) -> bool {
        self > stored.unwrap_or(Self::ZERO)
    }
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

/// Serialize a live item together with its MREC stamp (ADR 0075 G-d).
#[must_use]
pub fn encode_stored_item_versioned(item: &Item, ver: MrecVersion) -> Vec<u8> {
    serde_json::to_vec(&StoredItem::VersionedItem {
        item: item.clone(),
        ver,
    })
    .expect("versioned stored item serializes")
}

/// Serialize a delete tombstone together with its MREC stamp (ADR 0075 G-d).
#[must_use]
pub fn encode_tombstone_versioned(ver: MrecVersion) -> Vec<u8> {
    serde_json::to_vec(&StoredItem::VersionedTombstone {
        ver,
        pk: None,
        sk: None,
    })
    .expect("versioned tombstone serializes")
}

/// [`encode_tombstone_versioned`] that also records the deleted item's key
/// (`pk`, `sk`), which an MREC shipper needs to ship the tombstone from a
/// base-row scan (ADR 0075 G-d M4).
#[must_use]
pub fn encode_tombstone_versioned_keyed(
    ver: MrecVersion,
    pk: &AttributeValue,
    sk: Option<&AttributeValue>,
) -> Vec<u8> {
    serde_json::to_vec(&StoredItem::VersionedTombstone {
        ver,
        pk: Some(pk.clone()),
        sk: sk.cloned(),
    })
    .expect("versioned tombstone serializes")
}

/// The key a keyed versioned tombstone carries (`None` for every other row,
/// and for a tombstone written without one).
#[must_use]
pub fn decode_tombstone_key(bytes: &[u8]) -> Option<(AttributeValue, Option<AttributeValue>)> {
    match serde_json::from_slice::<StoredItem>(bytes).ok()? {
        StoredItem::VersionedTombstone {
            pk: Some(pk), sk, ..
        } => Some((pk, sk)),
        _ => None,
    }
}

/// Decode bytes read from the data plane back into an item, or `None` for an
/// absent key or a tombstone. A versioned row (MREC) decodes to its item; use
/// [`decode_stored_item_versioned`] to also see the stamp.
///
/// # Errors
/// Returns a message describing the decode failure if the stored bytes are
/// not a valid encoded item. The caller (`animus_dynamo::wire::
/// decode_stored_item`) wraps this into its own `WireError::serialization`.
pub fn decode_stored_item(bytes: &[u8]) -> Result<Option<Item>, String> {
    decode_stored_item_versioned(bytes).map(|(item, _)| item)
}

/// Decode a stored row into `(item or tombstone, MREC stamp)`: the stamp is
/// `None` for an unversioned row (every row of a non-MREC table, and an MREC
/// table's rows from before it was converted), which a caller orders as
/// [`MrecVersion::ZERO`].
///
/// # Errors
/// As [`decode_stored_item`].
pub fn decode_stored_item_versioned(
    bytes: &[u8],
) -> Result<(Option<Item>, Option<MrecVersion>), String> {
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
        StoredItem::Item(item) => (Some(item), None),
        StoredItem::Tombstone => (None, None),
        StoredItem::VersionedItem { item, ver } => (Some(item), Some(ver)),
        StoredItem::VersionedTombstone { ver, .. } => (None, Some(ver)),
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
    fn versioned_rows_round_trip_and_unversioned_rows_decode_unchanged() {
        let mut item = Item::new();
        item.insert("id".into(), s("u1"));
        let ver = MrecVersion {
            wall_ms: 1_700_000_000_123,
            logical: 2,
            region_id: 0xdead_beef,
        };
        let live = encode_stored_item_versioned(&item, ver);
        assert_eq!(stored_item_version(&live), Some(1));
        assert_eq!(
            decode_stored_item_versioned(&live).unwrap(),
            (Some(item.clone()), Some(ver))
        );
        assert_eq!(decode_stored_item(&live).unwrap(), Some(item.clone()));
        let tomb = encode_tombstone_versioned(ver);
        assert_eq!(stored_item_version(&tomb), Some(1));
        assert_eq!(
            decode_stored_item_versioned(&tomb).unwrap(),
            (None, Some(ver))
        );
        assert_eq!(decode_stored_item(&tomb).unwrap(), None);
        // The unversioned encodings are byte-identical to before and carry no stamp.
        assert_eq!(
            decode_stored_item_versioned(&encode_stored_item(&item)).unwrap(),
            (Some(item), None)
        );
        assert_eq!(
            decode_stored_item_versioned(&encode_tombstone()).unwrap(),
            (None, None)
        );
    }

    #[test]
    fn mrec_version_total_order_is_wall_then_logical_then_region() {
        let v = |wall_ms, logical, region_id| MrecVersion {
            wall_ms,
            logical,
            region_id,
        };
        assert!(MrecVersion::ZERO < v(0, 0, 1));
        assert!(v(1, 9, 9) < v(2, 0, 0));
        assert!(v(2, 0, 9) < v(2, 1, 0));
        assert!(v(2, 1, 1) < v(2, 1, 2));
        assert_eq!(v(2, 1, 2).cmp(&v(2, 1, 2)), std::cmp::Ordering::Equal);
    }

    #[test]
    fn next_local_is_strictly_above_the_stored_stamp_and_deterministic() {
        let v = |wall_ms, logical, region_id| MrecVersion {
            wall_ms,
            logical,
            region_id,
        };
        // No stored stamp: the write's own clock, logical 0.
        assert_eq!(MrecVersion::next_local(None, 100, 7), v(100, 0, 7));
        // Clock ahead of the stored stamp: logical resets.
        assert_eq!(
            MrecVersion::next_local(Some(v(90, 5, 9)), 100, 7),
            v(100, 0, 7)
        );
        // Tie on the wall part: logical bumps, region is the local one.
        assert_eq!(
            MrecVersion::next_local(Some(v(100, 5, 9)), 100, 7),
            v(100, 6, 7)
        );
        // Clock behind (skew / a remote stamp from the future): causality wins.
        assert_eq!(
            MrecVersion::next_local(Some(v(150, 2, 9)), 100, 7),
            v(150, 3, 7)
        );
        // Always strictly greater, whatever the stored region id.
        for stored in [v(100, 0, 0), v(100, 0, u32::MAX), v(200, 3, 1), v(0, 0, 0)] {
            assert!(MrecVersion::next_local(Some(stored), 100, 1) > stored);
        }
        // Logical overflow bumps the wall part instead of wrapping.
        let top = v(100, u32::MAX, 3);
        assert!(MrecVersion::next_local(Some(top), 50, 1) > top);
    }

    #[test]
    fn supersedes_is_strict_and_treats_unversioned_as_zero() {
        let v = |wall_ms, logical, region_id| MrecVersion {
            wall_ms,
            logical,
            region_id,
        };
        assert!(v(1, 0, 0).supersedes(None));
        assert!(!MrecVersion::ZERO.supersedes(None));
        assert!(v(2, 0, 0).supersedes(Some(v(1, 9, 9))));
        assert!(!v(2, 0, 1).supersedes(Some(v(2, 0, 1))));
        assert!(!v(2, 0, 1).supersedes(Some(v(2, 0, 2))));
        assert!(v(2, 0, 3).supersedes(Some(v(2, 0, 2))));
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
