//! The schema slice a self-contained evaluated write carries (ADR 0054 step
//! 2), and the pure core that derives an item write's index rows and
//! change-log record from it.
//!
//! See [`WriteSchema`]'s own doc for the full "why apply cannot read
//! `Metadata`" rationale, and [`derive_kind_writes`]'s for the extraction
//! this module is built from (`animusd::dynamo::kind_writes_for_item`,
//! byte-identical output).

use serde::{Deserialize, Serialize};

use crate::index::{self, ChangeRecord};
use crate::{AttributeValue, Item, TableSchema, stored};

/// A projected subset of an index row's attributes (mirrors DynamoDB's own
/// `ProjectionType`) — a pure, `animus-item`-local copy of the control
/// plane's `animus_control::schema::IndexProjection`. Duplicated rather than
/// imported for the same layering reason `animus-tablet`'s `escape` is
/// duplicated rather than imported into this crate (see the crate root
/// doc's "The escape duplication" section): the control plane sits ABOVE
/// this crate in the dependency graph (it names `TableSchema`/
/// `AttributeValue` itself, so a reverse dependency would invert it), and
/// apply must never read it live anyway (see [`WriteSchema`]'s own doc).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Projection {
    /// Every attribute (`ALL`).
    All,
    /// Only the base and index key attributes (`KEYS_ONLY`).
    KeysOnly,
    /// The key attributes plus these named extras (`INCLUDE`).
    Include(Vec<String>),
}

/// One LSI's definition, as [`derive_kind_writes`] needs it — a pure,
/// narrowed projection of `animus_control::schema::IndexDef`.
///
/// Carries no `hash_attribute` of its own: an LSI's hash key is always the
/// base table's own partition key (that is what makes it "local" rather
/// than "global") — unlike a GSI's `IndexDef`, which names a separate hash
/// attribute. A GSI needs no entry here at all: `derive_kind_writes` never
/// writes a GSI row (that is the asynchronous drain's job, reading the
/// change-log record this module derives) — see [`WriteSchema::lsis`]'s
/// own doc.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsiDef {
    /// The index's name.
    pub name: String,
    /// The item attribute this LSI sorts by (its own "alternate" sort key).
    pub sort_attribute: String,
    /// This index's declared projection.
    pub projection: Projection,
}

/// The schema slice a `KvCommand::KindEval` entry carries (`animus-cp-data`,
/// ADR 0054 step 2, Decision mechanism 1): exactly the three lookups
/// `animusd::dynamo::kind_writes_for_item` used to perform against
/// replicated `Metadata` (`Metadata::table_indexes`, `schema_for`,
/// `table_change_records_carry_images`), frozen at **propose** time.
///
/// ## Why this exists — deliberately closing off a live catalog read
///
/// The state machine this schema slice feeds (`animus-cp-data`'s apply
/// path) has **no access to control-plane `Metadata` at all** — not an
/// omitted convenience, a structural boundary. `Metadata` is replicated by
/// a *separate* Raft group, so two replicas applying the identical log
/// entry against two different (in-flight-changing) catalog reads could
/// derive two different sets of index rows and **diverge**. Carrying this
/// frozen slice inside the entry itself makes apply a pure function of
/// `(entry, engine state)` — deterministic by construction, not by a
/// coincidence of catalog timing. The leader still reads `Metadata` exactly
/// as before (to build this slice at propose time); it simply no longer
/// reads the **item**, which is the thing that goes stale between propose
/// and apply (ADR 0054's Decision section, mechanism 1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteSchema {
    /// The table's key schema (partition/sort attribute names) — needed
    /// only for an LSI row's own key-attribute allowlist (a private
    /// `projected_item` helper in this module).
    pub key: TableSchema,
    /// Every **local** (LSI) index declared on this table, in no
    /// particular order. A **global** (GSI) index needs no entry here: a
    /// write commits no GSI row directly — the asynchronous drain
    /// (`animusd::index_drain`) derives GSI rows later, from the change-log
    /// record this module derives, which is why `change_records_carry_
    /// images` below is a bare `bool` rather than a full GSI list.
    pub lsis: Vec<LsiDef>,
    /// Whether this table's change records carry the old/new item images
    /// (a stream or at least one secondary index — local or global —
    /// consumes them) or are image-less markers (ADR 0049 §1). A frozen
    /// snapshot of `animusd::table_change_records_carry_images`'s own rule.
    pub change_records_carry_images: bool,
}

/// The attributes one LSI row keeps, per its declared projection —
/// `animus-item`'s own narrow (LSI-only) analogue of
/// `animusd::dynamo::projected_item`, which stays in `animusd` unmodified
/// (it also projects **GSI** rows for the asynchronous drain, which needs
/// the full `animus_control::schema::IndexDef` — including a GSI's own
/// separate hash attribute — that this crate cannot depend on; see
/// [`WriteSchema`]'s own doc). `None` projection input means "every
/// attribute" (`ALL`).
fn projected_item(item: &Item, base: &TableSchema, lsi: &LsiDef) -> Item {
    let keep: Option<Vec<&str>> = match &lsi.projection {
        Projection::All => None,
        Projection::KeysOnly => Some(Vec::new()),
        Projection::Include(extra) => Some(extra.iter().map(String::as_str).collect()),
    };
    let Some(extra) = keep else {
        return item.clone();
    };
    // The key attributes are always present, whatever the projection: the
    // base table's own keys (so the row can name its item) plus this LSI's
    // own sort attribute — an LSI's hash attribute is always the base
    // table's own partition key, already named above.
    let mut names: Vec<&str> = vec![base.partition_key.as_str()];
    if let Some(sk) = &base.sort_key {
        names.push(sk.as_str());
    }
    names.push(lsi.sort_attribute.as_str());
    names.extend(extra);
    item.iter()
        .filter(|(name, _)| names.contains(&name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Everything [`derive_kind_writes`] derives for one item mutation: the
/// multi-kind `(row kind, logical key, value-or-tombstone)` writes (the
/// base row first, then any LSI diff) plus the one change-log record to
/// append alongside them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KindWrites {
    /// `(row kind, logical key, value)` — `None` writes a tombstone. The
    /// base row is always first, at `kind_base`.
    pub writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)>,
    /// The one change-log record this write appends: `(key prefix, encoded
    /// record)`, completed by the caller with the entry's own commit
    /// timestamp (mirrors `animus_cp_data::KvCommand::KindBatch::
    /// change_log`'s own completion rule — this module never mints a
    /// timestamp itself, since it is pure and has no clock).
    pub change_log: (Vec<u8>, Vec<u8>),
}

/// The pure core of `animusd::dynamo::kind_writes_for_item` (ADR 0054 step
/// 2): given the schema slice a write was accepted under, derive every
/// base/LSI row and the change-log record one item mutation commits — a
/// byte-identical extraction of the leader-side function of the same
/// shape, moved here so `animus-cp-data`'s apply path can run the identical
/// logic the leader used to run before the log, with no change to its
/// output. `animusd::dynamo::kind_writes_for_item` becomes a thin wrapper
/// around this function (building a [`WriteSchema`] from `Metadata` and
/// delegating).
///
/// `token_prefix` is this item's own ADR 0022 partition token
/// (`animus_tablet::partition_token(storage_key(pk, None))`) — passed in
/// rather than computed here, since this crate deliberately carries no
/// `animus-tablet` dependency (see the crate root doc's "The escape
/// duplication" section for why token-hashing stays out of this crate);
/// the only caller (`animus-cp-data`) already depends on `animus-tablet`
/// and computes it once per write. `kind_base`/`kind_lsi` are the caller's
/// own row-kind byte constants (`animus_cp_data::KIND_BASE`/`KIND_LSI`),
/// passed in for the identical layering reason — this crate sits below the
/// crate that defines them.
#[allow(clippy::too_many_arguments)] // one item write's full identity + before/after, mirrors kind_writes_for_item
#[must_use]
pub fn derive_kind_writes(
    schema: &WriteSchema,
    pk: &AttributeValue,
    sk: Option<&AttributeValue>,
    token_prefix: &[u8],
    base_value: Vec<u8>,
    old: Option<&Item>,
    new: Option<&Item>,
    ttl_expired: bool,
    kind_base: u8,
    kind_lsi: u8,
) -> KindWrites {
    let base_key = {
        let mut key = token_prefix.to_vec();
        key.extend_from_slice(&index::base_row_key(pk, sk));
        key
    };
    let mut writes: Vec<(u8, Vec<u8>, Option<Vec<u8>>)> =
        vec![(kind_base, base_key, Some(base_value))];

    for lsi in &schema.lsis {
        let old_alt = old.and_then(|i| i.get(&lsi.sort_attribute));
        let new_alt = new.and_then(|i| i.get(&lsi.sort_attribute));
        // Remove the row the previous value occupied, unless it is the very
        // row the new value writes (an unchanged sort attribute) — deleting
        // and re-putting the same key in one entry would depend on
        // ordering.
        if let Some(prev) = old_alt
            && old_alt != new_alt
        {
            let mut key = token_prefix.to_vec();
            key.extend_from_slice(&index::lsi_row_key(pk, &lsi.name, prev, sk));
            writes.push((kind_lsi, key, None));
        }
        if let Some(next) = new_alt {
            let item = new.expect("a new alt value implies a new item");
            let mut key = token_prefix.to_vec();
            key.extend_from_slice(&index::lsi_row_key(pk, &lsi.name, next, sk));
            writes.push((
                kind_lsi,
                key,
                Some(stored::encode_stored_item(&projected_item(
                    item,
                    &schema.key,
                    lsi,
                ))),
            ));
        }
    }

    // The sort key's raw bytes: the full storage key minus the
    // partition-key prefix is exactly that suffix.
    let base_sk = index::base_row_key(pk, sk)[index::base_partition_prefix(pk).len()..].to_vec();
    // ADR 0049 §1: the record always exists; only its *shape* follows the
    // table's declarations. With a stream or an index the images ride
    // along (view-type projection is read-time; the drain/LSI fidelity
    // contract needs the old image); with neither, an image-less marker is
    // the whole record.
    let carries_images = schema.change_records_carry_images;
    let record = ChangeRecord {
        base_sk,
        old_image: if carries_images { old.cloned() } else { None },
        new_image: if carries_images { new.cloned() } else { None },
        seeded: false,
        marker: !carries_images,
        staged: false,
        ttl_expired,
    };
    let mut prefix = token_prefix.to_vec();
    prefix.extend_from_slice(&index::change_prefix(pk));
    KindWrites {
        writes,
        change_log: (prefix, record.encode()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AttributeValue;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_owned())
    }

    fn simple_schema(carries_images: bool) -> WriteSchema {
        WriteSchema {
            key: TableSchema::simple("pk"),
            lsis: Vec::new(),
            change_records_carry_images: carries_images,
        }
    }

    #[test]
    fn a_plain_insert_writes_only_the_base_row_and_a_marker() {
        let pk = s("alice");
        let schema = simple_schema(false);
        let mut item = Item::new();
        item.insert("pk".to_owned(), pk.clone());
        item.insert("age".to_owned(), s("30"));
        let derived = derive_kind_writes(
            &schema,
            &pk,
            None,
            &[1, 2, 3, 4, 5, 6, 7, 8],
            stored::encode_stored_item(&item),
            None,
            Some(&item),
            false,
            0x00,
            0x01,
        );
        assert_eq!(derived.writes.len(), 1, "no LSI declared, no LSI row");
        assert_eq!(derived.writes[0].0, 0x00);
        assert!(derived.writes[0].2.is_some());
        let record = ChangeRecord::decode(&derived.change_log.1).expect("record decodes");
        assert!(record.marker, "no stream/index ⇒ image-less marker");
        assert!(record.old_image.is_none() && record.new_image.is_none());
    }

    #[test]
    fn an_lsi_diff_removes_the_stale_row_and_writes_the_new_one() {
        let pk = s("alice");
        let lsi = LsiDef {
            name: "byAge".to_owned(),
            sort_attribute: "age".to_owned(),
            projection: Projection::All,
        };
        let schema = WriteSchema {
            key: TableSchema::simple("pk"),
            lsis: vec![lsi],
            change_records_carry_images: true,
        };
        let mut old_item = Item::new();
        old_item.insert("pk".to_owned(), pk.clone());
        old_item.insert("age".to_owned(), s("30"));
        let mut new_item = old_item.clone();
        new_item.insert("age".to_owned(), s("31"));

        let derived = derive_kind_writes(
            &schema,
            &pk,
            None,
            &[1, 2, 3, 4, 5, 6, 7, 8],
            stored::encode_stored_item(&new_item),
            Some(&old_item),
            Some(&new_item),
            false,
            0x00,
            0x01,
        );
        // base row + old LSI row tombstone + new LSI row.
        assert_eq!(derived.writes.len(), 3);
        assert_eq!(derived.writes[0].0, 0x00);
        assert_eq!(derived.writes[1].0, 0x01);
        assert!(
            derived.writes[1].2.is_none(),
            "the stale LSI row tombstones"
        );
        assert_eq!(derived.writes[2].0, 0x01);
        assert!(derived.writes[2].2.is_some());
        let record = ChangeRecord::decode(&derived.change_log.1).expect("record decodes");
        assert!(!record.marker);
        assert_eq!(record.old_image.as_ref(), Some(&old_item));
        assert_eq!(record.new_image.as_ref(), Some(&new_item));
    }

    #[test]
    fn an_unchanged_sort_attribute_does_not_delete_and_reput_the_same_lsi_row() {
        let pk = s("alice");
        let lsi = LsiDef {
            name: "byAge".to_owned(),
            sort_attribute: "age".to_owned(),
            projection: Projection::KeysOnly,
        };
        let schema = WriteSchema {
            key: TableSchema::simple("pk"),
            lsis: vec![lsi],
            change_records_carry_images: true,
        };
        let mut item = Item::new();
        item.insert("pk".to_owned(), pk.clone());
        item.insert("age".to_owned(), s("30"));
        item.insert("extra".to_owned(), s("z"));

        let derived = derive_kind_writes(
            &schema,
            &pk,
            None,
            &[9, 9, 9, 9, 9, 9, 9, 9],
            stored::encode_stored_item(&item),
            Some(&item),
            Some(&item),
            false,
            0x00,
            0x01,
        );
        assert_eq!(
            derived.writes.len(),
            2,
            "no delete-then-reput of the same row"
        );
        let projected = stored::decode_stored_item(derived.writes[1].2.as_ref().unwrap())
            .unwrap()
            .unwrap();
        assert!(
            !projected.contains_key("extra"),
            "KEYS_ONLY projection drops non-key attributes"
        );
    }

    #[test]
    fn a_delete_carries_no_new_image_and_the_base_row_tombstones() {
        let pk = s("alice");
        let schema = simple_schema(true);
        let mut item = Item::new();
        item.insert("pk".to_owned(), pk.clone());
        let derived = derive_kind_writes(
            &schema,
            &pk,
            None,
            &[0, 0, 0, 0, 0, 0, 0, 1],
            stored::encode_tombstone(),
            Some(&item),
            None,
            false,
            0x00,
            0x01,
        );
        // The base row still writes `Some(bytes)` — the ENCODED tombstone
        // marker (`stored::encode_tombstone`), not the write-tuple's own
        // engine-level `None` (a physical delete of the key, which the
        // base row never uses): the data plane has no native delete (ADR
        // 0010), so "deleted" is recorded as a live tombstone VALUE.
        assert_eq!(
            derived.writes[0].2.as_deref(),
            Some(stored::encode_tombstone().as_slice())
        );
        let record = ChangeRecord::decode(&derived.change_log.1).expect("record decodes");
        assert_eq!(record.event_name(), "REMOVE");
    }
}
