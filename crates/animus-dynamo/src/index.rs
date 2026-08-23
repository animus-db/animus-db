//! Materialized secondary-index key/value codec (ADR 0041).
//!
//! Pure, deterministic, no I/O — `BTreeMap` only (ADR 0003). This module owns
//! every byte layout ADR 0041 introduces, so the layout is testable in isolation
//! and the `animusd` edge and the drain agree by construction.
//!
//! ## Within-table keys
//!
//! Like [`crate::storage_key`], the keys here are **within-table**: they do
//! *not* carry the ADR 0022 partition token. The `animusd` edge prepends
//! `partition_token(..)` when it assembles the physical data-plane key, because
//! that is where `animus-tablet` lives. Each builder documents which value the
//! caller must token-hash — the base partition key for a base/LSI/marker key,
//! the **index hash value** for a GSI row.
//!
//! ## Row kinds are scopes, not key bytes (ADR 0041 §3)
//!
//! A base tablet holds four kinds of row, separated **above the partition
//! token** by the `StorageScope` prefix — *not* by a discriminator inside the
//! key:
//!
//! ```text
//! physical:  escape(table) || KIND || token(escape(pk)) || escape(pk) || …
//!            └────── scope prefix ──────┘└──────── logical key ────────┘
//!
//! KIND_BASE       base rows       escape(pk) || sk
//! KIND_LSI        LSI rows        escape(pk) || escape(index) || escape(alt_sort) || sk
//! KIND_CHANGE     change records  escape(pk) || hlc
//! KIND_FOOTPRINT  footprints      escape(pk)
//! ```
//!
//! (Within-table again: the token shown above is prepended at the edge.) All
//! four scopes belong to **one tablet's Raft group** and share **one**
//! `KeyRange`, so one `PutBatch` still writes every kind as a single atomic Raft
//! entry, and a split or merge moves all four at once.
//!
//! The kinds are `u8` **scope selectors** here rather than bytes this module
//! emits — [`base_row_key`] returns exactly [`crate::storage_key`]'s ADR 0022
//! layout, unchanged. Two reasons the kind must stay out of the logical key:
//! a tablet is a `[start, end)` range over *token* space (so a kind above the
//! token would stop its ownership being one contiguous range), and
//! `RaftKvNode::txn_stage` **asserts** a logical key leads with the token,
//! slicing `anchor[..TOKEN_BYTES]` and deriving every transaction intent span
//! from it.
//!
//! A **GSI row** lives in its own hidden table's tablets
//! ([`index_table_name`]), so it needs neither a kind nor a scope of its own:
//!
//! ```text
//! GSI row       escape(ihash) || escape(isort)? || escape(base_pk) || base_sk
//! ```
//!
//! The trailing `escape(base_pk) || base_sk` both disambiguates two items that
//! share an index key and makes the base key recoverable by peeling escaped
//! segments ([`parse_gsi_row_key`]).

use serde::{Deserialize, Serialize};

use crate::{AttributeValue, Item, escape};

/// The separator between a base table's name and its index's name in a hidden
/// index table's name (ADR 0041 §1).
///
/// `$` is not legal in a DynamoDB table/index name (`[A-Za-z0-9_.-]`), so an
/// index table's name cannot collide with a user table's.
/// That is **enforced, not assumed** — `Metadata::apply`'s `CreateTableSchema`
/// arm rejects a user table name containing it, alongside the existing
/// `syskv::is_reserved_name` gate.
pub const INDEX_TABLE_SEPARATOR: char = '$';

/// The hidden table a global secondary index's rows live in: `<base>$<index>`
/// (ADR 0041 §1). It gets its own per-table hash ring, tablets, split/merge, GC
/// and storage scope, exactly like a user table.
#[must_use]
pub fn index_table_name(base: &str, index: &str) -> String {
    format!("{base}{INDEX_TABLE_SEPARATOR}{index}")
}

/// Split a hidden index table's name back into `(base, index)`, or `None` if
/// `name` is an ordinary table name.
///
/// Splits at the **first** separator: a base table's name can never contain one
/// (see [`INDEX_TABLE_SEPARATOR`]), so the first occurrence is always the one
/// [`index_table_name`] inserted, even though an index name cannot contain one
/// either.
#[must_use]
pub fn split_index_table_name(name: &str) -> Option<(&str, &str)> {
    name.split_once(INDEX_TABLE_SEPARATOR)
}

/// Whether `name` is a hidden index table rather than a user table.
#[must_use]
pub fn is_index_table_name(name: &str) -> bool {
    name.contains(INDEX_TABLE_SEPARATOR)
}

/// The exclusive upper bound of the half-open range covering exactly the keys
/// that start with `prefix`: the prefix with its final byte incremented.
///
/// Every key starting with `prefix` compares less than that bound, and the bound
/// itself is the first key past them. Panics on an empty prefix or one ending
/// `0xFF` — neither can occur for a prefix this module builds, since each ends in
/// `escape`'s `0x00` terminator.
#[must_use]
pub fn range_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    let last = end.last_mut().expect("a key prefix is never empty");
    assert!(*last != 0xFF, "a key prefix built here never ends in 0xFF");
    *last += 1;
    end
}

// ---------------------------------------------------------------------------
// Base-tablet keys — one builder per scope (token-hash the base partition key)
// ---------------------------------------------------------------------------
//
// Every key below leads with `escape(pk)`, so all four kinds land in the same
// token range and therefore the same tablet — which is what lets one `PutBatch`
// write them atomically (ADR 0041 §2/§4). Keys in *different* scopes may be
// byte-identical (a footprint key and a base partition prefix both being bare
// `escape(pk)`); they cannot collide, because the scope prefix separates them
// physically.

/// The within-table key of a base item row: `escape(pk) || sk`, in the
/// `KIND_BASE` scope.
///
/// Byte-identical to [`crate::storage_key`] — ADR 0022's layout, unchanged by
/// ADR 0041. Kept as a named alias so index-maintenance code reads uniformly
/// across the four scopes. Token-hash `pk`.
#[must_use]
pub fn base_row_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    crate::storage_key(pk, sk)
}

/// The prefix every base row of `pk`'s partition starts with: `escape(pk)`. A
/// base `Query` is `[base_partition_prefix, range_end(base_partition_prefix))`
/// within the `KIND_BASE` scope.
#[must_use]
pub fn base_partition_prefix(pk: &AttributeValue) -> Vec<u8> {
    escape(&pk.key_bytes())
}

/// The within-table key of one LSI row, in the `KIND_LSI` scope:
/// `escape(pk) || escape(index) || escape(alt_sort) || sk`.
///
/// `sk` is the **base** sort key, which makes the row unique when two items in
/// the partition share an `alt_sort` value, and lets [`parse_lsi_row_key`]
/// recover the base key. Token-hash `pk` — leading with the base partition key
/// puts the row in the base row's tablet, which is what makes it atomic with the
/// base write (ADR 0041 §2).
#[must_use]
pub fn lsi_row_key(
    pk: &AttributeValue,
    index: &str,
    alt_sort: &AttributeValue,
    sk: Option<&AttributeValue>,
) -> Vec<u8> {
    let mut key = lsi_index_prefix(pk, index);
    key.extend_from_slice(&escape(&alt_sort.key_bytes()));
    if let Some(sk) = sk {
        key.extend_from_slice(&sk.key_bytes());
    }
    key
}

/// The prefix every row of one LSI within one base partition starts with:
/// `escape(pk) || escape(index)`. An LSI `Query` narrows within this.
#[must_use]
pub fn lsi_index_prefix(pk: &AttributeValue, index: &str) -> Vec<u8> {
    let mut key = escape(&pk.key_bytes());
    key.extend_from_slice(&escape(index.as_bytes()));
    key
}

/// The within-table key of a change-log record, in the `KIND_CHANGE` scope:
/// `escape(pk) || hlc`.
///
/// `hlc` must be a **fixed-width, big-endian, order-preserving** encoding of the
/// write's HLC commit timestamp, so records within a partition sort in commit
/// order — the ordering DynamoDB Streams will depend on (ADR 0041 §4a). The
/// codec keeps it opaque so this crate needs no dependency on the HLC's home
/// crate; the edge supplies the bytes.
#[must_use]
pub fn change_record_key(pk: &AttributeValue, hlc: &[u8]) -> Vec<u8> {
    let mut key = change_prefix(pk);
    key.extend_from_slice(hlc);
    key
}

/// The prefix every change record of `pk`'s partition starts with:
/// `escape(pk)`. The drain (and, later, a stream shard reader) scans
/// `[change_prefix, range_end(change_prefix))` of the `KIND_CHANGE` scope in
/// commit order.
#[must_use]
pub fn change_prefix(pk: &AttributeValue) -> Vec<u8> {
    escape(&pk.key_bytes())
}

/// The within-table key of a partition's GSI footprint, in the
/// `KIND_FOOTPRINT` scope: `escape(pk)`.
///
/// One per **partition key**, not per item: see [`IndexFootprint`].
#[must_use]
pub fn footprint_key(pk: &AttributeValue) -> Vec<u8> {
    escape(&pk.key_bytes())
}

// ---------------------------------------------------------------------------
// GSI row keys (token-hash the index hash value)
// ---------------------------------------------------------------------------

/// The within-table key of one GSI row, in its own hidden table's keyspace:
/// `escape(ihash) || escape(isort)? || escape(base_pk) || base_sk`.
///
/// `isort` is `None` for a hash-only GSI. Token-hash **`ihash`** — a GSI has its
/// own ring over its own hash key, which is exactly why it is a separate table
/// and why its maintenance is asynchronous (ADR 0041 §1/§4).
#[must_use]
pub fn gsi_row_key(
    ihash: &AttributeValue,
    isort: Option<&AttributeValue>,
    base_pk: &AttributeValue,
    base_sk: Option<&AttributeValue>,
) -> Vec<u8> {
    let mut key = escape(&ihash.key_bytes());
    if let Some(isort) = isort {
        key.extend_from_slice(&escape(&isort.key_bytes()));
    }
    key.extend_from_slice(&escape(&base_pk.key_bytes()));
    if let Some(sk) = base_sk {
        key.extend_from_slice(&sk.key_bytes());
    }
    key
}

/// The prefix every row sharing one GSI hash value starts with: `escape(ihash)`.
/// A GSI `Query` is `[gsi_hash_prefix, range_end(gsi_hash_prefix))`, narrowed by
/// any sort condition over the following `escape(isort)` segment.
#[must_use]
pub fn gsi_hash_prefix(ihash: &AttributeValue) -> Vec<u8> {
    escape(&ihash.key_bytes())
}

/// The prefix of the rows sharing one GSI hash **and** sort value:
/// `escape(ihash) || escape(isort)`. The `Equals` case of a sort condition.
#[must_use]
pub fn gsi_hash_sort_prefix(ihash: &AttributeValue, isort: &AttributeValue) -> Vec<u8> {
    let mut key = escape(&ihash.key_bytes());
    key.extend_from_slice(&escape(&isort.key_bytes()));
    key
}

/// The base key a GSI row points at, recovered from the row's own key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GsiRowRef {
    /// The index hash value's raw key bytes.
    pub hash: Vec<u8>,
    /// The index sort value's raw key bytes (`None` for a hash-only GSI).
    pub sort: Option<Vec<u8>>,
    /// The base partition key's raw key bytes.
    pub base_pk: Vec<u8>,
    /// The base sort key's raw key bytes — empty for a simple base table.
    pub base_sk: Vec<u8>,
}

/// Recover `(hash, sort, base_pk, base_sk)` from a GSI row key.
///
/// `composite` says whether the index declares a sort attribute — the layout is
/// otherwise ambiguous, since a hash-only index's `escape(base_pk)` sits exactly
/// where a composite index's `escape(isort)` would. The caller knows the index's
/// shape from its `IndexDef`, so this is a codec parameter rather than a guess.
///
/// Returns `None` on a malformed key (a segment whose escape never terminates,
/// or too few segments for the declared shape).
#[must_use]
pub fn parse_gsi_row_key(key: &[u8], composite: bool) -> Option<GsiRowRef> {
    let (hash, rest) = peel_escaped(key)?;
    let (sort, rest) = if composite {
        let (s, rest) = peel_escaped(rest)?;
        (Some(s), rest)
    } else {
        (None, rest)
    };
    let (base_pk, base_sk) = peel_escaped(rest)?;
    Some(GsiRowRef {
        hash,
        sort,
        base_pk,
        base_sk: base_sk.to_vec(),
    })
}

/// The base key an LSI row points at, recovered from the row's own key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LsiRowRef {
    /// The base partition key's raw key bytes (the LSI hashes by it).
    pub base_pk: Vec<u8>,
    /// The index name.
    pub index: String,
    /// The alternate sort value's raw key bytes.
    pub alt_sort: Vec<u8>,
    /// The base sort key's raw key bytes — empty for a simple base table.
    pub base_sk: Vec<u8>,
}

/// Recover `(base_pk, index, alt_sort, base_sk)` from an LSI row key.
///
/// The caller must already know this is an LSI row — it came from the
/// `KIND_LSI` scope, which is what identifies it now that no discriminator
/// rides in the key. Returns `None` only if a segment is malformed or the index
/// name is not UTF-8.
#[must_use]
pub fn parse_lsi_row_key(key: &[u8]) -> Option<LsiRowRef> {
    let (base_pk, rest) = peel_escaped(key)?;
    let (index, rest) = peel_escaped(rest)?;
    let (alt_sort, base_sk) = peel_escaped(rest)?;
    Some(LsiRowRef {
        base_pk,
        index: String::from_utf8(index).ok()?,
        alt_sort,
        base_sk: base_sk.to_vec(),
    })
}

/// Peel one [`escape`]d segment off the front of `bytes`, returning its decoded
/// contents and the remainder past its `0x00 0x00` terminator.
///
/// `None` if the segment never terminates or contains an invalid `0x00 b`
/// pair — the escape only ever emits `0x00 0x01` (an escaped zero) or
/// `0x00 0x00` (the terminator).
fn peel_escaped(bytes: &[u8]) -> Option<(Vec<u8>, &[u8])> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b != 0x00 {
            out.push(b);
            i += 1;
            continue;
        }
        match bytes.get(i + 1)? {
            0x01 => {
                out.push(0x00);
                i += 2;
            }
            0x00 => return Some((out, &bytes[i + 2..])),
            _ => return None,
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Footprint
// ---------------------------------------------------------------------------

/// One GSI row a base item currently occupies.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FootprintEntry {
    /// The index's name — also names the hidden table the row lives in
    /// ([`index_table_name`]).
    pub index: String,
    /// The row's **full data-plane key**, token included.
    ///
    /// Stored whole rather than as its `(hash, sort)` components on purpose:
    /// the drain's only use for a footprint entry is to *delete the row it
    /// names*, and a key it can pass straight to a write needs no rebuilding —
    /// which would otherwise mean reconstructing `AttributeValue`s from raw key
    /// bytes just to re-derive a key the writer already had.
    pub key: Vec<u8>,
}

/// Where a base **item**'s GSI rows currently live (ADR 0041 §4).
///
/// The drain is *derivative*: it recomputes the desired rows from the base
/// item's current value and deletes whatever this footprint names that the
/// recomputation did not produce. That is what makes a stale row structurally
/// impossible rather than something a sweeper has to hunt — so this records
/// *locations only*, never values, and is the sole authority on where a stale row
/// might be.
///
/// Keyed per **partition key** ([`footprint_key`]) but scoped per **item**: a
/// composite base table's partition holds many items, so entries carry the base
/// sort key they belong to.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexFootprint {
    /// Per base sort key (empty for a simple table), the GSI rows that item
    /// occupies — kept **sorted by `base_sk`**, so the encoding is deterministic
    /// (ADR 0003) and lookup is a binary search.
    ///
    /// A sorted `Vec` rather than a `BTreeMap<Vec<u8>, _>` because a JSON object
    /// key must be a string: a byte-keyed map cannot round-trip through
    /// `serde_json` at all (`key must be a string`). Sorting is maintained by
    /// [`IndexFootprint::set_item`], the only mutator.
    #[serde(default)]
    pub items: Vec<ItemFootprint>,
}

/// One base item's GSI rows within an [`IndexFootprint`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemFootprint {
    /// The item's base sort key bytes — empty for a simple base table.
    #[serde(default)]
    pub base_sk: Vec<u8>,
    /// The GSI rows this item currently occupies.
    pub entries: Vec<FootprintEntry>,
}

impl IndexFootprint {
    /// Encode for storage. Deterministic — sorted `items` plus `serde_json`,
    /// matching the crate's convention for data-plane values.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("footprint serializes")
    }

    /// Decode a stored footprint, or `None` if the bytes are corrupt.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }

    /// The entries recorded for one base sort key (empty slice if none).
    #[must_use]
    pub fn for_item(&self, base_sk: &[u8]) -> &[FootprintEntry] {
        match self
            .items
            .binary_search_by(|i| i.base_sk.as_slice().cmp(base_sk))
        {
            Ok(at) => &self.items[at].entries,
            Err(_) => &[],
        }
    }

    /// Replace one item's entries, dropping the item entirely when `entries` is
    /// empty so a fully-unindexed item leaves no residue. Keeps `items` sorted.
    pub fn set_item(&mut self, base_sk: Vec<u8>, entries: Vec<FootprintEntry>) {
        let found = self
            .items
            .binary_search_by(|i| i.base_sk.as_slice().cmp(&base_sk));
        match (found, entries.is_empty()) {
            (Ok(at), true) => {
                self.items.remove(at);
            }
            (Ok(at), false) => self.items[at].entries = entries,
            (Err(_), true) => {}
            (Err(at), false) => self.items.insert(at, ItemFootprint { base_sk, entries }),
        }
    }

    /// Whether the footprint records nothing at all — the caller deletes the
    /// footprint row rather than storing an empty one.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Change record
// ---------------------------------------------------------------------------

/// One mutation of one base item (ADR 0041 §4/§4a).
///
/// The GSI drain reads these **derivatively** — as a signal that a key is dirty,
/// reconciling toward the base item's current value rather than replaying
/// `old_image`/`new_image`. DynamoDB Streams will read the same records
/// **literally and in order**, which is why the log is append-only and carries
/// both images even though the drain alone would not need either.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeRecord {
    /// The mutated item's base sort key bytes (empty for a simple table). The
    /// partition key is implicit in the record's own key.
    #[serde(default)]
    pub base_sk: Vec<u8>,
    /// The item before the mutation — `None` for an insert.
    pub old_image: Option<Item>,
    /// The item after the mutation — `None` for a delete.
    pub new_image: Option<Item>,
    /// `true` only for the ADR 0045 §2 backfill seeder's own synthetic,
    /// image-less dirty marker (`animusd::index_drain::backfill_seed_tick`)
    /// — never set by a live write. `#[serde(default)]` so a
    /// pre-this-change record already on disk decodes as `false` (a real
    /// write), which is exactly right: no pre-existing record was ever a
    /// seed marker before this field existed. Exists so the Streams read
    /// path (`animusd::dynamo_streams`) can filter these out — real
    /// DynamoDB emits **no** stream event for a GSI backfill's own coverage
    /// sweep over pre-existing data (ADR 0045's "Phantom no-image seed
    /// records" follow-up); the GSI drain itself never reads this field, by
    /// design (it re-derives from a live base-row scan regardless).
    #[serde(default)]
    pub seeded: bool,
    /// `true` for an **image-less marker record** — the record a table with
    /// no stream and no secondary index writes for every mutation under the
    /// universal kind-write path (ADR 0049 §1). A marker is a dirty-key
    /// signal for change-log consumers that re-read current rows (the split
    /// build's tail, a future CDC); it is never a stream event: real
    /// DynamoDB has no stream on such a table at all, and a stream enabled
    /// *later* begins at enable, never retroactively (ADR 0049's "images
    /// follow the stream declaration; the record itself follows nothing").
    /// The Streams read path filters markers exactly as it filters `seeded`
    /// records — see [`ChangeRecord::consumer_hidden`]. `#[serde(default)]`
    /// so every pre-existing record decodes as a real write.
    #[serde(default)]
    pub marker: bool,
    /// `true` for a **stage marker** (ADR 0049 §3): the image-less record
    /// `KvCommand::TxnStage`'s apply arm writes for the anchor key it
    /// stages, so a change-log consumer re-reading dirty keys (ADR 0050's
    /// split-build tail) can observe a freshly staged intent envelope. A
    /// stage marker always also sets [`marker`](Self::marker) — the
    /// `staged` flag only records *which kind* of marker this is (an
    /// intent appeared, and the state it points at may later revert on
    /// abort — harmless: consumers re-read whatever is current). Never a
    /// stream event: the transaction's real record materializes at
    /// `TxnResolve`, at a strictly later HLC in the same log
    /// (materialize-at-resolve, ADR 0046 Decision 2 — unchanged).
    /// `#[serde(default)]` like its two siblings.
    #[serde(default)]
    pub staged: bool,
    /// `true` for a delete the **TTL reaper** produced (ADR 0051 §7,
    /// `animusd::ttl_reaper`) rather than a client `DeleteItem` — a real,
    /// consumer-**visible** stream event carrying both images exactly like
    /// an ordinary delete (unlike [`seeded`](Self::seeded)/
    /// [`marker`](Self::marker)/[`staged`](Self::staged), so this flag is
    /// deliberately absent from [`consumer_hidden`](Self::consumer_hidden)).
    /// It exists purely so the Streams read path can render the record's
    /// `userIdentity` as `{"PrincipalId": "dynamodb.amazonaws.com", "Type":
    /// "Service"}` — real DynamoDB's own documented way for a consumer to
    /// distinguish a system-driven TTL expiry from a user delete (the
    /// "archive expired items to cold storage" pattern). `#[serde(default)]`
    /// so every pre-existing record decodes as a real client write.
    #[serde(default)]
    pub ttl_expired: bool,
}

impl ChangeRecord {
    /// `true` when this record must never surface as a stream event: the
    /// ADR 0045 §2 backfill seeder's synthetic dirty marker (`seeded`), an
    /// ADR 0049 §1 image-less marker record (`marker`), or an ADR 0049 §3
    /// stage marker (`staged` — always also `marker` today, listed here as
    /// defense-in-depth so a staged record stays hidden even if the two
    /// flags ever diverge). One predicate so the sealed and open
    /// `GetRecords` serve paths (and any future consumer-facing reader) can
    /// never drift on which records are consumer-visible — change-log
    /// *consumers* (the GSI drain, the split build) deliberately ignore
    /// this: to them every record is a dirty-key signal.
    #[must_use]
    pub fn consumer_hidden(&self) -> bool {
        self.seeded || self.marker || self.staged
    }

    /// The DynamoDB Streams event name this record represents.
    ///
    /// Present now because it is a pure function of the two images and belongs
    /// with the record's definition; the Streams wire surface that will emit it
    /// is deferred to its own ADR.
    #[must_use]
    pub fn event_name(&self) -> &'static str {
        match (&self.old_image, &self.new_image) {
            (None, Some(_)) => "INSERT",
            (Some(_), Some(_)) => "MODIFY",
            (Some(_), None) => "REMOVE",
            // The only records ever constructed with neither image are the
            // backfill-seed marker (`seeded: true`, ADR 0045 §2) and the
            // ADR 0049 §1 image-less marker record (`marker: true`) — pure
            // dirty markers for change-log consumers, which never call
            // this. The Streams read path (`animusd::dynamo_streams`)
            // filters every `consumer_hidden()` record out before
            // `stream_record_json` ever reaches this function (ADR 0045
            // follow-up "E1"), so this arm is unreachable from that caller
            // in practice; kept as a no-op rather than a panic for any
            // other decode path that might land here.
            (None, None) => "MODIFY",
        }
    }

    /// Encode for storage.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("change record serializes")
    }

    /// Decode a stored change record, or `None` if the bytes are corrupt.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.to_owned())
    }

    #[test]
    fn index_table_name_round_trips() {
        let name = index_table_name("users", "byEmail");
        assert_eq!(name, "users$byEmail");
        assert!(is_index_table_name(&name));
        assert_eq!(split_index_table_name(&name), Some(("users", "byEmail")));
        assert!(!is_index_table_name("users"));
        assert_eq!(split_index_table_name("users"), None);
    }

    #[test]
    fn the_base_layout_is_adr_0022_unchanged() {
        // ADR 0041 moved the row kind into the scope prefix precisely so this
        // stays true: nothing existing moves on disk.
        for (pk, sk) in [(s("alice"), None), (s("alice"), Some(s("42")))] {
            assert_eq!(
                base_row_key(&pk, sk.as_ref()),
                crate::storage_key(&pk, sk.as_ref())
            );
        }
    }

    #[test]
    fn every_kind_leads_with_the_partition_key_so_all_share_one_tablet() {
        // The atomicity property (ADR 0041 §2/§4): a tablet is a range over the
        // token of `escape(pk)`, so every kind leading with `escape(pk)` lands in
        // the same tablet and one PutBatch can write them as one Raft entry.
        let pk = s("alice");
        let prefix = escape(&pk.key_bytes());
        for key in [
            base_row_key(&pk, Some(&s("z"))),
            lsi_row_key(&pk, "byAge", &s("a"), Some(&s("z"))),
            change_record_key(&pk, &7u64.to_be_bytes()),
            footprint_key(&pk),
        ] {
            assert!(key.starts_with(&prefix), "every kind leads with escape(pk)");
        }
    }

    #[test]
    fn identical_bytes_in_different_scopes_are_not_a_collision() {
        // A footprint key and a base partition prefix are both bare escape(pk).
        // They coexist because the scope prefix separates them physically — the
        // reason `row_kind()` no longer exists.
        let pk = s("alice");
        assert_eq!(footprint_key(&pk), base_partition_prefix(&pk));
        assert_eq!(footprint_key(&pk), change_prefix(&pk));
    }

    #[test]
    fn a_base_range_excludes_a_partition_whose_key_is_a_prefix_of_this_one() {
        // The escape is prefix-free, so "alice" and "alicia"/"alic" cannot bleed
        // into each other even though one key's bytes prefix another's.
        let short = base_partition_prefix(&s("alic"));
        let long = base_row_key(&s("alice"), None);
        assert!(!long.starts_with(&short));
        assert!(long < short.clone() || long >= range_end(&short));
    }

    #[test]
    fn change_records_sort_in_commit_order() {
        let pk = s("alice");
        let earlier = change_record_key(&pk, &1u64.to_be_bytes());
        let later = change_record_key(&pk, &2u64.to_be_bytes());
        assert!(
            earlier < later,
            "a fixed-width big-endian HLC sorts in order"
        );
    }

    #[test]
    fn composite_gsi_row_key_recovers_the_base_key() {
        let row = gsi_row_key(&s("eng"), Some(&s("2026")), &s("alice"), Some(&s("42")));
        let parsed = parse_gsi_row_key(&row, true).expect("parses");
        assert_eq!(parsed.hash, b"eng");
        assert_eq!(parsed.sort.as_deref(), Some(&b"2026"[..]));
        assert_eq!(parsed.base_pk, b"alice");
        assert_eq!(parsed.base_sk, b"42");
    }

    #[test]
    fn hash_only_gsi_row_key_recovers_the_base_key() {
        let row = gsi_row_key(&s("eng"), None, &s("alice"), None);
        let parsed = parse_gsi_row_key(&row, false).expect("parses");
        assert_eq!(parsed.hash, b"eng");
        assert_eq!(parsed.sort, None);
        assert_eq!(parsed.base_pk, b"alice");
        assert!(parsed.base_sk.is_empty());
    }

    #[test]
    fn gsi_row_keys_recover_values_containing_zero_bytes() {
        // The escape doubles embedded 0x00, so a value containing one must still
        // peel back byte-for-byte — the property the whole layout rests on.
        let hash = AttributeValue::B(vec![0x00, 0xFF, 0x00]);
        let pk = AttributeValue::B(vec![0x00]);
        let row = gsi_row_key(&hash, None, &pk, None);
        let parsed = parse_gsi_row_key(&row, false).expect("parses");
        assert_eq!(parsed.hash, vec![0x00, 0xFF, 0x00]);
        assert_eq!(parsed.base_pk, vec![0x00]);
    }

    #[test]
    fn one_gsi_hash_values_rows_are_contiguous_and_sorted_by_index_sort() {
        let prefix = gsi_hash_prefix(&s("eng"));
        let end = range_end(&prefix);
        let a = gsi_row_key(&s("eng"), Some(&s("2025")), &s("bob"), None);
        let b = gsi_row_key(&s("eng"), Some(&s("2026")), &s("alice"), None);
        let other = gsi_row_key(&s("sales"), Some(&s("2026")), &s("alice"), None);

        assert!(a >= prefix && a < end);
        assert!(b >= prefix && b < end);
        assert!(a < b, "sorted by index sort value, not by base key");
        assert!(other < prefix || other >= end);

        // An Equals sort condition narrows to a sub-prefix.
        let eq = gsi_hash_sort_prefix(&s("eng"), &s("2026"));
        assert!(b.starts_with(&eq));
        assert!(!a.starts_with(&eq));
    }

    #[test]
    fn lsi_row_key_recovers_the_base_key_and_index() {
        let row = lsi_row_key(&s("alice"), "byAge", &s("030"), Some(&s("42")));
        let parsed = parse_lsi_row_key(&row).expect("parses");
        assert_eq!(parsed.base_pk, b"alice");
        assert_eq!(parsed.index, "byAge");
        assert_eq!(parsed.alt_sort, b"030");
        assert_eq!(parsed.base_sk, b"42");
    }

    #[test]
    fn lsi_row_keys_recover_values_containing_zero_bytes() {
        // Same prefix-freedom property the GSI rows rely on, across the extra
        // index-name segment an LSI row carries.
        let pk = AttributeValue::B(vec![0x00, 0x01]);
        let alt = AttributeValue::B(vec![0x00]);
        let row = lsi_row_key(&pk, "by\u{0}Age", &alt, None);
        let parsed = parse_lsi_row_key(&row).expect("parses");
        assert_eq!(parsed.base_pk, vec![0x00, 0x01]);
        assert_eq!(parsed.index, "by\u{0}Age");
        assert_eq!(parsed.alt_sort, vec![0x00]);
        assert!(parsed.base_sk.is_empty());
    }

    #[test]
    fn two_lsis_on_one_partition_do_not_interleave() {
        let pk = s("alice");
        let by_age = lsi_index_prefix(&pk, "byAge");
        let by_city = lsi_index_prefix(&pk, "byCity");
        let age_row = lsi_row_key(&pk, "byAge", &s("zzz"), None);
        let city_row = lsi_row_key(&pk, "byCity", &s("aaa"), None);

        assert!(age_row >= by_age && age_row < range_end(&by_age));
        assert!(city_row >= by_city && city_row < range_end(&by_city));
        assert!(
            age_row < city_row,
            "index name orders ahead of the sort value"
        );
    }

    #[test]
    fn footprint_round_trips_and_drops_emptied_items() {
        let mut fp = IndexFootprint::default();
        assert!(fp.is_empty());
        fp.set_item(
            b"42".to_vec(),
            vec![FootprintEntry {
                index: "byEmail".to_owned(),
                key: b"rowkey".to_vec(),
            }],
        );
        let decoded = IndexFootprint::decode(&fp.encode()).expect("decodes");
        assert_eq!(decoded, fp);
        assert_eq!(decoded.for_item(b"42").len(), 1);
        assert!(decoded.for_item(b"other").is_empty());

        fp.set_item(b"42".to_vec(), vec![]);
        assert!(fp.is_empty(), "an emptied item leaves no residue");
    }

    #[test]
    fn footprint_stays_sorted_however_items_are_inserted() {
        let entry = |n: &str| FootprintEntry {
            index: n.to_owned(),
            key: b"rowkey".to_vec(),
        };
        let mut fp = IndexFootprint::default();
        // Deliberately out of order — the encoding must not depend on call order
        // (ADR 0003), and `for_item`'s binary search depends on the invariant.
        for sk in [b"30".as_slice(), b"10", b"20"] {
            fp.set_item(sk.to_vec(), vec![entry("i")]);
        }
        let keys: Vec<&[u8]> = fp.items.iter().map(|i| i.base_sk.as_slice()).collect();
        assert_eq!(keys, vec![b"10".as_slice(), b"20", b"30"]);

        // Every item is still findable, and an overwrite does not disturb order.
        fp.set_item(b"20".to_vec(), vec![entry("j")]);
        assert_eq!(fp.for_item(b"20")[0].index, "j");
        assert_eq!(fp.for_item(b"10")[0].index, "i");
        let keys: Vec<&[u8]> = fp.items.iter().map(|i| i.base_sk.as_slice()).collect();
        assert_eq!(keys, vec![b"10".as_slice(), b"20", b"30"]);

        // Removing the middle item keeps the rest ordered and findable.
        fp.set_item(b"20".to_vec(), vec![]);
        assert!(fp.for_item(b"20").is_empty());
        let keys: Vec<&[u8]> = fp.items.iter().map(|i| i.base_sk.as_slice()).collect();
        assert_eq!(keys, vec![b"10".as_slice(), b"30"]);
    }

    #[test]
    fn change_record_round_trips_and_names_its_event() {
        let item: Item = [("a".to_owned(), s("1"))].into_iter().collect();
        let insert = ChangeRecord {
            base_sk: b"42".to_vec(),
            old_image: None,
            new_image: Some(item.clone()),
            seeded: false,
            marker: false,
            staged: false,
            ttl_expired: false,
        };
        assert_eq!(insert.event_name(), "INSERT");
        assert_eq!(
            ChangeRecord::decode(&insert.encode()).as_ref(),
            Some(&insert)
        );

        let modify = ChangeRecord {
            base_sk: Vec::new(),
            old_image: Some(item.clone()),
            new_image: Some(item.clone()),
            seeded: false,
            marker: false,
            staged: false,
            ttl_expired: false,
        };
        assert_eq!(modify.event_name(), "MODIFY");

        let remove = ChangeRecord {
            base_sk: Vec::new(),
            old_image: Some(item),
            new_image: None,
            seeded: false,
            marker: false,
            staged: false,
            ttl_expired: false,
        };
        assert_eq!(remove.event_name(), "REMOVE");
        assert_eq!(ChangeRecord::decode(b"garbage"), None);

        // ADR 0049 §3: a stage marker is consumer-hidden — via `staged`
        // itself (defense-in-depth), not only via the `marker` flag every
        // real stage marker also sets.
        let stage_marker = ChangeRecord {
            base_sk: Vec::new(),
            old_image: None,
            new_image: None,
            seeded: false,
            marker: false,
            staged: true,
            ttl_expired: false,
        };
        assert!(stage_marker.consumer_hidden());
        assert_eq!(
            ChangeRecord::decode(&stage_marker.encode()).as_ref(),
            Some(&stage_marker)
        );

        // ADR 0051 §7: a TTL-reaper delete is a REAL, consumer-visible
        // event (both images ride along exactly like an ordinary delete) —
        // `ttl_expired` must never make `consumer_hidden()` true.
        let ttl_delete = ChangeRecord {
            base_sk: Vec::new(),
            old_image: Some([("a".to_owned(), s("1"))].into_iter().collect()),
            new_image: None,
            seeded: false,
            marker: false,
            staged: false,
            ttl_expired: true,
        };
        assert!(!ttl_delete.consumer_hidden());
        assert_eq!(ttl_delete.event_name(), "REMOVE");
        assert_eq!(
            ChangeRecord::decode(&ttl_delete.encode()).as_ref(),
            Some(&ttl_delete)
        );
    }

    #[test]
    fn peel_escaped_rejects_malformed_segments() {
        assert_eq!(peel_escaped(b"a"), None, "unterminated");
        assert_eq!(peel_escaped(&[0x00, 0x02]), None, "invalid escape pair");
        assert_eq!(peel_escaped(&[0x00]), None, "truncated escape pair");
    }
}
