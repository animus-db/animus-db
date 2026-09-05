//! AnimusDB's pure DynamoDB-style **item model** (ADR 0054 step 1).
//!
//! This crate holds the data model and evaluation logic that used to live in
//! `animus-dynamo`'s wire adapter, moved **below** it: `AttributeValue`/
//! `Item`/`TableSchema`, the order-preserving key encodings ([`escape`],
//! [`storage_key`], [`numkey`]), the pure `ConditionExpression`/
//! `SortKeyCondition` predicates ([`condition`]), the `UpdateExpression`
//! data model and its apply-time evaluator ([`update`]), the stored-item
//! codec ([`stored`]), the item-size accounting DynamoDB's own capacity
//! formula and `apply_update`'s size cap both need ([`size`]), and the
//! materialized secondary-index key/footprint/change-record derivation
//! ([`index`]).
//!
//! ## Why this crate exists
//!
//! ADR 0054 moves DynamoDB write evaluation (condition check, update
//! expression, index/change-record derivation) from the tablet's Raft
//! *leader* into the tablet's Raft **apply** path, so every replica derives
//! the same result deterministically from the log rather than trusting a
//! leader-computed byte string an apply-time OCC precondition has to guard.
//! `animus-cp-data` is the crate whose apply path will do that evaluation,
//! and it is a protocol-agnostic KV state machine — it cannot depend on
//! `animus-dynamo`, a wire crate, without inverting the layering. This crate
//! is the item model extracted to sit **below both**: `animus-dynamo` keeps
//! the HTTP/JSON wire encoding and re-exports everything that moved here so
//! every existing `animus_dynamo::X` path keeps compiling unchanged; a later
//! ADR 0054 step adds this crate as a dependency of `animus-cp-data` too.
//!
//! ## What must stay pure, and why
//!
//! Every module here is **pure**: no I/O, no storage engine, no network, no
//! `Env` (ADR 0003) — `BTreeMap`/`BTreeSet` only. That is not merely the
//! existing style carried over; it is the property that makes the ADR 0054
//! plan sound. `animus-cp-data`'s apply path runs identically on every
//! replica from the same committed log entry: if evaluation here reached
//! into a clock, an RNG, a HashMap, or any other non-deterministic seam,
//! replicas would apply the same entry to different results and diverge.
//! This crate therefore does **not** depend on `animus-env` — no crate that
//! only computes over already-decoded values needs the `Env` seam, and a
//! dependency on it here would be a standing invitation to reach for
//! wall-clock time or unseeded randomness right where determinism matters
//! most.
//!
//! ## Key encoding
//!
//! A storage key is `escape(partition_key) || sort_key`, where [`escape`] is
//! order-preserving and prefix-free (`0x00 -> 0x00 0x01`, `0x00 0x00`
//! terminator). All items in a partition are therefore contiguous and
//! ordered by sort key, so a `Query` is a single range scan over the
//! partition's prefix. Numbers (`N`) are carried through the
//! order-preserving [`numkey`] codec so the stored/scanned key order equals
//! DynamoDB's own numeric order (ADR 0063).
//!
//! ## The escape duplication (ADR 0023)
//!
//! `animus-tablet` also defines its own `escape`/`TableName`-shaped
//! primitives, deliberately duplicated rather than imported: `animus-tablet`
//! sits below this crate's ancestor (`animus-dynamo`) in the dependency
//! graph, and a reverse dependency would invert it. That reasoning is
//! unchanged by this move — this crate's [`escape`] is a **relocation** of
//! `animus-dynamo`'s pre-existing copy, not a new duplicate, and
//! `animus-tablet`'s own copy stays exactly as it was. See
//! `crates/animus-tablet/CLAUDE.md`'s own note on why its copy exists.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub mod condition;
pub mod index;
pub mod numkey;
pub mod size;
pub mod stored;
pub mod update;
pub mod write_schema;

pub use condition::{Comparator, ConditionError, ConditionExpression, SortKeyCondition};
pub use index::{
    ChangeRecord, FootprintEntry, GsiRowRef, IndexFootprint, ItemFootprint, LsiRowRef,
    index_table_name, is_index_table_name, split_index_table_name,
};
pub use size::{MAX_ITEM_SIZE_BYTES, item_size, value_size};
pub use stored::{decode_stored_item, encode_stored_item, encode_tombstone};
pub use update::{
    PathSegment, UpdateAction, UpdateError, UpdateExpr, UpdateOperand, apply_update,
    format_update_path,
};
pub use write_schema::{KindWrites, LsiDef, Projection, WriteSchema, derive_kind_writes};

/// A DynamoDB-style attribute value (a useful subset). Beyond the scalar
/// types (`S`/`N`/`B`/`BOOL`/`NULL`), this carries the **document** types
/// `M` (a nested attribute map) and `L` (a heterogeneous list), and the
/// homogeneous **set** types `SS` (string set), `NS` (number set), and `BS`
/// (binary set).
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
    /// Map (`M`) — a nested attribute map (a document type).
    M(BTreeMap<String, AttributeValue>),
    /// List (`L`) — an ordered, heterogeneous list of values (a document type).
    L(Vec<AttributeValue>),
    /// String set (`SS`) — a set of unique strings, kept sorted/deduplicated.
    SS(Vec<String>),
    /// Number set (`NS`) — a set of unique numbers (text), sorted/deduplicated.
    NS(Vec<String>),
    /// Binary set (`BS`) — a set of unique binary values, sorted/deduplicated.
    BS(Vec<Vec<u8>>),
}

impl AttributeValue {
    /// Byte encoding used when an attribute is part of a key. String/number/
    /// binary sort by these bytes, and for every one of the three that
    /// bytewise order equals DynamoDB's own order — including `N`, via the
    /// order-preserving codec in [`numkey`] (ADR 0063: sign class byte,
    /// biased exponent, digit run). See that ADR/module for the full design.
    ///
    /// Only scalar types are valid key attributes in DynamoDB; the document
    /// and set types return an empty encoding (the schema/registry layers
    /// reject them as keys before this is reached).
    pub(crate) fn key_bytes(&self) -> Vec<u8> {
        match self {
            AttributeValue::S(s) => s.clone().into_bytes(),
            AttributeValue::N(n) => numkey::encode(n).unwrap_or_else(|| {
                // A key attribute reaching this point has already been
                // validated as a well-formed DynamoDB `N` by the wire layer
                // (`numkey::encode` only returns `None` for malformed text or
                // an exponent outside DynamoDB's own documented range, which
                // a well-formed `N` never has) — this fallback exists so a
                // read path never panics on data that somehow got here
                // anyway, not because it is expected to be hit.
                n.clone().into_bytes()
            }),
            AttributeValue::B(b) => b.clone(),
            AttributeValue::Bool(b) => vec![u8::from(*b)],
            AttributeValue::Null
            | AttributeValue::M(_)
            | AttributeValue::L(_)
            | AttributeValue::SS(_)
            | AttributeValue::NS(_)
            | AttributeValue::BS(_) => Vec::new(),
        }
    }
}

/// An item: a map from attribute name to value.
pub type Item = BTreeMap<String, AttributeValue>;

/// A table's key schema.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

/// Order-preserving, prefix-free escape: a key's encoding never prefixes
/// another's, so a partition's items stay contiguous and sort-ordered.
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
/// distributed data plane without instantiating a local-engine `Table`.
#[must_use]
pub fn storage_key(pk: &AttributeValue, sk: Option<&AttributeValue>) -> Vec<u8> {
    let mut key = escape(&pk.key_bytes());
    if let Some(sk) = sk {
        key.extend_from_slice(&sk.key_bytes());
    }
    key
}
