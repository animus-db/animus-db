//! Consumed-capacity accounting for the DynamoDB wire adapter (ADR 0006).
//!
//! DynamoDB reports, per request, how much *capacity* the request consumed, in
//! **capacity units**. AnimusDB has no provisioned throughput and does not
//! throttle, so nothing here gates a request — this is a **reporting** surface,
//! and it exists because the AWS SDKs, and a great deal of client-side
//! telemetry written against them, read `ConsumedCapacity` off every response.
//!
//! ## Why this is a formula and not a measurement
//!
//! A capacity unit is not "bytes we actually moved". DynamoDB defines it as a
//! *documented arithmetic function of the item's logical size*: the same item
//! costs the same units whatever the storage engine did underneath. So this
//! module computes the published formula over the decoded [`Item`] rather than
//! instrumenting the write path — which is both what makes the numbers agree
//! with DynamoDB's and what makes them unit-testable as pure functions.
//!
//! The one thing we deliberately do **not** do is invent a *provisioned* number
//! to report alongside them. `ConsumedCapacity` says what this request cost;
//! it never implies a limit, because there isn't one.
//!
//! ## The size rule
//!
//! An item's size is the sum, over its attributes, of the UTF-8 length of the
//! attribute **name** plus the size of its **value**. Value sizes follow
//! DynamoDB's published rules — notably numbers, which cost roughly one byte
//! per two significant digits rather than their text length, and the document
//! types, which carry a per-element overhead. The formula itself
//! ([`item_size`]/[`value_size`]) lives in `animus_item::size` now (ADR 0054
//! step 1) — [`crate::wire::apply_update`] enforces the same
//! `animus_item::MAX_ITEM_SIZE_BYTES` cap on its own post-fold result, so
//! there is exactly one copy of the formula for both callers to share; this
//! module re-exports it unchanged.

#[cfg(test)]
use crate::AttributeValue;
use crate::Item;
use serde_json::{Map, Value};

/// Bytes per read capacity unit (a strongly-consistent read of ≤ 4 KB costs 1).
const READ_UNIT_BYTES: usize = 4096;

/// Bytes per write capacity unit (a write of ≤ 1 KB costs 1).
const WRITE_UNIT_BYTES: usize = 1024;

/// The `ReturnConsumedCapacity` selector carried by every data-plane request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnConsumedCapacity {
    /// `NONE` (the default) — the response carries no `ConsumedCapacity` at all.
    #[default]
    None,
    /// `TOTAL` — one aggregate number for the table and all its indexes.
    Total,
    /// `INDEXES` — the aggregate *plus* a per-index breakdown.
    Indexes,
}

impl ReturnConsumedCapacity {
    /// Whether the response should carry a `ConsumedCapacity` at all.
    #[must_use]
    pub fn wanted(self) -> bool {
        self != ReturnConsumedCapacity::None
    }

    /// Whether the per-index breakdown is wanted (`INDEXES` only).
    #[must_use]
    pub fn detailed(self) -> bool {
        self == ReturnConsumedCapacity::Indexes
    }
}

/// The size in bytes DynamoDB attributes to one value/whole item, for
/// capacity purposes. Moved to `animus-item` (ADR 0054 step 1) — it is
/// needed there too, by `apply_update`'s own [`animus_item::MAX_ITEM_SIZE_BYTES`]
/// cap — and re-exported here unchanged so `capacity::item_size`/
/// `capacity::value_size` keep resolving for every existing caller.
pub use animus_item::{item_size, value_size};

/// The read capacity units a read of `bytes` costs.
///
/// A strongly-consistent read costs one unit per 4 KB, rounded up; an
/// eventually-consistent read costs **half** that, which is why this returns a
/// float rather than an integer — `0.5` is a real DynamoDB answer, not a
/// rounding artefact.
#[must_use]
pub fn read_units(bytes: usize, consistent: bool) -> f64 {
    let whole = bytes.max(1).div_ceil(READ_UNIT_BYTES) as f64;
    if consistent { whole } else { whole / 2.0 }
}

/// The write capacity units a write of `bytes` costs: one unit per 1 KB,
/// rounded up, and never zero — even deleting a key that was never there is a
/// write.
#[must_use]
pub fn write_units(bytes: usize) -> f64 {
    bytes.max(1).div_ceil(WRITE_UNIT_BYTES) as f64
}

/// What one request cost, ready to encode into a response.
///
/// Built up by the edge as it learns what a request touched, then rendered by
/// [`ConsumedCapacity::encode`] at whichever granularity the request asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct ConsumedCapacity {
    /// The table the units are attributed to.
    pub table: String,
    /// Units charged against the base table itself.
    pub table_units: f64,
    /// Per-GSI units, by index name.
    pub global_indexes: Vec<(String, f64)>,
    /// Per-LSI units, by index name.
    pub local_indexes: Vec<(String, f64)>,
    /// The granularity the request asked for.
    pub detail: ReturnConsumedCapacity,
}

impl ConsumedCapacity {
    /// A capacity report charging `units` to `table` and nothing to any index.
    #[must_use]
    pub fn table_only(table: &str, units: f64, detail: ReturnConsumedCapacity) -> Self {
        Self {
            table: table.to_string(),
            table_units: units,
            global_indexes: Vec::new(),
            local_indexes: Vec::new(),
            detail,
        }
    }

    /// The aggregate across the base table and every index — the number
    /// `TOTAL` reports.
    #[must_use]
    pub fn total(&self) -> f64 {
        self.table_units
            + self.global_indexes.iter().map(|(_, u)| u).sum::<f64>()
            + self.local_indexes.iter().map(|(_, u)| u).sum::<f64>()
    }

    /// Multiply every charge by `factor`, for the transactional paths — a
    /// transaction costs **twice** what the same work costs outside one,
    /// because it is committed in two phases.
    #[must_use]
    pub fn scaled(mut self, factor: f64) -> Self {
        self.table_units *= factor;
        for (_, u) in &mut self.global_indexes {
            *u *= factor;
        }
        for (_, u) in &mut self.local_indexes {
            *u *= factor;
        }
        self
    }

    /// The `ConsumedCapacity` JSON object.
    ///
    /// `TOTAL` renders the table name and the aggregate. `INDEXES` adds the
    /// breakdown: a `Table` object for the base table's own share, and one
    /// entry per index that was actually charged. An index that cost nothing is
    /// omitted rather than reported as zero, matching DynamoDB — a write that
    /// did not touch an index does not appear under it.
    #[must_use]
    pub fn encode(&self) -> Value {
        let mut obj = Map::new();
        obj.insert("TableName".into(), Value::String(self.table.clone()));
        obj.insert("CapacityUnits".into(), units(self.total()));
        if self.detail.detailed() {
            let mut table = Map::new();
            table.insert("CapacityUnits".into(), units(self.table_units));
            obj.insert("Table".into(), Value::Object(table));
            if let Some(map) = index_map(&self.global_indexes) {
                obj.insert("GlobalSecondaryIndexes".into(), map);
            }
            if let Some(map) = index_map(&self.local_indexes) {
                obj.insert("LocalSecondaryIndexes".into(), map);
            }
        }
        Value::Object(obj)
    }
}

/// `{"<index>": {"CapacityUnits": n}, ..}`, or `None` when nothing was charged.
fn index_map(entries: &[(String, f64)]) -> Option<Value> {
    let charged: Vec<&(String, f64)> = entries.iter().filter(|(_, u)| *u > 0.0).collect();
    if charged.is_empty() {
        return None;
    }
    let mut map = Map::new();
    for (name, u) in charged {
        let mut one = Map::new();
        one.insert("CapacityUnits".into(), units(*u));
        map.insert(name.clone(), Value::Object(one));
    }
    Some(Value::Object(map))
}

/// A capacity number as JSON. Always a float, so `1` renders as `1.0` — the
/// AWS SDKs type this field as a double, and a bare integer round-trips
/// differently in some of them.
fn units(value: f64) -> Value {
    serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// The `ReturnItemCollectionMetrics` selector on a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReturnItemCollectionMetrics {
    /// `NONE` (the default) — the response carries no `ItemCollectionMetrics`.
    #[default]
    None,
    /// `SIZE` — report the collection's key and a size estimate.
    Size,
}

impl ReturnItemCollectionMetrics {
    /// Whether the response should carry metrics at all.
    #[must_use]
    pub fn wanted(self) -> bool {
        self != ReturnItemCollectionMetrics::None
    }
}

/// One write's item-collection report.
///
/// An **item collection** is every row sharing one partition-key value, across
/// the base table and its local secondary indexes. DynamoDB reports this only
/// for a table that *has* an LSI, because that is the only case where a
/// collection is a bounded thing (10 GB) rather than an incidental grouping —
/// and this adapter keeps that rule.
///
/// `bytes` is an **upper bound** on the collection, taken from the tablet that
/// necessarily contains it (see `animusd::dynamo::collection_bytes_at_leader`);
/// `None` means no bound was available and the estimate is omitted rather than
/// guessed at.
#[derive(Debug, Clone, PartialEq)]
pub struct ItemCollectionMetrics {
    /// The partition-key attribute name and value naming the collection.
    pub key: Item,
    /// The upper bound in bytes, if one was available.
    pub bytes: Option<u64>,
}

/// Bytes per gigabyte, as DynamoDB's `SizeEstimateRangeGB` counts them.
const BYTES_PER_GB: f64 = 1_073_741_824.0;

impl ItemCollectionMetrics {
    /// The `ItemCollectionMetrics` JSON object, or `None` when there is
    /// nothing to report.
    ///
    /// The range is `[0, bound]`. DynamoDB's field is a *range* bracketing an
    /// estimate rather than a single figure, which is exactly the right shape
    /// for what we can honestly say: the lower end is zero because we do not
    /// measure the collection itself, and the upper end is a real bound. That
    /// is a weaker claim than DynamoDB's own estimate and a true one, where a
    /// fabricated midpoint would be neither.
    ///
    /// `encode_key` is the caller's `AttributeValue` encoder, passed in so
    /// this type stays independent of `wire`'s private encoding.
    #[must_use]
    pub fn encode(&self, encode_key: impl Fn(&Item) -> Value) -> Option<Value> {
        let bytes = self.bytes?;
        let mut obj = Map::new();
        obj.insert("ItemCollectionKey".into(), encode_key(&self.key));
        let upper = bytes as f64 / BYTES_PER_GB;
        obj.insert(
            "SizeEstimateRangeGB".into(),
            Value::Array(vec![units(0.0), units(upper)]),
        );
        Some(Value::Object(obj))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `value_size`/`item_size`'s own unit tests moved to `animus-item::size`
    // along with the functions (ADR 0054 step 1).

    #[test]
    fn reads_round_up_per_4kb_and_halve_when_eventually_consistent() {
        assert_eq!(read_units(1, true), 1.0);
        assert_eq!(read_units(4096, true), 1.0);
        assert_eq!(read_units(4097, true), 2.0);
        // An eventually-consistent read is half price — `0.5` is a real
        // DynamoDB answer, which is why these are floats.
        assert_eq!(read_units(1, false), 0.5);
        assert_eq!(read_units(4097, false), 1.0);
        // An empty read still costs: capacity is never zero for work done.
        assert_eq!(read_units(0, true), 1.0);
    }

    #[test]
    fn writes_round_up_per_1kb_and_are_never_free() {
        assert_eq!(write_units(1), 1.0);
        assert_eq!(write_units(1024), 1.0);
        assert_eq!(write_units(1025), 2.0);
        // Deleting a key that was never there is still a write.
        assert_eq!(write_units(0), 1.0);
    }

    #[test]
    fn total_reports_one_number_and_indexes_breaks_it_down() {
        let cc = ConsumedCapacity {
            table: "t".into(),
            table_units: 1.0,
            global_indexes: vec![("g".into(), 2.0)],
            local_indexes: vec![("l".into(), 1.0)],
            detail: ReturnConsumedCapacity::Total,
        };
        assert_eq!(cc.total(), 4.0);
        let total = cc.encode();
        assert_eq!(total["TableName"], "t");
        assert_eq!(total["CapacityUnits"], 4.0);
        // TOTAL carries no breakdown at all.
        assert!(total.get("Table").is_none());
        assert!(total.get("GlobalSecondaryIndexes").is_none());
        assert!(total.get("LocalSecondaryIndexes").is_none());

        let detailed = ConsumedCapacity {
            detail: ReturnConsumedCapacity::Indexes,
            ..cc
        }
        .encode();
        // The aggregate is unchanged; the breakdown is added beside it.
        assert_eq!(detailed["CapacityUnits"], 4.0);
        assert_eq!(detailed["Table"]["CapacityUnits"], 1.0);
        assert_eq!(
            detailed["GlobalSecondaryIndexes"]["g"]["CapacityUnits"],
            2.0
        );
        assert_eq!(detailed["LocalSecondaryIndexes"]["l"]["CapacityUnits"], 1.0);
    }

    #[test]
    fn an_uncharged_index_is_omitted_rather_than_reported_as_zero() {
        let cc = ConsumedCapacity {
            table: "t".into(),
            table_units: 1.0,
            global_indexes: vec![("touched".into(), 1.0), ("untouched".into(), 0.0)],
            local_indexes: vec![("none".into(), 0.0)],
            detail: ReturnConsumedCapacity::Indexes,
        };
        let encoded = cc.encode();
        let gsis = &encoded["GlobalSecondaryIndexes"];
        assert!(gsis.get("touched").is_some());
        assert!(
            gsis.get("untouched").is_none(),
            "a write that did not touch an index does not appear under it"
        );
        // A whole section with nothing charged in it is dropped, not left empty.
        assert!(encoded.get("LocalSecondaryIndexes").is_none());
    }

    #[test]
    fn a_transaction_costs_double() {
        let cc = ConsumedCapacity {
            table: "t".into(),
            table_units: 1.0,
            global_indexes: vec![("g".into(), 2.0)],
            local_indexes: Vec::new(),
            detail: ReturnConsumedCapacity::Indexes,
        }
        .scaled(2.0);
        assert_eq!(cc.table_units, 2.0);
        assert_eq!(cc.global_indexes[0].1, 4.0);
        assert_eq!(cc.total(), 6.0);
    }

    #[test]
    fn units_render_as_floats() {
        // The SDKs type this field as a double; a bare integer round-trips
        // differently in some of them.
        let cc = ConsumedCapacity::table_only("t", 1.0, ReturnConsumedCapacity::Total);
        let text = serde_json::to_string(&cc.encode()).expect("serializes");
        assert!(
            text.contains("\"CapacityUnits\":1.0"),
            "expected a float, got {text}"
        );
    }

    #[test]
    fn item_collection_metrics_encode_a_bounded_range() {
        let mut key = Item::new();
        key.insert("pk".to_string(), AttributeValue::S("p1".into()));
        let m = ItemCollectionMetrics {
            key: key.clone(),
            bytes: Some(1_073_741_824), // exactly 1 GiB
        };
        let encoded = m
            .encode(|item| {
                let mut obj = Map::new();
                for name in item.keys() {
                    obj.insert(name.clone(), Value::String("encoded".into()));
                }
                Value::Object(obj)
            })
            .expect("a bound was available");
        assert_eq!(encoded["ItemCollectionKey"]["pk"], "encoded");
        let range = encoded["SizeEstimateRangeGB"].as_array().expect("array");
        // Lower end is zero: we report a bound, not a measurement.
        assert_eq!(range[0], 0.0);
        assert_eq!(range[1], 1.0);
    }

    #[test]
    fn no_bound_means_no_report_rather_than_a_guess() {
        // `None` bytes reaches us only across a forwarding hop from a peer
        // predating the field. Omitting the whole object is the honest
        // answer; emitting `[0, 0]` would assert the collection is empty.
        let mut key = Item::new();
        key.insert("pk".to_string(), AttributeValue::S("p1".into()));
        let m = ItemCollectionMetrics { key, bytes: None };
        assert!(m.encode(|_| Value::Null).is_none());
    }

    #[test]
    fn a_sub_gigabyte_bound_is_a_fraction_not_rounded_up() {
        let mut key = Item::new();
        key.insert("pk".to_string(), AttributeValue::S("p1".into()));
        let m = ItemCollectionMetrics {
            key,
            bytes: Some(536_870_912), // half a GiB
        };
        let encoded = m.encode(|_| Value::Null).expect("bound");
        assert_eq!(encoded["SizeEstimateRangeGB"][1], 0.5);
    }

    #[test]
    fn return_item_collection_metrics_defaults_to_none() {
        assert_eq!(
            ReturnItemCollectionMetrics::default(),
            ReturnItemCollectionMetrics::None
        );
        assert!(!ReturnItemCollectionMetrics::None.wanted());
        assert!(ReturnItemCollectionMetrics::Size.wanted());
    }

    #[test]
    fn none_is_the_default_and_wants_nothing() {
        assert_eq!(
            ReturnConsumedCapacity::default(),
            ReturnConsumedCapacity::None
        );
        assert!(!ReturnConsumedCapacity::None.wanted());
        assert!(ReturnConsumedCapacity::Total.wanted());
        assert!(!ReturnConsumedCapacity::Total.detailed());
        assert!(ReturnConsumedCapacity::Indexes.wanted());
        assert!(ReturnConsumedCapacity::Indexes.detailed());
    }
}
