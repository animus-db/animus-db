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
//! types, which carry a per-element overhead.

use crate::{AttributeValue, Item};
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

/// The size in bytes DynamoDB attributes to one value, for capacity purposes.
///
/// These are DynamoDB's published rules, not `serde` byte counts:
///
/// - `S` — its UTF-8 length.
/// - `N` — **one byte per two significant digits, plus one**. A number is
///   stored in a compact decimal form, so its cost does not track the text we
///   received it as: `1.0000000000000000000` and `1` cost the same, and a
///   38-digit number costs 20 bytes rather than 38.
/// - `B` — its raw length (the *decoded* bytes; base64 is a transport
///   encoding and is not charged for).
/// - `BOOL` / `NULL` — one byte.
/// - `L` / `M` — the sum of the elements plus **3 bytes per element**, and 3
///   bytes for the container. A map's element cost includes its key name.
/// - `SS` / `NS` / `BS` — the sum of the members, by the scalar rules above.
#[must_use]
pub fn value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(s) => s.len(),
        AttributeValue::N(n) => number_size(n),
        AttributeValue::B(b) => b.len(),
        AttributeValue::Bool(_) | AttributeValue::Null => 1,
        AttributeValue::L(items) => 3 + items.iter().map(|v| value_size(v) + 3).sum::<usize>(),
        AttributeValue::M(map) => {
            3 + map
                .iter()
                .map(|(k, v)| k.len() + value_size(v) + 3)
                .sum::<usize>()
        }
        AttributeValue::SS(set) => set.iter().map(String::len).sum(),
        AttributeValue::NS(set) => set.iter().map(|n| number_size(n)).sum(),
        AttributeValue::BS(set) => set.iter().map(Vec::len).sum(),
    }
}

/// DynamoDB's size for a number: one byte per two significant digits, plus one
/// (and one more for a negative number).
///
/// "Significant" is doing real work here. DynamoDB stores a number as a
/// normalized decimal — a coefficient and an exponent — so the sign, the
/// decimal point, leading zeros **and trailing zeros** are all presentation and
/// none of them are charged for. That normalization is what makes the cost a
/// property of the *number* rather than of the text we received it as: `100`,
/// `100.0` and `1.0E+2` are one number written three ways and must cost the
/// same, exactly as they must compare equal (the same invariant the filter
/// comparator holds). Counting the characters instead would price `100.0`
/// below `100`, which is the sort of quietly-wrong answer a client can never
/// detect from its own response.
fn number_size(text: &str) -> usize {
    let negative = text.starts_with('-');
    let digits: Vec<u8> = text.bytes().filter(u8::is_ascii_digit).collect();
    // The coefficient is what is left after both ends' zeros go. Zero itself
    // has no significant digits at all, and is never negative however it was
    // written (`-0` is `0`).
    let significant = match digits.iter().position(|d| *d != b'0') {
        None => 0,
        Some(start) => {
            let end = digits
                .iter()
                .rposition(|d| *d != b'0')
                .expect("a non-zero digit exists");
            end - start + 1
        }
    };
    if significant == 0 {
        return 1;
    }
    // Two digits share a byte, rounding up, plus one byte of overhead — and one
    // more to carry the sign.
    significant.div_ceil(2) + 1 + usize::from(negative)
}

/// The size in bytes DynamoDB attributes to a whole item: for each attribute,
/// the UTF-8 length of its name plus the size of its value.
#[must_use]
pub fn item_size(item: &Item) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + value_size(value))
        .sum()
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn n(text: &str) -> AttributeValue {
        AttributeValue::N(text.to_string())
    }

    #[test]
    fn a_number_costs_by_significant_digits_not_by_text_length() {
        // One significant digit: one byte for the half-digit, one of overhead.
        assert_eq!(value_size(&n("1")), 2);
        assert_eq!(value_size(&n("7")), 2);
        // Two digits share a byte.
        assert_eq!(value_size(&n("12")), 2);
        assert_eq!(value_size(&n("123")), 3);
        assert_eq!(value_size(&n("1234")), 3);
        // 38 digits — DynamoDB's documented maximum — is 20 bytes, not 38.
        let thirty_eight = "1".repeat(38);
        assert_eq!(value_size(&n(&thirty_eight)), 20);
    }

    #[test]
    fn the_same_number_written_differently_costs_the_same() {
        // The property that matters: cost is a function of the number, not of
        // the characters it arrived as. Were this to regress to a character
        // count, `100.0` would price below `100` and a client could never tell
        // from its own response.
        for equivalent in [["100", "100.0"], ["1", "1.000"], ["0.5", "0.50"]] {
            assert_eq!(
                value_size(&n(equivalent[0])),
                value_size(&n(equivalent[1])),
                "{} and {} are one number",
                equivalent[0],
                equivalent[1]
            );
        }
        // Leading zeros are presentation too.
        assert_eq!(value_size(&n("0007")), value_size(&n("7")));
    }

    #[test]
    fn zero_is_never_negative_and_costs_the_base_byte() {
        assert_eq!(value_size(&n("0")), 1);
        assert_eq!(value_size(&n("0.000")), 1);
        // `-0` is `0`: it must not pick up the sign byte a real negative does.
        assert_eq!(value_size(&n("-0")), 1);
        // A real negative does.
        assert_eq!(value_size(&n("-1")), value_size(&n("1")) + 1);
    }

    #[test]
    fn scalar_sizes_follow_the_published_rules() {
        assert_eq!(value_size(&AttributeValue::S("hello".into())), 5);
        // UTF-8 length, not character count.
        assert_eq!(value_size(&AttributeValue::S("é".into())), 2);
        // Binary is charged on the decoded bytes; base64 is transport.
        assert_eq!(value_size(&AttributeValue::B(vec![0u8; 30])), 30);
        assert_eq!(value_size(&AttributeValue::Bool(true)), 1);
        assert_eq!(value_size(&AttributeValue::Null), 1);
    }

    #[test]
    fn documents_carry_a_per_element_overhead_and_sets_do_not() {
        let list = AttributeValue::L(vec![
            AttributeValue::S("ab".into()),
            AttributeValue::S("cd".into()),
        ]);
        // 3 for the container + (2 + 3) per element.
        assert_eq!(value_size(&list), 3 + 5 + 5);

        let mut map = BTreeMap::new();
        map.insert("k".to_string(), AttributeValue::S("ab".into()));
        // 3 for the container + name + value + 3. A map charges for its keys.
        assert_eq!(value_size(&AttributeValue::M(map)), 3 + 1 + 2 + 3);

        // A set charges its members and nothing else.
        let set = AttributeValue::SS(vec!["ab".into(), "cde".into()]);
        assert_eq!(value_size(&set), 5);
        let numbers = AttributeValue::NS(vec!["1".into(), "22".into()]);
        assert_eq!(value_size(&numbers), 4);
    }

    #[test]
    fn an_items_size_charges_its_attribute_names() {
        let mut item = Item::new();
        item.insert("pk".to_string(), AttributeValue::S("abc".into()));
        item.insert("flag".to_string(), AttributeValue::Bool(false));
        // "pk" + "abc" + "flag" + 1
        assert_eq!(item_size(&item), 2 + 3 + 4 + 1);
        assert_eq!(item_size(&Item::new()), 0);
    }

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
