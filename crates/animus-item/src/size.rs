//! DynamoDB's published item/value **size** formula (ADR 0006), moved here
//! (ADR 0054 step 1) because it is needed by two independent callers now:
//! `animus-dynamo::capacity` (which re-exports [`item_size`]/[`value_size`]
//! for `ConsumedCapacity` accounting) and [`crate::update::apply_update`],
//! which enforces [`MAX_ITEM_SIZE_BYTES`] on its own post-fold result. Both
//! need the identical function — this is the one copy.
//!
//! ## Why this is a formula and not a measurement
//!
//! A capacity unit is not "bytes we actually moved". DynamoDB defines it as a
//! *documented arithmetic function of the item's logical size*: the same item
//! costs the same units whatever the storage engine did underneath. So this
//! module computes the published formula over the decoded [`crate::Item`]
//! rather than instrumenting the write path — which is both what makes the
//! numbers agree with DynamoDB's and what makes them unit-testable as pure
//! functions.
//!
//! ## The size rule
//!
//! An item's size is the sum, over its attributes, of the UTF-8 length of the
//! attribute **name** plus the size of its **value**. Value sizes follow
//! DynamoDB's published rules — notably numbers, which cost roughly one byte
//! per two significant digits rather than their text length, and the document
//! types, which carry a per-element overhead.

use crate::{AttributeValue, Item};

/// AWS's per-item size cap: 400 KB (`409600` bytes). Enforced on every
/// decoded **write** item at the `animus-dynamo` wire edge (`PutItem`,
/// `BatchWriteItem`'s `PutRequest`s, `TransactWriteItems`'s `Put` actions),
/// and re-enforced here on [`crate::update::apply_update`]'s post-fold
/// result: that decode-time check alone can't cover a read-modify-write,
/// since the item as it exists before the update may already be under the
/// cap but the applied actions can push it over.
pub const MAX_ITEM_SIZE_BYTES: usize = 409_600;

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
}
