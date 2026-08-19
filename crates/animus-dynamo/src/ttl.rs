//! DynamoDB-style TTL (ADR 0051): the pure expiry predicate over an item's
//! declared TTL attribute.
//!
//! A table may declare **one** TTL attribute name. An item is *expired* when
//! it carries that attribute, the attribute is a DynamoDB `N`, and its value
//! (an absolute **Unix epoch second**) is **strictly less than** "now".
//!
//! This module makes no clock call of its own (ADR 0003 — no I/O/storage/
//! network/clock/rand in this crate, `BTreeMap`/`BTreeSet` only): the caller
//! supplies `now_epoch_secs`. AWS-faithfully, we are **read-path silent**:
//! this module never filters a `GetItem`/`Query`/`Scan` result — expired
//! items stay visible until a background reaper (built in `animusd`, over
//! `env.now()`) deletes them. `is_expired`/`expires_at` are exactly the pure
//! predicate that reaper calls; nothing here decides *when* to run it.

use crate::{AttributeValue, Item};

/// Real DynamoDB refuses to expire (and therefore never deletes) an item
/// whose declared TTL is more than this many seconds in the past — a guard
/// against the single most common TTL foot-gun: a client that writes
/// **milliseconds** instead of **seconds** into the TTL attribute. An
/// epoch-millis value used as if it were epoch-seconds is off by a factor of
/// ~1000, which for any timestamp anywhere near "now" lands tens of thousands
/// of years in the *future* (harmless — it just never expires) but for a
/// small/zero/negative counter value lands enormously far in the *past*,
/// which without this guard would make the reaper treat the mistake as "expire
/// immediately" and delete the item (or, at table-wide scale, the whole
/// table) the moment TTL is enabled. Five years is AWS's own published
/// window for this same protection.
pub const MAX_PAST_EXPIRY_SECS: u64 = 5 * 365 * 24 * 60 * 60;

/// The item's declared expiry epoch second under `attribute`, or `None` when
/// the attribute is absent or is not usable as a TTL value. This function is
/// **not** clock-dependent — it only extracts and parses the stored value;
/// see [`is_expired`] for the "is this actually expired now" question.
///
/// Rules (each mirrors a real DynamoDB behavior, deliberately — a TTL
/// attribute is easy to misconfigure and this crate must fail the same way
/// production DynamoDB does):
///
/// - **Attribute absent → `None`.** Most items in a TTL-enabled table have
///   no TTL value at all and must never expire; this is the common case, not
///   an edge case.
/// - **Attribute present but not an `N`** (`S`/`B`/`BOOL`/`NULL`/`M`/`L`, or
///   any set type `SS`/`NS`/`BS`) **→ `None`.** Real DynamoDB silently
///   ignores a TTL attribute of the wrong type — it does not error and does
///   not delete the item. Matched here rather than treated as a malformed
///   item.
/// - **An `N` whose text does not parse under the grammar below → `None`**
///   (never panics) — an unparseable value is exactly as inert as a missing
///   one.
/// - **Parsing grammar**: an optional leading `-`, one or more decimal
///   digits, and an optional `.`-prefixed fractional part of one or more
///   digits — e.g. `1700000000`, `-5`, `1700000000.9`. The fractional part is
///   **truncated toward zero** (DynamoDB's own behavior: `1700000000.9`
///   means second `1700000000`, not `1700000001`), so it only ever narrows
///   the parsed value, never rounds it.
/// - **Exponent notation is deliberately NOT supported.** `1.7e9` is a
///   syntactically valid DynamoDB `N` in general, but a TTL attribute is
///   always written by application code as a plain epoch-second integer in
///   practice, and reimplementing full scientific-notation parsing (with its
///   own truncation-toward-zero questions) has no real payoff here. An `N`
///   using exponent notation is treated as "not a usable TTL value"
///   (`None`) — a documented simplification, not a silent misparse: it is
///   rejected by the grammar above (an `e`/`E` is trailing garbage), never
///   misread as some other number.
/// - **A negative value parses to `Some(0)` rather than wrapping into
///   `u64`.** A negative epoch second is already in the past for any real
///   clock, and this crate's public expiry type is `u64` (matching every
///   other epoch-second value in this codebase) — rather than pick an
///   arbitrary signed representation just to carry a value that is always
///   treated as "expired since before Unix time began" anyway, folding it to
///   `0` preserves that exact meaning with no panic and no wraparound.
///   ([`is_expired`]'s [`MAX_PAST_EXPIRY_SECS`] guard is what keeps this from
///   being an instant, unconditional deletion — see there.)
#[must_use]
pub fn expires_at(item: &Item, attribute: &str) -> Option<u64> {
    match item.get(attribute)? {
        AttributeValue::N(text) => parse_ttl_seconds(text),
        _ => None,
    }
}

/// Parse a DynamoDB `N`'s text under the TTL grammar documented on
/// [`expires_at`]: `-?[0-9]+(\.[0-9]+)?`, truncated toward zero, folded to
/// `Some(0)` if negative. `None` for anything else (empty string, exponent
/// notation, stray characters, a bare `-`, a bare `.`, etc).
fn parse_ttl_seconds(text: &str) -> Option<u64> {
    let bytes = text.as_bytes();
    let mut idx = 0;
    let negative = bytes.first() == Some(&b'-');
    if negative {
        idx += 1;
    }
    let int_start = idx;
    while idx < bytes.len() && bytes[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == int_start {
        // No integer digits at all (e.g. "", "-", ".5", "-.5").
        return None;
    }
    let int_part = &text[int_start..idx];
    if idx < bytes.len() {
        // Anything after the integer part must be exactly one fractional
        // segment (`.` + digits), truncated away; anything else (an
        // exponent, stray text) is unparseable.
        if bytes[idx] != b'.' {
            return None;
        }
        idx += 1;
        let frac_start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == frac_start {
            // A trailing "." with no fractional digits (e.g. "5.").
            return None;
        }
        if idx != bytes.len() {
            // Trailing garbage after the fractional digits (e.g. "1.5e9").
            return None;
        }
    }
    let magnitude: u64 = int_part.parse().ok()?;
    Some(if negative { 0 } else { magnitude })
}

/// Is `item` expired under `attribute`, as of `now_epoch_secs`?
///
/// True exactly when [`expires_at`] returns a value that is:
/// 1. **strictly less than** `now_epoch_secs` (an expiry equal to "now" is
///    *not* yet expired — DynamoDB's own boundary), and
/// 2. no more than [`MAX_PAST_EXPIRY_SECS`] before `now_epoch_secs` — an
///    expiry further in the past than that is treated as **not expired**,
///    the milliseconds-vs-seconds safety guard documented on that constant.
///    (`now_epoch_secs - expiry == MAX_PAST_EXPIRY_SECS` is still within the
///    window — the boundary itself still counts as expired; only a value
///    *strictly older* than the window is spared.)
///
/// An absent/unusable TTL attribute ([`expires_at`] returning `None`) is
/// never expired.
#[must_use]
pub fn is_expired(item: &Item, attribute: &str, now_epoch_secs: u64) -> bool {
    let Some(expiry) = expires_at(item, attribute) else {
        return false;
    };
    if expiry >= now_epoch_secs {
        return false;
    }
    let age = now_epoch_secs - expiry;
    age <= MAX_PAST_EXPIRY_SECS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item_with(attr: &str, value: AttributeValue) -> Item {
        let mut item = Item::new();
        item.insert(attr.to_owned(), value);
        item
    }

    // --- expires_at ---------------------------------------------------

    #[test]
    fn expires_at_is_none_when_attribute_absent() {
        let item = Item::new();
        assert_eq!(expires_at(&item, "ttl"), None);
    }

    #[test]
    fn expires_at_is_none_for_every_non_numeric_type() {
        let cases = [
            AttributeValue::S("1700000000".into()),
            AttributeValue::B(vec![1, 2, 3]),
            AttributeValue::Bool(true),
            AttributeValue::Null,
            AttributeValue::M(std::collections::BTreeMap::new()),
            AttributeValue::L(vec![]),
            AttributeValue::SS(vec!["1700000000".into()]),
            AttributeValue::NS(vec!["1700000000".into()]),
            AttributeValue::BS(vec![vec![1]]),
        ];
        for value in cases {
            let item = item_with("ttl", value.clone());
            assert_eq!(
                expires_at(&item, "ttl"),
                None,
                "expected None for non-N attribute value {value:?}"
            );
        }
    }

    #[test]
    fn expires_at_parses_a_plain_integer() {
        let item = item_with("ttl", AttributeValue::N("1700000000".into()));
        assert_eq!(expires_at(&item, "ttl"), Some(1_700_000_000));
    }

    #[test]
    fn expires_at_truncates_a_fractional_value_toward_zero() {
        let item = item_with("ttl", AttributeValue::N("1700000000.9".into()));
        assert_eq!(expires_at(&item, "ttl"), Some(1_700_000_000));
    }

    #[test]
    fn expires_at_folds_a_negative_value_to_zero() {
        let item = item_with("ttl", AttributeValue::N("-5".into()));
        assert_eq!(expires_at(&item, "ttl"), Some(0));
        let item = item_with("ttl", AttributeValue::N("-5.9".into()));
        assert_eq!(expires_at(&item, "ttl"), Some(0));
    }

    #[test]
    fn expires_at_rejects_exponent_notation() {
        let item = item_with("ttl", AttributeValue::N("1.7e9".into()));
        assert_eq!(expires_at(&item, "ttl"), None);
    }

    #[test]
    fn expires_at_rejects_unparseable_text() {
        for text in [
            "", "-", ".", ".5", "-.5", "5.", "abc", "1.2.3", "1_000", "1 000", "+5",
        ] {
            let item = item_with("ttl", AttributeValue::N(text.into()));
            assert_eq!(expires_at(&item, "ttl"), None, "expected None for {text:?}");
        }
    }

    // --- is_expired ------------------------------------------------------

    #[test]
    fn is_expired_false_when_attribute_absent() {
        let item = Item::new();
        assert!(!is_expired(&item, "ttl", 1_700_000_100));
    }

    #[test]
    fn is_expired_false_for_a_wrong_type_attribute() {
        let item = item_with("ttl", AttributeValue::S("1".into()));
        assert!(!is_expired(&item, "ttl", 1_700_000_100));
    }

    #[test]
    fn is_expired_true_when_strictly_in_the_past() {
        let item = item_with("ttl", AttributeValue::N("1700000000".into()));
        assert!(is_expired(&item, "ttl", 1_700_000_001));
    }

    #[test]
    fn is_expired_false_when_in_the_future() {
        let item = item_with("ttl", AttributeValue::N("1700000000".into()));
        assert!(!is_expired(&item, "ttl", 1_699_999_999));
    }

    #[test]
    fn is_expired_false_when_exactly_equal_to_now() {
        // Strictly-less-than, not less-than-or-equal.
        let item = item_with("ttl", AttributeValue::N("1700000000".into()));
        assert!(!is_expired(&item, "ttl", 1_700_000_000));
    }

    #[test]
    fn is_expired_true_for_fractional_truncation_into_the_past() {
        // 1700000000.9 truncates to 1700000000, which is < 1700000001.
        let item = item_with("ttl", AttributeValue::N("1700000000.9".into()));
        assert!(is_expired(&item, "ttl", 1_700_000_001));
    }

    #[test]
    fn is_expired_false_for_unparseable_text() {
        let item = item_with("ttl", AttributeValue::N("not-a-number".into()));
        assert!(!is_expired(&item, "ttl", 1_700_000_100));
    }

    #[test]
    fn is_expired_true_for_a_negative_value_within_the_safety_window() {
        // Folds to expiry 0; "now" close enough to the epoch that age <=
        // MAX_PAST_EXPIRY_SECS.
        let item = item_with("ttl", AttributeValue::N("-1".into()));
        assert!(is_expired(&item, "ttl", MAX_PAST_EXPIRY_SECS));
    }

    #[test]
    fn is_expired_false_for_a_negative_value_beyond_the_safety_window() {
        // Folds to expiry 0; any realistic "now" (e.g. today's epoch,
        // ~1.76e9) is far more than 5 years past 0.
        let item = item_with("ttl", AttributeValue::N("-1".into()));
        assert!(!is_expired(&item, "ttl", 1_700_000_000));
    }

    #[test]
    fn is_expired_true_exactly_at_the_five_year_boundary() {
        let now = 2_000_000_000u64;
        let expiry = now - MAX_PAST_EXPIRY_SECS;
        let item = item_with("ttl", AttributeValue::N(expiry.to_string()));
        assert!(is_expired(&item, "ttl", now));
    }

    #[test]
    fn is_expired_false_just_past_the_five_year_boundary() {
        let now = 2_000_000_000u64;
        let expiry = now - MAX_PAST_EXPIRY_SECS - 1;
        let item = item_with("ttl", AttributeValue::N(expiry.to_string()));
        assert!(!is_expired(&item, "ttl", now));
    }

    #[test]
    fn is_expired_true_just_inside_the_five_year_boundary() {
        let now = 2_000_000_000u64;
        let expiry = now - MAX_PAST_EXPIRY_SECS + 1;
        let item = item_with("ttl", AttributeValue::N(expiry.to_string()));
        assert!(is_expired(&item, "ttl", now));
    }
}
