//! Query sort-key conditions and a `ConditionExpression` subset for conditional
//! writes (ADR 0006). Both are **pure, deterministic** predicates over the
//! crate's [`AttributeValue`] / [`Item`] model — no I/O, no storage.
//!
//! ## Sort-key conditions
//!
//! A `Query` always pins the partition key to a single value (`pk = ..`) and may
//! additionally narrow the sort key with one of: equality (`sk = v`), a range
//! (`sk BETWEEN lo AND hi`), or a prefix (`begins_with(sk, p)`). Each maps to a
//! byte range over a partition's contiguous keyspace plus an exact predicate, so
//! a caller can scan the partition and keep the matching rows.
//!
//! ## Condition expressions
//!
//! A minimal `ConditionExpression` subset for `PutItem` / `DeleteItem`:
//! `attribute_not_exists(attr)`, `attribute_exists(attr)`, and attribute
//! equality (`attr = :v`). These are evaluated against the *current* stored item
//! (or its absence) before a write commits — the DynamoDB conditional-write
//! contract. The fuller expression grammar (AND/OR/NOT, comparators, functions)
//! is deferred.

use serde::{Deserialize, Serialize};

use crate::{AttributeValue, Item};

/// The sort-key half of a `Query` key condition. The partition key is always an
/// equality (handled by the caller); this narrows within that partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortKeyCondition {
    /// `sk = value`.
    Equals(AttributeValue),
    /// `sk BETWEEN lo AND hi` (inclusive on both ends).
    Between(AttributeValue, AttributeValue),
    /// `begins_with(sk, prefix)` — only meaningful for string/binary sort keys.
    BeginsWith(AttributeValue),
}

impl SortKeyCondition {
    /// Whether an item's sort-key value satisfies this condition. Comparison is
    /// over the same key-bytes used for storage ordering (so it agrees with the
    /// scan range below).
    #[must_use]
    pub fn matches(&self, sort_value: &AttributeValue) -> bool {
        let v = sort_value.key_bytes();
        match self {
            SortKeyCondition::Equals(target) => v == target.key_bytes(),
            SortKeyCondition::Between(lo, hi) => {
                let (lo, hi) = (lo.key_bytes(), hi.key_bytes());
                v >= lo && v <= hi
            }
            SortKeyCondition::BeginsWith(prefix) => v.starts_with(&prefix.key_bytes()),
        }
    }
}

/// A minimal DynamoDB `ConditionExpression` subset, evaluated against the
/// item currently stored at the target key (`None` when absent).
///
/// **`Serialize`/`Deserialize` (ADR 0046 U3)**: this now rides the wire
/// inside `ClientRequest::KindWriteItem` — the leader-side write evaluator's
/// forwarding payload — so a caller's condition travels with the request
/// instead of being evaluated (and compiled away) only at the edge that
/// received it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Comparator {
    /// `=`
    Eq,
    /// `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

impl Comparator {
    /// Whether an `Ordering` between two comparable operands satisfies this
    /// comparator. `None` means the operands were not comparable at all
    /// (mismatched types), which DynamoDB treats as *false* for every
    /// comparator — including `<>`, so a type mismatch never accidentally
    /// satisfies a not-equal.
    #[must_use]
    fn holds(self, ord: Option<std::cmp::Ordering>) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let Some(ord) = ord else { return false };
        match self {
            Comparator::Eq => ord == Equal,
            Comparator::Ne => ord != Equal,
            Comparator::Lt => ord == Less,
            Comparator::Le => ord != Greater,
            Comparator::Gt => ord == Greater,
            Comparator::Ge => ord != Less,
        }
    }
}

/// Order two `AttributeValue`s the way DynamoDB compares them, or `None` when
/// they are not mutually comparable.
///
/// Numbers compare **numerically**, not by their textual bytes. That differs
/// deliberately from [`AttributeValue::key_bytes`], whose lexicographic
/// number ordering is a documented simplification of *key* ordering: a key's
/// order has to agree with how rows are stored, while a filter is evaluated
/// in memory over an item and has no such constraint. Comparing `"10"` and
/// `"9"` as text would make `price > :p` quietly wrong for ordinary data.
///
/// Strings compare by UTF-8 bytes and binary by unsigned bytes, both matching
/// DynamoDB. Every other type — and every cross-type pair — is incomparable.
#[must_use]
fn compare_values(a: &AttributeValue, b: &AttributeValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (AttributeValue::N(x), AttributeValue::N(y)) => compare_numeric(x, y),
        (AttributeValue::S(x), AttributeValue::S(y)) => Some(x.as_bytes().cmp(y.as_bytes())),
        (AttributeValue::B(x), AttributeValue::B(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Compare two DynamoDB numbers given as text, without going through a float.
///
/// DynamoDB allows 38 significant digits, which `f64` cannot hold, so parsing
/// would silently lose precision on exactly the large identifiers people use
/// as numeric keys. This compares sign, then integer magnitude, then the
/// fractional part digit by digit. `None` if either side is not a number.
#[must_use]
fn compare_numeric(x: &str, y: &str) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;

    /// `(negative, integer digits, fractional digits)`, zero-normalised.
    fn parts(v: &str) -> Option<(bool, String, String)> {
        let v = v.trim();
        let (neg, rest) = match v.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, v.strip_prefix('+').unwrap_or(v)),
        };
        let (int, frac) = rest.split_once('.').unwrap_or((rest, ""));
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
            return None;
        }
        let int = int.trim_start_matches('0');
        let frac = frac.trim_end_matches('0');
        // Normalise -0 to 0 so the sign check below cannot order them apart.
        let neg = neg && !(int.is_empty() && frac.is_empty());
        Some((neg, int.to_owned(), frac.to_owned()))
    }

    let (xn, xi, xf) = parts(x)?;
    let (yn, yi, yf) = parts(y)?;
    if xn != yn {
        // Exactly one is negative; the negative one is smaller.
        return Some(if xn {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    // Same sign: compare magnitudes, then flip for negatives.
    let magnitude = xi
        .len()
        .cmp(&yi.len())
        .then_with(|| xi.cmp(&yi))
        .then_with(|| {
            // Pad the fractions to equal length so a digit-wise compare is
            // positional (`.5` vs `.45`).
            let width = xf.len().max(yf.len());
            let pad = |f: &str| format!("{f:0<width$}");
            pad(&xf).cmp(&pad(&yf))
        });
    Some(if xn { magnitude.reverse() } else { magnitude })
}

/// Value equality, numeric-aware.
///
/// Structural equality is wrong for numbers: DynamoDB carries them as text, so
/// `1.10`/`1.1` and `-0`/`0` are the same number written differently. Falls
/// back to structural equality for everything `compare_values` cannot order
/// (documents, sets, `BOOL`, `NULL`) and for cross-type pairs, which are
/// unequal.
#[must_use]
fn values_equal(a: &AttributeValue, b: &AttributeValue) -> bool {
    match compare_values(a, b) {
        Some(ord) => ord == std::cmp::Ordering::Equal,
        None => a == b,
    }
}

/// The `attribute_type` type codes, as DynamoDB spells them on the wire.
#[must_use]
fn type_code(v: &AttributeValue) -> &'static str {
    match v {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::Bool(_) => "BOOL",
        AttributeValue::Null => "NULL",
        AttributeValue::M(_) => "M",
        AttributeValue::L(_) => "L",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
    }
}

/// DynamoDB's `size()` of an attribute: bytes for `S`/`B`, element count for
/// the document and set types, and **no size at all** for `N`/`BOOL`/`NULL`
/// (a `size()` comparison against those is false rather than an error, as on
/// a missing attribute).
#[must_use]
fn size_of(v: &AttributeValue) -> Option<usize> {
    match v {
        AttributeValue::S(s) => Some(s.len()),
        AttributeValue::B(b) => Some(b.len()),
        AttributeValue::M(m) => Some(m.len()),
        AttributeValue::L(l) => Some(l.len()),
        AttributeValue::SS(v) => Some(v.len()),
        AttributeValue::NS(v) => Some(v.len()),
        AttributeValue::BS(v) => Some(v.len()),
        AttributeValue::N(_) | AttributeValue::Bool(_) | AttributeValue::Null => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionExpression {
    /// `attribute_not_exists(attr)` — true iff the item is absent or lacks `attr`.
    AttributeNotExists(String),
    /// `attribute_exists(attr)` — true iff the item is present and has `attr`.
    AttributeExists(String),
    /// `attr <op> value`. A missing attribute, or one of an incomparable
    /// type, is false for **every** operator including `<>`.
    Compare(String, Comparator, AttributeValue),
    /// `attr BETWEEN lo AND hi`, inclusive on both ends.
    Between(String, AttributeValue, AttributeValue),
    /// `attr IN (:a, :b, ..)` — equality against any listed value.
    In(String, Vec<AttributeValue>),
    /// `begins_with(attr, prefix)` — `S` and `B` only.
    BeginsWith(String, AttributeValue),
    /// `contains(attr, operand)` — substring for `S`, membership for the set
    /// and list types.
    Contains(String, AttributeValue),
    /// `attribute_type(attr, :code)` — `"S"`, `"N"`, `"B"`, `"BOOL"`,
    /// `"NULL"`, `"M"`, `"L"`, `"SS"`, `"NS"`, `"BS"`.
    AttributeType(String, String),
    /// `size(attr) <op> value` — bytes for `S`/`B`, element count for the
    /// document and set types.
    Size(String, Comparator, AttributeValue),
}

impl ConditionExpression {
    /// Evaluate the condition against the current item at the key (`None` when
    /// no live item exists). A write proceeds only when this returns `true`.
    #[must_use]
    pub fn evaluate(&self, current: Option<&Item>) -> bool {
        match self {
            ConditionExpression::AttributeNotExists(attr) => {
                current.is_none_or(|item| !item.contains_key(attr))
            }
            ConditionExpression::AttributeExists(attr) => {
                current.is_some_and(|item| item.contains_key(attr))
            }
            ConditionExpression::Compare(attr, op, value) => {
                let Some(actual) = current.and_then(|item| item.get(attr)) else {
                    // A missing attribute satisfies no comparison — `<>`
                    // included. DynamoDB has no three-valued logic here.
                    return false;
                };
                // Equality and inequality work for *every* type (two maps can
                // be equal); ordering only for the comparable scalars.
                match op {
                    // Equality works for every type (two maps can be equal) and
                    // must be numeric-aware; ordering only for the comparable
                    // scalars.
                    Comparator::Eq => values_equal(actual, value),
                    Comparator::Ne => !values_equal(actual, value),
                    _ => op.holds(compare_values(actual, value)),
                }
            }
            ConditionExpression::Between(attr, lo, hi) => {
                let Some(actual) = current.and_then(|item| item.get(attr)) else {
                    return false;
                };
                Comparator::Ge.holds(compare_values(actual, lo))
                    && Comparator::Le.holds(compare_values(actual, hi))
            }
            ConditionExpression::In(attr, values) => current
                .and_then(|item| item.get(attr))
                .is_some_and(|actual| values.iter().any(|v| values_equal(actual, v))),
            ConditionExpression::BeginsWith(attr, prefix) => {
                match (current.and_then(|item| item.get(attr)), prefix) {
                    (Some(AttributeValue::S(v)), AttributeValue::S(p)) => v.starts_with(p),
                    (Some(AttributeValue::B(v)), AttributeValue::B(p)) => v.starts_with(p),
                    _ => false,
                }
            }
            ConditionExpression::Contains(attr, operand) => {
                match current.and_then(|item| item.get(attr)) {
                    Some(AttributeValue::S(v)) => match operand {
                        AttributeValue::S(needle) => v.contains(needle.as_str()),
                        _ => false,
                    },
                    Some(AttributeValue::SS(vs)) => match operand {
                        AttributeValue::S(needle) => vs.contains(needle),
                        _ => false,
                    },
                    Some(AttributeValue::NS(vs)) => match operand {
                        AttributeValue::N(needle) => vs
                            .iter()
                            .any(|v| compare_numeric(v, needle) == Some(std::cmp::Ordering::Equal)),
                        _ => false,
                    },
                    Some(AttributeValue::BS(vs)) => match operand {
                        AttributeValue::B(needle) => vs.contains(needle),
                        _ => false,
                    },
                    Some(AttributeValue::L(items)) => {
                        items.iter().any(|i| values_equal(i, operand))
                    }
                    _ => false,
                }
            }
            ConditionExpression::AttributeType(attr, code) => current
                .and_then(|item| item.get(attr))
                .is_some_and(|actual| type_code(actual) == code),
            ConditionExpression::Size(attr, op, value) => {
                let Some(size) = current.and_then(|item| item.get(attr)).and_then(size_of) else {
                    return false;
                };
                // `size()` yields a number, so the comparison is numeric.
                op.holds(compare_values(&AttributeValue::N(size.to_string()), value))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> AttributeValue {
        AttributeValue::S(v.into())
    }

    #[test]
    fn equals_matches_only_exact() {
        let cond = SortKeyCondition::Equals(s("b"));
        assert!(cond.matches(&s("b")));
        assert!(!cond.matches(&s("a")));
        assert!(!cond.matches(&s("bb")));
    }

    #[test]
    fn between_is_inclusive() {
        let cond = SortKeyCondition::Between(s("b"), s("d"));
        for hit in ["b", "c", "d"] {
            assert!(cond.matches(&s(hit)), "{hit}");
        }
        for miss in ["a", "e"] {
            assert!(!cond.matches(&s(miss)), "{miss}");
        }
    }

    #[test]
    fn begins_with_matches_prefix() {
        let cond = SortKeyCondition::BeginsWith(s("ab"));
        assert!(cond.matches(&s("ab")));
        assert!(cond.matches(&s("abc")));
        assert!(!cond.matches(&s("ac")));
        assert!(!cond.matches(&s("a")));
    }

    #[test]
    fn attribute_not_exists() {
        let cond = ConditionExpression::AttributeNotExists("pk".into());
        assert!(cond.evaluate(None));
        let mut item = Item::new();
        item.insert("other".into(), s("x"));
        assert!(cond.evaluate(Some(&item)));
        item.insert("pk".into(), s("k"));
        assert!(!cond.evaluate(Some(&item)));
    }

    #[test]
    fn attribute_exists_and_equals() {
        let mut item = Item::new();
        item.insert("pk".into(), s("k"));
        item.insert("v".into(), AttributeValue::N("1".into()));
        assert!(ConditionExpression::AttributeExists("pk".into()).evaluate(Some(&item)));
        assert!(!ConditionExpression::AttributeExists("pk".into()).evaluate(None));
        assert!(
            ConditionExpression::Compare("v".into(), Comparator::Eq, AttributeValue::N("1".into()))
                .evaluate(Some(&item))
        );
        assert!(
            !ConditionExpression::Compare(
                "v".into(),
                Comparator::Eq,
                AttributeValue::N("2".into())
            )
            .evaluate(Some(&item))
        );
        assert!(
            !ConditionExpression::Compare(
                "v".into(),
                Comparator::Eq,
                AttributeValue::N("1".into())
            )
            .evaluate(None)
        );
    }

    fn n(v: &str) -> AttributeValue {
        AttributeValue::N(v.into())
    }

    fn item_of(pairs: &[(&str, AttributeValue)]) -> Item {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    /// Numbers compare **numerically**, not as text. This is the assertion
    /// that fails if the comparison ever falls back to `key_bytes`, whose
    /// lexicographic number order would make 9 > 10.
    #[test]
    fn numbers_compare_numerically_not_lexicographically() {
        let item = item_of(&[("v", n("10"))]);
        let gt = |target: &str| {
            ConditionExpression::Compare("v".into(), Comparator::Gt, n(target))
                .evaluate(Some(&item))
        };
        assert!(
            gt("9"),
            "10 > 9 — lexicographically \"10\" < \"9\" would say otherwise"
        );
        assert!(!gt("11"));

        // Negatives, decimals, and precision beyond f64.
        let cmp = |a: &str, op: Comparator, b: &str| {
            ConditionExpression::Compare("v".into(), op, n(b))
                .evaluate(Some(&item_of(&[("v", n(a))])))
        };
        assert!(cmp("-5", Comparator::Lt, "3"));
        assert!(cmp("-5", Comparator::Lt, "-3"));
        assert!(
            cmp("-0", Comparator::Eq, "0"),
            "-0 and 0 are the same number"
        );
        assert!(cmp("0.5", Comparator::Gt, "0.45"));
        assert!(
            cmp("1.10", Comparator::Eq, "1.1"),
            "trailing zeros do not change value"
        );
        assert!(
            cmp(
                "123456789012345678901234567890",
                Comparator::Gt,
                "123456789012345678901234567889"
            ),
            "38-digit precision must survive — an f64 round-trip would call these equal"
        );
    }

    /// A missing attribute is false for every operator, `<>` included —
    /// DynamoDB has no three-valued logic here.
    #[test]
    fn a_missing_attribute_satisfies_no_comparison() {
        let item = item_of(&[("other", n("1"))]);
        for op in [
            Comparator::Eq,
            Comparator::Ne,
            Comparator::Lt,
            Comparator::Le,
            Comparator::Gt,
            Comparator::Ge,
        ] {
            assert!(
                !ConditionExpression::Compare("v".into(), op, n("1")).evaluate(Some(&item)),
                "missing attribute must not satisfy {op:?}"
            );
        }
    }

    /// Ordering across incomparable types is false, but `<>` is plain
    /// inequality and still holds.
    #[test]
    fn cross_type_ordering_is_false_but_inequality_holds() {
        let item = item_of(&[("v", s("abc"))]);
        assert!(
            !ConditionExpression::Compare("v".into(), Comparator::Gt, n("1")).evaluate(Some(&item))
        );
        assert!(
            ConditionExpression::Compare("v".into(), Comparator::Ne, n("1")).evaluate(Some(&item)),
            "a string is not that number"
        );
    }

    #[test]
    fn between_in_begins_with_and_contains() {
        let item = item_of(&[
            ("num", n("5")),
            ("name", s("hello world")),
            ("tags", AttributeValue::SS(vec!["a".into(), "b".into()])),
            ("list", AttributeValue::L(vec![n("1"), s("x")])),
        ]);
        let ev = |c: ConditionExpression| c.evaluate(Some(&item));

        assert!(ev(ConditionExpression::Between(
            "num".into(),
            n("1"),
            n("10")
        )));
        assert!(
            ev(ConditionExpression::Between("num".into(), n("5"), n("5"))),
            "inclusive"
        );
        assert!(!ev(ConditionExpression::Between(
            "num".into(),
            n("6"),
            n("10")
        )));

        assert!(ev(ConditionExpression::In(
            "num".into(),
            vec![n("3"), n("5")]
        )));
        assert!(!ev(ConditionExpression::In(
            "num".into(),
            vec![n("3"), n("4")]
        )));

        assert!(ev(ConditionExpression::BeginsWith(
            "name".into(),
            s("hello")
        )));
        assert!(!ev(ConditionExpression::BeginsWith(
            "name".into(),
            s("world")
        )));

        assert!(
            ev(ConditionExpression::Contains("name".into(), s("lo wo"))),
            "substring"
        );
        assert!(
            ev(ConditionExpression::Contains("tags".into(), s("b"))),
            "set membership"
        );
        assert!(!ev(ConditionExpression::Contains("tags".into(), s("z"))));
        assert!(
            ev(ConditionExpression::Contains("list".into(), s("x"))),
            "list membership"
        );
    }

    #[test]
    fn attribute_type_and_size() {
        let item = item_of(&[
            ("s", s("abcd")),
            ("num", n("42")),
            ("list", AttributeValue::L(vec![n("1"), n("2"), n("3")])),
        ]);
        let ev = |c: ConditionExpression| c.evaluate(Some(&item));

        assert!(ev(ConditionExpression::AttributeType(
            "s".into(),
            "S".into()
        )));
        assert!(!ev(ConditionExpression::AttributeType(
            "s".into(),
            "N".into()
        )));
        assert!(ev(ConditionExpression::AttributeType(
            "list".into(),
            "L".into()
        )));

        assert!(
            ev(ConditionExpression::Size(
                "s".into(),
                Comparator::Eq,
                n("4")
            )),
            "bytes"
        );
        assert!(
            ev(ConditionExpression::Size(
                "list".into(),
                Comparator::Eq,
                n("3")
            )),
            "elements"
        );
        assert!(ev(ConditionExpression::Size(
            "s".into(),
            Comparator::Gt,
            n("3")
        )));
        assert!(
            !ev(ConditionExpression::Size(
                "num".into(),
                Comparator::Gt,
                n("0")
            )),
            "a number has no size(), so the comparison is false rather than an error"
        );
    }
}
