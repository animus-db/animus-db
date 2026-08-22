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

/// Add two DynamoDB numbers given as text, exactly.
///
/// `ADD` is how DynamoDB increments a counter, so this has to be exact for the
/// same reason [`compare_numeric`] does: 38 significant digits do not survive
/// an `f64`, and a counter that silently loses its low digits at scale is
/// worse than one that refuses to move. The arithmetic is done on the decimal
/// digits — align the fractions, then add or subtract magnitudes according to
/// the signs.
///
/// `None` if either side is not a number.
#[must_use]
pub(crate) fn add_numeric(x: &str, y: &str) -> Option<String> {
    let (xn, xi, xf) = decimal_parts(x)?;
    let (yn, yi, yf) = decimal_parts(y)?;

    // Align the fractional digits so both sides are plain integers scaled by
    // the same power of ten.
    let scale = xf.len().max(yf.len());
    let widen = |i: &str, f: &str| format!("{i}{f:0<scale$}");
    let xd = widen(&xi, &xf);
    let yd = widen(&yi, &yf);

    let (neg, mut digits) = if xn == yn {
        (xn, add_digits(&xd, &yd))
    } else {
        match cmp_digits(&xd, &yd) {
            std::cmp::Ordering::Equal => (false, "0".to_string()),
            std::cmp::Ordering::Greater => (xn, sub_digits(&xd, &yd)),
            std::cmp::Ordering::Less => (yn, sub_digits(&yd, &xd)),
        }
    };

    // Re-insert the decimal point `scale` digits from the right.
    if digits.len() <= scale {
        digits = format!("{:0>width$}", digits, width = scale + 1);
    }
    let split = digits.len() - scale;
    let (int, frac) = digits.split_at(split);
    let int = int.trim_start_matches('0');
    let frac = frac.trim_end_matches('0');
    let int = if int.is_empty() { "0" } else { int };
    let sign = if neg && !(int == "0" && frac.is_empty()) {
        "-"
    } else {
        ""
    };
    Some(if frac.is_empty() {
        format!("{sign}{int}")
    } else {
        format!("{sign}{int}.{frac}")
    })
}

/// `(negative, integer digits, fractional digits)` for a decimal string.
#[must_use]
fn decimal_parts(v: &str) -> Option<(bool, String, String)> {
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
    Some((neg, int.to_owned(), frac.to_owned()))
}

/// Compare two equal-scale digit strings by magnitude.
#[must_use]
fn cmp_digits(a: &str, b: &str) -> std::cmp::Ordering {
    let (a, b) = (a.trim_start_matches('0'), b.trim_start_matches('0'));
    a.len().cmp(&b.len()).then_with(|| a.cmp(b))
}

/// Schoolbook addition over digit strings.
#[must_use]
fn add_digits(a: &str, b: &str) -> String {
    let (mut a, mut b) = (a.bytes().rev(), b.bytes().rev());
    let mut out = Vec::new();
    let mut carry = 0u8;
    loop {
        let (x, y) = (a.next(), b.next());
        if x.is_none() && y.is_none() && carry == 0 {
            break;
        }
        let sum = (x.map_or(0, |d| d - b'0')) + (y.map_or(0, |d| d - b'0')) + carry;
        out.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    out.reverse();
    String::from_utf8(out).expect("digits are ASCII")
}

/// Schoolbook subtraction, `a - b`, where `a >= b` by magnitude.
#[must_use]
fn sub_digits(a: &str, b: &str) -> String {
    let (mut a, mut b) = (a.bytes().rev(), b.bytes().rev());
    let mut out = Vec::new();
    let mut borrow = 0i8;
    loop {
        let (x, y) = (a.next(), b.next());
        if x.is_none() && y.is_none() {
            break;
        }
        let mut d = i8::try_from(x.map_or(0, |d| d - b'0')).expect("digit")
            - i8::try_from(y.map_or(0, |d| d - b'0')).expect("digit")
            - borrow;
        if d < 0 {
            d += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push(b'0' + u8::try_from(d).expect("digit"));
    }
    out.reverse();
    let s = String::from_utf8(out).expect("digits are ASCII");
    let t = s.trim_start_matches('0');
    if t.is_empty() { "0".into() } else { t.into() }
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
    /// `a AND b` — both must hold.
    And(Box<ConditionExpression>, Box<ConditionExpression>),
    /// `a OR b` — either may hold.
    Or(Box<ConditionExpression>, Box<ConditionExpression>),
    /// `NOT a`.
    Not(Box<ConditionExpression>),
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
            // Short-circuiting matches DynamoDB's own evaluation and, more
            // importantly, keeps every leaf's "false when absent" semantics
            // intact under composition: `NOT attribute_exists(a)` is true for
            // a missing `a` precisely because the leaf is false, not unknown.
            ConditionExpression::And(a, b) => a.evaluate(current) && b.evaluate(current),
            ConditionExpression::Or(a, b) => a.evaluate(current) || b.evaluate(current),
            ConditionExpression::Not(inner) => !inner.evaluate(current),
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

    /// Composition evaluates short-circuiting, and — the subtle part — each
    /// leaf's "false when the attribute is absent" survives under `NOT`.
    /// `NOT a = :v` is true for a missing `a` because the leaf is false, not
    /// unknown; DynamoDB has no three-valued logic here.
    #[test]
    fn boolean_composition_evaluates() {
        let item = item_of(&[("a", n("1")), ("b", n("2"))]);
        let eq =
            |name: &str, v: &str| ConditionExpression::Compare(name.into(), Comparator::Eq, n(v));
        let ev = |c: ConditionExpression| c.evaluate(Some(&item));

        assert!(ev(ConditionExpression::And(
            Box::new(eq("a", "1")),
            Box::new(eq("b", "2"))
        )));
        assert!(!ev(ConditionExpression::And(
            Box::new(eq("a", "1")),
            Box::new(eq("b", "9"))
        )));
        assert!(ev(ConditionExpression::Or(
            Box::new(eq("a", "9")),
            Box::new(eq("b", "2"))
        )));
        assert!(!ev(ConditionExpression::Or(
            Box::new(eq("a", "9")),
            Box::new(eq("b", "9"))
        )));
        assert!(ev(ConditionExpression::Not(Box::new(eq("a", "9")))));
        assert!(!ev(ConditionExpression::Not(Box::new(eq("a", "1")))));

        // A missing attribute: the leaf is false, so NOT makes it true.
        assert!(ev(ConditionExpression::Not(Box::new(eq("missing", "1")))));
        // And `NOT attribute_exists` agrees with `attribute_not_exists`.
        assert_eq!(
            ev(ConditionExpression::Not(Box::new(
                ConditionExpression::AttributeExists("missing".into())
            ))),
            ev(ConditionExpression::AttributeNotExists("missing".into()))
        );
    }

    /// Nesting composes to arbitrary depth and respects the tree it was given.
    #[test]
    fn nested_composition_respects_its_tree() {
        let item = item_of(&[("a", n("1")), ("b", n("2")), ("c", n("3"))]);
        let eq =
            |name: &str, v: &str| ConditionExpression::Compare(name.into(), Comparator::Eq, n(v));
        // (a=1 OR b=9) AND c=3  -> true
        assert!(
            ConditionExpression::And(
                Box::new(ConditionExpression::Or(
                    Box::new(eq("a", "1")),
                    Box::new(eq("b", "9"))
                )),
                Box::new(eq("c", "3"))
            )
            .evaluate(Some(&item))
        );
        // The two groupings genuinely diverge on the same item: with a=9,
        // b=2, c=9 the OR-first tree holds (b=2 satisfies the inner OR, but
        // c=9 fails the AND) while the AND-first tree does not.
        let other = item_of(&[("a", n("9")), ("b", n("2")), ("c", n("9"))]);
        assert!(
            !ConditionExpression::And(
                Box::new(ConditionExpression::Or(
                    Box::new(eq("a", "1")),
                    Box::new(eq("b", "2"))
                )),
                Box::new(eq("c", "3"))
            )
            .evaluate(Some(&other)),
            "(a=1 OR b=2) AND c=3 fails because c=9"
        );
        assert!(
            !ConditionExpression::Or(
                Box::new(eq("a", "1")),
                Box::new(ConditionExpression::And(
                    Box::new(eq("b", "2")),
                    Box::new(eq("c", "3"))
                ))
            )
            .evaluate(Some(&other)),
            "and a=1 OR (b=2 AND c=3) fails too, for a different reason"
        );
        // Where they diverge: a=1, b=2, c=9.
        let diverge = item_of(&[("a", n("1")), ("b", n("2")), ("c", n("9"))]);
        assert!(
            !ConditionExpression::And(
                Box::new(ConditionExpression::Or(
                    Box::new(eq("a", "1")),
                    Box::new(eq("b", "2"))
                )),
                Box::new(eq("c", "3"))
            )
            .evaluate(Some(&diverge)),
            "(a=1 OR b=2) AND c=3 is false — c=9"
        );
        assert!(
            ConditionExpression::Or(
                Box::new(eq("a", "1")),
                Box::new(ConditionExpression::And(
                    Box::new(eq("b", "2")),
                    Box::new(eq("c", "3"))
                ))
            )
            .evaluate(Some(&diverge)),
            "but a=1 OR (b=2 AND c=3) is true — a=1 alone satisfies it. \
             Same leaves, different tree, different answer: this is what \
             precedence has to get right."
        );
    }

    /// `ADD` increments counters, so the arithmetic must be exact — an `f64`
    /// round-trip loses the low digits of exactly the large identifiers
    /// people count with.
    #[test]
    fn decimal_addition_is_exact() {
        let add = |a: &str, b: &str| add_numeric(a, b).expect("numbers");
        assert_eq!(add("1", "1"), "2");
        assert_eq!(add("0", "0"), "0");
        assert_eq!(add("9", "1"), "10", "carry");
        assert_eq!(add("99", "1"), "100", "carry chain");
        assert_eq!(add("1.5", "2.25"), "3.75", "fraction alignment");
        assert_eq!(add("0.1", "0.2"), "0.3", "no binary-float artefact");
        assert_eq!(add("5", "-3"), "2", "mixed signs");
        assert_eq!(add("3", "-5"), "-2");
        assert_eq!(add("-3", "-5"), "-8");
        assert_eq!(add("5", "-5"), "0", "and never -0");
        assert_eq!(add("1.10", "0.90"), "2", "trailing zeros normalise away");
        assert_eq!(
            add("99999999999999999999999999999999999999", "1"),
            "100000000000000000000000000000000000000",
            "38 digits carry exactly — f64 would round this"
        );
        assert_eq!(add("-0", "0"), "0");
        assert!(add_numeric("abc", "1").is_none());
    }
}
