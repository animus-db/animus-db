//! Query sort-key conditions and a `ConditionExpression` subset for conditional
//! writes (ADR 0006). Both are **pure, deterministic** predicates over the
//! crate's [`AttributeValue`] / [`Item`] model — no I/O, no storage.
//!
//! ## Sort-key conditions
//!
//! A `Query` always pins the partition key to a single value (`pk = ..`) and may
//! additionally narrow the sort key with one of: a comparison (`sk = v`,
//! `sk < v`, `sk <= v`, `sk > v`, `sk >= v` — every `KeyConditionExpression`
//! comparator AWS supports except `<>`, which is not part of that grammar), a
//! range (`sk BETWEEN lo AND hi`), or a prefix (`begins_with(sk, p)`). None of
//! these narrow the *scan range* itself — the partition's whole contiguous
//! keyspace is still scanned; each is a **filter** applied to the rows that
//! scan returns (`SortKeyCondition::matches`/`matches_raw`), the identical
//! mechanism `BETWEEN` always used.
//!
//! ## Condition expressions
//!
//! A minimal `ConditionExpression` subset for `PutItem` / `DeleteItem`:
//! `attribute_not_exists(attr)`, `attribute_exists(attr)`, and attribute
//! equality (`attr = :v`). These are evaluated against the *current* stored item
//! (or its absence) before a write commits — the DynamoDB conditional-write
//! contract. The fuller expression grammar (AND/OR/NOT, comparators, functions)
//! is deferred.
//!
//! **A missing attribute vs. a wrong-typed one are not the same outcome.**
//! Every leaf here is false when the target attribute is absent (DynamoDB has
//! no three-valued logic). But `size()`/`begins_with()`/`contains()` are
//! *functions* with a fixed operand-type domain, and applying one to an
//! attribute that **exists** with a type outside that domain is a real
//! DynamoDB `ValidationException` at evaluation time, not a false
//! comparison — [`ConditionExpression::evaluate`] surfaces that as
//! `Err(ConditionError)` instead of `Ok(false)`. Plain comparators
//! (`Compare`/`Between`/`In`) stay `Ok(false)` on a type mismatch between two
//! *supplied* operands (`attr > :v` where `attr` and `:v` disagree) — that is
//! DynamoDB's own documented comparator behavior, distinct from a function's
//! operand-domain violation, and is unaffected by this distinction.

use serde::{Deserialize, Serialize};

use crate::{AttributeValue, Item};

/// The sort-key half of a `Query` key condition. The partition key is always an
/// equality (handled by the caller); this narrows within that partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortKeyCondition {
    /// `sk <op> value` for any of DynamoDB's `KeyConditionExpression`
    /// comparators (`=`, `<`, `<=`, `>`, `>=`) — reusing [`Comparator`]
    /// wholesale rather than a narrower enum of its own. `Ne` (`<>`) is a
    /// legal [`Comparator`] value but never reaches here: AWS's own
    /// `KeyConditionExpression` grammar has no not-equal operator, so the
    /// wire decoder (`animus_dynamo::wire::decode_sort_condition`) rejects it
    /// before a `SortKeyCondition` is ever built. `matches` still handles it
    /// correctly if constructed directly (e.g. in a test), since it costs
    /// nothing extra to support.
    Compare(Comparator, AttributeValue),
    /// `sk BETWEEN lo AND hi` (inclusive on both ends).
    Between(AttributeValue, AttributeValue),
    /// `begins_with(sk, prefix)` — only meaningful for string/binary sort keys.
    BeginsWith(AttributeValue),
}

impl SortKeyCondition {
    /// Whether an item's sort-key value satisfies this condition.
    ///
    /// `N` values compare **numerically** (via [`compare_numeric`], through
    /// [`sort_key_cmp`]), not by their raw text bytes: a number sort key is
    /// stored as decimal text (`storage_key`'s documented simplification), so
    /// a byte compare would put `"9"` after `"15"` and make
    /// `sk BETWEEN 5 AND 15` wrongly exclude `sk = 9`. This is exactly the
    /// key-ordering-vs-filter split [`compare_values`]'s doc comment already
    /// draws: the scan *range* still walks the engine's byte-ordered keyspace
    /// (unaffected — it can only widen, never narrow, since it stays keyed on
    /// `key_bytes`), but this in-memory filter over the rows that range
    /// returns must agree with DynamoDB's actual numeric semantics. `S`/`B`
    /// are unaffected: their `key_bytes` already sorts the way DynamoDB
    /// compares them, and this keeps using it (also making every comparator
    /// consistent with `Between` rather than diverging on `N` alone).
    ///
    /// A caller holding only a sort key's **raw stored bytes** (off an engine
    /// scan, with no type tag) must use [`Self::matches_raw`] instead — see
    /// its own doc for why passing raw bytes here directly would silently
    /// defeat the numeric compare for `N`.
    #[must_use]
    pub fn matches(&self, sort_value: &AttributeValue) -> bool {
        match self {
            SortKeyCondition::Compare(op, target) => {
                op.holds(Some(sort_key_cmp(sort_value, target)))
            }
            SortKeyCondition::Between(lo, hi) => {
                sort_key_cmp(sort_value, lo) != std::cmp::Ordering::Less
                    && sort_key_cmp(sort_value, hi) != std::cmp::Ordering::Greater
            }
            SortKeyCondition::BeginsWith(prefix) => {
                sort_value.key_bytes().starts_with(&prefix.key_bytes())
            }
        }
    }

    /// Whether a sort key's **raw, on-disk key bytes** (as recovered from a
    /// scanned storage key — everything after the escaped partition-key
    /// prefix) satisfy this condition.
    ///
    /// A raw sort-key byte string is exactly [`AttributeValue::key_bytes`]'s
    /// encoding (`storage_key`'s doc) — for `N` that is decimal *text*, not a
    /// numeric-order-preserving layout. A caller with only those bytes has no
    /// type tag to hand [`Self::matches`] directly; wrapping them as an
    /// opaque [`AttributeValue::B`] would compare by raw bytes even against
    /// an `N` operand (`sort_key_cmp`'s numeric arm only fires when **both**
    /// sides are literally the `N` variant), silently reproducing the exact
    /// byte-vs-numeric bug `matches` exists to fix (issue #373) — every
    /// production call site used to do exactly that. This reinterprets the
    /// raw bytes as the type this condition's own operand(s) declare (a real
    /// sort key's condition operands are always one consistent declared
    /// type) before delegating to [`Self::matches`], so a caller never has to
    /// reason about the distinction itself.
    #[must_use]
    pub fn matches_raw(&self, raw_sort_bytes: &[u8]) -> bool {
        let value = if self.is_numeric() {
            match std::str::from_utf8(raw_sort_bytes) {
                Ok(text) => AttributeValue::N(text.to_owned()),
                // Not valid UTF-8 text, so it cannot be the decimal text an
                // `N` is stored as — fall back to a raw compare rather than
                // panicking on data that disagrees with the condition's type.
                Err(_) => AttributeValue::B(raw_sort_bytes.to_vec()),
            }
        } else {
            AttributeValue::B(raw_sort_bytes.to_vec())
        };
        self.matches(&value)
    }

    /// Whether this condition's own operand(s) are `N` — see
    /// [`Self::matches_raw`]. `begins_with` is only ever meaningful for
    /// `S`/`B` sort keys in DynamoDB, so it is never numeric.
    #[must_use]
    fn is_numeric(&self) -> bool {
        let is_n = |v: &AttributeValue| matches!(v, AttributeValue::N(_));
        match self {
            SortKeyCondition::Compare(_, v) => is_n(v),
            SortKeyCondition::Between(lo, hi) => is_n(lo) || is_n(hi),
            SortKeyCondition::BeginsWith(_) => false,
        }
    }

    /// The DynamoDB `AttributeType` code(s) (`S`/`N`/`B`) of this condition's
    /// own operand(s) — for a caller with schema access (`animusd`) to
    /// validate against a declared sort-key type before the condition is
    /// ever evaluated, mirroring how a plain comparator's operand type would
    /// be checked against `AttributeDefinitions` in real DynamoDB.
    #[must_use]
    pub fn operand_type_codes(&self) -> Vec<&'static str> {
        match self {
            SortKeyCondition::Compare(_, v) => vec![type_code(v)],
            SortKeyCondition::Between(lo, hi) => vec![type_code(lo), type_code(hi)],
            SortKeyCondition::BeginsWith(v) => vec![type_code(v)],
        }
    }
}

/// Order two sort-key values the way [`SortKeyCondition::matches`] needs:
/// numerically for a pair of `N`s, by raw key bytes otherwise (`S`/`B`, and
/// any pair `compare_numeric` can't parse as numbers — sort keys are always
/// same-typed in practice, so this fallback is defensive, not a real path).
#[must_use]
fn sort_key_cmp(a: &AttributeValue, b: &AttributeValue) -> std::cmp::Ordering {
    if let (AttributeValue::N(x), AttributeValue::N(y)) = (a, b)
        && let Some(ord) = compare_numeric(x, y)
    {
        return ord;
    }
    a.key_bytes().cmp(&b.key_bytes())
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

/// Why a `ConditionExpression`/filter evaluation could not produce a
/// true/false answer. This is DynamoDB's real `ValidationException` for a
/// function (`size`/`begins_with`/`contains`) applied to an *existing*
/// attribute whose type is outside that function's operand domain — e.g.
/// `size()` on an `N`. It is deliberately distinct from an ordinary `false`
/// result (a missing attribute, or two comparable-but-unequal/mismatched
/// operands): AWS itself only raises this for the function's own operand,
/// never for a plain comparator's type mismatch. The message text mirrors
/// AWS's own wording so it reads believably once it reaches a wire client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConditionError {
    pub message: String,
}

impl ConditionError {
    #[must_use]
    fn invalid_operand_type(function: &str, actual: &AttributeValue) -> Self {
        Self {
            message: format!(
                "Incorrect operand type for operator or function; operator or function: {function}, operand type: {}",
                type_code(actual)
            ),
        }
    }
}

impl std::fmt::Display for ConditionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ConditionError {}

/// DynamoDB's `size()` of an attribute: bytes for `S`/`B`, element count for
/// the document and set types. `None` for `N`/`BOOL`/`NULL`, which have no
/// notion of size — the caller (only ever reached once the attribute is
/// known to *exist*) turns that into a [`ConditionError`], matching real
/// DynamoDB: `size()` on an existing attribute of one of those three types is
/// a runtime `ValidationException`, not a false comparison. A *missing*
/// attribute never reaches `size_of` at all — the caller's own `None` arm
/// keeps that case a plain `false`, exactly as before.
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
    /// `begins_with(attr, prefix)` — `S` and `B` only. An existing attribute
    /// of any other type is a `ValidationException`
    /// ([`ConditionError`]), matching real DynamoDB; a missing attribute is
    /// still `false`, and an `S`/`B` attribute compared against a
    /// mismatched-type `prefix` literal is still `false` too (that mismatch
    /// is between two supplied operands, not a domain violation of the
    /// attribute itself).
    BeginsWith(String, AttributeValue),
    /// `contains(attr, operand)` — substring for `S`, membership for the set
    /// and list types. An existing attribute of any other type (`N`, `B`,
    /// `BOOL`, `NULL`, `M`) is a `ValidationException` ([`ConditionError`]),
    /// matching real DynamoDB; a missing attribute is still `false`, and an
    /// operand of the wrong element type against an otherwise-valid
    /// container is still `false` too, the same supplied-operands-mismatch
    /// distinction `BeginsWith` draws.
    Contains(String, AttributeValue),
    /// `attribute_type(attr, :code)` — `"S"`, `"N"`, `"B"`, `"BOOL"`,
    /// `"NULL"`, `"M"`, `"L"`, `"SS"`, `"NS"`, `"BS"`.
    AttributeType(String, String),
    /// `size(attr) <op> value` — bytes for `S`/`B`, element count for the
    /// document and set types. An **existing** attribute of type `N`,
    /// `BOOL`, or `NULL` has no `size()` at all — real DynamoDB rejects the
    /// call with a `ValidationException`, surfaced here as
    /// `Err(`[`ConditionError`]`)` rather than a false comparison. A
    /// *missing* attribute is unaffected: it still evaluates to `false`,
    /// the same as every other leaf.
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
    /// no live item exists). A write proceeds only when this returns
    /// `Ok(true)`.
    ///
    /// # Errors
    /// `Err(ConditionError)` when a `size()`/`begins_with()`/`contains()` leaf
    /// is applied to an **existing** attribute whose type is outside that
    /// function's operand domain — the runtime `ValidationException` real
    /// DynamoDB raises there (see each variant's own doc). A missing
    /// attribute, or a plain comparator's type mismatch between two supplied
    /// operands, is unaffected and stays `Ok(false)`.
    pub fn evaluate(&self, current: Option<&Item>) -> Result<bool, ConditionError> {
        match self {
            ConditionExpression::AttributeNotExists(attr) => {
                Ok(current.is_none_or(|item| !item.contains_key(attr)))
            }
            ConditionExpression::AttributeExists(attr) => {
                Ok(current.is_some_and(|item| item.contains_key(attr)))
            }
            ConditionExpression::Compare(attr, op, value) => {
                let Some(actual) = current.and_then(|item| item.get(attr)) else {
                    // A missing attribute satisfies no comparison — `<>`
                    // included. DynamoDB has no three-valued logic here.
                    return Ok(false);
                };
                // Equality and inequality work for *every* type (two maps can
                // be equal); ordering only for the comparable scalars.
                Ok(match op {
                    // Equality works for every type (two maps can be equal) and
                    // must be numeric-aware; ordering only for the comparable
                    // scalars.
                    Comparator::Eq => values_equal(actual, value),
                    Comparator::Ne => !values_equal(actual, value),
                    _ => op.holds(compare_values(actual, value)),
                })
            }
            ConditionExpression::Between(attr, lo, hi) => {
                let Some(actual) = current.and_then(|item| item.get(attr)) else {
                    return Ok(false);
                };
                Ok(Comparator::Ge.holds(compare_values(actual, lo))
                    && Comparator::Le.holds(compare_values(actual, hi)))
            }
            ConditionExpression::In(attr, values) => Ok(current
                .and_then(|item| item.get(attr))
                .is_some_and(|actual| values.iter().any(|v| values_equal(actual, v)))),
            ConditionExpression::BeginsWith(attr, prefix) => {
                match current.and_then(|item| item.get(attr)) {
                    None => Ok(false),
                    Some(AttributeValue::S(v)) => Ok(match prefix {
                        AttributeValue::S(p) => v.starts_with(p),
                        // The literal argument's type doesn't match — a
                        // supplied-operands mismatch, not a domain violation
                        // of `attr` itself, so this stays `false`.
                        _ => false,
                    }),
                    Some(AttributeValue::B(v)) => Ok(match prefix {
                        AttributeValue::B(p) => v.starts_with(p),
                        _ => false,
                    }),
                    // `attr` itself is outside begins_with's S/B domain.
                    Some(actual) => {
                        Err(ConditionError::invalid_operand_type("begins_with", actual))
                    }
                }
            }
            ConditionExpression::Contains(attr, operand) => {
                match current.and_then(|item| item.get(attr)) {
                    None => Ok(false),
                    Some(AttributeValue::S(v)) => Ok(match operand {
                        AttributeValue::S(needle) => v.contains(needle.as_str()),
                        _ => false,
                    }),
                    Some(AttributeValue::SS(vs)) => Ok(match operand {
                        AttributeValue::S(needle) => vs.contains(needle),
                        _ => false,
                    }),
                    Some(AttributeValue::NS(vs)) => Ok(match operand {
                        AttributeValue::N(needle) => vs
                            .iter()
                            .any(|v| compare_numeric(v, needle) == Some(std::cmp::Ordering::Equal)),
                        _ => false,
                    }),
                    Some(AttributeValue::BS(vs)) => Ok(match operand {
                        AttributeValue::B(needle) => vs.contains(needle),
                        _ => false,
                    }),
                    Some(AttributeValue::L(items)) => {
                        Ok(items.iter().any(|i| values_equal(i, operand)))
                    }
                    // `attr` itself (N/B/BOOL/NULL/M) is outside contains's
                    // string/set/list domain.
                    Some(actual) => Err(ConditionError::invalid_operand_type("contains", actual)),
                }
            }
            ConditionExpression::AttributeType(attr, code) => Ok(current
                .and_then(|item| item.get(attr))
                .is_some_and(|actual| type_code(actual) == code)),
            ConditionExpression::Size(attr, op, value) => {
                let Some(actual) = current.and_then(|item| item.get(attr)) else {
                    return Ok(false);
                };
                let Some(size) = size_of(actual) else {
                    return Err(ConditionError::invalid_operand_type("size", actual));
                };
                // `size()` yields a number, so the comparison is numeric.
                Ok(op.holds(compare_values(&AttributeValue::N(size.to_string()), value)))
            }
            // Short-circuiting matches DynamoDB's own evaluation and, more
            // importantly, keeps every leaf's "false when absent" semantics
            // intact under composition: `NOT attribute_exists(a)` is true for
            // a missing `a` precisely because the leaf is false, not unknown.
            // `?` on each side means an operand-type error anywhere in the
            // tree still short-circuits `&&`/`||` exactly like a plain
            // boolean would — the right side simply never runs once the left
            // side has already settled (or failed) the outcome.
            ConditionExpression::And(a, b) => Ok(a.evaluate(current)? && b.evaluate(current)?),
            ConditionExpression::Or(a, b) => Ok(a.evaluate(current)? || b.evaluate(current)?),
            ConditionExpression::Not(inner) => Ok(!inner.evaluate(current)?),
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
        let cond = SortKeyCondition::Compare(Comparator::Eq, s("b"));
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

    /// Issue #373: `SortKeyCondition::matches` used to compare `N` sort keys
    /// by raw `key_bytes`, which orders decimal text lexicographically —
    /// `"9" > "15"` as text — so `sk BETWEEN 5 AND 15` wrongly excluded
    /// `sk = 9`. Numeric compare fixes exactly this.
    #[test]
    fn n_between_compares_numerically_not_lexicographically() {
        let cond = SortKeyCondition::Between(n("5"), n("15"));
        for hit in ["5", "9", "10", "15"] {
            assert!(cond.matches(&n(hit)), "{hit}");
        }
        for miss in ["4", "16", "2"] {
            assert!(!cond.matches(&n(miss)), "{miss}");
        }
    }

    #[test]
    fn n_between_with_negatives_and_decimals() {
        let cond = SortKeyCondition::Between(n("-10"), n("-2"));
        for hit in ["-10", "-9", "-2"] {
            assert!(cond.matches(&n(hit)), "{hit}");
        }
        for miss in ["-11", "-1", "0"] {
            assert!(!cond.matches(&n(miss)), "{miss}");
        }

        // A decimal midpoint that a byte compare would also place wrong: "9.5"
        // as text falls between "10" and "2" lexicographically at all.
        let cond = SortKeyCondition::Between(n("9"), n("10.5"));
        assert!(cond.matches(&n("9.5")));
        assert!(cond.matches(&n("10.5")), "inclusive upper bound");
        assert!(!cond.matches(&n("10.6")));
    }

    /// `N` equality is numeric too, so differently-written text for the same
    /// number still matches — consistent with `Between` rather than
    /// diverging on `Equals` alone (mirrors `values_equal`'s own contract).
    #[test]
    fn n_equals_is_numeric_not_textual() {
        let cond = SortKeyCondition::Compare(Comparator::Eq, n("1.10"));
        assert!(
            cond.matches(&n("1.1")),
            "trailing zero doesn't change value"
        );
        assert!(!cond.matches(&n("1.11")));

        let cond = SortKeyCondition::Compare(Comparator::Eq, n("0"));
        assert!(cond.matches(&n("-0")), "-0 and 0 are the same number");
    }

    /// `S`/`B` sort keys are unaffected by the `N` fix — they keep comparing
    /// by raw key bytes, which already matches DynamoDB for those types.
    #[test]
    fn s_and_b_sort_keys_still_compare_by_bytes() {
        let cond = SortKeyCondition::Between(s("b"), s("d"));
        for hit in ["b", "c", "d"] {
            assert!(cond.matches(&s(hit)), "{hit}");
        }
        for miss in ["a", "e"] {
            assert!(!cond.matches(&s(miss)), "{miss}");
        }
        assert!(
            !SortKeyCondition::Compare(Comparator::Eq, s("b")).matches(&s("B")),
            "byte-exact, case-sensitive"
        );

        let b = |v: &[u8]| AttributeValue::B(v.to_vec());
        let cond = SortKeyCondition::Between(b(&[1, 0]), b(&[3, 0]));
        assert!(cond.matches(&b(&[2, 0])));
        assert!(!cond.matches(&b(&[4, 0])));
        assert!(SortKeyCondition::Compare(Comparator::Eq, b(&[1, 2])).matches(&b(&[1, 2])));
        assert!(!SortKeyCondition::Compare(Comparator::Eq, b(&[1, 2])).matches(&b(&[1, 2, 0])));
    }

    /// Issue #373's follow-on: `<`/`<=`/`>`/`>=` go through the exact same
    /// [`SortKeyCondition::Compare`]/[`sort_key_cmp`] machinery `Equals`
    /// already did, so an operator × type matrix over `S`, `N` (mixed digit
    /// counts, negatives), and `B` is the right level to prove them at — a
    /// byte-lexicographic regression on any one operator would fail exactly
    /// one row of this table.
    #[test]
    fn range_operators_over_s_type_sort_keys() {
        let lt = |v: &str| SortKeyCondition::Compare(Comparator::Lt, s(v));
        let le = |v: &str| SortKeyCondition::Compare(Comparator::Le, s(v));
        let gt = |v: &str| SortKeyCondition::Compare(Comparator::Gt, s(v));
        let ge = |v: &str| SortKeyCondition::Compare(Comparator::Ge, s(v));

        assert!(lt("m").matches(&s("b")));
        assert!(!lt("m").matches(&s("m")));
        assert!(!lt("m").matches(&s("z")));

        assert!(le("m").matches(&s("b")));
        assert!(le("m").matches(&s("m")));
        assert!(!le("m").matches(&s("z")));

        assert!(!gt("m").matches(&s("b")));
        assert!(!gt("m").matches(&s("m")));
        assert!(gt("m").matches(&s("z")));

        assert!(!ge("m").matches(&s("b")));
        assert!(ge("m").matches(&s("m")));
        assert!(ge("m").matches(&s("z")));
    }

    #[test]
    fn range_operators_over_n_type_sort_keys_mixed_digit_counts_and_negatives() {
        let lt = |v: &str| SortKeyCondition::Compare(Comparator::Lt, n(v));
        let le = |v: &str| SortKeyCondition::Compare(Comparator::Le, n(v));
        let gt = |v: &str| SortKeyCondition::Compare(Comparator::Gt, n(v));
        let ge = |v: &str| SortKeyCondition::Compare(Comparator::Ge, n(v));

        // A byte-lexicographic compare would put "9" after "15" — the exact
        // shape issue #373 reported for BETWEEN, now checked for every
        // ordering operator too.
        assert!(gt("9").matches(&n("15")), "15 > 9 numerically");
        assert!(!lt("9").matches(&n("15")), "15 is not < 9");
        assert!(lt("15").matches(&n("9")), "9 < 15 numerically");
        assert!(!gt("15").matches(&n("9")), "9 is not > 15");

        assert!(le("10").matches(&n("10")), "<= is inclusive at equality");
        assert!(!lt("10").matches(&n("10")), "< is exclusive at equality");
        assert!(ge("10").matches(&n("10")), ">= is inclusive at equality");
        assert!(!gt("10").matches(&n("10")), "> is exclusive at equality");

        // Negatives and decimals.
        assert!(lt("-2").matches(&n("-10")), "-10 < -2");
        assert!(!lt("-10").matches(&n("-2")), "-2 is not < -10");
        assert!(gt("0.45").matches(&n("0.5")));
        assert!(!gt("0.5").matches(&n("0.45")));
    }

    #[test]
    fn range_operators_over_b_type_sort_keys() {
        let b = |v: &[u8]| AttributeValue::B(v.to_vec());
        let lt = |v: &[u8]| SortKeyCondition::Compare(Comparator::Lt, b(v));
        let gt = |v: &[u8]| SortKeyCondition::Compare(Comparator::Gt, b(v));

        assert!(lt(&[2, 0]).matches(&b(&[1, 0])));
        assert!(!lt(&[2, 0]).matches(&b(&[2, 0])));
        assert!(!lt(&[2, 0]).matches(&b(&[3, 0])));

        assert!(!gt(&[2, 0]).matches(&b(&[1, 0])));
        assert!(!gt(&[2, 0]).matches(&b(&[2, 0])));
        assert!(gt(&[2, 0]).matches(&b(&[3, 0])));
    }

    /// [`SortKeyCondition::matches_raw`] is the one production callers
    /// actually use (a scanned key's sort segment has no type tag) — this is
    /// the regression that would have caught the pre-existing gap where every
    /// call site wrapped raw bytes as `B` and silently lost the numeric
    /// compare for `N` sort keys, even after `matches` itself was fixed.
    #[test]
    fn matches_raw_reinterprets_bytes_by_the_conditions_own_declared_type() {
        let between = SortKeyCondition::Between(n("5"), n("15"));
        // "9" is only 1 byte vs "15"'s 2 — a raw `B` compare would place it
        // outside the range exactly like the original issue #373 bug.
        assert!(between.matches_raw(b"9"));
        assert!(between.matches_raw(b"5"));
        assert!(between.matches_raw(b"15"));
        assert!(!between.matches_raw(b"16"));
        assert!(!between.matches_raw(b"4"));

        let gt = SortKeyCondition::Compare(Comparator::Gt, n("9"));
        assert!(gt.matches_raw(b"15"), "15 > 9 numerically, not by bytes");
        assert!(!gt.matches_raw(b"2"));

        // S/B sort keys are unaffected — raw bytes already sort the way
        // DynamoDB compares them, so matches_raw and matches agree exactly.
        let s_between = SortKeyCondition::Between(s("b"), s("d"));
        assert!(s_between.matches_raw(b"c"));
        assert!(!s_between.matches_raw(b"e"));
    }

    #[test]
    fn operand_type_codes_reports_the_conditions_own_declared_type() {
        assert_eq!(
            SortKeyCondition::Compare(Comparator::Gt, n("1")).operand_type_codes(),
            vec!["N"]
        );
        assert_eq!(
            SortKeyCondition::Between(s("a"), n("1")).operand_type_codes(),
            vec!["S", "N"],
            "mixed-type operands are reported verbatim, not unified"
        );
        assert_eq!(
            SortKeyCondition::BeginsWith(AttributeValue::B(vec![1])).operand_type_codes(),
            vec!["B"]
        );
    }

    #[test]
    fn attribute_not_exists() {
        let cond = ConditionExpression::AttributeNotExists("pk".into());
        assert!(cond.evaluate(None).unwrap());
        let mut item = Item::new();
        item.insert("other".into(), s("x"));
        assert!(cond.evaluate(Some(&item)).unwrap());
        item.insert("pk".into(), s("k"));
        assert!(!cond.evaluate(Some(&item)).unwrap());
    }

    #[test]
    fn attribute_exists_and_equals() {
        let mut item = Item::new();
        item.insert("pk".into(), s("k"));
        item.insert("v".into(), AttributeValue::N("1".into()));
        assert!(
            ConditionExpression::AttributeExists("pk".into())
                .evaluate(Some(&item))
                .unwrap()
        );
        assert!(
            !ConditionExpression::AttributeExists("pk".into())
                .evaluate(None)
                .unwrap()
        );
        assert!(
            ConditionExpression::Compare("v".into(), Comparator::Eq, AttributeValue::N("1".into()))
                .evaluate(Some(&item))
                .unwrap()
        );
        assert!(
            !ConditionExpression::Compare(
                "v".into(),
                Comparator::Eq,
                AttributeValue::N("2".into())
            )
            .evaluate(Some(&item))
            .unwrap()
        );
        assert!(
            !ConditionExpression::Compare(
                "v".into(),
                Comparator::Eq,
                AttributeValue::N("1".into())
            )
            .evaluate(None)
            .unwrap()
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
                .unwrap()
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
                .unwrap()
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
                !ConditionExpression::Compare("v".into(), op, n("1"))
                    .evaluate(Some(&item))
                    .unwrap(),
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
            !ConditionExpression::Compare("v".into(), Comparator::Gt, n("1"))
                .evaluate(Some(&item))
                .unwrap()
        );
        assert!(
            ConditionExpression::Compare("v".into(), Comparator::Ne, n("1"))
                .evaluate(Some(&item))
                .unwrap(),
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
        let ev = |c: ConditionExpression| c.evaluate(Some(&item)).unwrap();

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
        let ev = |c: ConditionExpression| c.evaluate(Some(&item)).unwrap();

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
    }

    /// `size()` on an **existing** `N`/`BOOL`/`NULL` attribute is a real
    /// DynamoDB `ValidationException`, not a false comparison — the fidelity
    /// gap this module used to have (flagged in review of the commit that
    /// introduced `size()`, fe0ce0c). A *missing* attribute is unaffected: it
    /// still evaluates to `false`, exactly like every other leaf.
    #[test]
    fn size_of_an_existing_n_bool_or_null_attribute_is_a_validation_error() {
        let item = item_of(&[
            ("num", n("42")),
            ("flag", AttributeValue::Bool(true)),
            ("nothing", AttributeValue::Null),
        ]);
        for (attr, type_code) in [("num", "N"), ("flag", "BOOL"), ("nothing", "NULL")] {
            let err = ConditionExpression::Size(attr.into(), Comparator::Gt, n("0"))
                .evaluate(Some(&item))
                .expect_err("size() on an existing N/BOOL/NULL attribute must error");
            assert_eq!(
                err.message,
                format!(
                    "Incorrect operand type for operator or function; operator or function: size, operand type: {type_code}"
                ),
                "message should match AWS's own ValidationException wording for {attr}"
            );
        }

        // A *missing* attribute is a different case entirely — still false,
        // never an error, same as before this fix.
        assert!(
            !ConditionExpression::Size("missing".into(), Comparator::Gt, n("0"))
                .evaluate(Some(&item))
                .unwrap(),
            "a missing attribute has no ValidationException — it's just false"
        );
        assert!(
            !ConditionExpression::Size("num".into(), Comparator::Gt, n("0"))
                .evaluate(None)
                .unwrap(),
            "no item at all is also just false, not an error"
        );
    }

    /// `begins_with()`/`contains()` on an **existing** attribute of a type
    /// outside their own operand domain are the same class of
    /// `ValidationException` as `size()` — found while auditing for the same
    /// bug shape. A missing attribute, and a same-domain attribute compared
    /// against a mismatched-type *literal*, both stay `false` (a supplied-
    /// operands mismatch, not a domain violation of the attribute itself).
    #[test]
    fn begins_with_and_contains_on_a_wrong_typed_existing_attribute_error_too() {
        let item = item_of(&[
            ("num", n("42")),
            ("flag", AttributeValue::Bool(true)),
            ("map", AttributeValue::M(item_of(&[("k", s("v"))]))),
            ("name", s("hello")),
        ]);

        let err = ConditionExpression::BeginsWith("num".into(), s("4"))
            .evaluate(Some(&item))
            .expect_err("begins_with on an existing N attribute must error");
        assert_eq!(
            err.message,
            "Incorrect operand type for operator or function; operator or function: begins_with, operand type: N"
        );
        assert!(
            !ConditionExpression::BeginsWith("missing".into(), s("4"))
                .evaluate(Some(&item))
                .unwrap(),
            "a missing attribute is still false"
        );
        assert!(
            !ConditionExpression::BeginsWith("name".into(), AttributeValue::N("1".into()))
                .evaluate(Some(&item))
                .unwrap(),
            "an S attribute against a mismatched-type literal is a supplied-operands \
             mismatch, still false, not a domain violation"
        );

        for (attr, type_code) in [("flag", "BOOL"), ("map", "M")] {
            let err = ConditionExpression::Contains(attr.into(), s("x"))
                .evaluate(Some(&item))
                .expect_err("contains on an existing BOOL/M attribute must error");
            assert_eq!(
                err.message,
                format!(
                    "Incorrect operand type for operator or function; operator or function: contains, operand type: {type_code}"
                )
            );
        }
        assert!(
            !ConditionExpression::Contains("missing".into(), s("x"))
                .evaluate(Some(&item))
                .unwrap(),
            "a missing attribute is still false"
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
        let ev = |c: ConditionExpression| c.evaluate(Some(&item)).unwrap();

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
            .unwrap()
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
            .evaluate(Some(&other))
            .unwrap(),
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
            .evaluate(Some(&other))
            .unwrap(),
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
            .evaluate(Some(&diverge))
            .unwrap(),
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
            .evaluate(Some(&diverge))
            .unwrap(),
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

/// Differential proptest for the decimal bignum ops (`decimal_parts`,
/// `add_digits`, `sub_digits`, and the `compare_numeric`/`add_numeric`
/// callers built on them) against [`bigdecimal::BigDecimal`], an
/// arbitrary-precision reference — a dev-only dependency, never a
/// production one.
///
/// **Scope.** DynamoDB's documented `N` contract is up to 38 significant
/// digits and a magnitude range of roughly ±1.0×10^126 (AWS's own docs).
/// This crate's implementation is a plain decimal-string bignum with **no
/// exponent notation and no digit-count cap of its own** — it accepts
/// arbitrarily long digit strings and is exact for all of them, so it is
/// already a superset of AWS's precision guarantee rather than a narrower
/// one. `BigDecimal` is likewise arbitrary-precision, so nothing here needs
/// to be scoped down to fit the reference — the generated strings are
/// bounded to 38 significant digits (matching the documented DynamoDB
/// contract this code exists to serve) purely to keep the generated inputs
/// realistic, not because either side would lose precision beyond that.
#[cfg(test)]
mod decimal_differential_tests {
    use super::*;
    use bigdecimal::{BigDecimal, Zero};
    use proptest::prelude::*;
    use std::cmp::Ordering;
    use std::str::FromStr;

    /// A DynamoDB-`N`-shaped decimal string: an optional sign, 1-38 integer
    /// digits, and an optional 0-20-digit fraction — plain decimal text, no
    /// exponent notation (this implementation doesn't parse it, and neither
    /// does DynamoDB's own `N` wire encoding). Includes leading zeros and
    /// `-0` on purpose: both are inputs `decimal_parts` must still normalize
    /// correctly, not just the canonical forms an SDK would send.
    fn decimal_string() -> impl Strategy<Value = String> {
        "-?[0-9]{1,38}(\\.[0-9]{0,20})?"
    }

    fn reference(v: &str) -> BigDecimal {
        BigDecimal::from_str(v).unwrap_or_else(|e| panic!("reference parse of {v:?}: {e}"))
    }

    proptest! {
        /// `compare_numeric` agrees with the reference ordering over every
        /// pair of in-contract decimal strings, including equal-value pairs
        /// written differently (`"1.10"` vs `"1.1"`, `"-0"` vs `"0"`).
        #[test]
        fn compare_numeric_matches_reference(a in decimal_string(), b in decimal_string()) {
            let want = reference(&a).cmp(&reference(&b));
            let got = compare_numeric(&a, &b).expect("both sides are in-contract decimals");
            prop_assert_eq!(got, want, "compare_numeric({:?}, {:?})", a, b);
        }

        /// `add_numeric` agrees with the reference sum's *value* (not its
        /// textual form — `BigDecimal`'s `PartialEq` is scale-normalizing,
        /// so `4` and `4.00` compare equal, matching `add_numeric`'s own
        /// trailing-zero normalization).
        #[test]
        fn add_numeric_matches_reference(a in decimal_string(), b in decimal_string()) {
            let want = reference(&a) + reference(&b);
            let sum = add_numeric(&a, &b).expect("both sides are in-contract decimals");
            let got = reference(&sum);
            prop_assert_eq!(got, want, "add_numeric({:?}, {:?}) = {:?}", a, b, sum);
        }

        /// `ADD` with a negated delta is DynamoDB's only subtraction path
        /// (there is no standalone `sub_numeric`), and internally exercises
        /// `sub_digits` whenever the two operands' signs differ. Check it
        /// against the reference difference directly.
        #[test]
        fn add_numeric_of_a_negated_operand_matches_reference_subtraction(
            a in decimal_string(),
            b in decimal_string(),
        ) {
            let negated_b = negate(&b);
            let want = reference(&a) - reference(&b);
            let diff = add_numeric(&a, &negated_b).expect("both sides are in-contract decimals");
            let got = reference(&diff);
            prop_assert_eq!(got, want, "add_numeric({:?}, {:?}) = {:?}", a, negated_b, diff);
        }

        /// `add_numeric` never produces a `-0`: DynamoDB counters must not
        /// surface a negative zero, and the reference's `is_zero()` is the
        /// value-level way to state "the result is exactly zero."
        #[test]
        fn add_numeric_normalizes_true_zero_without_a_minus_sign(
            a in decimal_string(),
        ) {
            let sum = add_numeric(&a, &negate(&a)).expect("a negated in-contract decimal");
            prop_assert!(
                reference(&sum).is_zero(),
                "a + (-a) must be exactly zero, got {:?}",
                sum
            );
            prop_assert!(
                !sum.starts_with('-'),
                "zero must never carry a sign: {:?}",
                sum
            );
        }

        /// `decimal_parts` round-trips through the reference: the sign,
        /// integer, and fractional digit strings it extracts recombine into
        /// exactly the value the reference parsed from the same input.
        #[test]
        fn decimal_parts_recombines_to_the_reference_value(v in decimal_string()) {
            let (neg, int, frac) = decimal_parts(&v).expect("in-contract decimal");
            let recombined = format!(
                "{}{}{}{}",
                if neg { "-" } else { "" },
                if int.is_empty() { "0" } else { int.as_str() },
                if frac.is_empty() { "" } else { "." },
                frac,
            );
            prop_assert_eq!(reference(&recombined), reference(&v), "decimal_parts({:?})", v);
        }
    }

    /// Flip a decimal string's sign textually (used to drive `add_numeric`
    /// down its subtraction path) — not a claim under test itself.
    fn negate(v: &str) -> String {
        match v.strip_prefix('-') {
            Some(rest) => rest.to_string(),
            None => format!("-{v}"),
        }
    }

    /// Sanity check on the ordering claim `compare_numeric_matches_reference`
    /// relies on being meaningful: distinct textual forms of the same value
    /// really do compare equal against the reference (otherwise the
    /// property above would be vacuous for exactly the cases that matter
    /// most — trailing zeros and `-0`).
    #[test]
    fn reference_treats_differently_written_equal_values_as_equal() {
        assert_eq!(reference("1.10").cmp(&reference("1.1")), Ordering::Equal);
        assert_eq!(reference("-0").cmp(&reference("0")), Ordering::Equal);
        assert_eq!(reference("00042").cmp(&reference("42")), Ordering::Equal);
    }
}
