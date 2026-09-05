//! Order-preserving byte encoding for DynamoDB `N` (number) values (roadmap
//! W-03, ADR 0063). **Wired** (W-03 step 3):
//! [`AttributeValue::key_bytes`](crate::AttributeValue::key_bytes) encodes
//! `N` through [`encode`], and `SortKeyCondition::matches_raw`
//! (`condition.rs`) decodes stored bytes through [`decode`] — see both
//! methods' own docs. `animus-tablet` was found to have no `N`-specific
//! encoding of its own to mirror (ADR 0063's Scope section) — this codec is
//! applied entirely inside this crate, before a value is ever handed to
//! `animus-tablet`'s `escape`/`partition_token`.
//!

//! ## Design
//!
//! Every DynamoDB number canonicalises to a **sign**, a **decimal
//! exponent**, and a **digit run** with no leading or trailing zeros:
//!
//! ```text
//! value = sign * 0.d1 d2 d3 … dn * 10^exp        (d1 != 0, dn != 0)
//! ```
//!
//! i.e. `exp` is the position of the decimal point relative to the first
//! significant digit — `1` is `0.1 × 10^1` (`exp = 1`), `0.05` is
//! `0.5 × 10^-1` (`exp = -1`), `123` is `0.123 × 10^3` (`exp = 3`), and
//! `1230` is `0.123 × 10^4` (`exp = 4`, distinguishing it from `123` even
//! though the two share a digit run). Zero has no sign, exponent, or digit
//! run of its own — every spelling of it (`0`, `-0`, `0.000`, `0e5`, …)
//! canonicalises to the single distinguished zero class.
//!
//! ## Layout
//!
//! ```text
//! byte 0:     0x02 negative | 0x03 zero | 0x04 positive
//! (zero stops here — [0x03] is the entire encoding)
//! bytes 1..3: exp + BIAS, 2-byte big-endian offset-binary
//! bytes 3..:  one byte per digit, value `digit + 1` (1..=10), then a 0x00
//!             terminator
//! ```
//!
//! `0x00` and `0x01` are deliberately never used for byte 0 (unlike the
//! `0x02..=0x04` class bytes) so an encoded number can never collide with
//! `animus_tablet::escape`'s own reserved bytes (`0x00` doubled to
//! `0x00 0x01`, terminated by `0x00 0x00`) at the point this codec is
//! eventually spliced into a composite key.
//!
//! **Why the terminator is required.** Without it, one digit run could be a
//! byte-string *prefix* of another's — `12` (as bytes `[…, 2, 3, 0x00]`)
//! would, without the trailing `0x00`, be a strict prefix of `123` (as
//! bytes `[…, 2, 3, 4]`), and a prefix relationship between two encodings
//! that are meant to be totally ordered, comparable siblings is exactly
//! what would let one of them silently swallow every key that extends it in
//! a range scan. The terminator byte is chosen (`0x00`) to sort *before*
//! every digit byte (`1..=10`), so a shorter digit run — which represents a
//! value with implied trailing zeros relative to a longer run sharing its
//! prefix (`1` vs `12`, both `exp = 1` vs `exp = 2`… but even within one
//! `exp` class, e.g. `100` vs `123`, both digit runs starting `1`) — always
//! sorts before the longer one, matching numeric order.
//!
//! **Why it still works inverted.** For negatives, every byte after byte 0
//! (the exponent bytes, every digit byte, and the terminator) is bitwise
//! inverted (`!b`). Bitwise NOT is order-reversing over fixed-width
//! unsigned integers (`!a > !b` iff `a < b`), so inverting turns "smaller
//! magnitude sorts first" into "larger magnitude sorts first" — exactly
//! what negative numbers need (`-123 < -12`, the more negative value having
//! the larger magnitude). The terminator inverts to `0xFF`, which is larger
//! than every inverted digit byte (`!1..=!10` is `0xF5..=0xFE`), so the
//! same prefix argument still holds in the inverted, magnitude-reversed
//! order: a shorter run (smaller magnitude, so it should sort *last* among
//! negatives sharing a prefix) now terminates in the byte that sorts
//! *after* every continuation, matching `-100 > -123`.
//!
//! ## Range
//!
//! `BIAS` (1024) comfortably covers DynamoDB's documented `N` range
//! (roughly `1E-130` to `9.9999999999999999999999999999999999999E+125`,
//! i.e. `exp` in roughly `-129..=126`) inside an unsigned 16-bit
//! offset-binary field (`-1024..=64511`). [`encode`] returns `None` rather
//! than panicking if a caller somehow presents an exponent outside that
//! representable window.

/// Offset applied to the decimal exponent before it is written as an
/// unsigned 16-bit big-endian integer. See the module doc's "Range" section.
const BIAS: i64 = 1024;

const CLASS_NEGATIVE: u8 = 0x02;
const CLASS_ZERO: u8 = 0x03;
const CLASS_POSITIVE: u8 = 0x04;

const TERMINATOR: u8 = 0x00;

/// `(negative, exp, digits)` for a nonzero canonical value, or the zero
/// sentinel (`digits` empty) for anything that canonicalises to zero.
/// `digits` holds one `0..=9` value per significant digit, with no leading
/// or trailing zero digit. `None` if `n` is not a valid decimal.
fn canonicalize(n: &str) -> Option<(bool, i64, Vec<u8>)> {
    let s = n.trim();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return None;
    }

    let mut i = 0usize;
    let neg = match bytes.first() {
        Some(b'-') => {
            i += 1;
            true
        }
        Some(b'+') => {
            i += 1;
            false
        }
        _ => false,
    };

    let mantissa_start = i;
    let mut int_digit_count = 0i64;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
        int_digit_count += 1;
    }
    let mut frac_digit_count = 0i64;
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
            frac_digit_count += 1;
        }
    }
    if int_digit_count == 0 && frac_digit_count == 0 {
        // No digits at all: neither `""`, `"-"`, `"."`, `"-."` nor
        // `"e5"` (an exponent with no mantissa) is a valid decimal.
        return None;
    }
    let mantissa_end = i;
    let mantissa: String = s[mantissa_start..mantissa_end]
        .bytes()
        .filter(|&b| b != b'.')
        .map(|b| b as char)
        .collect();

    let mut exp_shift: i64 = 0;
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        i += 1;
        let exp_neg = match bytes.get(i) {
            Some(b'-') => {
                i += 1;
                true
            }
            Some(b'+') => {
                i += 1;
                false
            }
            _ => false,
        };
        let exp_digits_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_digits_start {
            // `e`/`E` with no exponent digits.
            return None;
        }
        let exp_text = &s[exp_digits_start..i];
        let magnitude: i64 = exp_text.parse().ok()?;
        exp_shift = if exp_neg { -magnitude } else { magnitude };
    }

    if i != bytes.len() {
        // Trailing garbage after the mantissa/exponent.
        return None;
    }

    let digit_bytes = mantissa.as_bytes();
    let mut point_pos = int_digit_count.checked_add(exp_shift)?;

    let mut start = 0usize;
    while start < digit_bytes.len() && digit_bytes[start] == b'0' {
        start += 1;
        point_pos = point_pos.checked_sub(1)?;
    }
    let mut end = digit_bytes.len();
    while end > start && digit_bytes[end - 1] == b'0' {
        end -= 1;
    }

    if start == end {
        // Every digit was zero: the distinguished zero value.
        return Some((false, 0, Vec::new()));
    }

    let digits: Vec<u8> = digit_bytes[start..end].iter().map(|b| b - b'0').collect();
    Some((neg, point_pos, digits))
}

/// Encode a DynamoDB `N` value's decimal text into an order-preserving byte
/// string: `bytewise_cmp(encode(a), encode(b)) == numeric_cmp(a, b)` for
/// every pair of valid decimals `a`, `b`. `None` if `n` is not a valid
/// decimal, or if its canonical exponent falls outside the range this
/// encoding can represent (see the module doc's "Range" section — no valid
/// DynamoDB `N` does).
///
/// See the module doc for the exact byte layout.
#[must_use]
pub fn encode(n: &str) -> Option<Vec<u8>> {
    let (neg, exp, digits) = canonicalize(n)?;

    if digits.is_empty() {
        return Some(vec![CLASS_ZERO]);
    }

    let biased = exp.checked_add(BIAS)?;
    let biased = u16::try_from(biased).ok()?;

    let mut out = Vec::with_capacity(1 + 2 + digits.len() + 1);
    out.push(if neg { CLASS_NEGATIVE } else { CLASS_POSITIVE });
    out.extend_from_slice(&biased.to_be_bytes());
    for &d in &digits {
        out.push(d + 1);
    }
    out.push(TERMINATOR);

    if neg {
        for b in out.iter_mut().skip(1) {
            *b = !*b;
        }
    }

    Some(out)
}

/// Render a canonical `(negative, exp, digits)` triple back to decimal text
/// that [`super::condition`]'s `decimal_parts`/`compare_numeric` (and
/// `bigdecimal::BigDecimal::from_str`) both accept: plain fixed-point text,
/// no exponent notation, sign only when negative.
fn render(neg: bool, exp: i64, digits: &[u8]) -> String {
    let digit_str: String = digits.iter().map(|d| (b'0' + d) as char).collect();
    let sign = if neg { "-" } else { "" };
    let len = digits.len() as i64;

    if exp <= 0 {
        let zeros = "0".repeat((-exp) as usize);
        format!("{sign}0.{zeros}{digit_str}")
    } else if exp >= len {
        let zeros = "0".repeat((exp - len) as usize);
        format!("{sign}{digit_str}{zeros}")
    } else {
        let split = exp as usize;
        format!("{sign}{}.{}", &digit_str[..split], &digit_str[split..])
    }
}

/// Decode an [`encode`]d byte string back to canonical decimal text (plain
/// fixed-point, e.g. `-123.45` — no exponent notation; see [`render`]).
/// `None` if `bytes` is not a well-formed encoding: an unrecognised class
/// byte, a truncated exponent field, a missing or misplaced terminator, a
/// digit byte outside `1..=10`, or trailing bytes after the terminator.
#[must_use]
pub fn decode(bytes: &[u8]) -> Option<String> {
    let class = *bytes.first()?;
    match class {
        CLASS_ZERO => {
            if bytes.len() == 1 {
                Some("0".to_string())
            } else {
                None
            }
        }
        CLASS_NEGATIVE | CLASS_POSITIVE => {
            let neg = class == CLASS_NEGATIVE;
            let rest = &bytes[1..];
            if rest.len() < 3 {
                // 2 exponent bytes + at least a terminator.
                return None;
            }

            let invert = |b: u8| if neg { !b } else { b };

            let exp_hi = invert(rest[0]);
            let exp_lo = invert(rest[1]);
            let biased = u16::from_be_bytes([exp_hi, exp_lo]);
            let exp = i64::from(biased) - BIAS;

            let digit_field = &rest[2..];
            let term_pos = digit_field.iter().position(|&b| invert(b) == TERMINATOR)?;
            if term_pos != digit_field.len() - 1 {
                // Bytes remain after the terminator.
                return None;
            }
            if term_pos == 0 {
                // No digits: zero must use the dedicated CLASS_ZERO byte.
                return None;
            }

            let mut digits = Vec::with_capacity(term_pos);
            for &raw in &digit_field[..term_pos] {
                let b = invert(raw);
                if !(1..=10).contains(&b) {
                    return None;
                }
                digits.push(b - 1);
            }

            Some(render(neg, exp, &digits))
        }
        _ => None,
    }
}

/// Cheap structural check that `bytes` is a well-formed [`encode`] output —
/// equivalent to `decode(bytes).is_some()`, exposed separately so a caller
/// (a later step's `matches_raw`) can ask "is this an encoded number" without
/// caring about the decoded text.
#[must_use]
pub fn is_encoded(bytes: &[u8]) -> bool {
    decode(bytes).is_some()
}

/// Numeric ordering of two decimal strings, purely as a comparison of their
/// [`encode`]d forms — a convenience for tests that want "numeric compare"
/// without threading bytes through by hand.
#[cfg(test)]
fn encoded_cmp(a: &str, b: &str) -> Option<std::cmp::Ordering> {
    Some(encode(a)?.cmp(&encode(b)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn zero_forms_encode_identically() {
        let z = encode("0").unwrap();
        assert_eq!(z, vec![CLASS_ZERO]);
        for form in ["0", "-0", "0.000", "0e5", "-0.0E-3", "+0"] {
            assert_eq!(encode(form).unwrap(), z, "form {form:?}");
        }
    }

    #[test]
    fn zero_decodes_to_canonical_zero() {
        assert_eq!(decode(&[CLASS_ZERO]).unwrap(), "0");
    }

    #[test]
    fn ordering_regressions() {
        // The exact bugs this codec fixes relative to lexicographic text
        // ordering (issue #373's byte-compare failure mode, generalised).
        let cases: &[(&str, &str)] = &[
            ("9", "15"),
            ("-10", "-5"),
            ("-5", "0"),
            ("0", "5"),
            ("0.5", "1"),
            ("1E10", "1E11"),
            ("123", "1230"),
            ("12", "123"),
            ("-123", "-12"),
        ];
        for &(lo, hi) in cases {
            assert_eq!(
                encoded_cmp(lo, hi),
                Some(Ordering::Less),
                "{lo:?} should encode less than {hi:?}"
            );
        }
    }

    #[test]
    fn round_trip_examples() {
        let cases = [
            "0",
            "-0",
            "1",
            "-1",
            "0.5",
            "-0.5",
            "0.05",
            "123",
            "1230",
            "12",
            "-123",
            "1E10",
            "1E11",
            "1.23E40",
            "-9.9999999999999999999999999999999999999E+125",
            "1E-129",
        ];
        for n in cases {
            let encoded = encode(n).unwrap_or_else(|| panic!("encode({n:?}) should succeed"));
            let decoded = decode(&encoded).unwrap_or_else(|| panic!("decode of {n:?} failed"));
            let re_encoded = encode(&decoded).unwrap();
            assert_eq!(
                re_encoded, encoded,
                "encode(decode(encode({n:?}))) != encode({n:?}); decoded = {decoded:?}"
            );
        }
    }

    #[test]
    fn decode_rejects_malformed_input() {
        assert_eq!(decode(&[]), None, "empty");
        assert_eq!(decode(&[0x01]), None, "unrecognised class byte");
        assert_eq!(decode(&[0x05]), None, "unrecognised class byte");
        assert_eq!(
            decode(&[CLASS_ZERO, 0x00]),
            None,
            "trailing garbage after zero"
        );
        assert_eq!(
            decode(&[CLASS_POSITIVE]),
            None,
            "truncated: no exponent bytes"
        );
        assert_eq!(
            decode(&[CLASS_POSITIVE, 0x04, 0x00]),
            None,
            "truncated: exponent only, no terminator"
        );
        assert_eq!(
            decode(&[CLASS_POSITIVE, 0x04, 0x00, 0x00]),
            None,
            "no digits before terminator (zero must use CLASS_ZERO)"
        );
        assert_eq!(
            decode(&[CLASS_POSITIVE, 0x04, 0x00, 0x0B, 0x00]),
            None,
            "digit byte out of range (0x0B > 10)"
        );
        assert_eq!(
            decode(&[CLASS_POSITIVE, 0x04, 0x00, 0x02, 0x00, 0xFF]),
            None,
            "trailing garbage after terminator"
        );
        // A valid positive encoding with the terminator missing entirely.
        assert_eq!(
            decode(&[CLASS_POSITIVE, 0x04, 0x00, 0x02, 0x03]),
            None,
            "missing terminator"
        );
    }

    #[test]
    fn class_byte_is_at_least_0x02_and_the_digit_run_terminates_exactly_once() {
        // Byte 0 (the class byte) is always >= 0x02, distinct from
        // `animus_tablet::escape`'s reserved 0x00/0x01 (see the module doc's
        // "Layout" section). Within the digit-run region specifically — the
        // 2-byte exponent field is an arbitrary offset-binary integer and
        // may legitimately contain a 0x00 byte, e.g. `encode("0.5")`'s
        // exponent is 0, biased to 0x0400 — every byte decodes to a digit
        // (`1..=10`, positive) or its inversion (negative) except the
        // final byte, which is the terminator (`0x00` positive, `0xFF`
        // negative) and appears exactly once, at the end.
        let samples = [
            "0",
            "-0",
            "1",
            "-1",
            "0.5",
            "-0.5",
            "9",
            "15",
            "-9",
            "-15",
            "123",
            "1230",
            "1E100",
            "-1E-100",
            "9.9999999999999999999999999999999999999E+125",
        ];
        for n in samples {
            let encoded = encode(n).unwrap();
            assert!(
                encoded[0] >= 0x02,
                "encode({n:?})[0] = {:#04x} should be >= 0x02",
                encoded[0]
            );
            if encoded == vec![CLASS_ZERO] {
                continue;
            }
            let neg = encoded[0] == CLASS_NEGATIVE;
            let terminator = if neg { 0xFF } else { 0x00 };
            let invert = |b: u8| if neg { !b } else { b };

            let digit_field = &encoded[3..];
            let term_positions: Vec<usize> = digit_field
                .iter()
                .enumerate()
                .filter(|&(_, &b)| b == terminator)
                .map(|(idx, _)| idx)
                .collect();
            assert_eq!(
                term_positions,
                vec![digit_field.len() - 1],
                "encode({n:?})'s digit run should contain exactly one terminator, at the end"
            );
            for &b in &digit_field[..digit_field.len() - 1] {
                let d = invert(b);
                assert!(
                    (1..=10).contains(&d),
                    "encode({n:?}) has a digit byte out of range: {b:#04x}"
                );
            }
        }
    }

    #[test]
    fn is_encoded_matches_encode_output_and_rejects_raw_text() {
        for n in [
            "0", "-0", "1", "-1", "0.5", "9", "15", "123", "1230", "-123", "1E10",
        ] {
            let encoded = encode(n).unwrap();
            assert!(
                is_encoded(&encoded),
                "is_encoded should accept encode({n:?})"
            );
        }
        assert!(
            !is_encoded(b"15"),
            "raw decimal text is not an encoded number"
        );
        assert!(!is_encoded(b""), "empty input is not an encoded number");
        assert!(
            !is_encoded(b"hello"),
            "arbitrary bytes are not an encoded number"
        );
    }

    #[test]
    fn encode_rejects_invalid_decimal_text() {
        for bad in [
            "", "-", ".", "-.", "e5", "1e", "1.2.3", "1-2", "abc", "1_000", " ", "1 2",
        ] {
            assert!(encode(bad).is_none(), "encode({bad:?}) should be None");
        }
    }

    #[test]
    fn encode_accepts_the_shapes_decimal_parts_accepts() {
        // Whitespace-trimmed, leading '+', and a trailing decimal point with
        // no fractional digits (`"42."`, integer digits present, fraction
        // empty) all parse successfully under `condition::decimal_parts`'s
        // own contract (its rejection is only "int empty AND frac empty");
        // this codec stays consistent with that.
        assert!(encode(" 42 ").is_some());
        assert!(encode("+42").is_some());
        assert_eq!(encode("42.").unwrap(), encode("42").unwrap());
    }
}

/// Differential proptest for [`encode`]/[`decode`] against
/// [`bigdecimal::BigDecimal`], an arbitrary-precision reference — a
/// dev-only dependency, never a production one. Mirrors the pattern
/// `condition.rs`'s own `decimal_differential_tests` established (ADR 0061
/// rung A5), reusing its `decimal_string()` shape as this module's
/// baseline generator and adding the shapes unique to this codec: exponent
/// notation (which `condition.rs`'s plain-text bignum deliberately doesn't
/// parse, but a DynamoDB `N` may carry, and this key encoding must),
/// deep leading-zero runs, and a handful of exact values that motivate
/// W-03 in the first place (`-0`, `0.0e3`, and adjacent prefix digit runs
/// like `12`/`123`/`1230`, whose *lexicographic* order disagrees with
/// their numeric order — the bug this module exists to fix at the byte
/// level).
///
/// **Range.** The exponent-notation generator is bounded to `-300..=300`
/// (comfortably inside DynamoDB's documented `N` range of roughly
/// `±1E-130..±1E126`, and inside the codec's own representable window —
/// see the module doc's "Range" section) so every generated string is
/// in-contract: [`encode`] is expected to succeed for all of them, never
/// `None`.
#[cfg(test)]
mod differential_tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use proptest::prelude::*;
    use std::str::FromStr;

    /// The exact shape `condition.rs::decimal_differential_tests` uses:
    /// an optional sign, 1-38 integer digits, and an optional 0-20-digit
    /// fraction — plain decimal text, no exponent notation.
    fn decimal_string() -> impl Strategy<Value = String> {
        "-?[0-9]{1,38}(\\.[0-9]{0,20})?"
    }

    /// A fixed-length 38-significant-digit run (DynamoDB's documented
    /// maximum), with a sign but no fraction — the "large digit run" shape
    /// `decimal_string()` only reaches by chance.
    fn digit38_run() -> impl Strategy<Value = String> {
        "-?[0-9]{38}"
    }

    /// `decimal_string()`, but with a deep, deliberate leading-zero run
    /// before the significant digits — exercises the exponent-adjusting
    /// leading-zero-stripping step of [`canonicalize`] much harder than
    /// `decimal_string()`'s own 1-38 unconstrained digits usually do.
    fn leading_zero_string() -> impl Strategy<Value = String> {
        "-?0{1,15}[0-9]{1,20}(\\.[0-9]{0,15})?"
    }

    /// A `decimal_string()` mantissa with an explicit `e`/`E` exponent —
    /// the notation `decimal_parts` (`condition.rs`) does not accept but
    /// this codec must, since it is legal DynamoDB `N` text.
    fn exponent_notation_string() -> impl Strategy<Value = String> {
        (decimal_string(), any::<bool>(), -300i32..=300i32).prop_map(|(mantissa, upper_e, exp)| {
            let e = if upper_e { 'E' } else { 'e' };
            format!("{mantissa}{e}{exp:+}")
        })
    }

    /// A handful of exact values worth weighting heavily: the zero forms
    /// and the adjacent prefix digit runs (`12`/`123`/`1230`, and their
    /// negatives) that a byte-wise-text-ordered key gets wrong today.
    fn gotcha_values() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("-0".to_string()),
            Just("0.0e3".to_string()),
            Just("0".to_string()),
            Just("9".to_string()),
            Just("15".to_string()),
            Just("-9".to_string()),
            Just("-15".to_string()),
            Just("12".to_string()),
            Just("123".to_string()),
            Just("1230".to_string()),
            Just("-12".to_string()),
            Just("-123".to_string()),
            Just("-1230".to_string()),
        ]
    }

    /// The generator every property below draws from: a weighted mix of
    /// the baseline shape and every "nasty" shape above.
    fn nasty_number_string() -> impl Strategy<Value = String> {
        prop_oneof![
            4 => decimal_string(),
            1 => digit38_run(),
            2 => leading_zero_string(),
            2 => exponent_notation_string(),
            3 => gotcha_values(),
        ]
    }

    fn reference(v: &str) -> BigDecimal {
        // `encode`/`decode` both trim surrounding whitespace (matching
        // `condition::decimal_parts`); `BigDecimal::from_str` does not, so
        // trim here too rather than adding whitespace-sensitivity to the
        // generator just to dodge it.
        BigDecimal::from_str(v.trim()).unwrap_or_else(|e| panic!("reference parse of {v:?}: {e}"))
    }

    proptest! {
        /// The core claim: bytewise-comparing two encodings agrees with
        /// comparing the values they represent, for every pair of
        /// in-contract decimal strings — including differently-spelled
        /// equal values (`"-0"` vs `"0"`) and adjacent prefix digit runs
        /// (`"12"` vs `"123"`).
        #[test]
        fn encode_orders_like_bigdecimal(a in nasty_number_string(), b in nasty_number_string()) {
            let want = reference(&a).cmp(&reference(&b));
            let ea = encode(&a).unwrap_or_else(|| panic!("encode({a:?}) should succeed for an in-contract decimal"));
            let eb = encode(&b).unwrap_or_else(|| panic!("encode({b:?}) should succeed for an in-contract decimal"));
            prop_assert_eq!(
                ea.cmp(&eb),
                want,
                "encode({:?}).cmp(encode({:?})) should equal the numeric comparison",
                a,
                b
            );
        }

        /// [`decode`] inverts [`encode`] at the *value* level: decoding an
        /// encoding recovers text that parses to the same number, even
        /// though the decoded text's own spelling need not match the
        /// input's (`decode(encode("1230"))` renders `"1230"`, but
        /// `decode(encode("0.000"))` renders `"0"`, not `"0.000"`).
        #[test]
        fn decode_round_trips_to_the_same_value(a in nasty_number_string()) {
            let encoded = encode(&a).unwrap_or_else(|| panic!("encode({a:?}) should succeed"));
            let decoded = decode(&encoded).unwrap_or_else(|| panic!("decode of encode({a:?}) should succeed"));
            prop_assert_eq!(
                reference(&decoded),
                reference(&a),
                "decode(encode({:?})) = {:?} is not the same value",
                a,
                decoded
            );
        }

        /// Canonical idempotence: re-encoding the text [`decode`] produced
        /// reproduces the exact same bytes — there is only one encoding per
        /// value, so a change of spelling never changes the key.
        #[test]
        fn re_encoding_the_decoded_text_is_byte_identical(a in nasty_number_string()) {
            let encoded = encode(&a).unwrap_or_else(|| panic!("encode({a:?}) should succeed"));
            let decoded = decode(&encoded).unwrap_or_else(|| panic!("decode of encode({a:?}) should succeed"));
            let re_encoded = encode(&decoded).unwrap_or_else(|| panic!("re-encoding decoded text {decoded:?} should succeed"));
            prop_assert_eq!(
                re_encoded,
                encoded,
                "encode(decode(encode({:?}))) != encode({:?})",
                a,
                a
            );
        }

        /// Prefix-freedom: no encoded numerically-distinct value's bytes
        /// are a byte-string prefix of another's — the property that
        /// makes the terminator byte load-bearing (see the module doc).
        #[test]
        fn distinct_values_never_encode_as_byte_prefixes(a in nasty_number_string(), b in nasty_number_string()) {
            prop_assume!(reference(&a) != reference(&b));
            let ea = encode(&a).unwrap();
            let eb = encode(&b).unwrap();
            prop_assert!(
                !eb.starts_with(ea.as_slice()),
                "encode({:?}) is a byte-prefix of encode({:?})",
                a,
                b
            );
            prop_assert!(
                !ea.starts_with(eb.as_slice()),
                "encode({:?}) is a byte-prefix of encode({:?})",
                b,
                a
            );
        }
    }
}
