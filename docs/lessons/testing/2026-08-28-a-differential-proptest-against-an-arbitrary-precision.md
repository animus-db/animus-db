# A differential proptest against an arbitrary-precision reference type must compare parsed *values*, not rendered text

**A differential proptest against an arbitrary-precision reference type
must compare parsed *values*, not rendered text** (ADR 0061 rung A5,
`add_numeric`/`compare_numeric` vs. `bigdecimal::BigDecimal`). This
crate's decimal arithmetic normalizes trailing zeros and `-0` (`"1.10" +
"0.90"` renders `"2"`, never `"2.00"`), and that normalization is a
deliberate, tested behavior, not a bug — so asserting on the reference's
*string* output would fail on exactly the inputs the test most needs to
cover. `BigDecimal`'s `PartialEq` is itself scale-normalizing (`4 ==
4.00`), so parsing this crate's result string back into the reference type
and comparing `BigDecimal == BigDecimal` sidesteps the whole issue for
free — no reference-side normalization step needed. Anyone reaching for a
reference-implementation differential test against a bignum/decimal type
should check whether the reference's equality is value- or
representation-based before writing the assertion, not after a spurious
failure sends them chasing a phantom bug in the code under test.
