# A "no stray terminator byte" invariant test must be scoped to the digit-run, not the whole encoding (W-03 step 2, `numkey.rs`)

Building `animus_dynamo::numkey` (an order-preserving codec for DynamoDB
`N` — sign class byte, then a fixed-width 2-byte offset-binary exponent
field, then a digit run terminated by `0x00`, everything after byte 0
bitwise-inverted for negatives), the first draft of a "the terminator byte
is the only `0x00` in the encoding" unit test scanned the *entire* output
including the exponent field. It failed immediately on `encode("0.5")`:
that value's exponent is `0`, which biases to `1024` (`0x0400`) — a
perfectly valid 2-byte field whose low byte legitimately is `0x00`. The
claim "no stray terminator byte" is only true of the **digit-run region**
(bytes are `1..=10`, so `0x00` can only be the terminator there by
construction); a fixed-width binary field elsewhere in the layout has no
such constraint and will contain every byte value across a large enough
sample, `0x00` included.

**Fixed** by rewriting the test to slice off the known-width exponent
field first (`&encoded[3..]`) and only assert the "0x00 appears exactly
once, at the end" claim over that remaining digit-run slice — the region
the invariant actually describes.

**General form**: when writing a "this reserved/terminator byte value
appears nowhere else in the encoding" test for a multi-field binary
layout, scope the scan to the specific field the invariant is *about*
(here: a variable-length, digit-constrained run), never the whole record —
a fixed-width field with no value constraint of its own (an exponent, a
length prefix, a checksum) is a false positive waiting to happen the
moment a real test input's field value happens to contain that byte. Pick
a test sample specifically to exercise the "reserved value at exponent
zero" boundary (`exp = 0` here) rather than only obviously-nonzero cases —
that's exactly the input that caught this.
