# A length-prefixed-chunk decoder validating "padding count" without validating "padding position" accepts corrupted input as valid (issue #849)

`base64_decode`'s padding check counted `=` characters per 4-byte chunk
(`chunk.iter().filter(|&&c| c == b'=').count()`) and rejected only `pad >
2` — it never checked *where* those `=` characters sat within the chunk,
so `"A=AA"` (`=` in the second position, not the tail) computed `pad = 1`,
treated the `=` as a zero sextet exactly like a real trailing pad, and
returned `Some(vec![0, 0])` instead of `None`. Every `B`/`BS` value on the
DynamoDB wire routed through this decoder, so a client's malformed base64
was silently replaced by unrelated bytes rather than rejected — the worst
class of decode bug, since it corrupts data instead of erroring.

**The general form**: a decoder for any length-prefixed or fixed-width
chunk format (base64, an escape scheme, a framed record) that checks *how
many* of a sentinel byte/marker appear in a chunk, without also checking
*where* they appear, is unsound the instant the format's grammar says the
marker is only legal in a specific position (RFC 4648 base64: `=` legal
only as a *trailing run* of the *final* quantum). Count-only validation
degenerates to "any subset of positions holding this marker is fine,"
which is a strictly looser grammar than the format defines — the sibling
codec in the same file (`base64url_decode`) already had this exact
strictness (canonical-only, no non-canonical trailing bits) as a named
design goal with its own regression test; the asymmetry between two
codecs in one module, one deliberately strict and one silently lenient
with no test ever probing the lenient one's edge cases, was itself a
signal worth noticing before assuming the leniency was intentional.
