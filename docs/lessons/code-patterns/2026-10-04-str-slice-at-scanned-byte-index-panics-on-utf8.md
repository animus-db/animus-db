# Slicing a `str` at a byte index you got by scanning bytes panics on UTF-8

**Found while:** triaging a libFuzzer `dynamo_request` crash (remote panic).

`wire::find_top_level` lowercased the haystack and then, for every byte index
`i`, evaluated `lower[i..].starts_with(" between ")`. `&str[i..]` panics when
`i` is inside a multi-byte char, so any non-ASCII char at paren depth 0 in a
Scan/Query/Condition expression crashed the request decoder. The gates never
saw it: every test expression was ASCII.

Rules: (1) when scanning byte-by-byte, compare on `as_bytes()` slices
(`bytes.get(i..i+n).is_some_and(|w| w.eq_ignore_ascii_case(pat))`), never slice
the `str`; (2) keep indices relative to the ORIGINAL string — `to_lowercase()`
can change byte length (`İ` U+0130 is 2 bytes, lowercases to 3), so offsets into
a lowercased copy do not index the original; `to_ascii_lowercase()` is
length-preserving but still does not make `[i..]` safe; (3) hits for an ASCII
needle are always char boundaries, so slicing the original at a hit is fine.
Any parser on an untrusted path needs non-ASCII test inputs (U+FFFD, `İ`, emoji).
