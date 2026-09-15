# A marker key built by truncating a tablet's own `range.start` to a fixed prefix is disjoint from real data (if it lives in its own kind scope) but is *not* thereby proven to stay within `[range.start, range.end)` — disjointness and containment are two different properties, and a `Vec<u8> ++ suffix` construction only gets you the first for free.

**A marker key built by truncating a tablet's own `range.start` to a fixed
prefix is disjoint from real data (if it lives in its own kind scope) but
is *not* thereby proven to stay within `[range.start, range.end)` —
disjointness and containment are two different properties, and a `Vec<u8>
++ suffix` construction only gets you the first for free.** ADR 0042/0043's
`KIND_CURSOR` cursor-row key (`animus-cp-data/src/cursor.rs`) mirrors
`txn.rs`'s `record_key` scheme (`token(8 bytes) || [0x00, TAG] ||
payload`) closely enough that the escape-disjointness proof transfers
verbatim — but `txn.rs`'s token is always the anchor write's *own*,
currently-being-written key (trivially in-range), while a cursor row's
token is a *tablet boundary value*, truncated to a fixed 8 bytes. Working
through whether `range.start[..8] ++ marker` can ever land at or past
`range.end` surfaces a genuine, if narrow, edge case (a `Binary`
partition key starting `0x00`, positioned exactly at a split boundary)
that the byte-comparison math does not rule out in general. The
house convention for this — `txn.rs`'s own "a residual, documented gap"
note about `split_key` not being token-aligned — is the right response:
state the gap explicitly in the code and defer it to a targeted corpus,
rather than either (a) assuming a structurally-disjoint key is also a
contained one, or (b) blocking a foundational PR on solving a rare edge
case a later fault-injection corpus is better positioned to stress
anyway. When adding any new marker/cursor key that must survive
`narrow_scope`/`widen_scope`/`engine_image`'s live-range bound, ask
disjointness and containment as two separate questions.
