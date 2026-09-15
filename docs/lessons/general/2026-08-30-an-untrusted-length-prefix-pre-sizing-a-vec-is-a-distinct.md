# An untrusted length-prefix pre-sizing a `Vec` is a distinct DoS class from an unbounded-panic one — bounds-checked reads don't close it (`animus-cp-data::codec`, `animus-storage::lsm`)

`codec.rs`'s own doc comment claimed decoding untrusted wire input was
"bounds-checked... never panics," and every individual field read really
was: `Cursor::take` checks `pos + n <= bytes.len()` before every slice, so
a truncated or malformed frame always surfaces as a clean `Err`, never an
out-of-bounds panic or a raw slice-index abort. What that guarantee does
**not** cover is a different failure mode entirely: about a dozen call
sites read an untrusted `u32`/`u64` **count** off the wire (an
`AppendEntries` entry count, a `KindBatch`/`TxnStage` write count, …) and
handed it straight to `Vec::with_capacity(n as usize)` **before** a single
one of its `n` elements had been validated against the remaining buffer.
For a multi-byte element type (`(Vec<u8>, Vec<u8>)`, a `LogEntry<KvCommand>`,
…), a single corrupted or adversarial length-prefix byte setting `n` near
`u32::MAX` turns into a request for hundreds of GB to several TB. Rust's
global allocator does not return an `Err` or unwind a panic for an
allocation that large or that exceeds `isize::MAX` — it calls
`handle_alloc_error`, which **aborts the whole process** (`SIGABRT`), not a
catchable failure at any level above it. Reproduced live: corrupting
`read_raft`'s `AppendEntries` entry-count field crashed
`cargo test -p animus-test --test raftkv_linearizable` outright — the whole
test binary, not one failing assertion. Since this reads off real
inter-node Raft/Kv wire traffic, it was a process-wide denial-of-service
vector reachable from any peer able to send a corrupted or crafted message
to a node, not a theoretical concern.

**The fix is not "validate `n` before allocating"** — there is no cheap way
to validate an element *count* against a buffer when each element's own
encoded size is variable (a byte string, a nested struct, …); the only
honest validation is decoding the elements themselves, which is exactly
what the loop right after the allocation already does. The fix instead
**caps the requested capacity**, never the actual number of elements
decoded: `Vec::with_capacity(n.min(1 << 20) as usize)`. A legitimate
message's cost is completely unchanged (a real `n` is always far below the
cap); a hostile/corrupted `n` can now request at most `1 << 20 *
size_of::<T>()` bytes up front — large enough to never matter for any real
message, small enough to never abort — and the *actual* loop below still
only ever pushes as many elements as the buffer genuinely contains, since
each element read still fails with a graceful `DecodeError` the moment the
buffer runs out (an attacker gets at most O(remaining buffer bytes) worth
of pushes/reallocations, never O(n) for an unbounded `n`). This was
already the house convention in two sibling decoders in the same crate
(`backup.rs`'s `declared.min(1 << 20)`, `segment.rs`'s
`declared_count.min(1 << 20)`) that simply hadn't been applied to
`codec.rs` yet — worth grepping for by name (`with_capacity(n as usize)`,
`with_capacity(len as usize)`, `with_capacity(count as usize)`, and their
`u64`/`usize`-cast siblings) whenever adding or reviewing a new hand-rolled
wire/WAL/manifest decoder, since nothing about this shape triggers a
compiler warning or a clippy lint — it looks like an ordinary, idiomatic
size-hint optimization.

**The same shape existed one layer down, in `animus-storage`'s WAL record
decoder and its manifest decoder** (`lsm.rs`) — found only by a deliberate
workspace-wide grep for the pattern once it was clear `codec.rs` wasn't the
only hand-rolled length-prefixed decoder in the tree. The WAL record
decoder's own count reads happen only after a CRC-32 check on the whole
frame (`try_parse_wal_frame`), which makes the "one corrupted byte survives
undetected" case astronomically unlikely for ordinary bit rot — but CRC-32
is not cryptographically adversary-resistant, and the shape is identical
either way, so it got the identical `.min(1 << 20)` cap as defense in
depth. The **manifest** decoder has no CRC protecting it at all (a plain
`Disk::replace`-swapped file), so a corrupted on-disk manifest byte was a
real, not merely theoretical, instance of the same abort — fixed
identically. `animus-cp-data`'s `txn.rs` (the value-envelope decoder) and
`split.rs` (the in-place-split fork marker) had the same shape too, in code
whose own doc comments describe it as "this crate only ever reads back
what it itself wrote" (a hard-bug, not a recoverable-error, doctrine) — but
both are engine-marker values that ride inside a tablet's `InstallSnapshot`
image, which is untrusted wire content until it's actually been applied;
"this crate wrote it" is true of the *normal* path, not a guarantee against
a corrupted/adversarial snapshot from a peer, so both got the same cap
rather than being left on the "assumed trusted" side of the line.

**General rule for reviewing (or writing) any hand-rolled length-prefixed
decoder — wire, WAL, or manifest — going forward**: bounds-checking every
individual read (via a `Cursor`-style `take` that checks the remaining
buffer) is necessary but not sufficient for allocation safety. Grep
specifically for `with_capacity` fed by any value that was itself just read
from the buffer being decoded, and cap it — dropping the pre-allocation
entirely (falling back to `Vec::new()`/organic `push` growth) is an equally
valid fix where performance doesn't matter enough to justify the extra
literal; this repo's own precedent leans toward the capped form since it
preserves the fast path's zero-realloc behavior for every legitimate
message. A doc comment claiming a decoder "never panics" or is
"bounds-checked" should be read as a claim about individual reads only,
never as a claim about allocation safety, unless it says so explicitly —
`codec.rs`'s own doc comment now does, and is the template for any other
decoder's doc that makes a similar claim.
