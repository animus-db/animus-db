# A trait method's signature inherits the first backend's error model, not the trait's real contract — an infallible `get`/`scan` copied from an in-memory type quietly became a footgun once a fallible on-disk backend implemented it too (2026-09-16).

**A trait method's signature inherits the first backend's error model, not
the trait's real contract — an infallible `get`/`scan` copied from an
in-memory type quietly became a footgun once a fallible on-disk backend
implemented it too (2026-09-16).** `animus-storage`'s `Snapshot::get`/`scan`
(`crates/animus-storage/src/lib.rs`) returned bare `Option<VersionedValue>`/
`Vec<(Key, VersionedValue)>` — infallible by signature — because the trait
was written against `MemorySnapshot`, whose reads genuinely cannot fail.
Once `LsmSnapshot` (a real on-disk backend, whose reads *can* fail: a
corrupt-block CRC mismatch, a `ProdEnv` disk I/O error, an exhausted
compaction-race retry budget) implemented the same trait, its `get`/`scan`
had no way to report an `Err` through the signature they were bound to, and
folded it into `Ok(None)`/an empty vec (`.ok().flatten()` /
`.unwrap_or_default()`) — a real storage failure silently indistinguishable
from a genuinely absent key or empty range (issue #845). The sibling
`StorageEngine::get`/`scan` on the very same trait file already returned
`Result` for exactly this reason, so the inconsistency was visible by
contrast once looked for, but nothing forced the comparison — the trait
compiled, every existing implementor (one) was satisfied, and the gap sat
latent with "no live blast radius" (every real call site read from
`StorageEngine` directly, not through a pinned `Snapshot`) until someone
read the two method sets side by side.

**The generalizable check**: when a trait method's signature is infallible,
ask whether that is a *property of the operation* or just a property of the
*only implementor that existed when it was written*. If a second, more
demanding implementor (on-disk vs. in-memory, networked vs. local, a real
backend vs. a test double) is plausible — even if none exists yet — signal
fallibility from the start, or grep for every sibling method on the *same*
trait/impl block with the *same* logical operation (a `StorageEngine::get`
next to a `Snapshot::get`) and check they agree. An infallible signature is
not free discipline; it's a claim about every future implementor, and the
compiler will happily let a later implementor lie about it via `.ok()`/
`.unwrap_or_default()` rather than fail to compile.
