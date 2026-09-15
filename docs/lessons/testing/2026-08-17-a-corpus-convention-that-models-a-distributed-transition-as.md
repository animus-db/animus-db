# A corpus convention that models a distributed transition as atomic can only ever test the FIXED ordering — it structurally cannot express the real race where a local, un-replicated effect lags the replicated commit; and a cache-fed fence cannot protect against the cache itself being stale — only the state machine's own `apply` arbitrates.

**A corpus convention that models a distributed transition as atomic can
only ever test the FIXED ordering — it structurally cannot express the
real race where a local, un-replicated effect lags the replicated
commit; and a cache-fed fence cannot protect against the cache itself
being stale — only the state machine's own `apply` arbitrates.**
(Mechanisms — the zero-copy split's `narrow_scope` lag window,
`in_declared_range`, and the `SealStreamShard` `expected_range` CAS —
deleted in ADR 0050 Train B rung 7: ranges are immutable and a split
retires its parent whole, so the transition window itself no longer
exists. Both full entries archived verbatim in
`docs/engineering-lessons-archive.md`; the apply-arbitrates half lives
on in `Freeze`'s own apply-time backstop.)
