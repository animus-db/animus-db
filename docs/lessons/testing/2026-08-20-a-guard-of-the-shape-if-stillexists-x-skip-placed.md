# A guard of the shape `if !still_exists(x) { skip }` placed immediately before unbounded-latency async I/O against `x` is a probability reducer, not a safety check

**A guard of the shape `if !still_exists(x) { skip }` placed immediately
before unbounded-latency async I/O against `x` is a probability reducer, not
a safety check** — the object can change state in the gap, and where the
callee already re-validates itself fresh on every call (the right design),
the caller's pre-check is an optimization only. The caller must still treat
the callee's own authoritative "no longer valid" answer as an *expected
outcome*, not a fatal one. A streams-lineage walker computed the next shard
id client-side from a locally-cached `Metadata` snapshot, checked
`tablets.contains_key`, then made two async round trips before `GetRecords`
landed; a split cutover retiring the tablet in that gap made the
speculatively-guessed epoch one that never existed, so the server's 400
`TrimmedDataAccessException` was the *correct* answer and the test panicked
on it. Note the scoping rule that came with the fix: handle such an expected
terminal error **at the one call site that can legitimately provoke it**,
never by widening a shared retry/allowlist helper — the same status on a
transactional-write path still means a real bug. (#299,
`crates/animusd/tests/streams_e2e.rs`, 2026-08-20.)
