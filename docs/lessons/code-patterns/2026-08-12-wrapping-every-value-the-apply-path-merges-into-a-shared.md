# Wrapping every value the apply path merges into a shared engine in a new envelope is a crate-internal change with a wide *test*-side blast radius, even when production callers are all safely routed through the crate's own accessors.

**Wrapping every value the apply path merges into a shared engine in a
new envelope is a crate-internal change with a wide *test*-side blast
radius, even when production callers are all safely routed through the
crate's own accessors.** Introducing the ADR 0018 §2/PR3 value envelope
(a leading tag byte on every committed value) required no changes
outside `animus-cp-data`'s own apply path and read accessors — every
production caller already went through `RaftKvNode::local_get`/
`local_scan`/`linearizable_get`/`read_at`/etc., which unwrap it. But two
*tests* in the same crate (`tests/reconciler.rs`,
`tests/reconciler_corpus.rs`) read the engine's raw stored bytes
directly (`storage.get(key).value`) to assert sibling-sparing/data-safety
invariants at the physical-key level — a deliberate, valid testing
technique that this change silently broke (both compiled and ran; they
just started comparing against bytes one tag short). **Grep every test
file for raw `storage.get`/`.scan`/`.entries` + `.value` access whenever
a change alters what the engine's *stored bytes* mean, not just what a
public accessor returns** — `cargo test`'s green/red signal alone caught
this fine here, but the fix (documenting the envelope and updating the
two call sites, one by expected-value literal, one by centralizing the
unwrap in the shared `assert_present` helper) is exactly the kind of
thing that's cheaper to anticipate than to debug from a confusing
off-by-one-byte assertion failure.
