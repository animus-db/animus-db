# When a pure free function's existing `#[cfg(test)]` module doesn't actually test the function it's named after, don't move it just because the function moved

**When a pure free function's existing `#[cfg(test)]` module doesn't
actually test the function it's named after, don't move it just because
the function moved** (same ADR 0061 rung A6 sweep). `confirm_futility_
tests` sits right next to `confirm_wait_is_futile` in `lib.rs` and reads,
from the name, like its unit tests -- but every test in it is a real
`#[tokio::test(flavor = "multi_thread")]` standing up a whole single-node
cluster and asserting on wall-clock timing and error-string shape of the
*wired* fast-fail behavior, never calling the predicate directly. Moving
it into the new bring-up-free `decide` module would have silently broken
that module's whole reason for existing (no `&self`/`ProdEnv`/`tokio`,
plain `#[test]`s only) for a module that only looks related by proximity
and naming. It stayed in `lib.rs`, documented as proving the wired
behavior rather than the predicate, and the predicate got its own fresh
truth-table tests in `decide` instead -- the two are complementary
coverage, not one relocated. General rule: before moving a test module
alongside an extracted function, read what it actually calls, not what
it's named after or sits beside.
