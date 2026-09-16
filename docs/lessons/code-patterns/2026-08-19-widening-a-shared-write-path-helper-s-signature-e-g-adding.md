# Widening a shared write-path helper's signature (e.g. adding a new trailing `bool`/enum discriminator) must be grepped across the *whole* crate, not just `tests/*.rs` — an in-crate `#[cfg(test)] mod` at the bottom of `lib.rs`/`dynamo.rs`/`index_drain.rs` calls the same private function and is invisible to a search scoped to the external test tree.

**Widening a shared write-path helper's signature (e.g. adding a new
trailing `bool`/enum discriminator) must be grepped across the *whole*
crate, not just `tests/*.rs` — an in-crate `#[cfg(test)] mod` at the
bottom of `lib.rs`/`dynamo.rs`/`index_drain.rs` calls the same private
function and is invisible to a search scoped to the external test
tree.** Threading `ChangeRecord::ttl_expired`/`kind_write_item_at_leader`'s
new `ttl_expired: bool` parameter (ADR 0051 §7) had five real call
sites, not the four a `crates/animusd/tests/` grep alone would find —
the fifth pair lived in `lib.rs`'s own `rmw_285_a`/`rmw_285_b`
in-crate regression module (issue #285, see this crate's own `CLAUDE.md`
for why those tests can't live in `tests/`). `grep -rn
"kind_write_item_at_leader(" crates/animusd/src/` (source, not just
`tests/`) is what actually finds every call site of a `pub(crate)`
helper this crate's own module-map documents as having in-crate test
consumers.
