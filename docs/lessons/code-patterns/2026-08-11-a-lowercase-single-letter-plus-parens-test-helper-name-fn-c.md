# A lowercase, single-letter-plus-parens test helper name (`fn c() -> T`) can collide with an equally-terse, ubiquitous local variable of a completely different type, and the resulting error reads as a type mismatch far from the real cause.

**A lowercase, single-letter-plus-parens test helper name (`fn c() -> T`)
can collide with an equally-terse, ubiquitous local variable of a
completely different type, and the resulting error reads as a type
mismatch far from the real cause.** Renaming a PR2-era `const C: NodeId`
(uppercase, never shadows anything) to a PR3-era `fn c() -> NodeId`
(lowercase, matching this codebase's `nid`-helper convention) collided with
`reconciler_corpus.rs`'s own near-universal `let mut c = Cluster::new(sim);`
scenario-harness variable — every `c()` call after that point parsed as
"call the local `Cluster` value named `c`," not the function, producing
"expected function, found `Cluster`" at a dozen unrelated-looking call
sites. **General rule: when a mechanical rename turns a `const` into a
`fn`, or otherwise introduces a new lowercase short binding, grep the
target file(s) for that exact identifier already in use as a *local
variable* before trusting the rename is safe** — a real type-level
namespace (`const`/`static`/type-level items don't shadow local `let`
bindings the same way a same-named `fn` at module scope does once called
with `()`) doesn't protect against this once the item becomes callable.
Fixed by renaming the function to a distinct name (`node_c`) instead of
chasing every shadowing call site. (`animus-cp-data/tests/
reconciler_corpus.rs`, ADR 0040 PR3.)
