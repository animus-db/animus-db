# A `NodeId` representation change (`u64` → a validated string) breaks tests that hardcode its *rendering*, not just its *type* — and these compile clean, so only a green-then-red gate catches them.

**A `NodeId` representation change (`u64` → a validated string) breaks tests
that hardcode its *rendering*, not just its *type* — and these compile
clean, so only a green-then-red gate catches them.** ADR 0040 PR3 changed
`NodeId`'s `Display` from a bare integer (`"0"`) to `"n0"`/an
operator-proposed string/an allocator-minted `"alloc-…"`. Two distinct
failure shapes, neither a compile error: (1) `accord_backoff.rs`'s
`sends_from` helper built its trace-grep needle as
`format!("SEND {from}->")` with `from: u64` interpolated bare (`"SEND
0->"`), but the actual trace line now renders `"SEND n0->n1"` — the needle
silently matched **zero** lines forever, so a `sends >= 4` liveness
assertion failed at its *frozen, fixed* seed on every run, not
intermittently. Fix: build the needle from the same `NodeId` the trace
formatter uses (`format!("SEND {}->", nid(from))`), never re-derive a
numeric-looking string independently. (2) A test asserting an
allocator-minted id "never collides with a small manual id" via `first >
nid(302)` silently flipped from true to false: `"alloc-1000000"` sorts
*before* `"n302"` lexicographically (`'a' < 'n'`), even though the ids are
genuinely disjoint by their reserved-prefix *namespace*. **General rule:
after any type whose `Display`/`Ord` semantics change from "numeric
magnitude" to "opaque string," grep every test for `format!` needles built
from the raw numeric seed instead of the real formatted value, and for
`<`/`>`/`>=`/`<=` comparisons that encode a magnitude assumption — both
compile fine and fail (or silently stop testing anything) only at
execution.** (`animus-consensus/tests/accord_backoff.rs`,
`animus-control/src/meta.rs::allocate_node_id_is_monotonic_and_disjoint_
from_small_manual_ids`, ADR 0040 PR3 — that specific test, and the
allocator/`"alloc-…"` mechanism it illustrated, were deleted in ADR 0040
PR4; the general rule above outlives it and still applies to any other
`Display`/`Ord` semantics change.)
