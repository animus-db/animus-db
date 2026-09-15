# Moving a `pub(crate)` item across a new crate boundary (ADR 0061 rung C1)

Carving `animus-node` out of `animusd` (wire types, `topology`, `decide`)
surfaced a mechanical but easy-to-miss step: every moved item that was
`pub(crate)` in `animusd` had to become `pub` in `animus-node`, and every
moved struct's individual fields that were `pub(crate)` (not just the
struct itself) had to widen too. `pub(crate)` scopes to the crate that
*declares* the item — once the item lives in a different crate than its
callers, `pub(crate)` silently becomes "unreachable from anywhere but
`animus-node`'s own tests," a privacy error at every call site in
`animusd`, including inside a `pub use` re-export shim (`pub use
animus_node::topology;` compiles, but `animusd::topology::decide_cp_route`
from outside `animus-node` does not, unless `decide_cp_route` itself is
`pub`). This is easy to miss because the compiler error at the *re-export*
site is clean (`pub use` of a private path is legal within the same crate,
since `lib.rs` is inside `animusd`, the crate importing it) — the failure
only shows up downstream, at whichever call site first tries to actually
*use* the re-exported name, with a plain "function is private" error that
doesn't obviously point back at the moved declaration's visibility. Two
concrete instances this rung hit: `PendingKindWrite`/`TxnTableWrite`'s
struct fields (some call sites build them via struct literal, not just the
`TxnTableWrite::plain` constructor, so the fields themselves — not just the
types — needed widening), and every function/enum in `topology.rs`/
`decide.rs` (all `pub(crate)` in the original single-crate layout, all
needing `pub` once `animusd` became a foreign-crate consumer). General
lesson: when moving code across a *new* crate boundary (as opposed to
refactoring within one crate), grep every moved item's visibility
modifier, not just its `pub`/private status — `pub(crate)` is exactly the
one visibility level that silently means something different depending on
which side of the boundary you're standing on, and `cargo build` catches
it late (at use, not at declaration) rather than at the point where the
mistake was actually made.
