# A default type parameter contains "genericize this type" blast radius the same way a type alias does — but it only resolves in *elided type positions*, never inside an enclosing generic scope (ADR 0061 rung C5 step 1, `CpGroup<E>`/`ClientCtx<E>`)

Rung C5 step 1 genericized `animusd`'s `CpGroup`/`SharedEngine`/
`ClusterEdgeState`/`CpRoute`/`ClientCtx` over `E: Env` **in place**
(same crate, nothing moved yet — see ADR 0061's fifth 2026-08-28
amendment). Unlike C3c's `ControlHandle` (a type consumed at exactly one
concrete instantiation, so a crate-local `type ControlHandle =
animus_node::control_handle::ControlHandle<ProdEnv, AnimusdRelayClient>;`
contained the whole diff to the type's own definition — see the entry
above), these five types are each still *named directly*, by their own
name, at roughly 200 call sites apiece throughout the crate — no single
alias site to bind. The equivalent seam here is a **default type
parameter on the definition itself**: `enum CpGroup<E: Env = ProdEnv>`,
`struct ClientCtx<E: Env = ProdEnv>`, etc. This is legal, ordinary Rust
(confirmed with a standalone `rustc` snippet before relying on it
workspace-wide) and has the identical effect for every call site that
never spells a type parameter: a bare `CpGroup`/`Option<ClientCtx>` in a
non-generic function, a struct field, or a `match` arm still means
`CpGroup<ProdEnv>`, so none of ~400 existing bare references anywhere
in the crate needed to change.

**The one place the analogy breaks, and the one worth knowing before
attempting this**: a default type parameter resolves to its **default**
whenever it is *elided in a type position* — including inside a
function that is *itself* generic over the identical-looking parameter
name. Verified directly:

```rust
enum CpGroup<E: Env = ProdEnv> { A(E) }
fn takes_ref<E: Env>(x: &CpGroup) {}   // x: &CpGroup<ProdEnv>, NOT &CpGroup<E>
```

Calling `takes_ref::<SimEnv>(&some_cp_group_e)` is a plain `E0308`
mismatch — the bare `CpGroup` in the signature never "sees" the
function's own `E`, because default-parameter resolution only consults
the default, never an enclosing scope's same-named parameter. The
practical consequence: once `CpGroup`/`ClientCtx` grew a default `E`,
every method/associated-fn signature *inside* the two `impl<E: Env>
ClientCtx<E> { .. }` blocks that named `CpGroup`/`CpRoute` explicitly
(`leader: &CpGroup`, `-> Option<CpRoute>`, …) had to be rewritten to
`<E>` by hand — about 30 sites, all caught by `cargo build` as plain
type-mismatch errors naming the exact line, not silently accepted with
the wrong meaning. Three sibling free functions the impl blocks call
into by name (`index_drain::{seal_now, pitr_seal_now, hot_read,
clear_backfill_cursor}`, `dynamo::{kind_write_item_at_leader,
eval_kind_txn_write, collection_bytes_at_leader}`, this crate's own
`median_split_key`) needed the identical `<E: Env>` treatment for the
same reason — a generic caller passing a `&ClientCtx<E>`/`&CpGroup<E>`
into a callee whose signature still says bare `ClientCtx`/`CpGroup`
hits the same mismatch, one level removed.

**What did NOT need fixing, and why it matters for scoping this kind of
change**: pattern-matching against an already-typed value
(`CpRoute::Local(leader) => ..`) and constructing a value whose type is
inferred from a `return`/call-argument context (`Some(CpRoute::
Local(leader))`) both resolve through ordinary type inference from the
already-known concrete type, not through default-parameter elision —
default resolution only applies when nothing else pins the type. Of the
roughly 90 `CpRoute`-mentioning lines inside `ClientCtx`'s two `impl`
blocks, only the 2 function *signatures* returning `CpRoute` needed a
change; the ~60 match arms and constructions elsewhere were untouched.
Anyone repeating this pattern for another type should expect the same
split: audit signatures (return types, parameter types, struct/enum
field types) exhaustively by hand or by grep, then let `cargo build`
catch anything missed — but expect the match/construction sites to be
free, and don't waste time annotating them defensively.
