# A "propose locally if leader" fast path keyed on a `ProdEnv`-typed handle silently degrades to relay-to-self under any other `Env` (ADR 0061 rung D3 PR 2a)

`ClientCtx::propose_schema` (`animusd/src/schema.rs`) is meant to propose
locally when this node is the control-plane leader and relay one hop to
the leader's node otherwise. The local-propose branch reads
`self.edge.leader_handle()`, and `ClusterEdgeState<E>::control` — before
this rung — was hardcoded `Arc<Mutex<Vec<RaftNode<ProdEnv>>>>` **regardless
of the enclosing `ClientCtx<E, R>`'s own generic `E`**. Under `SimEnv` that
field was therefore always empty, `leader_handle()` always answered
`None`, and *every* schema proposal took the relay branch — including one
issued on the node genuinely leading the control group, which then
relayed `ProposeSchema` to **itself**; the receiving side's own
`ProposeSchema` handler re-resolves the leader the identical way and
re-relays, recursing until the caller's own timeout. This produced no
compile error and no test failure for a long time, because nothing under
`SimEnv` had previously exercised the local-propose path at all (every
prior `SimCluster`/`ClientCtx<SimEnv, _>` fixture in this crate bypassed
`propose_schema` entirely, proposing directly on a raw `RaftNode` handle
instead) — the bug was latent, not merely undiscovered, for months of
prior rungs.

**The general shape to watch for**: a "propose/act locally if I'm the
authority, else forward" fast path that is gated on a field or handle
whose *type* is pinned to one concrete `Env`/backend implementation,
inside a component that is otherwise `E`-generic. The type system cannot
catch this — the code compiles and even runs correctly under the pinned
concrete type (`ProdEnv` in production), so nothing *fails* until a
second, different concrete type (`SimEnv`) is actually driven through that
exact path for the first time. Grep for this shape specifically when
generic-izing a component that used to be concrete: a field/handle whose
declared type names a concrete `Env` implementor (not the generic `E`)
inside an `impl<E: Env> Foo<E>` block is the tell — even if every existing
caller happens to only ever instantiate `E = ProdEnv` today.

**The fix pattern**: widen the field's type to the generic `E` and update
every constructor to register a real `E`-typed handle (not just the
`ProdEnv` one). Doing this can surface further concrete-type leaks
elsewhere that only compiled because the *old* narrow field made a call
site's own downstream value concrete too — see the next entry.
