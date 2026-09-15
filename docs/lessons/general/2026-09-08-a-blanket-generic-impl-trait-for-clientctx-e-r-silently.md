# A blanket generic `impl Trait for ClientCtx<E, R>` silently narrows production's own dispatch to whatever the generic sibling covers — a newtype keeps the production impl concrete, not an interception layer inside the off-limits accept loop (ADR 0061 rung H, C-08 PR 2, 2026-09-08 — corrected same day, in review)

A first cut of this rung widened `impl AdminHost for ClientCtx`/
`impl console::ConsoleBackend for ClientCtx` **in place** to `impl<E: Env,
R: RelayClient> .. for ClientCtx<E, R>` — the same shape every earlier
generic-dispatch rung (D2/D3/D4/F/G) used for a *free function*. The
difference a blanket trait impl introduces, missed at first: `ClientCtx`
(the bare, default-type-parameter alias) is production's own concrete
type, so widening its *one* `impl` block doesn't add a second, parallel
path the way widening a free function does — it **replaces** the only
`AdminHost`/`ConsoleBackend` implementation `ClientCtx` has, for every
monomorphization including `E = ProdEnv, R = AnimusdRelayClient`. Once
`action_data_dynamo`/`add_gsi`/`drop_gsi`'s shared body was forced to call
the narrower `execute_routed_as_generic` (the only way the body can
compile for a generic `E, R` at all), **every** caller of the trait —
including production's own dashboard proxy and real console clients —
silently lost whatever the generic dispatch core doesn't cover, with no
way to get it back by editing the trait impl alone: there is no way to
keep both a generic `impl Trait for ClientCtx<E, R>` and a separate,
more-capable `impl Trait for ClientCtx` (the concrete default) coexisting
— Rust's coherence rules forbid two impls of the same trait for
overlapping type parameters, and `animus_node::console::route`'s own
`&dyn ConsoleBackend` **trait-object** dispatch has no inherent-method-
priority escape hatch to prefer one impl over the other for a single
concrete type either. A first attempted fix reached for a concrete
interception layer inside `console.rs::serve`/`handle_conn` (special-case
two routes ahead of `route`'s own dispatch) — this compiled, passed every
test, and was still wrong: it edited a file no reviewer read as
off-limits, gave `console.rs` its own duplicated route-parsing/JSON-helper
logic that would silently drift from `animus_node::console`'s own the
moment that crate's route shapes changed, and threaded a concrete
`ClientCtx` into `serve`/`handle_conn`'s own signatures — a real change to
the production console path this rung's own non-goals explicitly forbade,
just one file removed from the trait impl itself.

**The actual fix, applied in review**: keep the concrete
`impl AdminHost for ClientCtx`/`impl console::ConsoleBackend for
ClientCtx` as the **production** impls, observably byte-identical to
before this rung (every dispatch call site stays the concrete
`execute_routed`/`execute_routed_as`, never `execute_routed_as_generic`),
and add a **second type**, `GenericAdminHost<E, R>(pub ClientCtx<E, R>)`/
`GenericConsoleBackend<E, R>(pub ClientCtx<E, R>)` — a one-field newtype,
not a new mechanism — with its own `impl<E: Env, R: RelayClient> Trait for
Generic*<E, R>` reaching the generic dispatch core instead. Coherence
allows this because the two impls target genuinely different types
(`ClientCtx<E, R>` vs. `Generic*Host<E, R>`), even though one always wraps
the other. `SimCluster::admin`/`console` (`sim_cluster.rs`) wrap
`self.ctx(node)` in the newtype before calling `animus_node::admin::
dispatch`/`console::route`; production's own `spawn_common_tail` keeps
passing a bare `Arc<ClientCtx>` as `Arc<dyn ConsoleBackend>`/handing a bare
`&ClientCtx` to `animus_node::admin::dispatch<H: AdminHost>`, completely
unaware the newtype exists. Both impls share every byte of request-
building/response-parsing logic (factored into small, `<E, R>`-generic or
plain free functions called by both `self`/`&self.0`) — only the one
dispatch-call line differs — so the two paths cannot quietly drift apart.
`console.rs`/`animus_node::admin::dispatch`/`animus_node::console::route`
are untouched, verified via `git diff` against the pre-rework baseline,
not merely asserted.

**The general form**: when a trait-bound widening is about to touch a
**blanket `impl Trait for ConcreteType`** — not a free function, not an
impl on a type the caller already constructs generically — stop and ask
whether `ConcreteType` is itself production's own default-instantiated
type (a struct with default type parameters, e.g. `ClientCtx<E: Env =
ProdEnv, ..>`). If so, widening that one `impl` block in place doesn't add
a parallel path the way widening a free function does; it *replaces*
production's own implementation for every trait method at once, including
whichever ones the generic dispatch core doesn't yet cover. The fix is a
newtype wrapper with its own separate `impl`, not an edit inside the
off-limits call chain the trait's own dispatcher (`route`/`dispatch`)
sits behind — even a "thin, logic-free" interception one file upstream of
that dispatcher is still a change to the production path this class of
rung's own non-goals exist to forbid.
