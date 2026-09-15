# A second default-typed generic parameter can force a supertrait bound that only shows up when a downstream call site needs it — and a blind regex conversion silently mis-targets `self`-free functions (ADR 0061 rung C5 step 3a/3b)

Two lessons from genericizing `ClientCtx<E: Env = ProdEnv>` a second time,
over `R: RelayClient = AnimusdRelayClient` (step 3a), then converting the
91 raw `tokio::time`/`tokio::spawn`/`tokio::select!` sites the resulting
`impl<E: Env, R: RelayClient> ClientCtx<E, R>` blocks still held (step
3b).

**1. A second generic parameter's supertrait bounds are invisible until a
*specific* method needs them — and the fix belongs on the trait, not on
one `impl` block.** Adding `R: RelayClient` alone compiled fine for every
method that never called `.clone()`/spawned a future capturing `self`.
The first sign of trouble was `txn_coordinator.rs`'s existing `let this =
self.clone()` (for a parallel resolve fan-out) and this rung's own
conversion of a `tokio::spawn(resolve_all(..))` into
`self.env.spawn_task(resolve_all(..))`: both need `ClientCtx<E, R>: Clone
+ Send + Sync + 'static` to hold **generically**, and nothing about `R:
RelayClient` implied any of those — `AnimusdRelayClient`, the one
concrete implementor that exists today, trivially satisfies all four as a
zero-sized type, so nothing forced the question until a specific method
in a specific file tried to rely on it. The tempting narrow fix — bound
just `txn_coordinator.rs`'s own `impl` block (`impl<E: Env, R: RelayClient
+ Clone + Send + Sync + 'static> ClientCtx<E, R>`) — turns out to not stay
narrow: `read_path.rs` calls `self.txn_status(..)`, a method defined in
that same stricter-bounded block, so the bound would have had to cascade
into every file that transitively calls into it, which given
`forwarding.rs`'s "calls into every sibling by name" role is most of the
five-module call graph. Adding `Clone + Send + Sync + 'static` as
supertrait bounds on `RelayClient` itself (mirroring `Env`'s own
supertrait shape exactly) closed it in one place instead — a design the
type's own pre-existing doc comment had already implicitly promised
("cheap to clone... the `RelayClient` implementor itself must make cheap
to clone") without the compiler enforcing it. When a widened generic
bound seems to demand a per-call-site or per-`impl`-block fix, check
whether the *shape* of what every caller needs is already the same shape
a sibling supertrait (here, `Env`) already carries — the fix usually
belongs on the trait.

**2. A regex-driven `tokio::time::Instant::now()` → `self.env.now()`
conversion is only safe inside a function that actually has `&self` —
and a file can have both kinds without any visual signal.** `write_path.rs`
has 13 methods on `impl<E: Env, R: RelayClient> ClientCtx<E, R>`, but 7 of
them (`poll_probe`, `cp_batch_local`, `cp_batch_propose`, `cp_put_local`,
`cp_delete_local`, `cp_kind_local`, `seed_rows_local`) are deliberately
`Self`-free — they take `leader: &CpGroup<E>` as their first parameter
instead of `&self`, a design choice from ADR 0061 rung C5 step 1 (they're
called from `dynamo.rs`'s free functions, which never construct a
`ClientCtx` reference just to reach them). A single crate-wide
`tokio::time::Instant::now()` → `self.env.now()` regex substitution
compiled clean in `read_path.rs`/`forwarding.rs`/`txn_coordinator.rs`
(every method there takes `&self`) but produced 14 `error[E0425]: cannot
find value \`self\`` sites in `write_path.rs` alone — caught immediately
by `cargo build`, never by re-reading the diff, since the substituted
line looks completely ordinary in isolation (`let deadline =
self.env.now().saturating_add(CLIENT_TIMEOUT);` reads fine whether or not
`self` is in scope three lines up). The fix was `leader.env()` instead
(`CpGroup<E>`'s own private `env(&self) -> &E` accessor, already visible
to any descendant module per the standing privacy-widening lesson) — a
mechanical, per-call-site substitution once the seven functions were
identified, but identifying them required reading every function
signature in the file first, not trusting the regex to have been
scoped correctly. **The general rule this reinforces**: a `sed`/regex
conversion pass over a body of `&self`-shaped code must never be trusted
crate-wide without an immediate `cargo build` right after — and the build
output's line numbers, not a visual diff, are what actually finds the
functions the assumption didn't hold for.

Two smaller findings from the same rung, worth a shorter note: a
`Self`-free associated function (`ClientCtx::cp_kind_local`, called
`ClientCtx::cp_kind_local(leader, ..)` with no `self` argument at all)
gives type inference nothing to pin a newly-added generic parameter on —
`error[E0283]: type annotations needed`, fixed with an explicit turbofish
at each of its three call sites, exactly as the compiler's own suggestion
said; and a pattern match against a type alias only resolves generically
when the alias itself is imported *as the generic item* — `crate::
ControlHandle::Local(raft)` (this crate's own `ProdEnv`/
`AnimusdRelayClient`-bound alias) fails to match a `ControlHandle<E, R>`
value for generic `E`/`R` with a plain `E0308`, confirmed with a two-line
standalone `rustc` snippet before touching the real file, and fixed by
importing `animus_node::control_handle::ControlHandle` (the generic enum)
directly under the same name in the one file that needed the generic
match, leaving every other file's `crate::ControlHandle` (still matched
against a concrete value there) untouched.
