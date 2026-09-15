# Superseded by ADR 0026/0028 for this use

**Superseded by ADR 0026/0028 for this use**: `animus-cp-data` no longer uses
`Coresident`/`sibling()` at all — every tablet a node hosts shares one env,
addressed by `stream` (ADR 0026 Stage B). The `Coresident` trait itself still
exists in `animus-env`/`animus-sim` (unused by cp-data now); the pattern
below (sub-trait, not supertrait) is still the right one if a future
capability needs it. Retained for historical record. **Extend the `Env` seam with a *sub-trait* bound only where used, not by widening
the supertrait — capabilities not every env has stay opt-in.** In-band tablet
split (ADR 0017 D) needs a node to mint a second inbox at runtime
(`sibling(id) -> Self`). Adding that to the `Env` supertrait would force *every*
env — `ProdEnv` included — to implement runtime inbox-minting (an unsolved
production-network problem) just to compile. Instead it's a separate
`Coresident: Env` trait that only the split path bounds on (`impl<E: Coresident,
S> RaftKvNode<E, S>`), so `SimEnv` implements it, `ProdEnv` doesn't yet, and
nothing else changes. Same shape as the metrics seam (additive, default-off) but
via a trait bound rather than a defaulted method, because it returns `Self`. Keep
the consumer generic over `Env` and inject the capability where needed (here, a
`SplitHook` closure built with a `Coresident` env), so the driver stays
`<E: Env>` and existing call paths (`split.rs`, hook = `None`) are byte-identical.
