# A cross-cutting seam (metrics, tracing) must be *additive* and observable without touching `SimEnv`.

**A cross-cutting seam (metrics, tracing) must be *additive* and observable
without touching `SimEnv`.** Add it to `Env` as a method with a **no-op default**
(a real shared no-op handle, not an `Option`, so record sites need no guard) —
the supertrait and every `E: Env` impl stay untouched. Keep it deterministic:
no wall clock (timestamps come from `Clock::now`), no I/O, no `HashMap` (snapshot
into a `BTreeMap`). To let a *sim test* read what a component records, thread a
recording handle into the component (e.g. `start_with_metrics`) rather than
overriding `SimEnv` — so `animus-sim` needs no change. (ADR 0015 / `animus-env`
`metrics.rs`.)
