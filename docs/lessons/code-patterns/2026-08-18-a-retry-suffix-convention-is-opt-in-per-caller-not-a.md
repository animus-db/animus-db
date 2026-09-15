# A `"; retry"`-suffix convention is opt-in per caller, not a property of the error itself — unifying call sites onto a shared primitive can silently drop a retry loop that used to live at a since-deleted higher layer

**A `"; retry"`-suffix convention is opt-in per caller, not a property of
the error itself — unifying call sites onto a shared primitive can
silently drop a retry loop that used to live at a since-deleted higher
layer** (issue #288). `FROZEN_REFUSAL` (ADR 0050's split-cutover freeze
refusal) is emitted deliberately in the house `"; retry"` shape, and the
low-level primitives that can hit it (`cp_kind_local`, `cp_kind_raw_
local`, `seed_rows_local`) all return it correctly. But *retrying* on
that suffix is something each caller has to opt into by actually writing
a loop — it doesn't happen automatically just because the string ends
the right way. `ClientCtx::cp_kind_write_item`/`cp_kind_write_raw` (the
two caller-facing entry points every Dynamo/CQL/raw-protocol write funnels
through since ADR 0049's write-path unification) were each a single
`cp_route` + one attempt, no loop at all — so a write racing a split's
freeze window got a terminal 500 instead of the retry every *other*
retryable-error caller in this file performs. The bug likely predates
the unification: an older, now-deleted higher layer plausibly retried
this for the plain-write path, and folding every write shape onto one
shared low-level primitive (rung 1 of ADR 0049) preserved the primitive's
own correct error shape while dropping whatever retry loop used to wrap
it above. **Audit method that would have caught this**: don't trust a
doc comment's claim about retry behavior (or an issue's own premise —
this one *also* wrongly assumed the plain-write arm already retried,
when it never had coverage either way) — trace each entry point down to
its terminal single-attempt primitive and grep for an actual `loop { ...
}` shape wrapping the call, the same discipline `cp_read`'s own
deadline-bounded loop already demonstrates as the house pattern. A
refactor that unifies several call sites onto one shared implementation
is exactly the moment a caller-side concern (retry, backoff, dedup) that
lived above the old, now-deleted per-shape code paths is most likely to
quietly vanish — grep for it explicitly rather than assuming the
unification preserved it.
(`crates/animusd/src/lib.rs::cp_kind_write_item`, `cp_kind_write_raw`.)
