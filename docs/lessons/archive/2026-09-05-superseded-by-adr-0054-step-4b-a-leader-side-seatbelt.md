# Superseded by ADR 0054 step 4b (a leader-side "seatbelt double-check" kept alongside an apply-evaluated write must predict the client's own decision, not replicate the byte-level mechanism it replaces)

Moved verbatim from `docs/engineering-lessons.md` when ADR 0054 step 4b
deleted the whole mechanism this entry describes (`predict_kind_eval_
decision`/`report_kind_eval_seatbelt_mismatch`, `Metric::
KindEvalSeatbeltMismatch`, and the `rmw285_confirm_gate` test hook it
names) — apply's own decision is the only one computed at all now, so
there is nothing left to compare it against. The generalized form (below)
keeps a pointer in the live log.

Cutting `kind_write_item_at_leader` over to `KvCommand::KindEval` (ADR 0054
step 3), the task brief for the kept seatbelt double-check assumed the
classic "two concurrent `ADD`s, one refused" scenario would be the thing the
mismatch metric catches. Tracing the actual code paths found this is not so:
the *old* seatbelt was a byte-level OCC check (`KindBatch.conditions =
vec![(base_key, raw_old)]`, comparing the leader's exact read bytes against
whatever is committed at apply) with no relationship to the client's own
`ConditionExpression` — a plain, unconditional `ADD` has no condition to
evaluate at all, so a leader-side prediction built from `condition.evaluate`
can *only* ever answer `Applied` for it, never `ConditionFailed`. Reproducing
the old byte-level seatbelt's staleness signal was not what was asked for,
and would have required inspecting engine state at apply time from the
leader side, which nothing exposes. The seatbelt double-check that actually
matters — and that the metric's own doc had to be worded around — is
narrower: it predicts what the SAME evaluation logic (`condition.evaluate` +
`apply_update`) would decide from the leader's own resolved read, and
compares that prediction against apply's confirmed decision. That only ever
disagrees for a **conditioned** write, and only because the value legitimately
changed between the leader's read and apply's own fresher one — which is
symmetric in principle (a leader's stale read can go stale in either
direction) but the task's intended semantics single out one direction as
"expected" (leader too pessimistic, apply succeeds) and the other as
"worth investigating" (leader too optimistic, apply rejects). Both directions
are mechanically ordinary races, not distinguishable by looking at a single
disagreement in isolation; the asymmetry is a policy call about which
direction, if it dominated the counter's rate over time, would suggest a
real evaluator divergence between the leader-side and apply-side code paths
(which *are* meant to agree) rather than ordinary timing — worth recording
in a comment precisely because nothing in the mechanism itself enforces it.

**Testing implication**: a regression that manufactures a specific
disagreement direction needs a *conditioned* write and a genuine timing race
between two writes to the same key, not just concurrent unconditional ones —
the existing `rmw285_confirm_gate` test hook (issue #285, already `#[cfg(test)]`
in `dynamo.rs`) that delays a write's own post-rmw_lock phase is exactly the
tool for this: arm it, let the gated write's leader-side read observe a
before-image its own `ConditionExpression` would reject, land a second,
ungated write in the gap that makes the condition become true, then let the
first write's entry apply against the now-favorable state. This is a cheap,
deterministic way to prove a "kept old evaluator vs. new evaluator" double
check both fires correctly and never fails the request — worth reusing for
any future ADR 0054-style migration that keeps a comparison-only legacy
evaluator alongside a cut-over one.
