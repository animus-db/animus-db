# A "reconciler-maintained latch" closing a stale-read window is only sound if it's checked against something at least as fresh as the read it guards — a periodically-updated flag derived from the reconciler's own tick cadence is not, no matter how the plan phrases it.

**A "reconciler-maintained latch" closing a stale-read window is only
sound if it's checked against something at least as fresh as the read it
guards — a periodically-updated flag derived from the reconciler's own
tick cadence is not, no matter how the plan phrases it.** Found delivering
ADR 0044 phase-1 PR4's `hot_read` scope-transition latch (the ADR 0043
residual): the literal ask was a boolean the reconciler sets when it
first notices a scope mismatch and clears once `narrow_scope` executes.
Since detection and execution happen in the *same* tick
(`Reconciler::tick`'s `gather_facts` → `plan` → execute is one atomic
pass with no other writer), such a flag is false throughout the entire
window that actually matters — from the moment a split commits in
`Metadata` until this replica's *next* tick even starts (bounded by
`metadata_watch` wake latency plus, worst case, a 500ms fallback poll) —
because the reconciler cannot raise a flag for a change it hasn't
observed yet. The sound fix skipped the "maintained flag" entirely and
cross-checked a value that has **no observation lag by construction**
(`RaftKvNode::scope_range()`, current the instant the reconciler mutates
it) against the **freshest obtainable** comparison point
(`metadata_fresh()`, never `effective_metadata()`/`metadata_cached()`) —
no new shared state, no periodic-refresh lag to reason about. **General
rule**: when a design calls for "a component maintains a flag reflecting
some external fact," check whether the flag-maintainer's own update
cadence has a lag the flag's consumer can't tolerate; if the maintainer's
*state* (not a derived boolean about that state) is already live and
read-only-safe to expose, prefer exposing the state itself and letting
the consumer do a live cross-check over inventing a flag that inherits
the maintainer's own staleness window. See ADR 0048 and ADR 0043's
residual section for the full incident and the D8 before/after evidence.
