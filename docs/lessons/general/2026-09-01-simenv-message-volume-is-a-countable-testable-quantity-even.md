# `SimEnv` message volume is a countable, testable quantity even though it costs nothing there (issues #532/#537, ADR 0009's third amendment)

`SimEnv`'s virtual clock does not advance for sending a message (no
network delay unless configured) and its disk model charges no time for
an op unless a delay is configured — so a resend flood that would be
catastrophic in real wall-clock or real network cost is, by design,
**free** under `SimEnv`: it costs no virtual time and (with the model's
own defaults) very little real time either. That is exactly why the
existing convergence-timing regression test for this issue
(`learner_catchup_under_load.rs`) could not, by itself, prove a resend fix
actually bounds volume — it could regress all the way back to thousands
of redundant sends per real chunk and still pass, since nothing about
convergence *timing* would necessarily change enough to notice. The fix:
a **separate** test asserting on message *count*, not time —
`animus-cp-data/tests/snapshot_resend_bound.rs`, built on the
already-existing deterministic, additive metrics seam (ADR 0015,
`Metric::CpSnapshotShips`, threaded via `RaftKvNode::start_with_metrics`)
for the numerator. The general point worth recording separately from this
specific fix: **cost-under-simulation and cost-in-reality are different
axes, and a test suite needs an assertion on each one it cares about** — a
mechanism that is free to *exercise* under `SimEnv` (which is exactly what
makes `SimEnv` good at finding it deterministically) can still be
expensive in the real system the simulation stands in for, and only a
test that directly counts the thing that's expensive in reality (messages
sent, bytes moved, disk ops issued) — not one that infers it from timing —
catches a regression in it.

A second, narrower finding from building that same test: **the
denominator matters as much as the numerator, and an externally-polled
denominator quietly undercounts.** The first draft of this test polled
the leader's own view of the peer's acked offset once per write round and
counted distinct values observed — a reasonable-looking approach that
turned out to badly underestimate real progress, because a genuine chunk
advance can happen well inside a single millisecond once a transfer is
flowing, and any poll coarser than that misses most of the transitions.
The measured "sends per distinct offset" ratio came out far worse than
reality on **already-fixed** code, which is a dangerous shape of bug in a
test: it looks like a real, reproducible regression signal, but chasing it
leads to tuning a knob that was never actually the problem. The fix was
to stop inferring the denominator externally at all and instead add a
tiny, exact, `#[cfg(test)]`-accessor-style counter *inside* the core
(`RaftCore::snapshot_chunk_advances`, bumped exactly once per genuine
new-offset chunk, at the one place — inside `snapshot_chunk_for` — that
can never miss a transition) — the same "don't infer from the outside
what the code already knows precisely on the inside" principle this
repo's engineering practices already state for production accessors,
applied here to a test's own measurement.
