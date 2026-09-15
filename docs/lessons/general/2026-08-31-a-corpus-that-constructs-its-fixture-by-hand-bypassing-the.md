# A corpus that constructs its fixture by hand, bypassing the real proposer, keeps exercising a superseded code path even after the proposer's own invariant changes — and a convergence-tolerant assertion can silently absorb the resulting slowdown (ADR 0062 rung 4, fork-first in-place split)

Implementing ADR 0062 rung 4 (`animusd::ClientCtx::trigger_split`'s
`InPlace` arm stops calling `split_child_placement`; both children get the
parent's own current `replicas` verbatim) also dropped the learner union
from `animus-cp-data`'s `bootstrap_voters` capture at `SplitTablet`'s apply
(`core_guard.config()` only, no more `.extend(core_guard.learners())`) —
`host.rs`'s Stage 1/2 learner-recruitment machinery itself is untouched
(its deletion is rung 5's job). The expectation going in, per the ADR's own
"Testing plan" section, was that `animus-cp-data/tests/
inplace_split_reconciler.rs`'s existing corpus — built around scenarios
that hand-construct an `InPlaceSplitIntent` with placement-disjoint child
homes (a node not among the parent's own current replicas) — would need
real rework: that premise is exactly what rung 4 makes impossible through
the real proposer. It did not fail at all, at `ANIMUS_INPLACE_SPLIT_SEEDS`
up to 30.

The reason is two independent facts compounding: (1) every scenario in
that file builds its `InPlaceSplitIntent`/`MetadataView` **directly**,
never through `trigger_split` — so `host.rs`'s Stage 1/2 (`AddSplitLearner`
recruiting the disjoint node as a real parent learner, catching it up, then
forking) runs exactly as it always did, completely unaffected by a
proposer-side computation change three call layers away it never goes
through; (2) removing the learner union from `bootstrap_voters` doesn't
strand the recruited node — it just means that node, having already
locally cloned the child's engine at fork time (as one of "every fork
participant"), is no longer *born* a voter of the child; it converges into
one anyway via the ordinary, unmodified Stage 5 `Reconfigure` (add-learner
→ catch-up → promote) after cutover, one extra hop later. Since the
scenario's own `converge()` helper polls to a bounded budget rather than
asserting on the exact tick something happens, that one extra hop is
invisible to the assertions — the scenario still reaches the identical
final converged state it always did, just slightly slower in virtual ticks
nobody was checking.

**General lesson**: before predicting that a semantic change to a
*proposer's* computation will break a corpus, check whether that corpus
actually calls the proposer at all, or whether — like this one, which
exists specifically to test the reconciler in isolation from
`animus-control`/`animusd` — it hand-builds the intermediate state the
proposer would have produced and hands it straight to the pure decision
function. A corpus built that way is testing the **consumer** of an
invariant, not the invariant's own production, and stays green regardless
of how the real producer's logic changes, right up until someone deletes
the consumer-side mechanism it was built around (which is a later rung's
job here, not this one's). And separately: a convergence-polling assertion
(the right shape for an eventually-consistent property, per this repo's
own "eventual properties get a converged-or-timeout poll" rule) is
*blind* to a regression that only adds latency within its own budget — if
a change is expected to remove a fast path and add a hop, that specific
claim needs its own timing-aware assertion (tick count, a tighter budget,
or an explicit intermediate-state check) rather than trusting the same
poll that already tolerated the fast path to also catch its absence.
