# A threshold-based "caught up" predicate measured as an absolute gap (`last_index - match_index <= threshold`) cannot distinguish "genuinely replicated" from "the log itself is short"

**A threshold-based "caught up" predicate measured as an absolute gap
(`last_index - match_index <= threshold`) cannot distinguish "genuinely
replicated" from "the log itself is short"** — found writing ADR 0058
Train 1's reconciler-adoption corpus
(`animus-cp-data/tests/reconciler_corpus.rs`,
`tests/learner_reconfigure.rs`). A test meaning to catch a newly-added
learner "still mid-catch-up" (e.g. to prove it survives a partition, or
that the old quorum keeps committing without it) partitioned the learner
immediately, then ticked the reconciler several times before asserting —
and the assertion failed, because on a log only a few entries long, a
learner with `match_index = 0` still satisfies `last_index - 0 <=
RECONFIGURE_LEARNER_CATCH_UP_THRESHOLD` (4) and gets promoted anyway,
despite having received exactly zero `AppendEntries`. This is not a bug
in `learner_caught_up` (the primitive is documented and used as designed
— a fixed absolute threshold, not a fraction of the log) — it is a
property of the design that every test exercising "still catching up"
must account for: either grow the log well past the threshold *before*
the fault (so a genuinely-unreplicated learner's gap stays provably
large regardless of how short the log started), or assert immediately
after the single tick that performs the add (a promotion cannot happen
in the same call that proposes the add, so the state right after is
unambiguous regardless of log length). **General rule**: when a
liveness/catch-up gate is an absolute distance rather than a ratio,
don't assume "hasn't replicated anything" and "gap is small" are the
same condition in a test fixture — they coincide only once the log is
long enough, and a fixture's own small scale can silently violate that
precondition.
