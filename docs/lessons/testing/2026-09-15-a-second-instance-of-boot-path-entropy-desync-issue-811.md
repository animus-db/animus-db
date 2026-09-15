# A second confirmed instance of "boot-path entropy desyncs fixed-seed corpus tests" — issue #811, not #899

A second, independent confirmation of the pattern
`docs/lessons/testing/2026-09-15-boot-path-entropy-desyncs-fixed-seeds.md`
already documents, found validating issue #667's fix against
`ANIMUS_CONTROL_SEEDS=30`: `control_corpus_is_convergent_and_durable`'s
`apply_task_stop_restart_under_load_late_3_s24` scenario (seed
`16076149617194574047`) livelocked (unbounded 100% CPU, never converging)
on this branch (issue #667's boot-time cluster-check changes, plus PR
#901's own compaction-defer fix) but passes in 0.24s on a clean checkout
of plain `origin/main` at the identical commit those changes are based on
and the identical named seed, confirmed 3 times.

Applying the existing lesson's own rule 1 (rather than assuming the new
code introduced the livelock): the actual root cause is a real,
independently-tracked, already-diagnosed bug — issue #811, the
`meta_apply_and_compact` "eligible is not done" apply-loop livelock, fixed
by PR #905 (a `did_work` gate) — that issue #667's own extra boot-path
network traffic/entropy draws (`begin_cluster_check`'s probe round) merely
shifted this specific named seed's random draws into the narrow timing
window that trips it. Confirmed directly: merging PR #905 into this
branch makes the identical isolated seed pass in 0.24s, and the full
30-seed corpus (previously unable to complete) runs clean in 102.65s.

**The addition to the existing lesson's rule 1**: "reproduce with an inert
perturbation" is one way to confirm a pre-existing bug is being exposed,
not caused — but checking out the *unmodified baseline itself* at the
identical seed is more direct when a real (non-inert) behavior change is
the suspected perturbation source, and is worth doing before assuming the
inert-perturbation experiment is the only diagnostic available. It also
answers a second, otherwise-unaskable question for free: whether the
baseline's own nightly corpus (a materially different seed depth/schedule
than any one named seed) is independently exposed to the same bug — here,
confirmed no, at this specific seed, though the underlying issue #811 bug
remains real and unfixed on `main` until PR #905 lands there too.
