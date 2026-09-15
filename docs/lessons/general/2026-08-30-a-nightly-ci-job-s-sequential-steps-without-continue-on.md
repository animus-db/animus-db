# A nightly CI job's sequential steps without `continue-on-error` let the first failure hide every corpus after it (`corpus-deep.yml`)

`corpus-deep.yml` ran all ten deep-seed fault-injection corpora as
sequential steps inside one job, with no `continue-on-error` and no
`if: always()` on any step. GitHub Actions' default behavior for a
plain sequential step list is: the first failing step aborts the job and
skips every step after it. Since the multi-tablet transaction corpus
(ADR 0018) happened to be listed first and was failing a real assertion at
nightly depth (`ANIMUS_TXN_SEEDS=40`, `check_kind_consistency` on
`compound_lossy_and_anchor_kill` — see the sibling lesson on that bug once
filed), every one of the other nine corpora was silently skipped on every
single nightly run since the workflow's creation — 13 consecutive runs,
all reporting "failure" for the same first-step reason, without a single
one of the other nine corpora ever actually executing at depth. The
workflow's own `conclusion: failure` status looked like a signal, but a
human (or an agent) glancing at "corpus-deep is red" would reasonably
assume the *listed* failing step was the whole story, not that nine other
tests never ran at all — the gap was invisible without opening the job's
step list and noticing the `skipped` conclusions.

The general lesson: a CI job whose value comes from *reporting on N
independent things* (here, ten unrelated corpora) must not let any one of
them gate whether the others get a chance to report. `continue-on-error:
true` + a per-step `id:` + a final `if: always()` aggregation step that
turns the recorded per-step outcomes back into one job-level pass/fail is
the fix — CI still goes red on a real regression, but only after every
corpus has actually run and had its result recorded, so `skipped` never
silently substitutes for a genuine result. Worth checking any other
multi-step job in this repo's CI for the same pattern before assuming its
"only some things are failing" read of a red run is complete.
