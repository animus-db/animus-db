# `continue-on-error` only routes around a step that *fails* — an unbounded step that hangs and loses its runner still takes the whole job down with it (`corpus-deep.yml`, issue #772)

The fix above (`continue-on-error: true` + per-step `id:` + `if: always()`
aggregation) assumes every step eventually *returns*, pass or fail. It says
nothing about a step that never does. On 2026-09-08 (run 34202959222) the
DynamoDB-wire corpus thrashed for 36 minutes (its usual passing time is
~7), the hosted runner was lost mid-step, and the whole job died with
neither "Report corpus results" nor the red-nightly-issue step ever
running — no report, no issue, the exact silent gap `continue-on-error`
was built to prevent (issue #554 above), just from a different cause: a
step with no upper bound on its own runtime, rather than a step that fails
fast. The fix is a per-step `timeout-minutes` on every `continue-on-error`
step, sized from observed passing durations (generous enough that a real
2-3x slowdown still passes, tight enough to cut a hang well short of the
job's default 6-hour ceiling): a step that times out is recorded
`cancelled` — a non-"success" outcome, same as any other failure for the
aggregation step — so the job proceeds to the remaining steps and the
report/issue-filing machinery gets to run and name the stuck corpus,
instead of the run just disappearing. Any `continue-on-error` step list
needs both halves — the outcome aggregation *and* a bound on each step's
own runtime — to actually guarantee every member gets a chance to report.
