# A required aggregate check must cover every PR check before you queue early, and path-filtered workflows cannot be required directly

**What happened (2026-11-03).** The maintainer wanted auto-merge enabled the
moment a PR opens. The `main` ruleset required only `gates` and `prod-liveness`,
and the merge queue is ALLGREEN with a 240-minute timeout, so chaos, fuzz, image,
demo-browsers, DCO, `s3-real-endpoint`, `upgrade-previous-release`,
`corpus-changed` and `test-archive` could all be red while the queue merged the
PR. "Wait until everything is green, then enable auto-merge" had been a manual
habit standing in for a missing gate.

**Why not just require them in the ruleset.** A required check that never
reports blocks the PR forever. A workflow with a `paths:` filter does not run
for an unrelated PR, so it never reports. `workflow_call` ignores `paths:`
too, so the fix is a `changes` job that computes the filters with plain
`git diff base...HEAD` (`scripts/ci-changes.sh`), conditional `uses:` calls, and
an aggregate that distinguishes an intentional skip from a failure.

**Rules.**
- One aggregate (`gates`, `if: always()`) lists every PR check in `needs`.
  A `needs` result of `skipped` is a pass only where the skip is intentional
  (wrong event, or the filter output is false); a job is also `skipped` when
  something it needs failed, so everything else must be `success`, and the
  `changes` job itself must be `success`.
- A called workflow's `concurrency.group` must not use `${{ github.workflow }}`
  (it is the caller's name, so it collides with and cancels the caller's own
  group); `github.event_name` and `github.event.pull_request` are also the
  caller's.
- A reusable workflow's jobs cannot request more permissions than the caller
  grants, even skipped ones, so publish jobs (`packages: write`, `id-token`)
  stay in a push-only workflow and the PR build check is its own reusable file.
- Every new PR-level check must be added to the aggregate's `needs` and to its
  result check, or it is advisory.
