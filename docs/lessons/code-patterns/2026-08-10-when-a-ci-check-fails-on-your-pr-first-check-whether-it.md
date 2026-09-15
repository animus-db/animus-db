# When a CI check fails on your PR, first check whether it also failed on already-merged PRs — a check that has *never* passed is misconfigured, not a verdict on your change.

**When a CI check fails on your PR, first check whether it also failed on
already-merged PRs — a check that has *never* passed is misconfigured, not a
verdict on your change.** The DCO check failed identically on every PR
(including merged ones) because the workflow invoked `tim-actions/dco` without
its *required* `commits` input (instant `Unexpected end of JSON input`), and —
second layer, only visible after the first fix — the repo's restricted default
token lacked `pull-requests: read` for the commit-listing step (`Resource not
accessible by integration`). Two generalizable checks: (1) an action's
README/`action.yml` lists required inputs — a bare `uses:` of an action that
needs inputs fails on every event; (2) a workflow calling the REST API needs
its permissions declared explicitly under restricted default-token settings.
Verify a workflow fix by letting the PR that changes it exercise itself
(`pull_request` workflows run from the PR's merge ref). (PR #83.)
