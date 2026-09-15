# A workflow triggered on both bare `push:` and `pull_request:` runs twice per commit on every same-repo PR branch, and every workflow wants an explicit `concurrency` group.

**A workflow triggered on both bare `push:` and `pull_request:` runs twice
per commit on every same-repo PR branch, and every workflow wants an
explicit `concurrency` group.** GitHub fires both events for a branch that
lives in the repo the PR targets, so `ci.yml`'s two jobs (`gates` +
`prod-liveness`) were being run twice over the same tree for an identical
verdict — pure duplicate load on the 2-vCPU shared runners this repo is
already timing-constrained by. Scope `push:` to `branches: [main]` and let
`pull_request:` cover everything under review; the `main` runs are still
worth keeping (the README badge reads them, and they cover a commit that
reaches `main` without a PR). The tradeoff to know about: a branch pushed
with no PR open now gets no CI until the PR exists. Separately, without a
`concurrency:` block a push train leaves N runs racing on the same branch,
so every workflow declares one: key on `${{ github.workflow }}-${{
github.ref }}` (a `pull_request` event's `github.ref` is that PR's own
`refs/pull/N/merge`, so this is already per-PR), and set
`cancel-in-progress` per *event*, not per workflow — `${{
github.event_name == 'pull_request' }}` cancels superseded PR runs while
leaving `main` pushes to queue and each merged commit to get its own
verdict. For a long scheduled job (`corpus-deep`) cancel nothing: a
nightly that never reports is a silent gap in the record, so an on-demand
dispatch queues behind it instead.
