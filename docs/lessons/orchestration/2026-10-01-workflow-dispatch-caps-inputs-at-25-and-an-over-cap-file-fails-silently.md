# `workflow_dispatch` caps inputs at 25, and an over-cap workflow fails silently

**What happened.** The ADR 0073 P1-D upgrade-restart stack (#1137/#1139) added one
`workflow_dispatch` input per new corpus to `.github/workflows/corpus-deep.yml`,
the convention every earlier corpus followed. `main` was already at exactly 25
inputs, GitHub's hard limit, so the file went to 26 and then 27. GitHub then
rejects the *whole workflow*: every push produced a `corpus-deep.yml` run that
failed instantly with **0 jobs**. Merging would have stopped every nightly deep
tier, not just the new ones. Per-push CI (`ci.yml`) stayed green, so nothing in
the PR's own checks flagged it. A reviewer caught it from the 0-job runs.

**Why it is easy to miss.** A 0-job workflow failure has no failing step and no
log to read. It shows up only as a red run in the Actions list for that workflow,
not as a check on the PR. "Add an input per corpus" was the established pattern,
so copying it looked correct.

**Rule.** Before adding a `workflow_dispatch` input, count them:
`python3 -c "import yaml;d=yaml.safe_load(open('.github/workflows/corpus-deep.yml'));print(len(d[True]['workflow_dispatch']['inputs']))"`
must stay ≤ 25. `corpus-deep.yml` is at the cap. A new corpus hard-codes its
nightly depth in the step's `env` (with a comment saying why) instead of adding an
input. After pushing a workflow change, check that the workflow's own run actually
started jobs, not just that the PR's checks are green.
