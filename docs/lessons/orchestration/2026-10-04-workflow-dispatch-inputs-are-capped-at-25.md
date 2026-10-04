# `workflow_dispatch` inputs are capped at 25 — a merge can silently cross it

**Context (S-08, PR #1175).** `corpus-deep.yml` gives every deep corpus its
own `workflow_dispatch` seed input. Two branches each added one input; each
branch alone was at or under the cap, but the merge of `main` into the S-08
branch produced 26 — and GitHub rejects a workflow with more than 25
dispatch inputs (the whole workflow file becomes invalid, so the nightly
deep tier would simply stop running, with no red per-push gate to say so).

**Rule.** When resolving a merge in a workflow file, count the combined
`workflow_dispatch.inputs` (`python3 -c 'import yaml; d=yaml.safe_load(open(F)); print(len(d[True]["workflow_dispatch"]["inputs"]))'`).
A new knob that does not need per-dispatch tuning (a fast corpus) gets a
fixed depth in the step's `env:` instead of a new input.
