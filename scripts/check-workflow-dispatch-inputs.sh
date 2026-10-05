#!/usr/bin/env bash
# GitHub rejects a workflow with more than 25 `workflow_dispatch` inputs, and
# the whole file becomes invalid: every run of it fails at parse time with no
# job ever starting. `corpus-deep.yml` gives most deep corpora their own seed
# input, so two branches that each add one can cross the cap only once merged
# (see docs/lessons/orchestration/2026-10-04-workflow-dispatch-inputs-are-capped-at-25.md).
# This makes that a per-push failure instead of a silently dead nightly tier.
set -euo pipefail

limit=25
status=0
for f in .github/workflows/*.yml .github/workflows/*.yaml; do
  [ -e "$f" ] || continue
  # Count the keys directly under `on.workflow_dispatch.inputs`.
  n=$(awk '
    /^  workflow_dispatch:/ { in_wd = 1; next }
    in_wd && /^  [^ ]/ { in_wd = 0 }
    in_wd && /^    inputs:/ { in_inputs = 1; next }
    in_inputs && /^ {0,4}[^ ]/ { in_inputs = 0 }
    in_inputs && /^      [A-Za-z0-9_-]+:/ { count++ }
    END { print count + 0 }
  ' "$f")
  if [ "$n" -gt "$limit" ]; then
    echo "error: $f has $n workflow_dispatch inputs; GitHub allows at most $limit." >&2
    echo "       Give a corpus that needs no per-dispatch tuning a fixed depth in its step env instead." >&2
    status=1
  fi
done
exit "$status"
