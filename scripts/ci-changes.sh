#!/usr/bin/env bash
# Path filters for the PR-only workflows ci.yml calls (chaos, fuzz, image,
# demo-browsers). GitHub's `paths:` filter does not apply to `workflow_call`,
# and a path-filtered workflow cannot be a required check (it never reports on
# an unrelated PR), so ci.yml's `changes` job evaluates the same lists here
# and calls the workflows conditionally; the required `gates` aggregate then
# treats "filter false -> job skipped" as a pass.
#
#   scripts/ci-changes.sh <base-ref>   # prints key=value lines for $GITHUB_OUTPUT
#   scripts/ci-changes.sh --none       # all false (push / merge_group)
#
# Each list mirrors the `paths:` its workflow used to carry as a pull_request
# trigger (chaos.yml / fuzz.yml keep theirs for `push`, image.yml keeps its
# own for `push`): when you change one, change the other.
set -euo pipefail

if [ "${1:?usage: ci-changes.sh <base-ref>|--none}" = "--none" ]; then
  files=""
else
  files="$(git diff --name-only "$1"...HEAD)"
fi

hit() { # hit <ERE> : does any changed file match?
  if [ -n "$files" ] && grep -Eq "$1" <<<"$files"; then echo true; else echo false; fi
}

# image.yml: Dockerfile/Cargo/crates/** minus the paths that cannot reach the
# three release binaries (see image.yml's header for why each is excluded).
image=false
if [ -n "$files" ]; then
  kept="$(grep -Ev '^crates/.+/(tests|benches)/|^crates/animus-(sim|test)/|^crates/animusd/src/sim_cluster[^/]*\.rs$|^crates/[^/]+/CLAUDE\.md$' <<<"$files" || true)"
  if [ -n "$kept" ] && grep -Eq '^(Dockerfile|\.dockerignore|Cargo\.toml|Cargo\.lock)$|^crates/|^\.github/workflows/(image|image-pr-build)\.yml$' <<<"$kept"; then
    image=true
  fi
fi

echo "chaos=$(hit '^crates/animusd/tests/chaos\.rs$|^crates/animusd/tests/chaos_support/|^crates/animusd/Cargo\.toml$|^\.github/workflows/chaos\.yml$')"
echo "fuzz=$(hit '^crates/|^fuzz/|^Cargo\.toml$|^Cargo\.lock$|^\.github/workflows/fuzz\.yml$')"
echo "demo=$(hit '^website/|^crates/animus-web/|^crates/animusd/|^crates/animus-sim/|^Cargo\.toml$|^Cargo\.lock$|^scripts/build-web-demo\.sh$|^scripts/demo-browser-smoke\.mjs$|^\.github/workflows/demo-browsers\.yml$')"
echo "image=${image}"
