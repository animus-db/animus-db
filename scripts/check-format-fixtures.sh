#!/usr/bin/env bash
# ADR 0073 Phase 0 guard: golden format fixtures are append-only.
#
# Every format's compatibility story rests on its checked-in fixtures under
# `**/tests/fixtures/formats/**` never changing meaning once landed (ADR
# 0073's Phase 0 conventions: "an existing checked-in fixture is never
# edited or deleted"). A human or an agent editing a fixture in place instead
# of adding a new version file would silently defeat the whole mechanism —
# this script makes that a hard CI failure instead of a code-review hope.
#
# What counts as "existing": any file already present under that glob at the
# merge base with `origin/main`. A brand-new file (a genuinely new format
# version) is always fine. A no-op pass is correct and expected until the
# first Phase 0 workstream actually adds a fixtures directory.
#
# Usage: scripts/check-format-fixtures.sh
# Exit status: 0 if no existing fixture was modified or deleted, 1 otherwise.

set -euo pipefail

# Not a git checkout (shouldn't happen in CI, but keep this script inert
# rather than exploding if ever run somewhere odd).
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "check-format-fixtures: not inside a git work tree, skipping" >&2
  exit 0
fi

# Resolve the merge base against origin/main. Fall back gracefully when
# origin/main isn't available locally (e.g. a shallow clone/scratch commit
# with no remote configured) by trying a plain `main`, and otherwise skip
# rather than fail the guard on missing ref information — this script's job
# is to catch fixture edits, not to enforce that a particular remote exists.
base_ref=""
for candidate in origin/main main; do
  if git rev-parse --verify --quiet "${candidate}" >/dev/null; then
    base_ref="${candidate}"
    break
  fi
done

if [ -z "${base_ref}" ]; then
  echo "check-format-fixtures: no origin/main or main ref found, skipping" >&2
  exit 0
fi

merge_base="$(git merge-base HEAD "${base_ref}" 2>/dev/null || true)"
if [ -z "${merge_base}" ]; then
  echo "check-format-fixtures: no merge base with ${base_ref}, skipping" >&2
  exit 0
fi

# Every fixture path that existed at the merge base. Filtered with a plain
# grep rather than a git pathspec glob: git's own `**` pathspec magic needs
# `:(glob)` to cross path separators and is easy to get subtly wrong, where
# a grep on a literal `/tests/fixtures/formats/` substring is unambiguous
# and matches this convention at any crate depth.
fixture_pattern='(^|/)tests/fixtures/formats/'
existing_fixtures="$(git ls-tree -r --name-only "${merge_base}" 2>/dev/null | grep -E "${fixture_pattern}" || true)"

if [ -z "${existing_fixtures}" ]; then
  echo "check-format-fixtures: no fixtures directory at merge base, nothing to check"
  exit 0
fi

# Diff HEAD against the merge base, restricted to that same set of paths.
# `--diff-filter=MD` catches Modified and Deleted; a Renamed/Copied fixture
# is treated as a delete-of-the-old-name (also disallowed: a fixture's own
# path, including its version number, is part of its identity).
violations="$(git diff --name-only --diff-filter=MD "${merge_base}" HEAD 2>/dev/null | grep -E "${fixture_pattern}" || true)"

if [ -n "${violations}" ]; then
  echo "check-format-fixtures: existing format fixtures were modified or deleted:" >&2
  echo "${violations}" >&2
  echo >&2
  echo "ADR 0073 Phase 0: a checked-in format fixture is append-only." >&2
  echo "A format change adds a NEW version file; it never edits or removes" >&2
  echo "an existing one. See docs/adr/0073-upgrade-compatibility.md." >&2
  exit 1
fi

echo "check-format-fixtures: OK (no existing fixture modified or deleted)"
exit 0
