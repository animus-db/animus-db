#!/usr/bin/env bash
# Build the pinned previous-release reference binary "R-1" (ADR 0073 Phase 3,
# D10) from source and print the path of its `animusd` on stdout (everything
# else goes to stderr), for `ANIMUS_UPGRADE_FROM_BIN`.
#
#   scripts/build-upgrade-from.sh            # build (or reuse) the pinned R-1
#   scripts/build-upgrade-from.sh --sha      # print only the resolved commit SHA
#
# The pin is `scripts/upgrade-from.txt` (a tag or a commit SHA; the release
# checklist moves it). The binary is cached by the resolved commit SHA under
# $ANIMUS_UPGRADE_FROM_CACHE (default ~/.cache/animus-upgrade-from), so a
# second call is instant and CI can cache that one directory. The build uses a
# throwaway `git worktree` with its own target dir, deleted afterwards; only
# the binary is kept. The ref's own `Cargo.lock` is honoured (`--locked`).
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"
pin_file="scripts/upgrade-from.txt"
ref="$(grep -v '^[[:space:]]*#' "$pin_file" | grep -v '^[[:space:]]*$' | head -n1 | tr -d '[:space:]')"
[ -n "$ref" ] || { echo "error: $pin_file names no ref" >&2; exit 1; }

if ! sha="$(git rev-parse --verify --quiet "${ref}^{commit}")"; then
  echo "fetching $ref ..." >&2
  git fetch --quiet origin "$ref" "refs/tags/$ref:refs/tags/$ref" 2>/dev/null || git fetch --quiet origin "$ref" || true
  sha="$(git rev-parse --verify "${ref}^{commit}")" || {
    echo "error: cannot resolve $ref (use a full clone: fetch-depth 0)" >&2
    exit 1
  }
fi
if [ "${1:-}" = "--sha" ]; then
  echo "$sha"
  exit 0
fi

cache="${ANIMUS_UPGRADE_FROM_CACHE:-$HOME/.cache/animus-upgrade-from}"
out="$cache/$sha/animusd"
if [ -x "$out" ]; then
  echo "reusing cached R-1 $ref ($sha)" >&2
  echo "$out"
  exit 0
fi

src="$cache/src-$sha"
mkdir -p "$cache/$sha"
rm -rf "$src"
git worktree prune
git worktree add --detach --quiet "$src" "$sha"
cleanup() {
  git worktree remove --force "$src" >/dev/null 2>&1 || rm -rf "$src"
  git worktree prune
}
trap cleanup EXIT
echo "building R-1 $ref ($sha) ..." >&2
(
  cd "$src"
  CARGO_TARGET_DIR="$cache/target-$sha" cargo build -p animusd --bin animusd --locked >&2
)
cp "$cache/target-$sha/debug/animusd" "$out"
rm -rf "$cache/target-$sha"
echo "built R-1 $ref ($sha)" >&2
echo "$out"
