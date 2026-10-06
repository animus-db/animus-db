# A real-process roll test must finalize one cluster version step at a time

`cluster finalize` raises the cluster version by exactly one step, but
`/admin/cluster-version`'s `safe_target` is this tree's `MAX_SUPPORTED`. When
main bumped `MAX_SUPPORTED` 2 -> 3 (G-d M1), `upgrade_previous_release` (one
finalize, then "every node observes `safe_target`") timed out with `active`
stuck at 2, on the merge ref only. Walk `active` up to `safe_target` with one
finalize per step instead of assuming a single finalize reaches it. Related:
`2026-10-06-version-bump-stale-hardcoded-max.md`. A cluster-version bump must
run `upgrade-previous-release` locally (`scripts/build-upgrade-from.sh`).
