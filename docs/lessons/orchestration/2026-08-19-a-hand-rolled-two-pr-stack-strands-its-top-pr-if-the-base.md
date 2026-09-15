# A hand-rolled two-PR stack strands its top PR if the base merges first — use `gh-stack` (or retarget to `main` before merging), and verify the default branch actually received the change (issue #279, 2026-08-19).

**A hand-rolled two-PR stack strands its top PR if the base merges first —
use `gh-stack` (or retarget to `main` before merging), and verify the
default branch actually received the change (issue #279, 2026-08-19).**
The #279 fix shipped as PR #304 based on PR #303's branch, stacked by hand
rather than with the repo's `gh-stack` convention. Both were merged within
18 seconds: #303's branch → `main` first, then #304 → that same branch. The
second merge is a no-op as far as `main` is concerned — its commit landed on
a branch that had already stopped feeding the default branch — so `main` got
#303's `DiskConfig::set_sync_delay` (groundwork) and *not* the driver fix it
was groundwork for, while GitHub cheerfully reported both PRs "Merged".
Every surface lies in this state: both PRs show green and merged, the branch
still exists carrying the work, and only two things give it away —
`git merge-base --is-ancestor <fix-sha> origin/main` returns false, and the
linked issue stays **open** with an empty `closed_by_pull_requests` (a
`Fixes #N` trailer only fires when the commit reaches the default branch,
which is the cheapest available "did this actually land" signal).
**Rules**: build a real stack with `gh-stack` so the tooling retargets the
child when the parent merges; if stacking by hand, retarget the child onto
`main` *before* anyone merges the parent, or merge strictly top-down; and
after any stacked merge, confirm the change is on the default branch rather
than trusting the PR's merged badge.
