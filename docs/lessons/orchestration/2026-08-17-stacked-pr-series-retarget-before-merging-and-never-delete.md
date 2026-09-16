# Stacked-PR series: retarget before merging, and never delete a branch that is still some open PR's base.

**Stacked-PR series: retarget before merging, and never delete a branch
that is still some open PR's base.** Two gotchas that recur when landing a
`gh-stack` series bottom-up: (1) before merging PR N+1, confirm its base
has been retargeted onto the branch its diff should be measured against
(once PR N merges, N+1's base must move to N's own base) — merging against
a stale base lands the remaining stack's commits in one PR, or shows a
reviewer a diff full of already-landed work; (2) deleting a merged PR's
branch while a later PR in the stack still names it as base can close that
open PR or corrupt its diff — delete stack branches only after the entire
series has landed.
