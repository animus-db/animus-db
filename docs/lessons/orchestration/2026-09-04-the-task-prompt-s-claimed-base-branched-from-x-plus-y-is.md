# The task prompt's claimed base ("branched from X plus Y") is not guaranteed true of the assigned worktree — verify it against `git log` before doing any research, and recover with a fast-forward merge, not by reading a different checkout (U-04, 2026-09-04).

**The task prompt's claimed base ("branched from X plus Y") is not
guaranteed true of the assigned worktree — verify it against `git log`
before doing any research, and recover with a fast-forward merge, not by
reading a different checkout (U-04, 2026-09-04).** A task described its
worktree as "origin/main plus wave-2 items already integrated," including
a specific dashboard tab and a wire-decoder hardening the new work
depended on. `git log --oneline -5` on the actual worktree showed plain
wave-1 `origin/main` — no wave-2 commits at all — yet several `Read`/
`Bash` calls against a *bare*, non-worktree-prefixed path (the shared
checkout root the "never hardcode the main checkout's path" entries above
warn about) returned content that *did* match the prompt's claim, because
that sibling checkout happened to have a different, more-advanced branch
checked out. Trusting those reads produced a design built against code
that did not exist on this worktree's branch and never would without
intervention — a different failure than the documented "silently
edited/read the wrong tree" pattern above: here the *assigned* worktree
itself was genuinely behind the prompt's stated base, and the bare-path
reads were actively misleading rather than merely off-target. Caught by
running `pwd` after an unrelated tool refusal, then diffing what a
worktree-prefixed `ls`/`grep` showed against the bare-path reads for the
same nominal file. **Fix, safely**: `git log --oneline --all | grep
<a distinctive commit subject from the missing work>` found the wave-2
commits reachable via a local branch ref (`git branch -a` showed it
checked out elsewhere, marked `+`) even though they weren't on this
worktree's `HEAD` ancestry; `git merge-base --is-ancestor HEAD
<that-branch>` confirmed the assigned branch was a strict ancestor (a
clean fast-forward, not a real merge — no divergent history to reconcile);
stashed the in-progress uncommitted edit (`git stash push -u`), ran `git
merge --ff-only <that-branch>`, then `git stash pop` (auto-merged cleanly
since the stashed edit touched a region the fast-forward hadn't). Re-ran
every earlier research step against the now-correct, worktree-scoped
files before continuing — a design built on stale premises needs
re-verification, not just a rebase. **Never** substitute "read it from the
bare checkout since it has the content I need" for actually getting the
assigned worktree to the right state — that checkout's commits may never
reach the worktree's branch at all (a sibling agent's unrelated,
never-to-be-merged work), and even when they coincidentally do, nothing
in that path's history is provably reachable from `HEAD` without checking
first.
