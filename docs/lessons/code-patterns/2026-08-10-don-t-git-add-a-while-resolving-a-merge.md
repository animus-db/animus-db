# Don't `git add -A` while resolving a merge

**Don't `git add -A` while resolving a merge** — it can sweep agent worktree
dirs in as embedded git repos. Stage explicit paths; `.claude/worktrees/` is
gitignored to prevent it.
