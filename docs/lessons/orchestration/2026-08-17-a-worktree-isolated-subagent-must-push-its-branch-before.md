# A worktree-isolated subagent must push its branch before reporting the work done or mergeable.

**A worktree-isolated subagent must push its branch before reporting the
work done or mergeable.** An orchestrator (or the user) cannot verify,
review, or recover work that exists only in the agent's local worktree, and
agent worktrees are churned/reclaimed (see the dead-agent entry above) far
more casually than a pushed branch ever is. "Mergeable" is a claim about
the *remote*: until `git push -u origin <branch>` has succeeded, report the
work as in-progress, never done. (The other half of subagent git hygiene —
never `cd` into the main checkout — is the entries above.)
