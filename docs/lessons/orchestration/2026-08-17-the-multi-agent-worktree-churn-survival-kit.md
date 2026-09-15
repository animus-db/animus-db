# The multi-agent worktree-churn survival kit.

**The multi-agent worktree-churn survival kit.** When several
worktree-isolated agents run in parallel, the individual failure modes
above compound, and the defenses are cheap enough to be standing practice:
(1) give every agent explicit absolute paths (its worktree root, the files
it owns) in its prompt rather than letting it derive them; (2) have agents
push checkpoints — push after each meaningful commit, not only at the end
— so a dead agent's work survives its worktree; (3) verify any PR an agent
reports creating actually exists (`gh pr view <N>`) — a report of "opened
PR #N" can outlive a failed creation; (4) poll on silence — an agent gone
quiet may be stalled or dead, so inspect its worktree and pushed-branch
state instead of waiting indefinitely.
