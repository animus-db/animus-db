# When an agent dies (API overload/stall/error), inspect its worktree before re-launching

**When an agent dies (API overload/stall/error), inspect its worktree before
re-launching** — its partial work is often intact and finishable (or resumable
via `SendMessage`); a lost worktree means redo. **Don't thrash re-launches
during an API overload** — wait for it to ease.
