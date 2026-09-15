# The shell's cwd can silently drift across worktrees mid-session

**The shell's cwd can silently drift across worktrees mid-session** (the
watchdog-stall/resume entry above is one confirmed mechanism), so any
command whose *meaning* depends on which tree it runs in — `git status`/
`diff`/`add`/`commit`, `cargo build`/`test`, anything resolving relative
paths — should be fused as `cd <worktree-abs-path> && pwd && <command>` in
a single invocation: the explicit `cd` re-anchors the command no matter
what the ambient cwd has drifted to, and the `pwd` echo leaves proof in
the transcript of where it actually ran. A bare `<command>` that trusts
the ambient cwd is the thing that silently runs against the wrong tree.
