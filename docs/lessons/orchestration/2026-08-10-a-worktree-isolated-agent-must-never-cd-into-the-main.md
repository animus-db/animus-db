# A worktree-isolated agent must never `cd` into the main checkout — even once, even to "just look."

**A worktree-isolated agent must never `cd` into the main checkout — even
once, even to "just look."** The harness already starts the agent's Bash
tool in its own worktree's directory; a `cd /path/to/main/checkout && ...`
prefix is pure reflex (muscle memory from non-worktree sessions), not
something the task ever required, and every subsequent command in that
shell then runs — and, if the agent commits, *commits* — against the
user's real local `main`, not the isolated branch it was supposed to be
building. This happened once in the ADR 0037 stack (a prior agent's stray
commit landed on the user's main and had to be found and dealt with
separately from the actual PR work). The correct discipline is simpler
than remembering not to `cd`: never construct a command with a `cd`
prefix pointing outside the current working directory at all — if a path
needs to be absolute, write the absolute path directly into the command
(`git -C "$(pwd)" ...` or, more simply, no `-C`/`cd` at all, since the
tool's cwd is already correct) rather than reaching for `cd first-dir &&`.
Separately: a tool that reads files by absolute path (e.g. `Read`) is
**not** guaranteed to be worktree-scoped the way the Bash tool's cwd is —
hardcoding the *main checkout's* path (as opposed to the assigned
worktree's path) in a `Read`/`Edit` call silently reads/writes the wrong
copy with no error (for `Read`) or a clear refusal (for `Edit`/`Write`,
which do check). Confirm `git rev-parse --show-toplevel` once at the start
of a session and reuse *that* prefix for every absolute path, rather than
assuming the repo's conventional root path is the same as the assigned
worktree.
