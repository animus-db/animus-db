# A worktree-isolated agent must never hardcode the main checkout's path, even for a `Read` — recurred (plan-syskv-ui / ADR 0038 PR6, 2026-08-10), with a refinement to "a clear refusal for `Edit`/`Write`" above: that refusal is not reliably active from the very first tool call of a session.

**A worktree-isolated agent must never hardcode the main checkout's path,
even for a `Read` — recurred (plan-syskv-ui / ADR 0038 PR6, 2026-08-10),
with a refinement to "a clear refusal for `Edit`/`Write`" above: that
refusal is not reliably active from the very first tool call of a
session.** Mid-session, after an
infrastructure-watchdog stall and resume, this agent's assigned worktree
path silently changed out from under it (env block said one path at
session start; the Bash tool's actual cwd for every call turned out to be
a *different* worktree entirely, discovered only via `pwd` + `git status`
after the resume). Several `Read`/`Edit` calls in between had used bare
`/home/guillaume/Code/animus-db/...` paths (no worktree segment at all) —
and **succeeded**, silently editing the shared main checkout, including
two `Edit` calls that added real content (not just a `Read`). Only a
*later* `Edit` attempting to *revert* that same file was refused with
"This session is now isolated in \<worktree\>; edit the worktree copy of
this file instead" — meaning the guard exists and does fire, but not from
the start of every session or across every call in one, so "Edit/Write
refuses" is not a safety net an agent can lean on; only session-start
verification is. **Recovery when this is discovered after the fact**:
`git diff` the polluted shared-checkout file to confirm every hunk is
really yours (here, confirmed via `git -C <main> diff -- <file>` showing
exactly the two intended additions, nothing else); if an *unrelated*
agent's own uncommitted edits are *also* present in the same shared
checkout (they were here — a sibling agent's in-flight `heartbeat_loop`
rework, and an untracked test file), leave those alone entirely and only
attempt to revert your own file. If the harness then refuses even a
read-only `git show`/`git diff`/`git checkout --` against that path (it
did, for every git subcommand, not just mutating ones, once the guard was
active), you cannot self-heal the pollution via git or `Edit`/`Write`
either — re-apply your intended change fresh in the correct worktree
(confirmed via `pwd` immediately before this recovery, not trusted from
the session's opening env block) and say so plainly in the final report;
do not claim the shared checkout was cleaned up when the tooling itself
prevented it.
