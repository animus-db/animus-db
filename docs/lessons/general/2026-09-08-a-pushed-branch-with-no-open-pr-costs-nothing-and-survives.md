# A pushed branch with no open PR costs nothing and survives a container loss — push implementation commits before the long gates, not after (2026-09-08)

A container rebuild between sessions lost two fully-written, fully-tested,
never-pushed branches' worth of work outright — the branch, its commits,
and everything in it existed only in the rebuilt-away container's local
`.git`. Re-implementing ADR 0061 rung H, C-08 PR 6 from scratch after this
loss is what this entry itself documents having to do.

**The fix is procedural, not technical, and it was already available**:
CI in this repo runs only on pull requests and on pushes to `main` (root
`CLAUDE.md`'s Commands section) — a branch pushed to `origin` with no PR
opened against it triggers no workflow, costs no CI minutes, and is
invisible to anyone not looking for it, while being fully durable against
exactly this failure mode. There is no reason to hold a working-tree-only
implementation commit back until every gate is green before pushing it
once — the commit already exists locally; getting it onto `origin` costs
one `git push -u origin <branch>` and loses nothing if a later gate finds
a bug (amend and `--force-with-lease` the same branch, same as any other
fixup).

**General rule**: on any task producing a implementation commit worth more
than a few minutes of re-typing, push the branch to `origin` as soon as it
exists and compiles — *before* running the long test/lint gate sequence,
not after. Open the actual PR only once those gates are green. A pushed-
but-PR-less branch is a free checkpoint in any repo with this same
PR-gated CI posture; treat "did I push yet" as a standing question after
every commit on a long task, the same reflex as checking `git status`
before a destructive command.
