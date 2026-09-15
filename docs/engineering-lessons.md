# Engineering lessons (living — keep this current)

This is the repo's institutional memory, moved out of the root `CLAUDE.md`
(which stays a thin, always-loaded entry point) so it can keep growing
without weighing down every session. As of 2026-09-14 it is split **one file
per entry** under [`docs/lessons/`](lessons/), so that concurrent PRs adding
lessons stop conflicting on a single append-only file — see Layout below.
The standing instruction in the root `CLAUDE.md` still governs the practice
itself: whenever you — human or agent — discover a non-obvious lesson,
gotcha, or better way of working *during a task*, **add a file under
`docs/lessons/<section>/`, with the *why*, in the same change**.

> **Note on deleted subsystems (2026-08-23).** Entries throughout this log cite
> **Accord** (`animus-consensus`, `AccordNode`/`AccordCore`, the
> `animus-test` Accord corpus in `tests/support/` + `corpus.rs` +
> `elle_accord.rs`) and the per-table **`ReplicationMode`** seam. All of that
> was **deleted** by [ADR 0019](adr/0019-cp-only-v1-defer-ap.md)'s 2026-08-23
> amendment — with CQL dropped (ADR 0053), DynamoDB's wire cannot express a
> replication mode, so AP became unselectable and Accord's remaining role
> vacuous. Those citations are kept deliberately and read as **historical**:
> the lessons they carry are general (checker teeth and workload design,
> collapsing a total order into one `u64`, composing rather than reshaping a
> proven core, one `Env`/inbox/WAL per hosted protocol instance) and apply
> directly to the surviving corpora — the CP raftkv corpus (ADR 0017) and the
> multi-tablet transaction corpus (ADR 0018). They are **not** moved to the
> archive, because unlike a superseded *lesson* the lesson here still stands;
> only its illustration is gone. The code is retrievable from git history if a
> citation needs chasing.

## Layout

Every lesson is its own file, named `YYYY-MM-DD-<slug>.md` (the date it was
recorded), whose first line is `# <title>` followed by the lesson body.
Filenames sort chronologically within a directory, oldest first. Entries
live under one of five directories:

| Directory | Holds |
|---|---|
| `docs/lessons/testing/` | Testing lessons |
| `docs/lessons/code-patterns/` | Code-pattern lessons |
| `docs/lessons/orchestration/` | Parallel-agent orchestration lessons |
| `docs/lessons/general/` | Chronological lessons that don't fit one of the three sections above |
| `docs/lessons/archive/` | Superseded entries — the mechanism they describe has been deleted or replaced, kept for historical record |

**To add a lesson: add a new file under the right directory, and edit
nothing else.** There is deliberately no index file enumerating entries —
that is the entire point of the split: two agents adding lessons in
parallel never touch the same line, so their PRs never conflict with each
other over this log. Read the directory relevant to your task before
starting non-trivial work; `rg <term> docs/lessons/` is how you search it —
grep it when debugging anything that feels like it might have happened
before.

**To archive an entry** whose specific mechanism has since been deleted or
replaced, `git mv` its file into `docs/lessons/archive/` verbatim (unedited)
— the *bug* it documents and the lesson drawn from it remain part of this
project's institutional memory even once the mechanism is gone. Where the
lesson is still generally applicable elsewhere, leave a one-line pointer
back to the archived file from wherever it still applies; otherwise the
move alone is enough.

Code comments, crate `CLAUDE.md` files, and ADRs written before the split
cite this log with sentences like "see `docs/engineering-lessons.md` on
X" — those still resolve, just via `rg X docs/lessons/` instead of a
location in one file.
