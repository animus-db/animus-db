# A crate `CLAUDE.md` that narrates its own PR history accretes without bound — the fix is to keep only what a fresh reader can't get elsewhere, not to keep everything that was once true.

**A crate `CLAUDE.md` that narrates its own PR history accretes without
bound — the fix is to keep only what a fresh reader can't get elsewhere,
not to keep everything that was once true.** By 2026-08-13,
`animus-control`/`animusd`/`animus-cp-data`'s guides had each grown past
~60–100K chars (`animusd` past the ~40K threshold this repo treats as the
memory-file warning tripwire) by accumulating three things that are all
cheaply *derivable* rather than load-bearing: (1) **test-file rosters** —
a paragraph per `tests/*.rs` restating what a `ls tests/` plus the file
name already tells you; (2) **PR-by-PR changelog narration** — "PR3
shipped X; PR4 closed X's gap with Y; PR5's own amendment then found Y
had a residual case Z" — which is exactly what `git log`/the ADR's own
PRn amendments already record, just harder to grep; (3) **method-by-
method API dumps** — a bullet per public function restating its
signature and one-line behavior, which the function's own doc comment
already says at the source. None of the three is *wrong*, but keeping
them in the guide means every future PR narrates its own history a
fourth time (in the ADR, in the commit message, in the PR description,
*and* in the guide), and the guide's actual load-bearing content —
invariants, gotchas, "never do X" — gets buried in restatement a reader
has to wade through every time. Fix: cut the roster/changelog/dump
material down to a current-state contract + a one-line pointer to the
ADR file or `tests/` directory; keep every invariant, embedded gotcha,
and prohibition verbatim (compressing prose is fine, dropping the actual
claim is not). Trimmed `animus-control/CLAUDE.md` 62.8K→40.0K,
`animusd/CLAUDE.md` 102.8K→54.1K, `animus-cp-data/CLAUDE.md`
99.8K→49.3K with no loss of the invariants/gotchas a fresh agent needs;
see each file's git history for the before/after diff shape. General
rule: when a guide file is this large, the question to ask before
writing a new paragraph into it isn't "is this true" but "is this
something only this file can tell a reader" — if `git log`, the ADR, or
the source's own doc comment already says it, point there instead of
restating it. (2026-08-13.)
