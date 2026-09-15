# General form (mechanism superseded): before implementing an optimization whose soundness rests on an invariant stated only in a summary doc (a `CLAUDE.md` gotcha bullet, a one-line ADR recap), grep the actual type/field the invariant is about and read its own doc comment — the summary is a pointer, not the source, and it can lag the code by exactly as long as nobody happened to need that bullet to be right.

**General form (mechanism superseded): before implementing an
optimization whose soundness rests on an invariant stated only in a
summary doc (a `CLAUDE.md` gotcha bullet, a one-line ADR recap), grep the
actual type/field the invariant is about and read its own doc comment —
the summary is a pointer, not the source, and it can lag the code by
exactly as long as nobody happened to need that bullet to be right.**
Generalizes the existing "before implementing a 'close this documented
gap' task, grep the code" rule (root `CLAUDE.md`) to invariants, not just
missing-feature claims. Originally learned investigating (and rejecting)
a proposed version-floor optimization on the now-deleted copy-based
split-build driver; see `docs/engineering-lessons-archive.md`'s "The
copy-based split-build driver" section for the full incident.
