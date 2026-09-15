# A doc appendix/amendment written and gated against one commit, then rebased onto a sibling PR that landed in between, carries stale "expected N passed" baselines that a clean rebase won't catch (2026-09-09, ADR 0061 rung I C-09 PR 5).

**A doc appendix/amendment written and gated against one commit, then
rebased onto a sibling PR that landed in between, carries stale
"expected N passed" baselines that a clean rebase won't catch
(2026-09-09, ADR 0061 rung I C-09 PR 5).** PR 5 was authored and its own
gate numbers predicted on top of PR 3's tip (420 `sim_cluster` tests),
under a hard no-`cargo` constraint; PR 4 (admin/console residue) landed
in the meantime and moved the real baseline to 424. Rebasing PR 5 onto
PR 4's tip only conflicts where both PRs' prose literally collides
(here: three doc files' appended sections) — it does **not** flag that
PR 5's own "424 total = PR 3's 420 + my 4" arithmetic, quoted in prose
untouched by the conflict, now needs to read "428 = PR 4's 424 + my 4".
A merge that keeps both sides' text in order is necessary but not
sufficient: after resolving structural conflicts, grep the merged
doc(s) for every predicted/expected count that names a baseline
("PR 3's own N", "up from N", "N passed") and recompute it against the
new base before trusting the doc — then confirm by actually running the
gate, which is what caught this one (428 passed, matching the corrected
prediction, not the stale 424 the unedited prose would have kept).
