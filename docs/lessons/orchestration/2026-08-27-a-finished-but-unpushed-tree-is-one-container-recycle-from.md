# A finished-but-unpushed tree is one container recycle from gone — commit per logical unit AND push the WIP branch after every commit, not just at the end (2026-08-27, ADR 0059 Train 3 PR②).

**A finished-but-unpushed tree is one container recycle from gone — commit
per logical unit AND push the WIP branch after every commit, not just at
the end (2026-08-27, ADR 0059 Train 3 PR②).** A first attempt at this
exact task (`RestoreTableToPointInTime`) ran for a long single session,
accumulated the full catalog/wire/replay-driver/e2e implementation, and
was lost in its entirety to a container recycle before a single commit
landed — every finding, every fix, every test had to be redone from
scratch by the next session. The fix is procedural, not technical, and
costs almost nothing: split the work into its natural logical units
(catalog+validation; replay mechanism; wire+driver; corpus; docs) *as a
task-planning decision made up front*, and after each one, commit **and**
`git push` immediately — including WIP-quality commits on a branch with
no PR yet. A branch pushed to the remote survives a container recycle;
an uncommitted working tree does not. This session split the redo into
exactly two implementation commits plus one corpus commit plus this docs
commit, pushing after each, so no unit larger than roughly "one build+test
cycle" was ever at risk again. **Generalizable rule**: for any
long-running implementation task in an ephemeral session, treat "commit
and push" as part of finishing a logical unit, not as a wrap-up step
saved for the end — the session's own lifetime is not a resource the task
plan can assume.
