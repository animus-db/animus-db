# A moving `origin/main` mid-merge is cheaper to handle by re-merging from scratch than by stacking two merge commits

While resolving this same TLS-branch merge, `origin/main` advanced again
(one more PR merged) partway through conflict resolution, before the
in-progress merge had been committed. Since nothing had been committed
yet, `git merge --abort` followed by a fresh `git merge origin/main`
against the new tip was cheaper and safer than committing the
already-resolved merge and layering a second merge commit on top: most of
the conflicts reappeared byte-for-byte identical (the append-only docs,
the same relay/TLS call sites) and were fixed by reapplying the same
edits, while the handful of genuinely new conflicts the second PR
introduced (a new `--auto-split-ops-rate` CLI usage line, a new wave-table
row) were then resolved once, in their final form, rather than resolved
once and then re-touched again in a follow-up merge commit. **General
form**: if `origin/main` moves while a merge's conflicts are still being
resolved and nothing has been committed, prefer aborting and re-merging
against the new tip over finishing the stale merge and chaining a second
one — a single merge commit against the true current tip is both the
cleaner history and, in practice, less total conflict-resolution work than
two.
