# Check the whole tree for conflict markers before committing a merge

**After resolving a merge, run `git diff --check` (or `git grep -nE '^(<<<<<<<|=======|>>>>>>>)( |$)'`)
across the whole tree before you commit.** A scripted resolution that rewrites only the first conflict
block in a file leaves the others in place, and `git add -A` stages them without complaint. That
happened when the replica-overrides branch was merged into the legacy-global-tables branch on
2026-10-22: `website/compatibility.html` had two conflict blocks, the script resolved one, and the
leftover `<<<<<<< HEAD` block reached `main` through the merge queue. The gates did not catch it,
because no build, test or lint reads the website. Counting the markers per file after resolving, and
failing on any non-zero count, would have caught it.
