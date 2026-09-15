# File reads taken shortly after a branch checkout can transiently disagree across tool families (Read/Edit vs Bash) — verify you're actually at HEAD before trusting either.

**File reads taken shortly after a branch checkout can transiently disagree
across tool families (Read/Edit vs Bash) — verify you're actually at HEAD
before trusting either.** An agent building against `origin/perf/cp-data-
snapshots-codec` initially saw a stale 1053-line `animus-cp-data/src/lib.rs`
via `Read` while the true tip was 1482 lines with a materially different
architecture (split consensus-loop/apply-task, wake-on-propose, a binary wire
codec) — caught only because a test file referenced methods the "current"
file didn't have. `git show HEAD:path` gave a third answer on repeated calls.
Recovery: `git status --short` + `git diff --stat HEAD -- path` both empty is
the only trustworthy "am I at HEAD" check; for a file where Bash-side
build/test is the actual gate, `git checkout -- path` + direct Bash
edits (sed/perl) are safer than Read/Edit if this is suspected. (PR #31.)
