# A baseline or release marker needs a green per-commit CI run, but workflow concurrency can cancel it

ADR 0073's Phase 0 baseline is "the merge commit where the last workstream
lands" (`9a9f972f`, #1104). The workflow's `concurrency` setting cancels an
in-progress `main` run when a newer push arrives, and the very next merge
(#1108) did exactly that: the baseline commit has no completed CI run of its
own. Verification had to be reconstructed from the next commit's green run
(`516759d4` = baseline + a test-only fix) plus a separate e2e run.

Why it matters: a marker that later policy (fixture immutability, N-1
compatibility) hangs off should be a commit whose green status is provable.
When a baseline or release marker is chosen, (1) check that its own `main`
run completed, (2) if it was cancelled, re-run CI on that exact SHA (or
record the covering run and what differs, as ADR 0073's amendment does), and
(3) avoid merging anything behind it until the run finishes.
