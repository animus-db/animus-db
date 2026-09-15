# "Converges slowly" and "never converges" produce the same panic message and need different investigation methods — instrument the poll itself before theorizing

**"Converges slowly" and "never converges" produce the same panic message
and need different investigation methods — instrument the poll itself
before theorizing** (2026-08-22, `streams_e2e.rs::multi_split_soak_
streamed_gsi_table_under_mixed_load`'s GSI-convergence `await_true(60,
"GSI never converged to one row per item", ..)` flake, ~1-in-5 at CI's
own `--test-threads=1`). Reading the production code first surfaced a
real, code-acknowledged candidate mechanism: a split's right child, when
its own range boundary isn't token-aligned (the common case for any real
string partition key — see `animus-cp-data/CLAUDE.md`'s `cursor.rs`
entry), computes a GSI-drain cursor-advance write that **routes off its
own tablet** and lands physically inside its left sibling's `KIND_CURSOR`
scope instead (`index_drain.rs::drain_tablet`'s own comment already names
this "a named, pre-existing follow-up"). That looked, on paper, like
exactly the "descheduled and never re-triggered" production bug the
investigation was supposed to rule in or out. **It wasn't** — tracing
through `cursor_min_watermark`'s min-over-rows semantics (a stray row can
only ever *depress* the computed watermark, never inflate it) shows the
misrouted write can only cause redundant re-reconciliation and delayed
trim on the affected tablets, never a wrong materialized row; an existing
regression (`split_right_childs_cold_start_re_reconciles_from_zero_
without_corrupting_the_gsi`) already proves the GSI itself stays correct
through exactly this shape. That structural argument was worth having,
but it was **theory** until measured: adding one `eprintln!` per poll
iteration (`GSI_POLL n=.. total=.. per_tag=.. elapsed_ms=..`, and a
second at the misroute's own write site) and reproducing under real
thread-oversubscription CPU contention turned up the actual failure
twice, and both times — plus every successful-but-slow run — the
per-poll total was **monotonically non-decreasing**, climbing steadily
to a timeout at 120/144 in one run and to full convergence at 65.5s and
66s in two others; the misroute eprintln never fired in any reproduction.
That per-poll trace is what actually answered the question, not the code
reading: a permanently-wrong or flat-forever count would have looked
identical to a slow-but-live one in the bare pass/fail result, so the
fix is a measured, named ceiling
(`GSI_CONVERGENCE_DEADLINE_SECS = 150`, ~2.3x the worst observed
convergence), not a production patch. **General rule**: when a
converged-or-timeout poll on an eventual property flakes, add a
per-iteration trace *before* forming a theory about the cause — a
plausible-looking code-level gap (especially one already flagged in a
comment) can be a real, harmless inefficiency rather than the operative
bug, and only the poll's own trajectory at the moment of failure can
tell the two apart. Corollary confirming this file's own prior entry on
the subject: raw local thread-oversubscription (`nproc`-many `yes`
loops) took roughly a dozen attempts (several clean passes, one
unrelated known flake, then the real one) to reproduce the failure here,
consistent with it being a real but lower-probability-than-CI shape (a
genuine 2-vCPU cgroup-quota-throttled runner is
documented elsewhere in this file as harder to trip via busy-loops); a
manual cgroup v1 CPU quota was attempted for a more faithful repro but
the sandbox's cgroup filesystem silently no-ops `cgroup.procs` writes
(confirmed via `/proc/<pid>/cgroup` after the write reported success) —
worth knowing before spending time on that route in this environment.
(`crates/animusd/tests/streams_e2e.rs`, `crates/animusd/src/
index_drain.rs`, `crates/animus-cp-data/src/cursor.rs`.)
