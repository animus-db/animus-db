# Changing a bulk write path's Raft-entry granularity is a throughput contract change, and the timing-budgeted e2e suites are its regression canary — bisect a "suddenly slow" suite before blaming the machine

**Changing a bulk write path's Raft-entry granularity is a throughput
contract change, and the timing-budgeted e2e suites are its regression
canary — bisect a "suddenly slow" suite before blaming the machine**
(2026-08-16, found delivering ADR 0049 Train A rung 2). Rung 1 replaced
`BatchWriteItem`'s one-`Batch`-entry-per-tablet fast path with per-item
`KindBatch` proposals (chunked, concurrent). Its own gates ran green, but
`backfill_seeder.rs::split_during_backfill_converges_with_correct_final_
gsi` — a populate-heavy test with a 60s convergence budget — went
deterministically red at the rung's tip (4/4, even single-threaded) while
the immediate pre-rung commit passed in 17s: an order-of-magnitude
convergence regression that a "flaky on this box" shrug would have
shipped. Two lessons: (1) an entry-granularity change (N single-key
entries where one multi-key entry used to be) multiplies per-entry apply/
confirm costs and must be treated as perf-sensitive — re-run the suites
whose comments document timing budgets several times before shipping;
(2) the 30-second bisect (run the suite once on the parent commit) is
what turns "pre-existing flake, dismissed" into "my rung's regression,
fixed" — never classify a red integration test as environmental without
that one run, exactly because this machine also has a *genuine*
environmental flake class (`AddrInUse` bring-up TOCTOU) to hide behind.
