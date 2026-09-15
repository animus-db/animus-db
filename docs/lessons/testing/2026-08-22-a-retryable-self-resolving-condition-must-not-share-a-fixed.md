# A retryable, self-resolving condition must not share a fixed budget with a real error — and the fix is a bigger ceiling on the SAME poll, not a new mechanism

**A retryable, self-resolving condition must not share a fixed budget with
a real error — and the fix is a bigger ceiling on the SAME poll, not a
new mechanism** (2026-08-22, `streams_e2e.rs::multi_split_soak_streamed_
gsi_table_under_mixed_load`). The test's `dynamo_retrying`/
`dynamo_retrying_transact` helpers already did the right thing
structurally: they only keep retrying while the status/message matches a
documented transient (the ADR 0050 F8 `"; retry"`-suffixed freeze→cutover
refusal, or the analogous election-wait 500), and fail immediately, zero
retries, on anything else — a genuine error was never at risk of being
masked. The actual defect was just that the shared ceiling (a flat 20s)
was sized for the steady-state "sub-second" cutover ADR 0050 documents,
not for what a cutover can take when the process is CPU-starved — and
this specific soak test deliberately drives a CASCADE of overlapping
splits (a 2KiB auto-split threshold against a continuously-written
table) specifically to stress that path. Root-caused by reading the
server side, not just the test: `ClientCtx::cp_kind_write_item`/
`cp_kind_write_raw` already retry the freeze refusal internally (issue
#288), but `ClientCtx::cp_txn` (the `TransactWriteItems` coordinator)
deliberately does not — matching real DynamoDB, where a cancelled
transaction is the client SDK's job to retry, not the server's to
absorb — so for the transact path this test's own client-side loop is
the *only* thing standing between a slow cutover and a spurious failure,
with no server-side safety net underneath it. Fix: widened the shared
ceiling from 20s to 90s (one named constant, `RETRYABLE_BLIP_DEADLINE`,
used by all three call sites that shared this weakness:
`dynamo_retrying`, `dynamo_retrying_transact`, and the structurally
separate `get_records_allow_trim`) — not a new polling mechanism, since
the loop already re-checked the real condition every pass rather than
asserting once. The general check this leaves behind: before touching a
timeout on a "flaky" `ProdEnv` test, verify **which** of the two
brackets an assertion falls into — does it currently retry/wait on
*anything*, or only on a specific documented-transient shape? If the
latter, and the shape match is airtight, widening that specific ceiling
is the correct fix, not the "don't bump the timeout" anti-pattern this
log warns about elsewhere — that warning is about hiding an
undiagnosed failure behind a longer wait, not about sizing a
already-scoped retry correctly.

**Also found, NOT fixed, flagged here per this repo's own
incidental-bug convention:** validating the fix by running the full
`streams_e2e` binary repeatedly with `--test-threads=1` (CI's actual
invocation, per `ci.yml`'s `prod-liveness` job — the *default*
unflagged `cargo test -p animusd --test streams_e2e` oversubscribes a
small runner by running 13 heavyweight real-thread cluster tests
concurrently, itself the exact anti-pattern the entry above this one
documents, and produced a different, non-representative failure at the
old `dynamo_retrying` retry site before the ceiling widened) surfaced a
**separate, pre-existing** flake in the same test: its GSI-convergence
check (`await_true(60, "GSI never converged to one row per item", ..)`,
a Query-based poll over the four tag partitions) timed out twice in 14
full-suite runs even after this fix, always with every other assertion
in the same run green — including the exactly-once delivery check
immediately before it, which is deliberately never loosened (issue
#298's own note). This is structurally identical to the bug this entry
fixes (a background-reconciler-driven eventual property on a fixed
ceiling, likely too tight for a cascade of splits under real
contention) but is a different code path (GSI backfill/query
convergence, not the transact retry budget) and was never reported
against — diagnosing and fixing it belongs in its own change with its
own investigation, not folded into this one.
