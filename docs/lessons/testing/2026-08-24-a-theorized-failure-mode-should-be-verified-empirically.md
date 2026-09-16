# A theorized failure mode should be verified empirically before a test is built around it — the read-before-write architecture can make an "obvious" race unreachable through the surface you'd naturally test it from (2026-08-24, ADR 0018's `CancellationReasons` amendment, issue #374 C2b).

**A theorized failure mode should be verified empirically before a test is
built around it — the read-before-write architecture can make an "obvious"
race unreachable through the surface you'd naturally test it from
(2026-08-24, ADR 0018's `CancellationReasons` amendment, issue #374 C2b).**
Building `TransactionConflict` reachability coverage, the natural test
looked like: stage one transaction's intent on a key and never decide it,
then send a real `TransactWriteItems` touching that same key through the
DynamoDB edge, expecting `StageOutcome::IntentBlocked` (the apply-time
writer-push-intents guard) to surface as `TransactionConflict`. It never
did — every attempt produced a generic, slow (~5s) failure instead. The
reason only became visible by tracing the actual call path: every DynamoDB
write action reads the item's *current value* first
(`ClientCtx::txn_stage_local` → `dynamo::eval_kind_txn_write` →
`cp_get_local_resolving`, needed to evaluate the action's own
`ConditionExpression` and diff LSI rows) — for a key already holding
another transaction's local pending intent, that read itself blocks
(`INTENT_WAIT_TIMEOUT`, 5s) or fails, so the apply-time guard the test
meant to exercise is never even reached; the write never gets far enough
to propose. The write path that DOES hit the guard directly — a raw,
already-known-value write (`TxnTableWrite::plain`, the plain client
protocol's own shape, no preceding read) — was reachable and fast (under
2s) once tried. **General rule**: when a test keeps producing an
unexpected result for what looks like a straightforward race, trace the
actual code path the request takes end to end (not just the two states
you expect to interact) before concluding the test needs a bigger timeout
or a cleverer timing trick — a front-loaded read, a cache, or an
early-return check can make a "later" failure mode structurally
unreachable from a particular entry point, and the fix is choosing a
different entry point (or documenting the narrower reachability, as this
amendment's own ADR section does) rather than fighting the timing harder.
