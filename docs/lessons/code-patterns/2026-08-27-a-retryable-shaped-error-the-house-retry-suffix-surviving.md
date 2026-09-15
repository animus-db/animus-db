# A retryable-shaped error (the house `"; retry"` suffix) surviving string formatting into a caller's own error type is not the same guarantee as something in that caller's *call chain* actually checking for it — audit every hop between the throw site and the nearest retry loop, not just the throw site's own wrapping (issue #412).

**A retryable-shaped error (the house `"; retry"` suffix) surviving string
formatting into a caller's own error type is not the same guarantee as
something in that caller's *call chain* actually checking for it — audit
every hop between the throw site and the nearest retry loop, not just the
throw site's own wrapping (issue #412).** `dynamo::kind_write_item_at_
leader`'s leader-side old-image read failure (`ClientCtx::
cp_get_local_resolving`, e.g. a "CP group leader moved; retry" leadership
race) already produced a message ending `"; retry"` even before this
fix — `internal(&format!("leader-side old-image read failed: {e}"))`
places the interpolated error last, so the suffix survived by construction
— and `ClientCtx::cp_kind_write_item`'s own retry loop (`read_should_
retry`, suffix-matched) already retried it correctly. The *identical*
wrapping in `eval_kind_txn_write` (the stage-time evaluator
`TransactWriteItems` uses) produced an equally well-shaped
`TxnAbortReason::Other(msg)` — but `ClientCtx::txn_prepare_pushing`'s own
bounded retry loop only pattern-matched `StageOutcome::IntentBlocked`
*inside* an `Ok` result; a stage attempt that failed **before ever
reaching propose** (this read failure, or the identical leadership race in
`leader.txn_stage`/`txn_stage_participant` returning `None`) came back as
an `Err` from `txn_prepare` and escaped the whole loop via `?` on the very
first attempt, regardless of how retryable-shaped its message was — no
code anywhere in that path ever called `.ends_with("; retry")` on it. It
then surfaced through `cp_txn`/`dynamo::run_transact`'s `TxnAbortReason::
Other` arm as a terminal `TransactionCanceledException` for a condition
the very next attempt would routinely have cleared. **The generalizable
check**: when auditing "does X get retried," don't stop at confirming the
error *carries* the retryable marker — trace every intermediate `?`/
`map_err` between the throw site and the nearest loop that actually
branches on that marker, since a marker that nothing reads is
indistinguishable, at the call site, from one that was never set. Fixed by
making `txn_prepare_pushing` catch `Err(TxnAbortReason::Other(msg)) if
msg.ends_with("; retry")` from `txn_prepare` itself and retry the stage
attempt exactly like `IntentBlocked` (safe because nothing was proposed
yet on that attempt, and re-invoking `txn_prepare` re-resolves `cp_route`
fresh each time — the same "safe to retry, re-route every attempt"
discipline the ordinary write path already has). Regression:
`issue_412_tests::txn_prepare_pushing_retries_a_leader_moved_read_
failure_to_success` (`dynamo::leader_read_failure_gate`, a deterministic
fault-injection hook mirroring `dynamo::rmw285_confirm_gate`'s idiom,
used since orchestrating a real leadership change mid-read is not
reliably reproducible on demand). (`crates/animusd/src/dynamo.rs`,
`crates/animusd/src/lib.rs::ClientCtx::txn_prepare_pushing`.)
