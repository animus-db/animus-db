# A per-item `WireError` raised inside a `TransactWriteItems` write action's evaluation does not keep its own error `code` by the time it reaches the wire (2026-08-24, issue #372 part 2).

**A per-item `WireError` raised inside a `TransactWriteItems` write action's
evaluation does not keep its own error `code` by the time it reaches the
wire (2026-08-24, issue #372 part 2).** `wire::apply_update`'s
`ValidationException` (the 400 KB post-update-result cap, closed this
change) propagates out of `eval_kind_txn_write` fine as a `WireError`, but
`ClientCtx::txn_stage_local` immediately `map_err`s it to a plain `String`
(`format!("txn prepare: leader-side evaluation failed: {e}")`) to satisfy
`cp_txn`'s `Result<_, String>` signature, and `run_transact`'s `Err(e)` arm
then wraps *that* string as `WireError::transaction_canceled(..)` —
`TransactionCanceledException`, unconditionally, regardless of what the
original code was. So the same validation failure surfaces as
`ValidationException` through `UpdateItem` (which calls `apply_update`
directly) but as `TransactionCanceledException` (with the original message
merely nested in the text) through `TransactWriteItems`'s `Update` action.
A test asserting the specific DynamoDB error code on a transactional write
path must know which of these two shapes applies rather than assuming the
bare `WireError` constructor's code survives — match on the nested message
substring there instead. (`animusd/tests/dynamo_item_size_cap.rs`.)
