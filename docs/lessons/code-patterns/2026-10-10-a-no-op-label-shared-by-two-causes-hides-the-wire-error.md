# One no-op outcome for two causes surfaces as the wrong wire error

**Issue #1203.** An `UpdateItem` with no `ConditionExpression` returned
`ConditionalCheckFailedException` while a `TransactWriteItems` held an intent on
the key. AWS answers `TransactionConflictException`.

**Why it happened.** `KvCommand::KindEval`'s apply arm recorded
`KindBatchOutcome::ConditionFailed` both for "the caller's condition evaluated
false" and "a foreign write intent sits on the key" (the code comment even called
this an accepted imprecision). Every layer above only saw one label, so the edge
could not tell them apart.

**What to do.** When one recorded outcome stands for several causes that map to
different client-visible errors, split the label at the source. Here the apply
decision (write nothing) is unchanged, so only the leader-local outcome label
gained a variant (`IntentBlocked`); no new replicated command or gate was needed
(contrast `2026-10-09-a-fix-that-changes-apply-needs-a-new-variant-...`, where
the fix changed what an entry *does*). Remember the cross-node hop: a new typed
`WireError` code must be added to `RELAYABLE_WIRE_ERROR_CODES`, or it degrades to
a 500 when the tablet leader is remote. Test it over a 3-node SimCluster, and
note that the evaluated path is only taken by writes that need evaluation
(`ReturnValues`, a condition, an `Update`); a blind `Put`/`Delete` on a plain
table takes `fast_marker_write` and never reads the key.
