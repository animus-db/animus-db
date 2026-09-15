# A "does this write need the old value" gate and the "does this write take the richer commit path" fast-path gate must be the *same* predicate, expressed once — not two conditions that happen to agree today.

**A "does this write need the old value" gate and the "does this write
take the richer commit path" fast-path gate must be the *same*
predicate, expressed once — not two conditions that happen to agree
today.** Building ADR 0042's stream write-path gate (`kind_writes_for_item`'s
`None` fast path widening from `indexes.is_empty()` to `!indexes.is_empty()
|| stream.is_some()`) surfaced a real, independent, pre-existing gap: the
DynamoDB edge's `PutItem`/`DeleteItem` handlers computed their own
`needs_old` (whether to pay for a pre-read of the item) from
`condition.is_some() || return_values == ReturnValues::AllOld` alone —
never from whether the write was actually about to route through the
kind-write path. An unconditional replace/delete on an *already-indexed*
table therefore silently skipped the read `kind_writes_for_item`'s own LSI
diff needs (to remove a stale row when the alt-sort attribute changes),
and — once streams could also pull a table onto that path — a stream's
`OLD_IMAGE`/`NEW_AND_OLD_IMAGES` change record would just as silently miss
its old image. `UpdateItem` and `BatchWriteItem`'s indexed branch had
independently, correctly always read old — only the two write paths
nobody had reason to touch since ADR 0041 shipped kept the narrower gate.
The fix factors both call sites' predicate into one function
(`table_takes_kind_write_path`) `kind_writes_for_item`'s own gate and every
write handler's `needs_old` both call — so the two structurally cannot
drift apart again. When a "do we need X" decision and a "does this path
apply" decision are supposed to always agree, don't let them be two
separately-maintained booleans; a passing test suite proves today's
agreement, not tomorrow's. (`crates/animusd/src/dynamo.rs`, ADR 0042 PR A3,
2026-08-14.) **Second confirmed instance, found wiring ADR 0049
(2026-08-16): the drift survived the fix's own review round.**
`BatchWriteItem`'s fast-path gate stayed `meta.table_indexes(table)
.is_empty()` — written against ADR 0041 (indexes-only), never re-checked
when ADR 0042 widened "takes the kind path" to include streams — so a
streamed-but-unindexed table's batch writes bypassed the kind path
entirely and its stream silently lost every one of them (no LSI existed
to corrupt, so nothing else surfaced it; found only because ADR 0049's
gate flip forced re-reading every gate site). The factored-predicate fix
above only protects call sites that *call the shared function* — grep for
raw re-derivations of the same condition (`table_indexes(`,
`table_stream(`) whenever the shared predicate's meaning widens, because
a site that never adopted the function is exactly the one no widening PR
ever touches. Regression: `stream_write_path_tests::
batch_write_on_a_streamed_table_emits_change_records` (red on the
pre-0049 code).
