# Making a conditional predicate constant-true silently universalizes every branch keyed on it — re-key each one to the property it actually meant, and grep the old predicate's consumers for documented instabilities first

**Making a conditional predicate constant-true silently universalizes
every branch keyed on it — re-key each one to the property it actually
meant, and grep the old predicate's consumers for documented
instabilities first** (ADR 0049 Train A rung 1 fixup 2, 2026-08-16). ADR
0049 flipped `table_takes_kind_write_path` to constant-true so every
table gets a change log. But `cp_txn`'s awaited-bounded resolve branch
was keyed on "stages any pending kind write" — a faithful proxy for that
predicate — so every plain-table transaction silently moved onto the
awaited `resolve_all_parallel` configuration. That exact configuration
was already documented, in `resolve_all_parallel`'s own comment, as
reproduced-red on `dynamo_txn`'s torn-pair hard-gate test when applied
universally ("green again scoped like this") — and the hard gate duly
went intermittently red again (2/7 solo here; a budget-expired ack racing
the writer's next same-key stage into `TXN_STAGE_PUSH_ATTEMPTS`
exhaustion), bisected across three rungs before the cause was spotted
sitting in a comment nobody re-read. The fix re-keys the branch on the
property D1's rationale actually names (`table_change_records_carry_
images` — an index/stream consumer exists whose visibility the await
protects), restoring the proven-stable fire-and-forget sequential
resolve for every marker-only transaction. **General rule**: a
universalization change's review checklist must include every branch
keyed on the predicate (or its proxies — here `pending.is_some()`), and
each one either genuinely wants the universal behavior or gets re-keyed
to the narrower property it was really about; the documented caveats of
the path being universalized (ADR text and load-bearing code comments
alike) are the first place to look for what will break.
