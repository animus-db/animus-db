# When a filed race note "does not reproduce," the deliverable is a test pinning the specific guard arm that closes it — found by asking "which exact check makes each interleaving in the note impossible?", then red-proving that check

**When a filed race note "does not reproduce," the deliverable is a test
pinning the specific guard arm that closes it — found by asking "which
exact check makes each interleaving in the note impossible?", then
red-proving that check** (2026-08-17, issue #266). A planning-time note
flagged a residual cross-node LSI orphan-row race beyond the ADR 0046 U3
funnel; re-verifying against the landed `TxnStage` stack showed three
guards jointly close it — `KindBatch.conditions`' unresolved-intent arm
(an intent is never a match), `TxnStage`'s C1 mandatory own-key OCC +
foreign-intent block, and `TxnResolve` materializing kind writes from
the intent-on-the-key only while that intent still stands. But the
first of those arms — the one the note's own scenario turns on — had
ZERO coverage (`kind_batch_conditions.rs` only ever exercised
committed-value/absence conditions), so "closed" rested on code
reading alone. The pinning test was cheap (stage an intent, land a
stale conditioned batch astride it, resolve, assert exactly one
derived row), and sabotaging the arm (`Intent => true`) proved it red.
General form: "verified closed" without a red-provable test for the
closing check is a claim about today's tree, not a regression
guarantee — and the *arm-level* gap hides easily because the
surrounding mechanism is otherwise well-tested.
(`animus-cp-data/tests/txn_kind_writes.rs`,
`animusd/tests/dynamo_index_writes.rs`.)
