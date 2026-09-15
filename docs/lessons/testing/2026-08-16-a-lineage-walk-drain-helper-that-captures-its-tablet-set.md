# A lineage-walk drain helper that captures its tablet set once, before the drain starts, cannot see a split that lands DURING the drain — and a "cascading" (third-generation) split is exactly the case a single controlled split in a smaller test never exercises.

**A lineage-walk drain helper that captures its tablet set once, before
the drain starts, cannot see a split that lands DURING the drain — and a
"cascading" (third-generation) split is exactly the case a single
controlled split in a smaller test never exercises.** A further,
purely-harness layer of D8's own flake (distinct from every bug above,
which were all real production duplication/loss at the split boundary):
`streams_e2e.rs`'s `drain_all_tablets_lineage` took its `tablets: &[
TabletId]` argument as a fixed set for the whole drain. Under D8's
sustained write pressure and tiny byte-auto-split threshold, a child
tablet minted by the test's own first split could itself split again
*while the drain was already mid-walk* — a real, correct auto-split, not
a bug — minting a grandchild tablet id nobody had ever handed the drain
helper. That grandchild's own change records were simply never read,
producing a spurious **deficit** (`delivered < expected`, distinct in
*direction* from the over-count bugs above) at a low but real rate
(~1/20 iterations) that had been sitting in the test's own doc comment as
an "adjudicate against, don't re-investigate" known limitation rather
than fixed — unlike the production bugs in the entries above, this one
needed no `src/` change at all, purely a harness fix. **Fixed**: the
helper now re-resolves the *live* shard chain every pass via a fresh
`DescribeStream`
call (`stream_tablet_ids`, paginating `ExclusiveStartShardId`/
`LastEvaluatedShardId` to a full page) — the same way any real DynamoDB
Streams consumer discovers a new shard exists, never by peeking at
`Metadata`'s tablet map directly for tablet *existence* (that stays
reserved for a tracked tablet's own chain-*length* read, which has no
wire equivalent this cheap). A newly discovered tablet id is folded in
with a fresh `next_epoch = 0`, never touching an already-tracked tablet's
in-flight open-tail iterator — preserving the resume-not-remint invariant
the entries above fixed. **General form**: any test helper that walks a
linearly-growing structure (a lineage, a partition set, a shard list) by
snapshotting its membership once up front is implicitly assuming the
structure is quiescent by the time the walk starts — under real
background pressure (an auto-split loop, a compaction, a rebalance) that
assumption can be false for the exact scenario the test exists to stress,
and the fix is to make the walk's own membership-discovery step live
(re-run every pass) rather than a one-time precondition. (2026-08-16,
`test/streams-e2e-walk-cascading-split`.)
