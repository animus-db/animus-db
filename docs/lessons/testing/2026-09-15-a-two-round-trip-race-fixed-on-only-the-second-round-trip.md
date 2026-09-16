# A two-round-trip race fixed on only the second round trip will recur on the first — grep every call site the stale check guards, not just the one that happened to fail last time (issue #745, the residual half of #299/#755)

`streams_e2e.rs`'s `drain_all_tablets_lineage` speculatively mints
`shardId-{tablet}-{chain_len}` as a tablet's open tail from a
locally-cached `Metadata` snapshot, guarded by a
`!node.metadata().tablets.contains_key(&tablet)` check. That check is a
snapshot, not a lease: a real split cutover can retire the tablet in the
gap between the check and either of the **two** requests the walker then
issues against the guessed epoch — `GetShardIterator` first, `GetRecords`
second. #299/#314/#755 diagnosed and fixed this race correctly, but only
for the request that happened to be the one a real cluster run had
actually hit: `GetRecords`, via a scoped `get_records_allow_trim` helper
that treats a terminal 400 `TrimmedDataAccessException` as "this tablet's
chain is done" instead of a bug. The `GetShardIterator` mint one line
above it, going through `dynamo_retrying`'s 200-or-500-only allowlist,
was never touched — and the exact same interleaving, one round trip
earlier, panics there instead. Confirmed reproducible: pre-fix, 1/30
failures under artificial CPU contention (six `yes > /dev/null` loads on
a 4-core box) with the identical error text and shard id as the original
report; 30/30 clean after adding the mint's own `get_shard_iterator_
allow_trim` sibling and routing its `None` (trimmed) outcome through the
same `LineageCursors::epoch_trimmed` arm the `GetRecords` race already
uses.

**General form**: when a stale-snapshot-then-act race spans multiple
sequential network round trips against the same guessed position, a fix
that patches only the round trip a report happened to catch is patching a
symptom, not the mechanism. Before closing out a "race landed on call X"
issue, grep every other call site gated by the same stale check for the
identical shape (a speculative mint from a cached view, followed by one
or more requests that can each independently observe the position having
gone stale) — each one is an equally live recurrence waiting for its own
report, at whatever probability its own round-trip window carries. Here
that meant re-checking `GetShardIterator` immediately after `GetRecords`
was fixed, rather than only after it flaked in CI a second time.

**Server semantics were never in question**: `dynamo_streams::
get_shard_iterator` re-evaluates `!meta.tablets.contains_key(&tablet)`
fresh at serve time and returns `TrimmedDataAccessException` for a
retired tablet's guessed epoch — already the documented mapping (ADR
0042's round-3 PR6 note: "`GetShardIterator` on an unknown/stale shard
id" is `TrimmedDataAccessException`, matching `GetRecords`'s own outcome
for the identical condition). A real DynamoDB Streams client never
guesses a shard id from a cache in the first place — it re-derives the
shard graph from `DescribeStream`'s `SequenceNumberRange` — so this whole
class of race is specific to this test harness's shortcut, not a product
gap; the fix stays entirely in `streams_e2e.rs`.
