# `DescribeStream` always appends a tablet's still-open successor epoch behind a just-sealed one while the stream stays enabled — a shard-count assertion after a seal must account for it (ADR 0061 rung G, C-07 PR 3, 2026-09-08)

Building `sim_cluster_dynamo_streams.rs`'s new post-seal scenarios (a
sealed-shard `GetRecords` read, `Limit` pagination, the `AT`/
`AFTER_SEQUENCE_NUMBER` iterator types, and a cross-node identical-token
read — four of the eight new scenarios), every one's first draft asserted
`DescribeStream`'s `Shards` array had exactly **one** entry after calling
`SimCluster::drive_stream_seal` once over a small, fully-drained backlog —
mirroring the module's own PR 2 scenario, which checks `Metadata::
stream_shards` (the sealed-rows-only catalog map) directly, never
`DescribeStream`'s own JSON response. All four failed identically: the
response actually carried **two** shards — the sealed epoch 0 (with a
`SequenceNumberRange.EndingSequenceNumber`) and a second, still-open epoch
1 with none. This is not a fixture bug or a `drive_stream_seal` quirk —
it's `dynamo_streams::describe_stream`'s own, entirely correct, documented
behavior (`current_open_epoch`, `resolve_label`): whenever a stream is
`enabled`, the response unconditionally appends one open-shard entry per
routable tablet at that tablet's *current* epoch, on top of however many
sealed rows the catalog already holds for it — a seal always advances the
open epoch counter, so the very next `DescribeStream` call after any seal
sees a fresh, empty successor shard it must still report (a real client
polls it and correctly sees nothing new yet).

**General lesson**: `Metadata::stream_shards`-based assertions (only
sealed rows exist there) and `DescribeStream`'s own JSON `Shards` array
(sealed rows **plus** the current open tail, while enabled) are answering
two different questions, and a test that seals once and then asserts an
exact `Shards.len()` must count the open tail too — the sealed shard is
always `shards[0]` (the array sorts ascending by epoch), never
`shards.last()` or the array's sole element, the moment more than zero
epochs have ever sealed. This generalizes past this one PR: any future
Streams scenario that seals a tablet and then inspects `DescribeStream`
needs the identical `+1` accounted for, and the fix here (assert `len ==
sealed_count + 1`, always index the sealed entries from the front) is the
reusable shape.
