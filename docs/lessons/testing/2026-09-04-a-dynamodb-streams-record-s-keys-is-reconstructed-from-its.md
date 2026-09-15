# A DynamoDB Streams record's `Keys` is reconstructed from its own `NewImage`/`OldImage` (`streams_wire::keys_from_images`), never decoded from `ChangeRecord.base_sk`

**A DynamoDB Streams record's `Keys` is reconstructed from its own
`NewImage`/`OldImage` (`streams_wire::keys_from_images`), never decoded
from `ChangeRecord.base_sk`** — found while writing W-03 step 4's stream
regression for ADR 0063's `N` key encoding. It is tempting to assume a
stream-record-keys test exercises `numkey::decode` the same way
`SortKeyCondition::matches_raw` does, since both ultimately answer "what
was this row's sort key" — but `base_sk` is only ever used to *position*
the record in its shard/sequence, and the value shown to a consumer comes
straight from the item text the write path already stored in the image. A
regression here (`dynamo_streams.rs::
stream_keys_carry_n_sort_key_values_across_mixed_magnitudes_and_signs`)
is real coverage — a break in `base_sk`'s own byte shape can still surface
as a missing/misordered record, since the change-log storage key is
`partition_prefix || base_sk || hlc` — but it is not equivalent to a
`numkey::decode` unit test, and a doc comment should say so explicitly
rather than let a reader assume the wire round-trip proves more than it
does.
