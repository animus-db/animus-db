# A streams test that bursts several writes and then reads only `DescribeStream`'s `shards[0]` is asserting on a seal-timing coincidence, not on exactly-once delivery.

**A streams test that bursts several writes and then reads only
`DescribeStream`'s `shards[0]` is asserting on a seal-timing coincidence, not
on exactly-once delivery.** The age-trigger seal arm sweeps on the hard-coded
200ms `INDEX_DRAIN_INTERVAL` tick, so a test whose `seal_age` is only a small
multiple of that (300ms) can have its burst straddle a tick under real
WAL-fsync-bound write latency and correctly produce *two* shards — a closed
one plus an open tail holding the last write(s). Reading only the first then
reports "must see every record exactly once" failing, which reads exactly
like a product exactly-once bug but is the product behaving correctly (the
missing record is always the *last-written* one — that signature is the
tell). Walk the whole chain `DescribeStream` returns, including the trailing
open shard, as `get_records_walks_the_shard_chain_and_drains_the_open_tail`
already did. Do **not** "fix" it by enlarging `seal_age` or asserting a shard
count: both only shrink the window, and the shard count is legitimate
timing-dependent product behavior a test must not fight. Confirmed by walking
the full chain and recovering every record, proving nothing was lost.
(`crates/animusd/tests/dynamo_streams.rs`, issue-less flake off `main`,
2026-08-20.)
