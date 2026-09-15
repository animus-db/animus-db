# `tokio::fs::File` writes are not ordered or durable until `flush().await` — a dropped handle completes its write in the background, so two sequential appends via separate handles can land INVERTED on disk, and a later `sync` on a fresh fd can fsync before the buffered write reaches the page cache.

**`tokio::fs::File` writes are not ordered or durable until `flush().await` —
a dropped handle completes its write in the background, so two sequential
appends via separate handles can land INVERTED on disk, and a later `sync` on a
fresh fd can fsync before the buffered write reaches the page cache.** This
broke "ack means durable" under ProdEnv and was the long-standing
`lsm_concurrent::scans_survive_concurrent_compaction` flake (an SSTable
recovered with its index at offset 0). Always `flush().await` before dropping a
write handle; found independently twice (PRs #26, #27). Corollary of the
documented "a flaky ProdEnv test is a real bug" rule.
