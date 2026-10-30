# Size corpus shards from measured wall time, and pin every fixed-seed test to shard 0 (issue #1271)

Raising a corpus's `shards` count is only safe after three audits, per corpus:

- **Measure per-tier cost first.** `lsm_stall` spans four test binaries (storage
  ~33 s, cp-data 2 s + 14 s, animusd ~31 s per seed in debug); every tier shards
  its seeds, so a uniform count works. Fixed-seed cells run on every shard and
  do not shrink, so a tier that is mostly fixed-seed would not benefit.
- **A test that is not a per-seed loop breaks on a shard.** In
  `sim_cluster_mixed_version_corpus` a "family is non-empty" assert fails on a
  shard owning no variant at small K, an aggregate "the capped decode fired on
  at least one seed" check sees only a slice, and the pinned-seed, uniqueness
  and determinism tests just repeat on every worker. Aggregates run over the
  full list on shard 0 (`corpus_cells_unsharded`), the rest return early unless
  `owns_unsharded()`. In `sim_world_mrec_corpus` `seeds()[0]` would panic on an
  empty shard slice.
- **Verify the partition, do not assume it.** `ANIMUS_SHARD_TRACE=1` prints the
  kept variant indices; run each shard at small K (e.g. 3 over 2 shards: `[0, 2]`
  and `[1]`) and, for cell-expanded corpora, diff the union of printed cell
  names against the unsharded run.
