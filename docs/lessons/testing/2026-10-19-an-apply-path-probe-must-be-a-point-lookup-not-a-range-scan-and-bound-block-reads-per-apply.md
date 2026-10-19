# An apply-path probe must be a Bloom-gated point lookup, and its block reads per apply need a bound

Issue #1349. After #1315 made SSTable point reads correct for keys whose
versions straddle blocks (by reading every candidate block), a follower on a
hot-key transactional load paid ~22 block reads (LZ4 + crc32 each, debug build)
per applied entry and fell behind the commit rate (`upgrade-previous-release`,
`local_group_not_caught_up`). The R-1 binary lacked #1315 and kept up, which is
why it showed only in a mixed roll.

Lessons:
- A correctness fix that widens a read (all blocks instead of one) multiplies
  the cost of every caller; re-measure the hottest callers. Here the worst were
  `TxnStage`'s per-key replay-ahead probes, a range scan with no Bloom gate.
- Use the structure the writer guarantees: versions are sorted oldest to newest
  across a table's blocks, so a newest-visible read walks backwards and stops
  at the first hit; table metadata (`min_version`/`max_version`) lets a read
  skip tables that cannot change the answer, so under monotonic versions the
  memtable or newest table answers alone.
- A SimEnv test can pin this deterministically: count blocks per apply
  (`block_read_count`), assert a small bound, and keep the old probe as a
  negative control on the same state so the bound provably bites.
- A wall-clock number needs a debug-build ProdEnv run
  (`tests/lsm_apply_read_amp_prod.rs`): 257 -> 1644 entries/s, 22.3 -> 0.27
  block reads per entry on 20 hot keys.
