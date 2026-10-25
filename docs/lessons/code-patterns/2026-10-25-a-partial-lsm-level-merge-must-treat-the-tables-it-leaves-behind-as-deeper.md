# A partial merge inside one LSM level must count the tables it leaves behind as "deeper" for tombstone GC, and must emit fewer tables than it consumed

Found while fixing #1389 (byte-based L0 trigger, L0 to L0 merges of a size-tiered subset).

- **Tombstone GC assumed "everything older is at a deeper level".** `run_compaction` reclaims a
  tombstone when no table at a level deeper than the target overlaps the key. That is only true when the
  compaction consumes the *whole* source level. An L0 to L0 merge of a subset leaves other L0 tables
  (which overlap by design) holding older values, so they must be added to `deeper_ranges` or the
  tombstone is dropped and the old value resurfaces. The random differential test did NOT catch it (L1
  usually overlaps every key, which already keeps the tombstone); a directed test did
  (`lsm_compaction_wa::l0_merge_keeps_a_tombstone_that_shadows_a_sibling_l0_table`: L1 holds only `zz*`
  keys, a larger L0 table holds `a`, a merge of two small L0 tables holds the tombstone for `a`).
  Mutation-check such a test by deleting the clause and watching it fail.
- **Never re-partition an L0 to L0 output.** `partition_records(.., target_table_bytes)` splits a merge
  into runs; with an input group bigger than the target it can emit as many tables as it consumed, the
  L0 count does not fall, and `compact_until_quiescent` loops forever (every `lsm_gc` test hung for
  minutes, no panic, no error). Anything that must strictly reduce a count to terminate has to be checked
  against the smallest option values the tests use (`target_table_bytes` of a few hundred bytes), not
  just the production ones.
- **A write-amplification test needs incompressible values.** SSTable blocks are lz4; a zero-filled value
  makes bytes-compacted-per-byte read below 1 and hides the effect. Fill values from a PRNG.
- **Check the negative control against the old code cheaply**: `git stash push <src file>` leaves the new
  test in place and the old policy compiled; a test that only uses public/doc-hidden accessors runs on both.
  (And never `pkill -f <pattern>` with a pattern that appears in your own command line.)
- **The L1 rewrite cost is the *overlapping* L1 bytes, not all of L1.** The first cut compared L0 bytes to
  `L1_total / fanout`; `compaction_io_budget_corpus` cell 7 (sequential keys, so L0 overlaps no L1 table)
  then pushed L0 to the stop level because it merged in L0 forever instead of draining into an L1 that
  would not be rewritten at all. A cost model for a compaction trigger must use the same overlap test the
  compaction itself uses.
