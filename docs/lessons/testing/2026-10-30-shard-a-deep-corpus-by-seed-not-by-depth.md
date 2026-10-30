# Fit a slow corpus into a CI budget by sharding its seeds, not by lowering its depth (issue #1271).

The PR job that runs a changed corpus at nightly depth took ~17 minutes for
`mrsc_cluster` against a 3-10 minute budget. Cutting the depth would reopen the
"born red, fails only nightly" hole the job exists to close, so the seed
variants are split across parallel workers instead: `ANIMUS_SEED_SHARD=i/n` runs
variants with index `% n == i` (animus-test `corpus.rs`).

- **Route every seed enumeration through one filter.** About 85 corpus files
  compute their seeds from an `ANIMUS_*_SEEDS` var; a third use the shared
  `seed_expand`/`for_each_seed` (sharded in one place), the rest build a
  `Vec<u64>` or loop `0..k` themselves and each needed `shard_retain` /
  `shard_range`. A knob some corpus ignores silently runs that corpus in full on
  every worker: a wasted runner at best. Grep `_SEEDS"` after adding a corpus.
- **Aggregate checks do not survive a split.** A negative control that asserts
  "at least one seed shows the failure" (`mrsc_negative_unpinned_policy`:
  `violated > 0`) or a meta-test over the whole cell set fails on a shard whose
  slice happens not to trigger it. Pin those to shard 0 over the full seed list
  (`corpus::owns_unsharded()`), and only raise a corpus's `shards` column after
  auditing them. Per-seed oracles ("this seed must fail") shard cleanly.
- **A list indexed by position (`seeds()[0]`, `.take(2)`) cannot be sharded**:
  call `corpus::reject_shard` so it fails loudly rather than panicking on an
  empty slice.
- **A malformed knob must panic**, not default to "unsharded": a typo would
  otherwise make every worker run the whole corpus and the job look healthy.
- **The empty matrix is an error in GitHub Actions**: gate the matrix job on a
  `select` output and let a final always-run aggregate job be the single check.
