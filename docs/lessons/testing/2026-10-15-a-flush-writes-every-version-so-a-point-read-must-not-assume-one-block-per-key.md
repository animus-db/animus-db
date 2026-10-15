# A point read of a table must not assume one block per key

Issue #1315 (`upgrade-previous-release` losing acked appends, ~1 run in 10 in
CI, 1 in 40-80 locally). Root cause: `SsTableReader::latest`/`get_at` read the
single block `block_for_key` returned (last block with `first_key <= key`).
Blocks are cut at 4 KiB with no regard for key boundaries and a memtable flush
writes every retained version, so a hot key with KiB-sized rows spreads its
versions over several blocks. Older versions then read as absent (or as a stale
row from an older table). `animus-cp-data`'s abort lookback
(`get_at(key, intent_version - 1)` for a v1 intent with no prior, which only
exists while the cluster-version gate is closed during a roll) turned that into
a permanently stale row on one replica; `KindEval` is applied per replica, so
the divergence never healed, and when that replica won leadership after a
restart-all every node "agreed" on the loss.

Lessons:
- A storage-level invariant ("a key lives in one block") that the writer never
  establishes is a latent bug; the first caller that reads history exposes it.
  Differential-test the engine against `MemoryEngine` for every historical read
  with rows large enough to span blocks (`lsm_multi_block_versions`).
- When "all replicas agree on the wrong value", check whether one replica
  diverged earlier and later became the authority (leader), rather than
  looking for a decision every node made wrongly.
- Dump the data dirs of a failing real-process run and read them with an
  offline engine opener; the live admin endpoints hid the memtable/encoding.
