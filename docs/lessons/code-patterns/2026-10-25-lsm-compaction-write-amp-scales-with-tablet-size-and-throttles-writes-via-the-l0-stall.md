# Write throughput that decays with history, at flat CPU, is the LSM L0 stall: read `storage_write_stall_micros_l0_stop` first

Investigation of "write/txn throughput falls 63 -> 22 txn/s over 25 min with no restarts"
(origin/main 77787f7, 3-node real-process cluster, 48 closed-loop clients; findings in the
investigation notes, not in the repo).

- **Symptoms that look like a txn/history problem but are not**: the same decay (-60 to -70 %)
  reproduces with plain single-item `PutItem` on unique keys, at constant per-node CPU (~1.2 cores,
  no saturated thread), flat fsync latency, and flat raft entries/proposals per op. Txn records and
  markers (#1380) add bytes but not the mechanism: a txn run that overwrites the same 1000 base keys
  loses only ~25 %.
- **The mechanism**: production `LsmOptions` flush a 64 KiB memtable and compact L0->L1 every 4 tables,
  rewriting every overlapping L1 table, i.e. all of L1 for hashed keys (and L1->L2 all of L2, ...).
  Bytes compacted per commit therefore grow with the tablet's size (1.3 KB -> 9 KB per commit in 15 min).
  One compaction runs at a time at a roughly fixed byte rate, so commit rate = compaction rate / bytes
  per commit (checked: 1.3 MB/s / 9 KB = 144/s measured 150/s), and once L0 hits the ADR 0081 stop level
  the group is stalled for up to ~50 % of wall time (`storage_write_stall_micros_l0_stop` ~530 ms/s).
- **How to see it fast**: per-commit ratios of `storage_compaction_bytes_merged`, `storage_bloom_misses`
  (= in-range SSTable probes) and `storage_sstable_block_reads` over time (`/admin/metrics` every 60 s,
  deltas divided by `cp_commits` of the *leader*), plus the two `storage_write_stall_micros_*` counters.
  CPU, strace and stack sampling mostly show "busy-ish and waiting" and do not localise it; a stack
  sample's share in `run_compaction` was ~37 % both early and late.
- **An intervention beats a story**: raising the flush/WAL-segment size to 2 MiB (env-patched, experiment
  only) cut compaction bytes per commit ~10x and gave 2-3x the end-state throughput; removing a suspected
  per-write probe read gave a constant +15-25 % but no change of slope. Use the slope, not the level.
- **Gotchas hit**: pick port bases below 32768 (ephemeral client sockets from a previous load run
  collide with node ports and the node dies with "Address in use"); `gdb -batch -ex 'thread apply all
  bt'` works on a debug-profile `animusd` with no debuginfo (symbols suffice) but every attach pauses
  the process; a worker's `ps`/`/proc` per-thread ticks need dividing by the window in seconds x 100
  before reading them as percent.
- **Related find**: the LSM bloom filter's false-positive rate is ~3.5 % (target 1 %) because the two
  base hashes are FNV-1a over the same bytes with different offset bases; a persisted-format fix needs
  a manifest version bump (ADR 0073).
