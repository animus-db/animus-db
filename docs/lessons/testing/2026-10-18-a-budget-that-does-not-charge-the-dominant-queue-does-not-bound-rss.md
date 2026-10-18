# A byte budget bounds RSS only if it charges the dominant queue: measure RSS against it, with the disk sized for the load

Issue #1296. ADR 0080's `QueuedBytes` budget shipped at 256 MiB with only a
flood test of its own counter (hwm 410 KB of a 2 MiB budget). The 150 s
density-rig run showed the charged bytes were about 5 % of RSS (about 100 MiB of
2.0-2.3 GB): the per-group propose bound (512 entries) bound first, and the
bulk of RSS was LSM and Raft-log state the budget does not charge. A budget
that works (a 32 MiB one bit cleanly) can still say nothing about RSS.

- To validate a memory budget, put the counter and RSS in the same output line
  (`queued_used`, `queued_hwm_max`, `rss_kb`) and compare orders of magnitude
  before reading anything into the cutoff.
- RSS in a 3-nodes-in-one-process rig is a process figure; divide by the node
  count before comparing it with a per-node pod limit, and say it is inferred.
- RSS ratchets in steps at compaction bursts: a 60 s run misses the plateau
  (1.0 GB at 60 s, 2.3 GB at 150 s, flat to 420 s). Use 150 s or longer.
- Size the disk for the load before a long write run. 64 KiB values at 150 puts/s
  filled the 5 GB of free space in 45 s and every later sample was an ENOSPC
  result, not the thing being measured. `dir_bytes` pinned at the free-space
  figure is the tell.
