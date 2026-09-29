# A live soak needs a no-write hold phase at the end

**Context.** The ADR 0026 inbox series (#1057/#1065/#1071) and #1062 were validated with a
20-minute bulk-seed plus PutItem soak on `animusd --cluster-control 3 --cluster-data 5
--auto-split-bytes 1000000`. A run of load alone cannot tell two things apart:

- memory that grows because the data grows (tablet count, memtables, caches);
- memory that grows because something leaks.

It also cannot tell a retry counter that tracks load from one that is driven by a loop feeding
itself.

**What the hold phase showed.** The next run added a 5-minute hold with no client writes at
the end.

- RSS went flat at the exact moment writes stopped (494,904 → 494,916 KB over 5 minutes). That
  confirmed the residual growth under load was driven by data, not a leak.
- `cp_snapshot_transfer_restarts` kept climbing at 27–105/min per node during the hold, with
  zero writes. So the transfer restarts were not a side effect of write-driven compaction. Some
  loop keeps re-arming them.
- `demux_frames_dropped_closed` also kept rising, which showed that peers were still sending to
  replicas that had already been released.
- A candidate fix aimed at the load-driven explanation (#1084) did not stop it. Without the
  hold, the load-only numbers would have looked "about the same as before" and stayed
  ambiguous.

**Rule.**
- End every live soak with a hold of at least 5 minutes and no writes, sampled at the same
  cadence as the load phase.
- Judge memory by whether it is flat during the hold, not only by its slope under load.
- Judge every monotonic retry, restart or drop counter by whether it goes flat during the hold.
  A counter that keeps rising with no load is a loop driving itself, and it is a bug in its own
  right.
