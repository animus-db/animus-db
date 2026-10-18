# A wait gate below Raft must never fail on elapsed time (#1338)

`LsmEngine::await_backpressure` ran after the write was applied and returned
`StorageError::Backend` after 10 000 x 1 ms polls. The raftkv apply task only
retries `StorageFull`, so plain overload (a slow disk or a long compaction, no
fault at all) panicked a tablet's apply task after ~10 s.

Rule: a stall below consensus may delay a committed entry but must not fail it.
Bound a wait gate by evidence of failure (N consecutive genuine maintenance
failures, ENOSPC excluded and surfaced as the recoverable class), never by
wall or virtual time. Also treat a "disk was full" flag as stale while a fresh
maintenance run is in flight, and test the stall with `DiskConfig::sync_delay`
(a slow-but-working disk), stepping the sim until the write has applied before
injecting a fault so it hits maintenance and not the write's own WAL commit.
