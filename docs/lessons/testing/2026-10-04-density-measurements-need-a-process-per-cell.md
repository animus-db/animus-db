# Density/RSS measurements need one fresh process per cell

Context: C-17 Tier 2 (`animus-cp-data/tests/group_density_cost.rs`). The first
version ran G=100 RF1, then RF3 cells in one process and subtracted the RSS
taken before hosting. The RF3 cell reported ~600 bytes/group: glibc reused pages
freed by the previous cell, so the delta was near zero. Run every (RF, G) cell
in its own child process (the test re-execs its own binary with an env knob) and
baseline RSS after the envs exist but before the first group is hosted.

Also learned:
- Starting a group's campaigning replica before its followers' streams are
  subscribed loses the first vote request; at 500+ groups a tail of groups never
  elected within minutes. Start followers first, leader last.
- Subtract a no-groups CPU baseline, and report `/proc/self/stat` ticks over a
  window of seconds (10 ms resolution); never sub-second windows.
- "Quiesced" is not "zero CPU": the per-group apply task still wakes every
  `APPLY_SAFETY_POLL` (250 ms). A threshold of "zero steady CPU" must be
  checked against that, not assumed from `next_deadline() == None`.

Follow-ups (same measurement):
- `commit_index() >= idx` right after a write measured 2-3 us for RF1: that is
  not durable+applied. Time until `commit_index`, `durable_index` and
  `engine_applied_index` all reach the index AND a `linearizable_get` reads the
  value back. On ext4 (ProdEnv `sync` = `File::sync_all`) that is ~22 ms for RF1
  and ~44 ms for RF3 (leader + follower fsync); on tmpfs it would be a lie.
- Per-group RSS for RF3 depends on how the groups were brought up: hosting 1000
  RF3 groups at once gave 26/29/39 KB per replica over three runs (4-32 s to
  elect) and 88 KB once while the host also ran at 206 s; staggered bring-up
  (`ANIMUS_DENSITY_BATCH=100`) gave 22 KB. Election thrash inflates RSS; never
  report one sample.
- All-in-one-process RF3 saturates 4 cores at ~1.3 cores per 1000 awake groups;
  past ~2000 groups the node starts losing leaders (term climbs, led count
  falls) at ~1.9 cores used. That is overload with no backpressure, not a
  steady-state cost: measure with RF1, or enable quiescence before the herd.
