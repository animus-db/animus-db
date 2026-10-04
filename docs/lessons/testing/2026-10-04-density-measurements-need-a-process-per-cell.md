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
