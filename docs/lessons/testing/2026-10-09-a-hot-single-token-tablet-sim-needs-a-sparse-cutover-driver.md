# A sim that drives split cutover every tick costs minutes of wall time

**Context.** The R-01 test for "a single-token tablet is never split"
(`sim_cluster_auto_split` scenario `j`) first ran a 120 s virtual window calling
`drive_inplace_split_cutover` on every node every 100 ms, the way
`poll_split_converged` does for a handful of ticks. It took ~9 s of wall time per
virtual second and never finished in a reasonable time (it looked like a hang).

**Lesson.** `drive_inplace_split_cutover` is expensive per call; the poll helpers only
get away with it because they converge in a few ticks. A scenario that must run a
*long* quiet window (several auto-split cooldowns) drives it only while a fork is in
flight (some tablet not `Active`, or the expected tablet count not yet reached) and
otherwise just advances time in coarse steps. Measure real time per virtual second on
a new scenario before widening the window, and keep a failing-without-the-fix
mutation run in the loop: here it failed in ~18 s once the loop was cheap.
