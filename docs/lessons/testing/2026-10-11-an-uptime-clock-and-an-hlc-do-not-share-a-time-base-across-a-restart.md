# An uptime clock and an HLC share a unit, not a time base: a restart splits them

**Context.** R-01 chaos `chaos_kill` (seed 777): after a whole-cluster power cut
a consistent read of one key hung for 60 s on the nodes that held an in-doubt
2PC intent (#1204). No fault was left; the txn was just never recovered.

**What happened.** `ClientCtx::txn_recover` gated a decision on
`env.now()/1e6 >= record.created_ts.wall_ms + RECOVERY_GRACE`. Both sides are
milliseconds and the code comment said so ("same clock that minted
`created_ts`") - true until a restart. `ProdEnv::now()` is process uptime; the
HLC is `max(uptime, every timestamp seen)` and is re-seeded at start from the
engine's persisted high-water mark. A process that restarted after 108 s of
uptime had `now` near 0 and a record at `wall_ms` 108 010, so the gate stayed
shut for ~108 s (and in a long-lived cluster, for as long as the old
incarnation had run). The HLC itself sits flat while it is ahead of uptime, so
reading "now" off the HLC does not help either.

**Lessons.**

- Comparing a stored HLC wall component against a local monotonic clock is only
  valid inside one process incarnation. For a liveness grace, measure elapsed
  time on your own clock from your own first observation (never earlier than
  the intended "grace since creation"), and let either test open the gate.
- SimEnv shares one virtual clock across a node restart, so a sim cannot see
  this by crash/restart alone. Model the restart with
  `Simulator::set_clock_skew_for(node, -(now - 1s))` (never so far back that
  `now()` saturates at 0: a frozen clock freezes every deadline and the read
  "hangs" for an unrelated reason) and assert the recovery latency in virtual
  time, not just eventual success: a poll loop whose iterations each block
  12 s of virtual time has a budget of minutes, and passes the broken code.
- A finding's guess ("the txn_resolver_loop line in the log") can be a
  different, rarer failure than the one a re-run shows: compare the repro's own
  evidence (here the stuck txn's `wall_ms` vs the node's uptime) before trusting
  the issue text.
