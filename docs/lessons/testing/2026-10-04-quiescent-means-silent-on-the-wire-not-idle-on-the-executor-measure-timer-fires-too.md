# "Quiesced" means silent on the wire, not idle on the executor: measure timer fires as well as messages

**Context.** C-17 Tier 1 (`animusd/src/sim_cluster_scale.rs`) measured an idle window over G quiesced
tablet groups on a `SimCluster` with `--quiesce-after` on. The first assertion written was the one
ADR 0048 and the crate guide promise: a quiesced group costs nothing. Counting only messages
(`TraceEvent::Send` by stream) agreed: 0 tablet-stream messages in 60 virtual seconds at 10, 50 and 100
groups. Counting `Simulator::stats()` (`task_polls`/`timer_fires`) disagreed: exactly **+12 timer fires
and +12 task polls per second per group**, independent of node count (3 and 9) and of G — 4 Hz per
replica, `animus-cp-data`'s `apply_loop` racing `ApplySignal` against `APPLY_SAFETY_POLL` (250 ms) even
while the group is quiesced. Quiescence stops Raft timers and heartbeats; it does not stop the apply
task's safety poll.

**Lessons.**
- Measure idle cost on both axes, wire (messages/bytes by stream, from the trace slice around the
  window) and executor (`SimStats` delta over the same window). A "silent" claim proven on one axis
  only is how a 4 Hz/replica wakeup survived a design whose docs said "apply-poll stops entirely".
- Subtract a zero-entity baseline cell (`..._g0`) before attributing a rate to the entity under test: the
  control plane alone is ~900 timer fires/s on 3 nodes and ~3300 on 9, and its all-to-all
  failure-detector heartbeats are O(nodes²) on the wire (170 msg/s on 3 nodes, 1130 on 9, no tablets).
- Assert the structural half (0 tablet messages, no group woke, control group committed 0 entries) and
  only *report* the cost half; a performance threshold in a SimEnv test turns a measurement into a flake.
- Scaling it: 12 wakeups/s/group x 1000 hosted groups = ~4000 timer wakeups/s per node at a replica
  share of 1/3 of the cluster's groups — the figure C-03 (ADR 0044 phase 3) needs.
