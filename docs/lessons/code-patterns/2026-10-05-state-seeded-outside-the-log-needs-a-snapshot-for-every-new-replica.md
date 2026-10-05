# State seeded outside the Raft log must be shipped as a snapshot to every new replica

**Found by**: issue #1229 (a split child moved wholesale by directed Placing lost
13 of 20 acked pre-split keys).

A split child's engine is cloned from its parent's, so its pre-fork rows are in no
log entry. Raft only ships a snapshot when `next_index <= snapshot_index`, and a
fresh group's `snapshot_index` is 0 until the first compaction, so a learner added
early was replicated the log from entry 1: it "caught up" on post-fork writes only,
was promoted, and the old homes then reclaimed the only copies of the rest.

Rules that generalize:

- Whenever a group's base state is created by something other than its log (clone,
  restore, seed), the log is not a faithful catch-up source until a snapshot base
  exists. Make "log from entry 1" unavailable for new peers (here
  `RaftCore::log_omits_base`), not "compact soon and hope".
- Derive such a flag from durable engine state (the split-trim marker), never from
  how the group happened to be started: a restart re-hosts through a different path.
- "Learner caught up" proves the learner matches the *log*, not the *data*. A test
  must read pre-fork data back from a replica set with **no overlap** with the old
  one; moving one or two of three replicas hides the bug because an old replica
  still holds the rows.
- The repro that found it was a harness-checked product bug: confirm with the
  `LsmEngine` backend plus real restarts (`new_with_lsm_engines`), not only
  `MemoryEngine`.
