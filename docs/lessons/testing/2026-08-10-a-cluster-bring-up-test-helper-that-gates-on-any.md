# A cluster-bring-up test helper that gates on `any(is_control_leader)` is wrong for a test that restarts a single node of a multi-node cluster

**A cluster-bring-up test helper that gates on `any(is_control_leader)` is
wrong for a test that restarts a single node of a multi-node cluster** — the
restarted node rejoins as a follower (the majority never went down), so it
never reports itself leader and the helper times out waiting for a
leadership signal that was never the actual readiness condition. A
single-node cluster's own restart test hides this (a 1-of-1 group is always
its own leader). For a restart-one-node-of-N test, wait for the node to
*catch up* instead — poll its admin/Raft view until `last_applied ==
commit_index && commit_index >= snapshot_index + log_len` (no leadership
requirement) — which is also the correct replay-completion gate before any
convergent post-restart assertion. (`animusd` ADR 0029 release-GC restart
test.)
