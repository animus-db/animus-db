# In a multi-refusal admin action that is deliberately local-leader-only, check leadership FIRST — every other refusal that reads local `Metadata` is only trustworthy once leadership is confirmed, since a follower's replica can genuinely lag the leader's own just-committed state.

**In a multi-refusal admin action that is deliberately local-leader-only,
check leadership FIRST — every other refusal that reads local `Metadata`
is only trustworthy once leadership is confirmed, since a follower's
replica can genuinely lag the leader's own just-committed state.**
`ClientCtx::admin_remove_member` (ADR 0032 PR3 decommission) originally
checked "is the member drained" (via `self.raft.metadata()`) before
checking "am I the leader" (`self.edge.leader_handle()`) — reads that
happened to agree on the *leader* node (where `self.raft` and the leader
handle are the same underlying core), but on a **follower** under load a
just-converged release-GC move can still be in flight over Raft
replication, so the follower's own stale metadata reported "still
referenced by 1 tablet" instead of the intended "not the control-plane
leader; retry on the leader" routing error — the wrong refusal reaching the
operator, not a wrong *decision* (the follower correctly refused, just for
a misleading reason). Invisible in an isolated single-test run (no
contention, replication is near-instant); it flaked exactly once under
`cargo test --workspace`'s parallel load, the same class of timing hazard
the "flaky ProdEnv test is a real bug" rule already covers, just showing up
as a wrong error string rather than a wrong outcome. Fix: check leadership
before any metadata-dependent refusal, mirroring "resolve the authority
first, then ask it questions" — the same shape as checking `is_leader()`
before trusting a quorum-derived fact elsewhere in this codebase.
(`admin_remove_member`; `tests/decommission.rs`'s follower-refusal
assertion.)
