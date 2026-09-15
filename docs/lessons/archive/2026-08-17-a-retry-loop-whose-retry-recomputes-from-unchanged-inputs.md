# A retry loop whose "retry" recomputes from unchanged inputs is a spin, not a retry — and only a cluster LARGER than the replication factor can prove a tablet-id-addressed forward

**A retry loop whose "retry" recomputes from unchanged inputs is a spin,
not a retry — and only a cluster LARGER than the replication factor can
prove a tablet-id-addressed forward** (ADR 0050 fork F5 fallout,
2026-08-17). Every tablet-id-addressed internal RPC (`SeedRows`,
`ForceSeal`, `TriggerAutoSplit`, `ClearBackfillCursor`, `StreamHotRead`)
used a resolve → relay-once → on-"not the leader here"-refusal
re-resolve-from-scratch loop, and one even documented that shape as
"correct (converged-or-timeout)". It converges only when the calling
node hosts a replica of the target tablet: the local replica's own
leader hint is what changes between iterations. With **no** local
replica, `resolve_cp_route`'s fallback deterministically returns the
tablet's *first* metadata replica every time, that follower refuses
with the real leader's address embedded in the refusal every time, and
the loop threw that hint away every time — an infinite spin dressed as
a retry. The split driver hit it the first time anyone ran a split on a
cluster with more nodes than RF: fork F5 places children at fresh
balance-chosen homes, so the parent's leader routinely hosts no replica
of one child, and seeding that child spun forever — the parent parked
`Splitting` holding every key with an empty `Building` child beside it,
indefinitely (the "auto-split made 2 new tablets but never rebalanced
the keys" field report). Every split e2e ran 3-node clusters at RF 3,
where every node hosts every tablet and the no-local-replica branch is
structurally unreachable. Two general forms: (1) for any retry loop,
name the input that CHANGES on a failed attempt — if the answer is
"none", it is a spin, and the fix is feeding the failure's own payload
(here: the refusal's leader hint) back into the next attempt, done once
at a shared choke point (`forward_to_tablet_leader`, now backing
`cp_forward` and every tablet-addressed RPC alike); (2) the existing
"test through a follower-connected node" rule is not enough for
tablet-addressed forwards — the caller must host *no replica at all* of
the target, which requires a cluster larger than RF
(`split_build.rs::split_completes_when_a_child_lives_off_the_parent_leader_node`
is the 5-node teeth).
