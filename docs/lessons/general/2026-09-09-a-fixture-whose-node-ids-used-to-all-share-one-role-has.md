# A fixture whose node ids used to ALL share one role has implicit "every id is a control voter" assumptions baked into loop targets and membership lists — heterogeneous roles surface every one (ADR 0061 rung L, C-12 PR 3)

`SimCluster::restart`'s control-bearing branch rebuilt a restarted node's
fresh `RaftNode<SimEnv>` with `all_ids = (0..self.nodes as u64).map(nid)
.collect()` as its own membership — correct for every scenario that ever
exercised it, because until this PR every node in `self.roles` genuinely
was a control voter, so `self.nodes` and the control voter count were
always equal. The instant `NodeRole::Data` became reachable (at
construction, this PR; or via a pre-existing `SimCluster::grow` call,
already possible before it) that equality silently stopped holding: `all_
ids` would list a non-voter data-only node as a Raft member on the very
next restart of an *original* node. Nothing caught this in review or by
`cargo check` — the bug is a value, not a type error, and no existing
`sim_cluster_growth.rs` scenario had ever combined `grow` with a `restart`
of an original (non-grown) node, so it was a real, previously-latent gap
with zero test coverage on either side of it. Found only by deliberately
re-deriving every "list of node ids" binding in the function from first
principles while widening it for a second role, not by symptom.

**The general form, worth checking whenever a fixture that used to have one
uniform node shape gains a second one**: grep every place the fixture binds
"every node" / "all ids" / "the whole node set" as a stand-in for "every
control voter" (or any other role-scoped subset) — a heartbeat target list,
a Raft membership list, an admin-info field, anything a helper's own doc
comment describes as "the control group" without actually deriving it from
the role array. Before the split these two sets were the same list by
construction, so using either name was harmless; the moment they diverge,
the wrong one silently starts including (or excluding) members it
shouldn't, and the failure mode is not a compile error or even a hang — a
membership-list-with-an-extra-non-voter can commit and elect just fine
under `SimEnv`'s forgiving virtual-time scheduler, it just isn't the group
production would ever actually form.

**A second instance of the identical shape, same PR**: `SimCluster::grow`
— which predates per-node roles and was itself the FIRST place a genuinely
heterogeneous node ever existed in this fixture — had its own two bugs from
exactly this class, just never named as such: it spawned `backup_janitor_
loop` unconditionally on its own grown data-only node (a control-plane-
leader-only loop that can never do anything useful there, since a `Remote`
control handle can never become control-plane leader — harmless in
practice, since the loop's own leader gate makes it a permanent no-op, but
production-inaccurate) and never spawned `ttl_reaper_loop` at all (a real
gap — production spawns one on every data-only node). Both were written
once, by hand, before there was a second, symmetric construction path
(`new_with_roles`'s own `NodeRole::Data` case) to cross-check against —
the cross-check is what surfaced both: factoring the two constructors'
shared loop-spawning logic together forced an explicit "which of these
loops does THIS role actually get, checked against production's own
`start_data_with_growth`" pass, rather than "copy whatever `grow`
happened to spawn last time." **Lesson**: when a second, symmetric
construction path for the same conceptual thing (a data-only node, here)
is about to exist, use building it as the forcing function to re-derive
the first one's own loop/membership set from production ground truth
directly, rather than copying it forward unexamined — a solo, uncross-
checked implementation is exactly where this class of bug hides.
