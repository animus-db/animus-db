# Mechanism superseded by ADR 0031 PR4

**Mechanism superseded by ADR 0031 PR4**: `cp_join_host_loop` (and its
per-tick unconditional re-narrow) is deleted — the tablet-host reconciler's
planner emits an explicit `NarrowScope` action whenever a hosted tablet's
metadata range shrank, so the re-sync is now a planned, ordered action
rather than a per-tick patch-up. The *lesson* (a cached per-node handle
derived from replicated state needs an explicit re-sync for every way that
state can change) is exactly what the reconciler design institutionalizes;
retained for the record. **A cached per-node handle derived from replicated state (here, a
`StorageScope`'s range) needs an explicit re-sync step for every way that
state can change in place — "it was correct when constructed" is not "it
stays correct."** The single-command split redesign (ADR 0028) gave a split
child's `RaftKvNode` a freshly-constructed, correctly-narrowed `StorageScope`
(via the normal join-host path, since a new tablet id is unseen by the
per-node `minted` claim set) — but the **source** tablet's `RaftKvNode`
predates the split, and nothing ever called `RaftKvNode::narrow_scope` on it
afterward, even though the primitive existed and was unit-tested in
isolation (`animus-cp-data/tests/narrow_scope.rs`) — its only call site in
the whole workspace. `animus-cp-data/CLAUDE.md`'s own doc for the redesign
even asserted `MetaCommand::SplitTablet` "narrows the source tablet's
`StorageScope` range," describing the *intent* as if the wiring existed,
when only the *replicated metadata* range narrowed — the per-node scope
object was silently stale forever. Symptom: `/admin/raftkv`'s `key_count`
for the parent tablet after a split kept reporting its pre-split (larger)
count — caught by a regression test asserting the parent + child counts sum
to the total instead of double-counting. Normal client reads never observed
it (routing resolves the target tablet from the narrowed *metadata* range
before ever reaching the stale scope), but any debug/admin surface that
reads a tablet's `StorageScope` directly by tablet id would. Fixed by having
the per-node `cp_join_host_loop` (`animusd`) — the loop that already polls
`Metadata.tablets` every tick — call `narrow_scope` unconditionally on an
**already-hosted** tablet too (previously it only acted on a tablet new to
its `minted` set), not just on first hosting; `narrow_scope` is a cheap,
idempotent mutex set, so doing it every tick regardless of whether the range
actually changed is safe. **When a redesign introduces a new "narrow/update
this cached thing" primitive, grep for its actual call sites, not just its
unit test — a primitive that exists and is tested in isolation but is never
wired into the production reaction loop is functionally dead code with a
green test suite.** (`animusd` `cp_join_host_loop`, `CpGroup::narrow_scope`,
`ClusterEdgeState::local_cp_member`;
`tests/admin_endpoint.rs::admin_raftkv_key_count_is_scoped_per_tablet_after_split`.)
