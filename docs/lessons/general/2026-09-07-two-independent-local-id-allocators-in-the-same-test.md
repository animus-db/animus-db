# Two independent local ID allocators in the same test fixture will eventually collide once a test exercises both paths in one run — derive both from the same live source of truth (ADR 0061 rung D3 PR 2a)

`SimCluster` grew two ways to create a tablet over its lifetime: a
hand-hosted path (`create_table_with_replication`, proposing `CreateTablet`
directly with an id from the fixture's own local counter,
`next_tablet_id`, starting at 1 and incrementing only on that method's own
calls) and a wire-provisioned path (`ClientCtx::provision_tablet`, reading
`Metadata::next_free_tablet_id()` — the *replicated*, live allocator —
fresh on every attempt). Every test before this rung used exactly one of
the two paths per cluster, so the two counters' id spaces never had reason
to overlap. The first test to use *both* in one cluster (three wire-created
tables, then one hand-hosted table for an unrelated reason — seeding a
table whose *name* needed to match a reserved shape) reproduced the
collision immediately: the hand-hosted call proposed `TabletId(1)`, a real
tablet id another table already held (from the three wire creates ahead of
it), the propose was rejected ("tablet already exists"), and the
hand-hosted method's own convergence poll timed out waiting for a tablet
that would never appear under that name.

**The general lesson**: two allocators for the "same kind of thing" in one
fixture/system, each independently monotonic, are safe only as long as
nothing ever exercises both in the same scope — which is exactly the kind
of invariant that erodes silently as a fixture grows new capabilities,
with no compiler or type-level signal that it has. The fix is never "make
the two counters agree" (a synchronization problem with its own races) but
"delete the second allocator and read the *one* real source of truth
fresh, every time" — here, deriving `create_table_with_replication`'s own
tablet id from `self.controls[leader].metadata().next_free_tablet_id()`
instead of its own field, the identical live read `provision_tablet`
already did. Worth grepping for on any test fixture that grows a second
"quick and dirty" id-minting shortcut alongside an already-existing
"ask the real system" one — the two are a latent collision waiting for the
first test that happens to combine them.
