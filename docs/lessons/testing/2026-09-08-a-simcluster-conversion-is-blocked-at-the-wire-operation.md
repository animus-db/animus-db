# A `SimCluster` conversion is blocked at the wire-operation level, not just the scenario-design level, whenever the underlying `dynamo:: dispatch_item_op` has no arm for the operation a test needs — and that gap can be narrower than a whole operation family (2026-09-08, ADR 0061 rung I C-09 PR 3).

**A `SimCluster` conversion is blocked at the wire-operation level, not
just the scenario-design level, whenever the underlying `dynamo::
dispatch_item_op` has no arm for the operation a test needs — and that
gap can be narrower than a whole operation family (2026-09-08, ADR 0061
rung I C-09 PR 3).** `UpdateTimeToLive` was widened onto the generic
(`SimEnv`-capable) dispatch path in ADR 0061 rung H (C-08 PR 2), but its
read-side sibling, `DescribeTimeToLive`, was not — `dispatch_item_op` has
no `Operation::DescribeTimeToLive` arm at all, so any `SimCluster`
scenario that calls it 500s with "this operation is not yet supported by
the generic... dispatch path." A test converted before actually running
it under `SimEnv` (this rung's own PR 3 was authored without compiling,
per its own task brief) can look complete and still be blocked on a gap
one operation away from a sibling that already works — always run the
new scenario before trusting the design doc's own "converts cleanly"
claim. When the fix needs a source file outside the task's own edit
scope (here, `crates/animusd/src/dynamo.rs`), the right move is not to
paper over it by asserting a different status code or skipping the
assertion — it is to leave the original real-socket test in place with a
one-line reason naming the missing dispatch arm, exactly as if the
fixture itself couldn't express the scenario at all. **Closed 2026-09-08
(C-09 PR 5)**: the fix was exactly as narrow as predicted —
`describe_time_to_live` widened to `<E: Env, R: RelayClient>` (a pure
signature change; it already took an unused `_ctx: &ClientCtx` and isn't
even `async`, so there was no `tokio::time` body to convert at all) plus
one new `Operation::DescribeTimeToLive` arm on `dispatch_item_op`,
mirroring `UpdateTimeToLive`'s own arm immediately above it. The two
reverted scenarios moved back into `sim_cluster_ttl.rs` unchanged from
this PR's own reverted draft, `tests/dynamo_ttl.rs` dropped to its one
true residual. The generalizable lesson stands regardless: a narrowed
generic dispatcher can be missing just the read-side or write-side half
of an otherwise-symmetric operation pair, worth checking for
specifically — not just "is the operation covered at all" — before
either reverting a scenario or writing a wider fix than the gap needs.
