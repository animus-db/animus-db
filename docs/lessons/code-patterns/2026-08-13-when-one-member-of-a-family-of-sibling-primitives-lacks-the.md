# When one member of a family of sibling primitives lacks the family's implicit behavior, a caller written from the family's reputation gets a structural, permanent failure — check the specific primitive's contract, not its siblings'.

**When one member of a family of sibling primitives lacks the family's
implicit behavior, a caller written from the family's reputation gets a
structural, permanent failure — check the specific primitive's contract,
not its siblings'.** `ClientCtx`'s CP write-side primitives almost all
auto-provision a table's first tablet on demand (`cp_put`,
`cp_kind_write`, `cp_batch_write`, `cp_batch_write_patient`, `cp_txn`,
the Dynamo edge's `quorum_write`) — but `cp_write` itself, the rawest of
them, does **not**: every existing caller provisioned upstream, so the
gap was invisible. The ADR 0041 GSI drain then wrote a *brand-new*
table's rows (a GSI's hidden index table, which nothing upstream ever
provisions) through `cp_write`, and the result wasn't slowness but
*never*: `cp_route` waited out its full `CLIENT_TIMEOUT` on a table with
no tablet, failed, and the next 200ms tick repeated it, forever — while
reading exactly like the "first convergence is just slow" hypothesis the
handoff note recorded. The fix (the drain provisions lazily, first tick
with records to apply) matters less than the diagnostic: when a
convergence loop makes zero progress ever, suspect a step whose
precondition is *never* established, and check who was supposed to
establish it. (`animusd/src/index_drain.rs::drain_tablet`, 2026-08-13.)
