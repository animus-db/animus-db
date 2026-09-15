# A DRIVER_APPLIED design (ADR 0038) makes every cache-backed read lag Raft by an amount bounded only by apply-task starvation — so a test polling such a read must poll for *forward progress* of the apply watermark, never against a flat deadline.

**A DRIVER_APPLIED design (ADR 0038) makes every cache-backed read lag Raft
by an amount bounded only by apply-task starvation — so a test polling such
a read must poll for *forward progress* of the apply watermark, never
against a flat deadline.** The `decommission_drains_removes_and_allows_id_
reuse` flake: `/admin/status` reads `Metadata` off the async apply task's
`cache`, deliberately decoupled from the consensus loop so a slow engine
merge can't trip an election — which means under `cargo test --workspace`-
scale CPU contention the apply task can sit frozen for 30s+ (instrumented:
`commit_index`/`last_applied` converged in <1s while `engine_applied_index`
made zero progress for a full 60s, then caught up fine) with nothing wrong.
A flat 30s deadline turns that legitimate lag into a "flake"; bumping it
just moves the cliff. The principled shape: poll the apply task's own
watermark (`/admin/raft`'s `engine_applied_index`), fail only when it
*stops advancing* for a generous idle window with the awaited effect still
absent (that is a real stall, not contention), plus a large overall
backstop against livelock-shaped progress. This is the converged-or-timeout
rule's second-order refinement: when the property's convergence has no
contention-independent bound, the timeout must be on *progress*, not on
*arrival*. (`animusd/tests/decommission.rs`, `ControlHandle::
engine_applied_index`.) `animusd/tests/cluster_growth.rs` had the identical
anti-pattern at several call sites (member-promotion, rebalance-convergence,
post-kill tablet repair, `/admin/peers` propagation) and got the same
treatment; the shape is now factored into a shared `support::
poll_until_or_stalled` helper (`animusd/tests/support/mod.rs`) rather than
hand-rolled per file.
