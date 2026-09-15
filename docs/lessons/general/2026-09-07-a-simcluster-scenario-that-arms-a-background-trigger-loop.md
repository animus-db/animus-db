# A `SimCluster` scenario that arms a background trigger loop and then keeps writing must drive that loop's own downstream dependency itself, not just wait (ADR 0061 rung D4 PR 2)

`sim_cluster_auto_split.rs`'s scenario (c) writes a burst of new items
AFTER calling `SimCluster::set_auto_split_thresholds` — and `SimCluster::
dynamo`/`put`/etc. all advance virtual time internally
(`spawn_and_capture`'s own `run_for`), so by the time a burst of several
writes has been issued, the auto-split loop has genuinely had ticks to
fire. A write landing on a tablet mid-fork gets refused with the house
`"; retry"` transient error (`index_drain::is_retryable_elsewhere`'s own
convention) — expected, real behavior. The first draft's retry helper just
waited (`run_for(200ms)`) and retried, which spun to its own attempt bound
and failed every time: `SimCluster` never spawns `index_drain::
change_consumer_loop` as a background task (it drives Streams/PITR/
GSI-drain machinery most fixtures don't need), so nothing was EVER going
to propose the `MetaCommand::CutoverSplit` that clears the freeze — the
tablet would stay `Splitting` forever no matter how long the retry helper
waited. The fix: the retry helper must itself call the fixture's own
manual driver (`SimCluster::drive_inplace_split_cutover`) on every retry
attempt, exactly like the poll loops that DO expect a fork to converge
already do. **General lesson, not specific to this one loop**: when a
`SimCluster` fixture provides a manual driver for a background mechanism
production wires as a loop this fixture doesn't spawn, EVERY caller that
can transitively depend on that mechanism completing — not just the ones
explicitly polling for it — needs to drive it, including a plain retry
helper that only looks like it's waiting out an unrelated transient.
