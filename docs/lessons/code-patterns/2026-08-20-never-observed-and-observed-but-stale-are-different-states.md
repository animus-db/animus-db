# "Never observed" and "observed but stale" are different states, and collapsing them by defaulting a new freshness field to `0` silently regresses every category the observer structurally never visits.

**"Never observed" and "observed but stale" are different states, and
collapsing them by defaulting a new freshness field to `0` silently regresses
every category the observer structurally never visits.** Adding the quiesce
freshness gate uniformly with a `0` default would have permanently blocked
quiescence for `Building` split children and hidden GSI-table tablets —
categories `change_consumer_loop` already, deliberately, never sweeps — thus
destroying ADR 0048's whole wakeup-reduction win, in a way the invariant test
("a group with a non-empty change log must never quiesce") could never catch,
because it only asserts the *safety* direction. A `u64::MAX` sentinel ("no
constraint") preserves prior behavior for the unvisited. Before tightening
any invariant fed by a periodic sweeper, enumerate which categories that
sweeper skips and argue each one's safety explicitly; a stricter rule applied
to a component that never receives the signal is a liveness regression, not a
safety win. (#302, `crates/animus-control/src/raft.rs`, 2026-08-20.)
