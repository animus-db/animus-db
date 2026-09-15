# A cross-task veto fed by a periodic sweep needs a freshness contract expressed in the same value space the consumer already gates on — a wall-clock stamp compared against an activity marker is not equivalent, even when the arithmetic looks right.

**A cross-task veto fed by a periodic sweep needs a freshness contract
expressed in the same value space the consumer already gates on — a
wall-clock stamp compared against an activity marker is not equivalent, even
when the arithmetic looks right.** `RaftCore`'s quiesce veto (ADR 0048 fork
D) was a bare `AtomicBool` refreshed by `animusd`'s 200ms
`change_consumer_loop`; a write landing between one sweep's observation and
the next left a stale `false` in place, and `RaftCore` had no way to know it,
so an idle-looking group could quiesce while still owing stream work. Two
traps sat in the obvious fixes. First, the natural stamp — record the sweep's
`Nanos`, require it `>= last_activity` — is **unsound**, because
`last_activity` is bumped at *propose* time while the sweep observes *applied
engine content*: a sweep racing that gap passes the check while describing
pre-write state. The sound version indexes the observation in the checker's
own coordinate space (`engine_applied_index()` compared against
`commit_index`). Second, a valid lower bound requires reading that index
**before** the scan it bounds; reading it after is symmetrically unsound, as
a write committing in between would be absent from the scan yet counted as
observed. General rule: when bridging an async-observed fact into a sync
invariant check, version the observation in the checker's coordinates, and
read the version before the observation. (#302,
`crates/animus-control/src/raft.rs`, `crates/animus-cp-data/src/lib.rs`,
`crates/animusd/src/index_drain.rs`, 2026-08-20.)
