# A real multi-chunk `InstallSnapshot` transfer completes in single-digit milliseconds of virtual time — a coarse `Vec<(Duration, Nemesis)>` fault schedule cannot reliably land "mid-transfer" (`animus-control`'s `control_corpus.rs`, PR③)

Composing a fault with an in-flight chunked snapshot transfer sounds like it
should fit this repo's usual `faults: Vec<(Duration, Nemesis)>` scenario
shape (schedule the fault at some duration into the run, mirroring every
`LeaderKill`/`PartitionLeader` cell elsewhere in this corpus). An
exploratory run (millisecond-granularity polling of a real, healed,
multi-chunk transfer between two real `RaftNode`s) found this doesn't work:
once the leader starts shipping chunks to a caught-up-eligible follower,
the WHOLE transfer — first chunk received through fully reassembled
`Metadata` — completes in roughly 3ms of virtual time, because this plane's
replication path has no artificial per-chunk delay and virtual time costs
no real wall-clock proportional to its size. A fault scheduled "2.2 seconds
into the run" has essentially zero chance of landing inside a 3ms window
whose START time itself isn't even known in advance (it depends on when
the leader notices the follower is behind past its compacted prefix, which
depends on heartbeat timing, election history, etc. — all seed-dependent).
The fix was to replace the duration guess with a **condition-based poll**:
step virtual time in small (200µs) increments, checking a directly
observable proxy for "the transfer has started but not finished" (the
receiving follower's `snapshot_index() > 0` — set from the FIRST chunk's
base index — while its reassembled state, e.g. `metadata().members.len()`,
is still short of the target), and inject the fault the instant that holds.
This lands inside the real window regardless of a given seed's exact
timing, and the test itself asserts it actually caught the window (rather
than the transfer racing past a coarse poll entirely) before proceeding —
so a future change that made the transfer effectively instantaneous would
fail loudly here instead of silently degrading into a fault-free no-op
cell. **General lesson: before reaching for a scenario harness's existing
`Duration`-based fault-scheduling shape to hit a specific in-flight window,
measure how long that window actually is in virtual time — a data-plane
"lazy, on-demand, lasts-until-idle" mechanism can complete in microseconds
to milliseconds once triggered, which no second-or-millisecond-granularity
fixed schedule can reliably intersect; a condition-based poll on a directly
observable proxy for "in progress" is the fix, and asserting the poll
actually caught the window (not just that the scenario as a whole
converged afterward) is what keeps the test honest about whether it
exercised the fault window at all.**
