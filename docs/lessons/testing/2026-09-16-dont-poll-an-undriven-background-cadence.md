# A converged-or-timeout poll is only as sound as the mechanism it waits on — if nothing in the test can drive it, its wall-clock budget is a real-thread-liveness bet, not a correctness check

A converged-or-timeout poll is the right shape for an eventual property, but
it is only as sound as the thing it waits on. Two `ProdEnv` tests
(`disable_survives_concurrent_periodic_seal_on_local_route`, in both a
DynamoDB Streams and a PITR variant) closed with a poll against a fixed-
cadence background `tokio` task — a `--stream-seal-bytes`/`--stream-seal-age`
size trigger evaluated once every 200ms — with a 20-second budget. The test
harness has **no way to drive that task's own tick**: it is a genuine
`loop { sleep(INTERVAL); .. }` spawned once at node start, with no test hook
to force a cycle. So the poll's actual bound was "however long it takes the
host's scheduler to run this task again," which is a real-thread-liveness
property, not something a fixed wall-clock budget can honestly promise —
under host contention it flaked, non-reproducibly, at a rate no amount of
synthetic CPU pressure on a quieter host could pin down after the fact.

The fix was not a wider timeout (an eventual property with an undriven
dependency has no timeout that is *right*, only ones that are *not yet wrong
often enough to notice*) and not a production change (the size/age trigger
itself, and the commit-wait loop it feeds, were independently verified
correct — instrumented and stress-tested under synthetic contention with no
sign of misbehavior). The actual bug was in the test's own sequencing: it
polled the undriven background mechanism for a property that a **different,
already-deterministic call one line later already guaranteed** — the same
suite's disable path forces a synchronous, retried-to-completion seal before
it ever returns success, and the test already trusted that call's own
`assert_eq!(status, 200)` unconditionally. Moving the coverage check to run
*after* that deterministic call, instead of racing it against the ambient
cadence beforehand, preserved the exact property under test ("no write is
ever silently lost") while removing the dependency on background-task
scheduling entirely.

**The generalizable check**: when a test's closing assertion is a
converged-or-timeout poll, ask what actually drives convergence. If it's a
fixed-interval background loop the test itself never ticks (no sim-style
`drive_*` hook, no direct call into the mechanism), the poll is testing
"does this host schedule background tasks fast enough" as much as it's
testing the property in the test's name — and a flake report against it
should look first for a nearby deterministic entry point already exercising
the same underlying mechanism, before assuming the mechanism itself needs a
production fix or the budget needs to grow. A poll against a driven
mechanism (a sim tick, an explicit force-call, a wake-based confirm) can be
trusted to converge quickly and predictably; a poll against an undriven
ambient cadence can only be trusted to converge *eventually*, which is a much
weaker and much less test-worthy guarantee.
