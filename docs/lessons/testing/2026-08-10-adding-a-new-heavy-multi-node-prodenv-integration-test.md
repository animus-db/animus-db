# Adding a new heavy multi-node `ProdEnv` integration test raises CPU/IO contention on every *other* test binary running in parallel under `cargo test`, and a pre-existing hard latency-bound assertion (e.g. a median write latency under some millisecond ceiling) can flake purely from that added load — no code regression in either test.

**Adding a new heavy multi-node `ProdEnv` integration test raises CPU/IO
contention on every *other* test binary running in parallel under `cargo
test`, and a pre-existing hard latency-bound assertion (e.g. a median write
latency under some millisecond ceiling) can flake purely from that added
load — no code regression in either test.** Confirm such a failure by
re-running the victim *in isolation* before treating it as real; a
release/GC-style loop that is a genuine no-op on a steady cluster (its
predicate returns empty, then iterates nothing) cannot be the cause. Same
family as the documented "a flaky ProdEnv test is a real bug" rule, with the
refinement that a *newly-added* heavy test can itself be the load source —
so the right move is isolate-and-reconfirm, not loosen the victim's bound.
