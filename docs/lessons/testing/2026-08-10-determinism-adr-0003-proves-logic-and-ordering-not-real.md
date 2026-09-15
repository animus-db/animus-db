# Determinism (ADR 0003) proves logic and ordering, not real-thread liveness.

**Determinism (ADR 0003) proves logic and ordering, not real-thread liveness.**
`SimEnv` is single-threaded + cooperative, so a `Mutex` guard held across an
`.await`, a lost waker, or a leader-election/group-commit deadlock can pass
every sim test and only hang under the real multi-threaded `ProdEnv`. Any
concurrency primitive (locks, waker handoffs, group commit, leader election)
needs a **real `#[tokio::test(flavor = "multi_thread")]` over `ProdEnv`,
timeout-guarded** so a deadlock fails loudly. (Found via the WAL group-commit
deadlock; pattern in `animus-storage/tests/lsm_concurrent.rs`.)
