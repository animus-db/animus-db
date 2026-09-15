# Triaging a `ProdEnv` suite failure: compare its wall-clock against a clean run first — a markedly *shorter* run points at starvation, not at a logic bug.

**Triaging a `ProdEnv` suite failure: compare its wall-clock against a
clean run first — a markedly *shorter* run points at starvation, not at a
logic bug.** The instinct is that a struggling run takes longer; the
opposite is true here, because these suites are timeout-guarded. A test
whose guard trips exits at the guard, while the same test passing runs
its full body, so the failing run finishes *sooner*.
`dynamo_index_writes` failed once at **57s** against a clean run's
**127s**, and the cause was nothing in the diff: it had been launched
alongside other `cargo` invocations of my own, and the CPU it lost to
them was enough to trip a guard. Six subsequent runs (3 on the branch, 3
on its base, run alone) all passed in ~127s.
Two rules follow. **Operationally**: run a `ProdEnv` integration sweep
*alone* — a concurrent `cargo test`/`clippy` on the same box is enough to
manufacture failures that look like real ones. **In triage**: before
reaching for the branch-vs-base comparison (which costs ~12 minutes
here), check whether the change is even *reachable* from the failing
suite — `grep`ping the suite for the new request field took seconds and
proved the new code paths were unreachable and the emitted bytes
identical, which is a stronger argument than any number of green reruns.
Run the comparison to confirm, not to decide.
