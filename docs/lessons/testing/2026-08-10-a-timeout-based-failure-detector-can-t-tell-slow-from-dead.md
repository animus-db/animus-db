# A timeout-based failure detector can't tell *slow* from *dead* — its bound is load-bearing, and the frozen corpus is what catches an over-aggressive one.

**A timeout-based failure detector can't tell *slow* from *dead* — its bound is
load-bearing, and the frozen corpus is what catches an over-aggressive one.** A
replica watching a transaction it doesn't coordinate only sees *phase* changes,
not the coordinator slowly gathering a quorum, so "recover after N quiet ticks"
will recover a live-but-slow (or transiently-partitioned-then-healing)
coordinator if N is too small. Worse, recovering a transaction that *would* have
committed re-orders it after every conflict committed meanwhile (Accord recovery
bumps the timestamp), and where execution is **LWW-by-execution-timestamp** that
silently loses later same-key writes. The bound must exceed a realistic
slow-commit / partition-and-heal window; safety also wants the recovered commit
ballot-fenced so a healed coordinator's late commit can't revert it. An
over-aggressive 600ms bound passed every targeted consensus test but failed the
`animus-test` Elle corpus (`wide_write`/`isolate_one`) — **run the frozen corpus
(`cargo test -p animus-test`) after any change to recovery/execution timing**, it
exercises interactions a single-feature test never will. (ADR 0011 failure-detector
slice.)
