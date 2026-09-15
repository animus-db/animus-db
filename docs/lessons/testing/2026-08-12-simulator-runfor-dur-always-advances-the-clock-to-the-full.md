# `Simulator::run_for(dur)` always advances the clock to the full deadline once idle — it does not "return early" just because the future you were driving already resolved.

**`Simulator::run_for(dur)` always advances the clock to the full
deadline once idle — it does not "return early" just because the future
you were driving already resolved.** `run_until`'s loop drains every ready
task, then either fires the next scheduled timeline event (if before the
deadline) or, once none remain, jumps `clock` straight to the deadline and
returns. A helper that calls `run_for(Duration::from_secs(2))` once *per
read* to drive a spawned `linearizable_get` therefore advances that read's
serve timestamp by a full 2 (virtual) seconds every single call — fine for
an ordinary read-reflects-a-write assertion, but fatal for anything that
cares about *how close together* consecutive reads' timestamps are (ADR
0018 §2/PR2b's ceiling-amortization test: the naive per-read-`run_for`
version advanced each read's ts by seconds, trivially exceeding the
500ms `HLC_MAX_OFFSET` window between every pair and making every read
propose its own ceiling). Fix: drive a whole sequential batch — spawn one
task that loops the operation N times with no artificial gap, then call
`run_for` **once** with a budget sized for the whole batch — which is also
what keeps consecutive real-world back-to-back operations' timestamps
genuinely close, matching the workload the amortization is meant to
cover. (`crates/animus-cp-data/tests/ts_cache.rs`.)
