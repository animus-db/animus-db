# A bounded wait that ends in `Err` below a consensus log is a hidden panic

Found while writing ADR 0081 (issue #1300). `LsmEngine::await_backpressure`
polls the memtable 10,000 times at 1 ms and then returns `Err`. It runs
*after* `log_and_apply`, so the write has already taken effect, and its only
production caller is the CP-data apply task applying a **committed** Raft
entry. That task retries exactly one error class (`StorageFull`,
`apply_stall.rs`); everything else reaches `flush_pending`'s
`assert!(halted, ...)`. So "give up loudly after 10 s" meant "panic the
tablet's apply task after 10 s of plain overload", with no fault involved.

Rules:

- Before adding or keeping a timeout/limit that returns an error from a
  storage or apply-path call, trace the error through every caller up to the
  apply task and write down which error classes it retries, which it asserts
  on, and which it swallows. A committed entry may be delayed forever but never
  failed or skipped.
- Put a delay or stall **before** the durable write, outside every lock and
  outside any "in-flight apply" counter (`applies_in_flight` gates flushes), so
  a waiting writer has done nothing yet and cannot hold up the work that
  relieves it.
- Refusal belongs at admission (ADR 0080's `Busy`, before the entry is
  appended), waiting belongs at the engine. Bound the *count of consecutive
  failures* that means "the engine is broken", never a duration that a slow but
  working compaction can exceed.
- A rate limit on background work needs an escape for the backlog it protects
  against (urgent work bypasses it and charges it into debt), and must never be
  waited on while holding a permit, a lock, or the single per-engine
  maintenance task.
