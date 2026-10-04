# Releasing a lock does not fix a stall when the observable waits on the lock holder's task (issue #1116)

`prod_compaction_persist_round` failed at a 2.1s worst confirm. The code-reading
hypothesis was "compaction's `env.replace` runs under `wal_lock`, freezing
persist rounds". Moving the replace's fsync out of the lock was necessary and
the first SimEnv test (a slow-replace disk model) **still failed at 3.0s**:
a write confirms via a linearizable read, which gates on `engine_applied`,
and only the apply task advances that — the same task that was running the
rewrite inline. Lesson: before declaring a stall fixed, trace what the
*observable* (here the confirm) waits on, not just the lock the suspect holds;
and let the failing sim test, not the hypothesis, say when it is done. The fix
needed both: stage outside the lock (`Disk::stage_replace`/`commit_staged`) and
run the rewrite in a spawned task with a one-in-flight slot.

Related traps found on the way: a shared `CARGO_TARGET_DIR` across worktrees
makes cargo's mtime fingerprints stale (a sibling worktree's older build
satisfies yours; symptom: "no method X" for code that exists) — touch sources
or use a private target dir. A crash test whose catch-up is instantaneous
cannot see a dropped final delta; give the disk a non-zero `sync_delay` so
rounds land between the phases (verified by mutating the final drain away).
