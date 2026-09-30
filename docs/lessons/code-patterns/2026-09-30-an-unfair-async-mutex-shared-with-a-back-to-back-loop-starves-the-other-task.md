# An unfair async mutex shared with a back-to-back loop starves the other task.

**`futures::lock::Mutex` is unfair: release wakes a waiter but does not hand it the
lock, so a task that re-locks before the woken waiter is polled wins.** A loop that
starts its next locked round the instant the previous one lands, with no yield point
between (here `drive`'s `persist_wal` under continuous proposals, its `select` polling
the persist arm first), barges ahead forever, and the other task (the ADR 0038 apply
task's compaction section) never runs: frozen `engine_applied_index`, stale
`metadata()`, unbounded `pending_apply`. Symptom in sim: a slow `SyncDelay` makes each
round long enough that the waiter is always mid-wait at release. Fix: a FIFO-fair
lock (`animus-control::fair_lock::FairMutex`: ticketed queue, release reserves the
lock for the head; no timer, so identical under `SimEnv`/`ProdEnv`, and cancel-safe).
A back-off sleep would also work but adds latency and a clock dependency. Note that
`SimEnv` charges `sync_delay` per `append` as well as per `sync`, so a round with N
records takes (N+1)x the delay; pick a test arrival rate below that capacity or the
backlog collapses for an unrelated reason. Any other `Arc<AsyncMutex>` shared between
a hot back-to-back loop and a background task (check `animus-cp-data`'s `wal_lock`)
has the same exposure.
