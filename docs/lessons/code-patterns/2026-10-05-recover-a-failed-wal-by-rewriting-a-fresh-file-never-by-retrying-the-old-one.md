# Recover a WAL that hit ENOSPC by rewriting a fresh file, never by retrying the old one

Issue #1185 (R-01 (d)). An ENOSPC on a WAL `append`/`sync` used to hit an
`assert!` and silently kill the group's consensus task until restart.

**Lesson.** After a failed append or fsync the file's tail is unknown: a torn
partial append may sit in it, and the kernel may have dropped dirty pages, so a
retried `fsync` on the same descriptor can return `Ok` for data that is gone
(fsyncgate). The only sound recovery is to mark the file *suspect*, never touch
it again, and write the whole in-memory log (`RaftCore::wal_image()`) to a
**fresh file** (`Disk::replace`, new descriptor) once space returns. Mark the
rounds durable only after that rewrite succeeds, so nothing is acked early. This
needs no requeue of drained records and no persisted-format change, because the
in-memory log is a superset of everything any failed round tried to write.

**Corollaries worth remembering.**

- A shared file (`SharedWal`) needs its own "armed by ENOSPC only" gate so a
  healthy sibling cannot append after the suspect tail; arm it on ENOSPC alone,
  not on every injected generic error, or unrelated fault cells change behaviour.
- A failed `replace` leaves a half-written `.tmp` sibling holding the very space
  you are waiting for: remove it.
- Skip the recovery attempt while a staged compaction rewrite is in flight; it
  shares the `.tmp` name.
- SimEnv injects ENOSPC on reads as well, so a restart during a 100% window
  reads an empty WAL. Do not combine them in a corpus.
- A flaky-disk (probabilistic) workload can finish inside the fault window, so
  "progress after the window" is only assertable where the workload outlives it.
  When a corpus assertion fails, check the test's assumption before the product
  code.
