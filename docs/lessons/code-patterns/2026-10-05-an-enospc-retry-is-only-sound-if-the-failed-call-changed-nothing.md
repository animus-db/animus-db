# An ENOSPC pause-and-retry is only sound if the failed call changed nothing (and a maintenance failure after a durable write must not fail the write)

**Context (issue #1218, disk-full in the LSM engine).** The apply task panicked
on any engine error. The tempting fixes are to swallow the error or to
`.expect` less; both lose or reorder committed state. The sound shape is *pause
inside the failing call and retry the identical call*, because the task is the
only engine writer and being blocked inside the call preserves order.

**What had to be true for that to work:**

- **The failed call must have changed nothing.** A WAL group commit applies
  only after a successful sync, so a failed commit is a no-op for the memtable;
  but on a real disk a failed `append` can leave a *short write* at the segment
  tail. A retry that appends behind it makes recovery refuse the file or drop
  the acked record. The fix is to cut the segment back to its last durable
  length before the next batch (a `replace`, atomic), and to do it before any
  rotation: sealing a segment that is empty or torn leaves a hole in the
  "probe forward until the first missing segment" discovery. `SimEnv` fails ops
  *cleanly*, so a sim-only test cannot see this: the test appends the torn
  bytes itself, and was mutation-checked (disable the repair, test fails).
- **Maintenance failing after a durable write must not fail the write.**
  `merge_batch` ran flush/compaction inline and returned its error, so a
  retrying caller would redo (or report as failed) a write that took effect.
  Inline maintenance ENOSPC is deferred instead; the next write retries it.
- **Cleanup of partial outputs is part of "changed nothing"**, and on a full
  disk it is also the space the retry needs. Remove a failed flush/compaction's
  outputs, but only delete after a failure that is *known* not to have swapped
  the manifest (ENOSPC fails the atomic `replace` before it swaps); any other
  manifest error leaves the outcome unknown and the file may now be named.
- **Retrying reads is fine, retrying writes needs idempotence.** Every
  apply-task write is LWW per `(key, version)`, which is what makes a retry
  after an ambiguous failure harmless.

**Test lesson.** A probabilistic fault window can be dodged by a single seed;
assert "the window really injected the failure" with corpus-wide counters, not
per seed. And prove a new corpus has teeth by disabling the fix and watching it
panic (the LSM disk-full cells did, at the apply task's read-ceiling marker).
