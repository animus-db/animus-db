# An ambiguous ack on a completion marker must be resolved by reading it back, never treated as "not written" (issue #707, ADR 0068)

`SimSegmentStore`'s put ack-lost fault (`crates/animus-sim/src/
segment_store.rs`) models something a real S3 client genuinely does: the
object is written to the store, and the caller still gets an `io::Error`
back — an intentionally ambiguous ack, the object-storage counterpart of
`ProposeResult::Accepted` meaning "appended locally", never "committed" (see
this file's own "Durable-before-visible" rule in the root `CLAUDE.md`). The
S3 export job (`animusd::dynamo::run_export_job_inner`) writes several
objects in a fixed, durable-before-visible order and `?`-propagates the
first error uniformly — correct for every *interior* write (`_started`,
each data chunk, `manifest-files.json`): a genuine failure there really
should fail the whole job, and ADR 0068 §9's own residual #3 already
accepts that a failed export's partial objects are not cleaned up. It is
**wrong** for the one **terminal** write whose mere presence a reader
trusts to mean "done" (`manifest-summary.json` here; a backup's own
completion marker, or any other single-object commit point, is the same
shape): treating that write's ambiguous error as "not written" produces a
torn completion — the object physically present while the catalog row
says `Failed` — exactly the invariant ADR 0068 as-built ("never a torn
`COMPLETED`") had already claimed to hold, and did not, until the nightly
corpus's depth-40 sweep drew the fault on that exact write
(`export_with_bucket_faults_converges_or_fails_cleanly`, seed
`12365148609929809193`).

**The fix is a readback, not a retry**: on an error from the terminal
write, `get` the same id back through the same `SegmentStore` handle.
Present with the exact bytes just attempted means the put landed despite
the error — treat the job as succeeded. Absent (or the readback itself
errors) means it is genuinely unwritten — propagate the original error
unchanged. Nothing is written a second time, so `SegmentStore::put`'s
write-once contract is never exercised twice for the same id, and no new
fault-injection surface is added.

**General lesson**: any store or RPC whose ack can be lost after the
underlying effect lands (this repo's own `Env` seams routinely model this
exactly to catch it) needs its callers to know which of their own writes
are "just data" — safe to fail outright and let the caller retry/fail the
whole operation — versus "the fact of my having happened is the whole
point" — a completion marker, a commit record, an idempotency-cache row.
Only the latter needs ambiguity resolved before trusting an error; wiring
the same resolution onto every write is unnecessary work, and wiring it
onto none of them (the bug here) silently breaks the one invariant the
marker exists to provide. When adding a new terminal/completion write to
any job that writes multiple objects/records in sequence, ask this
question explicitly rather than defaulting to uniform `?`-propagation.
