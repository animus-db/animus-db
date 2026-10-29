# A scan error must never read as "nothing there"

Issue #1419: `CpGroup::pending_changes_key_order` ended in
`scan(..).await.ok().into_iter().flatten()`, so an LSM read error looked exactly
like an empty change log. Its callers act on emptiness: `seal_now` /
`pitr_seal_now` return `Ok(None)` ("nothing pending"), which lets a stream or
PITR disable complete; `trim_janitor` and the quiesce-veto sweep treat the
group as clean; the MREC shipper reported itself caught up; `hot_read` returned
an empty `GetRecords` page.

Rules:
- A function whose callers decide a *destructive or completing* action from
  "the result is empty" returns `Result`, never `Vec`. `.ok()` / `unwrap_or_default()`
  on a storage read is only acceptable where empty is the correct degraded
  answer, and that needs a comment saying why.
- Every caller picks its own safe reading of "unknown": seal and trim return a
  retryable error (`...; retry`, so the disable path retries inside its deadline),
  the quiesce veto is held, the MREC shipper records a failure and waits,
  `gsi_caught_up` answers "not caught up", `hot_read` returns an error reply.
- Test it with a real read failure, not a stub: flush the LSM so the scan reads
  SSTable blocks, then `DiskConfig::set_error_prob(1.0)` while the consumer runs
  (`sim_cluster_seal_races::*_scan_error_is_retried_not_read_as_an_empty_log`).
  Assert the error, that nothing was sealed or deleted, and that the same call
  succeeds exactly-once after the disk heals.
- Still swallowing (out of scope of #1419, same hazard): `local_scan_kind*`,
  `scan_hot_change_max` (group-start seed of the `LATEST` iterator position),
  `cursor_rows_with_token`, `raw_scoped_keys`.
