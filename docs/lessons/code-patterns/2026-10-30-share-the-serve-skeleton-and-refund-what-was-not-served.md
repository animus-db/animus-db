# Share the eventual-read skeleton; refund a charge for a read that served nothing

Issues #1419 (option B) and #1424. Three forwarded eventual-read arms each
hand-rolled "resolve tablet, scope check, precharge, serve, postcharge,
refuse", and they had drifted: a failed engine scan (`.ok().into_iter().flatten()`)
became an empty page while the pre-charge stayed debited. Rules:

- One helper owns the skeleton (`ClientCtx::serve_stale_read`); the per-arm
  parts (scope check, serve call) are closures. The serve step returns a
  three-way outcome (`Served{bytes}` / `NotServed` / `Failed`), so an engine
  error cannot be spelled as an empty `Served`.
- A charge taken before the work must be undone when nothing was delivered
  (`throttle_refund_read`), on the local and forwarded paths alike; test both
  with a real read failure (flush the LSM, `DiskConfig::set_error_prob(1.0)`).
- `Disk::read_at` of a missing file used to return an empty vector in both
  `ProdEnv` and `SimEnv`; every caller reads a file it knows exists, so the
  empty result only masked the cause as a "short sstable block read". It is
  `NotFound` now. Keep the two `Env`s identical on error semantics.
- Still swallowing (same hazard, point reads or non-client paths): the
  `.get(..).ok().flatten()` family in `animus-cp-data`, `cursor_rows_with_token`,
  `raw_scoped_keys`, `scan_hot_change_max`, `txn_verify_staged`, the TTL
  reaper's `local_scan_kind` wrapper.
