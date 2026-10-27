# An unbounded delete decided from a start-of-tick snapshot must revalidate after it scans

Issue #1410: `disable_survives_concurrent_periodic_seal_on_local_route` lost one
acked change record (11 sealed of 12) about once in 20-40 runs under CPU
contention. The final label's only shard started exactly at the previous
label's last `hlc_range.1`, and every scan before it was clean (no scan errors,
no rescan growth, no Intent envelopes), so the record was gone from the hot log
before the final seal ran.

Root cause: `index_drain::trim_janitor` takes the change-consumer tick's
`meta`, snapshotted at the top of the tick. With `stream_enabled == false` and
no other term it deletes the whole hot tail (`trim_all`: "the final seal already
moved everything into a segment"). That reasoning is only true if the stream is
still disabled when the scan happens. A starved tick that snapshotted the
disabled table, then ran after `UpdateTable(StreamEnabled: true)` and the first
new-label `PutItem`, scanned that unsealed record and deleted it. The final
disable's `seal_now` then found nothing to seal. Not a `seal_now`, watermark or
scan-error bug.

Fix: scan first, then, only for the unbounded delete, re-read `metadata_fresh()`
and skip the tick if any term (stream, PITR, GSI, MREC) appeared. Anything in
the scan was committed before the fresh read, so "no term" at that point proves
no scanned record belongs to a live term. Bounded deletes (`<= trim_point`) are
safe from a stale snapshot because the bound is already consumed.

Rules:
- A destructive action whose safety rests on "nothing new can exist" must check
  that against a read taken after it has enumerated what it will destroy, not a
  snapshot taken before.
- The window is the tick's own length, so it only opens under starvation. A
  loaded-host flake is a hint to look for a snapshot/act gap.
- Regression test: `sim_cluster_seal_races::a_stale_snapshot_trim_never_deletes_a_reenabled_streams_records`
  holds the stale snapshot across a re-enable and calls `trim_janitor` directly.
