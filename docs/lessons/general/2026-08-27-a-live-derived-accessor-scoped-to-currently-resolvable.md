# A live-derived accessor scoped to "currently resolvable" state silently drops to a degenerate value once its inputs are torn down — freeze the figure while it's still derivable (ADR 0059 §3, Train 1 PR④, `BackupRow::total_bytes`)

`Metadata::backup_total_bytes` sums a backup's captured bytes via
`backup_manifest_tablet_progress`, which resolves each pinned tablet to its
**currently live** `split_lineage` descendant(s) (`live_split_descendants`)
— deliberately, so a tablet that split mid-capture is credited via whichever
descendant actually reported, never double-counted against a stale,
split-superseded ancestor's own orphan report. That "live" scoping is
exactly right while the source table still exists. The moment the whole
table is dropped, every one of the backup's tablets vanishes from
`Metadata::tablets` at once, `live_split_descendants` returns nothing for
every pinned tablet (its own fallback path only returns `[ancestor]` when
`self.tablets.contains_key(&ancestor)`), and `backup_total_bytes` silently
collapses to `0` — even though the catalog row and its per-tablet progress
records are untouched (ADR 0024's own explicit carve-out is working exactly
as designed for *those*). Nothing in `backup_total_bytes`'s own signature or
doc comment flags this: it reads as an ordinary accessor, and it answers
`0` — a legitimate-looking value, not an error or a panic — so a caller has
no signal that the answer just became meaningless.

This was caught by re-reading the accessor's own implementation against ADR
0059 §3's "a backup outlives its source table" promise while wiring
`DescribeBackup`'s size field, not by a failing test (none existed yet that
dropped a table between capture-complete and describe — added afterward,
`dynamo_backup.rs`'s `create_backup_round_trip_survives_table_drop_and_
janitor_reclaims`, asserting the reported size is byte-identical before and
after the drop). The fix generalizes: **any accessor whose answer depends on
resolving through currently-live state (a tablet map, a membership set, a
liveness check) must have its result frozen into durable storage at the one
point in the object's lifecycle where that resolution is still meaningful,
if any later consumer needs the answer to survive the inputs going away.**
Re-deriving on every read is only safe for the lifetime of whatever the
derivation depends on; past that lifetime it isn't "stale," it's simply
answering a different, degenerate question that happens to typecheck.
`BackupRow::total_bytes` is exactly this freeze, written once by
`MetaCommand::CompleteBackup`'s own apply arm at the last moment the live
accessor is still authoritative.
