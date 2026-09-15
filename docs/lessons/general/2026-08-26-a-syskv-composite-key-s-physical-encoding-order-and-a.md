# A syskv composite key's physical encoding order and a `Metadata` map's logical key order are two separate decisions (ADR 0059 §3)

Adding the backup catalog's per-tablet progress collection
(`Metadata::backup_tablet_progress: BTreeMap<(BackupId, TabletId), _>`)
needed a `syskv` key for the same `(backup_id, tablet)` pair. The existing
precedent (`index_backfill_key(tablet, index)`) encodes its fixed-width
field first — `TabletId`'s 8 bytes, then the variable-length index name —
so `decode_index_backfill_id` never has to guess where the boundary is.
That precedent's field order happens to *also* match `Metadata::
index_backfill`'s own map-key order, which made it easy to assume the two
orders are the same constraint. They aren't: the physical key only needs
its fixed-width field first (a decoding requirement); the `Metadata`
field's own tuple order is a separate, independent choice about what
reads naturally for that collection's own consumers (here, ADR 0059 §3
explicitly wants `(backup_id, tablet)` — a `DescribeBackup` reader groups
by backup first). Encoding `backup_progress_key` as `(tablet, backup_id)`
while keeping `Metadata::backup_tablet_progress`'s key as `(BackupId,
TabletId)` satisfies both constraints at once, at the cost of one
documented swap at the two points that cross the boundary
(`mirror.rs`'s `apply_put`/`apply_delete`). Worth stating plainly rather
than silently reusing the sibling kind's field order out of habit: check
each key shape's *own* two constraints (decodability, and what the owning
collection's readers want) rather than assuming a lookalike precedent's
order was load-bearing in both places it happened to hold.
