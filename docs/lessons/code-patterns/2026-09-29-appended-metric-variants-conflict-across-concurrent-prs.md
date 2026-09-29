# Appended `Metric` variants conflict across concurrent PRs; keep the already-merged slot first

**Context**: `animus-env`'s `Metric` enum is append-only (the array index in
`Metric::ALL` is the sink slot), so every PR that adds a counter edits the same
four places at the tail: the enum, `ALL` (with its hard-coded length), and the
name table. Two concurrent PRs (#1062's `SpawnedTaskHandlesTracked` and #1095's
four `Cp*` removal counters) both appended after `DemuxFramesDroppedOverflow`
and conflicted in all four hunks.

**Lesson**: resolve by keeping both, with the variant that already landed on
`main` keeping its slot and the still-open PR's variants following it. Apply the
same order in the enum, `ALL`, and the name table, and set the `ALL` length to
the sum (the compiler only catches a wrong length, not a wrong order).
Grep for `Metric; N` after the merge.
