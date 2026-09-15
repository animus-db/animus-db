# A state that must never be observable in an intermediate, half-formed shape has to be minted by ONE replicated command, not a mint-then-tag pair — even when the gap between the two commits is expected to be small in practice (issue #593, ADR 0059 §9).

**A state that must never be observable in an intermediate, half-formed
shape has to be minted by ONE replicated command, not a mint-then-tag
pair — even when the gap between the two commits is expected to be small
in practice (issue #593, ADR 0059 §9).** `pitr_snapshot_loop` used to
propose `MetaCommand::BeginBackup` to mint a PITR base snapshot's row,
then — only once it observed that row exist — a separate
`MetaCommand::MarkBackupPitrBase` to tag it. The ADR's own text called
the gap between the two commits "a vanishingly brief window," a
self-healing sweep's worth of accepted residual, not a defended
two-phase-commit property. It was not vanishing: any consumer filtering
PITR base snapshots out of a user-visible view by consulting
`Metadata::pitr_base_backups` (`ListBackups`'s default `USER` filter, the
console's per-table backups projection) could observe the row as an
ordinary untagged `Creating` backup for the real committed window between
the two proposals — reproduced end to end by a test polling fast enough
(`console_table_config::table_detail_shows_pitr_status_and_backups`).
Fixed by folding the tag into the minting command itself (`BeginBackup`
gained a `pitr_base: bool` field, applied atomically with the row in the
same `Metadata::apply` arm) and deleting the side-tag command outright —
along with its self-healing sweep, which no longer has anything to heal.
**The general rule**: prefer widening the minting command's own signature
(a bool/enum field, `error[E0063]`-enumerated across every existing
construction site — the same compiler-driven fan-out `BackupRow::
backup_name`'s own addition to `BeginBackup` already went through, Train 1
PR④) over a second side-tag command, even when the side-tag avoids
touching more call sites at the point it's added. A documented "brief
window" plus a self-healing sweep is itself a signal the two-command
design's gap was already known to be real, not merely theoretical — that
combination is worth treating as a standing invitation to fold the tag
into the mint instead, the next time a similar shape is proposed. See
`docs/adr/0059-backup-restore.md`'s 2026-09-04 as-built amendment for the
full incident and `animus-control::meta::tests::
begin_backup_pitr_base_tags_atomically_with_the_mint` for the regression.
