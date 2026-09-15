# Superseded by ADR 0044 (split-only tablets: tablet merge removed)

ADR 0044 (2026-08-14) removed tablet merge entirely — `MetaCommand::
MergeTablets`, `Metadata::merged_tablets`/`absorbed_by`, and
`animus-cp-data`'s `HostAction::WidenScope`/`Absorb` reconciler reaction no
longer exist anywhere in this repository. Every entry below was written
against that now-deleted mechanism. Two of the three lessons drawn from them
still generalize beyond merge specifically — a pointer back to each remains
in `engineering-lessons.md` at the point each entry used to live.
