# General form (mechanism superseded): when one loop batches and a sibling loop over the same rows doesn't, that asymmetry is the bug — and a "safe" zero/empty starting watermark is not free when a cheap, sound starting value is available from a pass the code already makes.

**General form (mechanism superseded): when one loop batches and a
sibling loop over the same rows doesn't, that asymmetry is the bug — and
a "safe" zero/empty starting watermark is not free when a cheap, sound
starting value is available from a pass the code already makes.**
Originally learned from the now-deleted copy-based split-build driver's
own tail-pass/bulk-pass batching asymmetry; see
`docs/engineering-lessons-archive.md`'s "The copy-based split-build
driver" section for the full incident (including the diagnostic that
made it obvious: one consensus entry == one Raft log index, so
`commit_index` growth divided by rows received IS the effective batch
size).
