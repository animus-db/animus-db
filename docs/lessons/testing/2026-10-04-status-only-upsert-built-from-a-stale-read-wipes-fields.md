# A "status-only" replace-style command built from a stale read wipes fields a concurrent command filled in

G-01 stage G-a's e2e-kind flaked with one *random* member left `labels: {}`
(e2e-0 on one run, e2e-1 on the next) although every pod was annotated and every
node's `RegisterNode` carried labels. Polling longer could not help: the labels
were gone for good. Cause: `MetaCommand::UpsertMember` replaces the whole row,
and the ADR 0012 detector's `Down`->`Active` promotion (and `admin_drain`) copy
`labels` from a read taken *before* the node's `RegisterNode` fill-in
(`fill_empty_labels`) committed; the stale upsert applied afterwards and wrote
the empty set back.

Lessons: (1) a field-filling command is only half the fix when another command
replaces the row wholesale from a read-modify-propose; audit every proposer of
the replacing command. (2) Fix it in the deterministic apply (an empty incoming
label set never wipes a non-empty one), not in the proposers: reads are always
stale relative to the log. (3) A `Metadata`-level unit test replaying the exact
interleaving (insert unlabelled, fill, stale upsert) reproduces it with no
cluster; e2e-only evidence of "random member differs per run" is the signature
of this kind of apply-order race, not of a slow poll.
