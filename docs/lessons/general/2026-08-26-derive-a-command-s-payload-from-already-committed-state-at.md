# Derive a command's payload from already-committed state at apply time, not from what the proposer captured (ADR 0059 §3)

`BeginBackup`'s manifest stub (the source table's schema snapshot and its
current tablet list) could have been computed once by the proposing wire
node and carried on the command, the way a naive first draft would write
it. That would reproduce the exact hazard `CreateTablet`/`BeginSplit`
already avoid: two proposers (or one proposer retried after a stale read)
computing the stub from two different snapshots would make `apply`'s
result depend on *which proposal landed first*, even though every replica
runs the identical deterministic function — a Raft replica's job is to
agree on one input and compute the same output, not to trust an
already-computed output riding along. The fix is the same one this
codebase already uses for `BeginSplit`'s child ranges and `CutoverSplit`'s
child recomputation: `BeginBackup`'s apply arm reads `self.schemas`/
`self.tablets_for_table` itself, at apply time, and derives the stub from
current agreed state — the command carries only the identity fields
(`backup_id`, `table`, the ADR-0051-style wall-clock stamp) that *can't*
be derived from replicated state. When a new command's payload could
either be captured by the proposer or derived from `Metadata` already
committed to that point, derive it — the proposer-captured version always
requires arguing every replica sees the same input, and pure derivation
makes that argument for free.
