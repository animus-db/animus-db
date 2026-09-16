# A defect whose trigger is a microsecond-wide thread interleaving cannot be closed by a test — make it unrepresentable, and say so where the test would have been (issue #279, 2026-08-18).

**A defect whose trigger is a microsecond-wide thread interleaving cannot
be closed by a test — make it unrepresentable, and say so where the test
would have been (issue #279, 2026-08-18).** Decoupling the CP-data
consensus loop's WAL `fsync` from its `select` (so a slow disk stops
livelocking a tablet group) means outbound vote grants / append accepts are
buffered until the persist covering them lands. Two successive attempts
shipped that and were reverted after failing the end-to-end gate; the
measured root cause was the WAL's *second* drainer — the apply task's
compaction rewrite, on another OS thread — taking `RaftCore::pending` in
the window between a step releasing the core lock and the loop's next look
at it. The loop then saw nothing left to persist, started no round, and the
buffered ack sat undelivered for up to 10.1 s, stalling the leader's commit
index. The instinct on the third attempt was "write the real-thread
regression that catches it." That test does not exist: with the bug
deliberately reintroduced, a 400-write `ProdEnv` run with compaction firing
a dozen times stayed green, and so did a two-node variant where the single
follower's ack is *required* for quorum — the window is simply too narrow
to hit by load, which is also why it took production split-during-backfill
traffic (many groups × constant compaction) to surface at all. What worked
instead was two structural closures: (1) one shared `drain_for_round`
helper with the round-claiming primitive private to the module, so a
drainer *cannot* take records without numbering them — the bug made
uncompilable; and (2) an unconditional `fully_durable` release (nothing
pending and no round in flight ⇒ everything buffered is already on disk),
which is correct no matter what any drainer did with round numbers. The
general rule: when a race's window is narrower than any test's resolution,
budget for making the state unrepresentable rather than for detecting it,
and write down in the test file what it does *not* prove — a real-thread
test that passes against the known bug is worse than no test, because the
next reader will trust it. (`crates/animus-cp-data/src/persist_round.rs`,
`crates/animus-cp-data/tests/prod_compaction_persist_round.rs`.)
