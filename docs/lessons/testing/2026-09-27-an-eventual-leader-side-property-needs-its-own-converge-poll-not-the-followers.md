# An eventual leader-side property needs its own converge-poll, not the follower's

`follower_aware_compaction.rs`'s `a_modestly_slowed_voter_...` test drained
a slow voter to catch-up with a converged-or-timeout poll (`nodes[slow].
engine_applied_index() >= commit_index`, exactly the rule this repo already
follows for eventual properties), then, in the very next line, asserted a
*different* eventual property — the LEADER's own compacted log length — as
a one-shot check, no polling at all.

That second property is not implied by the first. `engine_applied_index()`
is a fact about the voter's own engine; the leader's own bookkeeping
(`match_index`, and therefore `RaftCore::compaction_floor`/`log_len()`)
only advances once the leader has *itself* received and processed that
voter's corresponding `AppendEntriesResp` — a separate message, on a
separate task's schedule (here, `apply_and_compact`'s periodic safety poll,
not re-triggered by a bare ack with no new entry to apply). A seed sweep
(40+ seeds; the pinned seed happened not to exhibit it) found the gap
directly: `leader_log_len` read in the hundreds at the exact instant the
voter's own catch-up was observed, then converged to the true bound within
1-2 more 100ms polls once given the chance.

**The generalizable rule**: when a test asserts two eventual properties in
sequence — especially when one belongs to a different actor/task than the
other — each needs its own converged-or-timeout poll. Confirming property A
has converged is not licence to assert property B as a one-shot; B may
depend on a *different* clock (a different node's bookkeeping, a
differently-scheduled background task) that A's own poll never advanced.

See `docs/adr/0017-per-tablet-raft-data-plane.md`'s 2026-09-27 amendment
follow-up for the full investigation and the fix
(`crates/animus-cp-data/tests/follower_aware_compaction.rs`).
