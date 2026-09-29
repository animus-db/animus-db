# Fixed (issue #1070): `handle_append_resp`'s ordinary success path set `next_index` with a bare `.insert`, not a `.max`, unlike its own `InstallSnapshot` sibling — and it could fully halt replication, not just slow it

**Update (2026-09-28, issue #1070): fixed.** The finding below was originally
recorded as "found but not fixed" while building issue #1064's own
regression test. Pushing that same test's sustained-writer harness out to
`ROUNDS = 300` (still well within a real-time-affordable budget once writes
are issued in coalesced bursts rather than one `run_for` per propose)
reproduced the predicted hazard directly and dramatically: with the bug
still present, `RaftKvNode::engine_applied_index()` for the learner reached
`22414` (of `25102` leader commits at that point) and then went **completely
flat** for the remainder of the run while `commit_index` kept climbing a
further `7400` — not a slowdown, a full and permanent replication halt to
that peer. The fix (`crates/animus-control/src/raft.rs`,
`handle_append_resp`'s ordinary success branch): `next_index` is now updated
via `self.next_index.entry(from).or_insert(1); *ni = (*ni).max(match_index +
1)`, mirroring `match_index`'s own `.max` two lines above and the
`InstallSnapshot` completion branch's own prior hardening. The reject branch
is untouched — a reject's own decrement is the correct, intentional Raft
backoff, never a monotonicity bug (see the new regression's own doc for why).
With the fix, the identical `ROUNDS = 300` run reaches `28714` of `32502`
commits (`88%`) with real, ongoing progress in the run's own final quarter
too. Regressions: `crates/animus-control/tests/
append_resp_next_index_monotonic.rs` (a hand-driven `RaftCore` unit cell:
out-of-order acks never regress `next_index`, confirmed red on the bare
`.insert` and green on the fix) and `crates/animus-cp-data/tests/
learner_snapshot_livelock_under_continuous_writer.rs`'s own raised `ROUNDS`
plus its new final-quarter-progress assertion (a whole-run applied/commit
ratio alone stays deceptively healthy long after a late freeze — see that
file's own module doc for why a later, not a halfway, sample point was
needed to reliably catch it).

The original finding, kept for the full context of how this was found:

**Context**: building the regression test for issue #1064's own heavier-load
finding (`crates/animus-cp-data/tests/
learner_snapshot_livelock_under_continuous_writer.rs`), a synchronous,
multi-propose-per-round writer against a learner with a real (8-200ms) disk
delay reproducibly got its own `match_index` stuck at exactly
`MAX_APPEND_ENTRIES_BATCH` (512) for the ENTIRE remainder of a long sustained
run, recovering only via periodic full `InstallSnapshot` cycles — never via
ordinary incremental `AppendEntries`, even though the learner's own
processing throughput should have been more than sufficient at the chosen
rate (confirmed via an identical write rate against a single-propose-per-
round shape, which converges cleanly). A single-propose-per-round shape
(mirroring `follower_aware_compaction.rs`'s own proven-sustainable rate for
a lagging voter) does NOT exhibit this stall; only a synchronous,
multi-propose-before-yielding burst shape does, at volumes large enough that
`replicate_now`'s own wake-on-propose coalescing still lets MANY overlapping
`AppendEntries` requests queue toward the same lagging peer before its first
ack has round-tripped back.

**What was found, not fixed (out of this task's scope per this repo's own
"an incidental pre-existing bug gets its own separate PR" convention)**:
`RaftCore::handle_append_resp`'s ordinary success branch
(`crates/animus-control/src/raft.rs`) updates `next_index` with a bare
`self.next_index.insert(from.clone(), match_index + 1)` — **not** a
`.entry(..).or_insert(..)` + `.max(..)`, unlike `match_index`'s own update
two lines above (`*m = (*m).max(match_index)`), and unlike its own sibling
in `handle_install_snapshot_resp`'s completion branch, which was
specifically hardened to be monotonic for exactly this reason (that
function's own comment: "`next_index` must be exactly as monotonic as
`match_index`... neither field's value can ever legitimately move backward
while that term holds"). If multiple `AppendEntries` requests are ever
concurrently outstanding to the same peer (which the ordinary,
uncapped-resend `replicate_to` path readily allows — unlike the snapshot
path's own `SnapshotResend`-capped resend discipline), and their responses
are processed out of the order their requests were built in, a STALE
success ack for a SMALLER `match_index` can overwrite a MORE RECENT,
LARGER advance — not merely fail to help, but actively regress
`next_index` backward, which then feeds the very next `replicate_to` call
and can produce a self-sustaining oscillation: the leader keeps re-sending
overlapping ranges, the peer keeps re-confirming roughly the same ceiling,
and real forward progress stalls indefinitely under sufficiently sustained
synchronous-burst load, recovering only via the entirely separate
`InstallSnapshot` path.

**Why this wasn't fixed here**: it is a genuine, pre-existing throughput/
correctness characteristic of the ordinary `AppendEntries` ack path,
unrelated to issue #1064's compaction/snapshot-restart mechanism or issue
#1061's departing-peer mechanism (the two things this session's diff
actually addresses) — fixing it would be exactly the "incidental pre-existing
bug discovered during a task" this repo's root `CLAUDE.md` says gets its own
separate PR with its own regression test, never a drive-by fix folded into
an unrelated diff.

**The generalizable lesson**: when ONE ack-processing path for a per-peer
"how far have they gotten" field is deliberately made monotonic (a
`.max()`-guarded update) specifically to close an out-of-order-response
hazard, audit every OTHER path that also writes the SAME field for the
identical hazard — a fix applied to one sibling handler (here,
`InstallSnapshot`'s completion branch) is not evidence the analogous
ordinary-path handler is safe, especially when the ordinary path's own
resend discipline (uncapped) is structurally more prone to producing
multiple concurrently-outstanding requests than the path that already got
the fix (capped resends). A repro for a follow-up investigation: drive a
synchronous multi-propose-per-round writer (not single-propose) against a
peer with a real, nonzero disk delay for long enough, and watch
`match_index` for signs of a value that stops advancing exactly at
`MAX_APPEND_ENTRIES_BATCH` (or any other fixed batch boundary) despite
continued write pressure and a peer that should be provably capable of
higher throughput.
