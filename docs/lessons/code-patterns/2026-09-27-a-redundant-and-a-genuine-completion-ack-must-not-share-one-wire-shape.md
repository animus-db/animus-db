# A "no-op, already redundant" reply and a genuine "just completed" reply must not share one wire shape

**Context**: `RaftCore::handle_install_snapshot`'s "already at least this far
along" short-circuit (fixed earlier the same day to compare against
`last_applied` instead of the lagging `snapshot_index` — see the sibling
entry on that fix) answers a redundant/stale offer with the exact same
`InstallSnapshotResp { last_index, next_offset }` shape a GENUINE
just-finished install uses, differing only in *which value* `last_index`
happens to hold. The short-circuit echoed `self.snapshot_index` — nonzero
the instant this node has ever compacted at all — while a real completion
carries the newly-installed base. Both are `last_index > 0` on the wire.

**What went wrong**: two independent leader-side consumers trusted
`last_index > 0` alone as "a completed install, full stop":
`animus-cp-data::record_kv_outbound` increments `Metric::
CpSnapshotInstalls` on any such outbound ack, and `handle_install_
snapshot_resp`'s "transfer complete" branch resets `next_index`/clears
`snapshot_offset` bookkeeping identically for both cases. Before the
same-day fix widened the short-circuit's own trigger condition (`last_index
<= last_applied`, replacing the far narrower `<= snapshot_index`), the
ambiguous reply was rare enough not to matter. Afterward it fires on
almost every stale/duplicate/late-arriving chunk once a peer has caught up
via ordinary `AppendEntries` — silently inflating the install-count metric
(breaking a brand-new retention regression test tuned against the old,
rare rate) and regressing an already-advanced peer's `next_index` backward
to the stale offer's base, immediately forcing a wholly unnecessary fresh
`InstallSnapshot`. The metric-conflation half of this bug is not specific
to the feature that surfaced it (follower-aware compaction retention) —
it is a pre-existing, unchanged-by-that-feature defect that over-counts
`CpSnapshotInstalls` on `main` alone the moment a redundant offer lands
with `snapshot_index > 0`; the widened condition only made the common case
common instead of rare.

**The fix**: give the two outcomes genuinely different wire shapes instead
of overloading one field's magnitude. The redundant/no-op reply now sends
`last_index: 0, next_offset: 0` — a shape `handle_install_snapshot_resp`'s
pre-existing "still mid-transfer" branch already treats as "peer has
nothing buffered, reset my bookkeeping for it" (its own `next_offset == 0
&& *tracked > 0` case), which clears the leader's stale
`snapshot_chunk_sent`/`snapshot_heartbeat_attempts` tracking without
touching `next_index`/`match_index` or counting an install. As
belt-and-suspenders, the genuine-completion branch's own `next_index`
update was also made monotonic (`max` with the existing value, mirroring
`match_index`'s own treatment two lines above) rather than a bare
overwrite — so even a different future source of a spurious low
`last_index` on this same reply variant can't regress a peer's already-
known-higher replication position.

**The generalizable lesson**: when a reply variant is reused for two
semantically different outcomes ("nothing happened, you're already
covered" vs. "something just completed at this position"), every field on
it is now ambiguous between the two meanings, and every consumer that
switches behavior on that field is implicitly assuming which meaning holds
— an assumption that can go unnoticed for a long time if the ambiguous
case is rare, and break loudly the moment an unrelated change (here, a
different, correct, same-day fix to the *condition* that decides which
branch is taken) makes it common. The fix is not to teach the ambiguous
case a smarter classification rule — it's to stop it from being ambiguous:
either add a distinct field/variant, or, where an existing "no-op" shape
already exists elsewhere in the same protocol (here, "still mid-transfer,
next_offset == 0"), route the no-op case through that. Regression:
`crates/animus-cp-data/tests/follower_aware_compaction.rs` (already
existed, tuned to this exact defect) and
`crates/animus-control/tests/stale_snapshot_no_rewind.rs` (already
asserted `last_index == 0` for this reply — the code just hadn't matched
its own regression test's expectation until this fix).
