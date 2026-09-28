# A per-peer retention floor must be checked against every OTHER mechanism keyed off the same peer's replication state, not just its own invariants

**Context**: building follower-aware compaction retention
(`RaftCore::compaction_floor`, ADR 0017's 2026-09-27 amendment) — a
leader's threshold-triggered compaction should not advance
`snapshot_index` past a merely-lagging peer's own `match_index`, so that
peer keeps catching up via ordinary `AppendEntries` instead of falling
into a full `InstallSnapshot` cycle.

**What went wrong**: the first version of the floor covered voters *and*
learners together. In isolation the floor is correct (it's a pure `min`
over tracked `match_index` values, excluded past a hard cap). But this
codebase has a SEPARATE, pre-existing mechanism keyed off a peer's own
replication progress: `state_machine_behind`/`needs_snapshot` (issue
#554) — a receiver whose own async apply task hasn't yet digested a
just-installed image reports `needs_snapshot: true` on every
`AppendEntriesResp` until it catches up, and `handle_append_resp`'s
`needs_snapshot` branch never runs the ordinary success path's
`next_index` advance. Retention deliberately keeps a leader's compaction
base close to a caught-up-ish peer's own position — which is exactly the
condition under which that peer is most likely to be mid-digest of a
recent install. The result, confirmed live with a `SimEnv` repro: a
200ms-disk learner needing *thousands* of small re-snapshot cycles (at
the pre-chunk-size-bump 1024-byte chunk size) to fully land, instead of
the single install its own catch-up should have needed. Not a livelock —
it still converges — but a real, measurable regression for exactly the
peer class (a fresh learner, ADR 0058 Train 1) whose catch-up contract
already IS "via `InstallSnapshot`, before ever being promoted," so an
occasional install for it was never something worth avoiding in the
first place.

**The fix**: scope the floor to voters only. A learner's own catch-up
path was never the flood this feature targets (ordinary voter replicas
of an established tablet falling behind under routine load); excluding
it from retention leaves its existing, already-designed-for-snapshots
contract untouched and sidesteps the `needs_snapshot` interaction
entirely for that case. A smaller, bounded version of the same
interaction can still occur for a plain VOTER (an ordinary Raft
`next_index` backoff can also drop it into the snapshot path
momentarily) — the regression test for the voter scenario tolerates a
small number of installs for exactly this reason rather than asserting
strictly zero.

**The generalizable lesson**: when a new per-peer policy is keyed off the
same tracked state (`match_index`, replication position) that an
EXISTING mechanism also reads or reacts to, checking the new policy's own
invariants in isolation is not enough — trace every other consumer of
that same state and ask whether the new policy changes the conditions
under which the existing one fires, especially anything with its own
"stay in this mode until an external signal clears it" latch (here,
`needs_snapshot` staying true until an async, differently-scheduled task
catches up). A `SimEnv` repro that varies the SLOWEST plausible peer
(here: a learner behind a 200ms disk) alongside the ordinary case is what
surfaced this — a repro built only around the "modest" case would have
looked clean.
