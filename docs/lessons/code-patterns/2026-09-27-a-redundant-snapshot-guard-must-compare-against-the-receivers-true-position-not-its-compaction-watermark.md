# A "redundant offer" short-circuit must compare against the receiver's true current position, not a watermark that lags it

**What happened.** Live on a 3-control/5-data cluster under
`--auto-split-bytes` with a heavy `BatchWriteItem` seed load, a follower's
apply task hard-panicked: `raftkv apply: HLC ts ... did not strictly exceed
the last applied ... witnessing chain is broken`
(`assert_ts_monotonic`, `crates/animus-cp-data/src/lib.rs`). Instrumented
replay showed the exact sequence: the follower installed a snapshot at
index 155, then a second at index 160, then applied entry 161 (ts 184363),
then entry 162 (ts 184395) — and THEN a **third, stale** `InstallSnapshot`
at index 160 landed and installed again, resetting `last_applied`/
`commit_index`/the log back to 160. The apply task re-applied entry 161's
(older) timestamp on top of the high-water mark 162 had already set.

**The mechanism.** `RaftCore::handle_install_snapshot`
(`crates/animus-control/src/raft.rs`, shared unchanged by
`animus-cp-data`'s per-tablet Raft groups) has an "already at least this
far along" short-circuit meant to make a redundant offer a no-op — ordinary
Raft §7 behavior. It compared the offer's `last_index` against
`self.snapshot_index`: the log's last COMPACTION point. But `snapshot_index`
only advances when compaction actually runs (a periodic background sweep),
while `self.last_applied` advances on every commit — the two routinely
diverge under real write load, with `last_applied` running ahead. Under a
leader that floods/restarts snapshot transfers (a separate, expected
consequence of compaction invalidating an in-flight transfer — see
`RaftCore::snapshot_upto`'s own doc), a follower can catch all the way up
to some index N via ordinary `AppendEntries` while a stale, already-obsolete
chunked transfer built at an earlier index M (`snapshot_index < M < N`) is
still in flight. That transfer's final chunk sailed past the guard — `M >
snapshot_index`, "not yet redundant" — and installed, silently rewinding
the follower's own state to M even though it had already gone strictly
past it.

**The fix**: compare against `self.last_applied` instead of
`self.snapshot_index`. It is always `>= snapshot_index` (the two coincide
only immediately after an install) and is the receiver's actual, current
"how far have I gotten" — not a proxy for it that a background sweep's own
cadence can leave arbitrarily stale. The pre-existing `&& !self.
state_machine_behind` override (issue #554: a node whose *engine* is known
to be behind its own log can't trust either watermark, and must accept even
a same-index offer) stays unchanged — it and this fix answer different
questions and compose without conflict.

**What to do.**

- **A "we already have this, skip it" guard is only as correct as the
  watermark it reads.** When a system keeps more than one watermark for
  "how far has this node gotten" — one advanced eagerly on every event (here,
  `last_applied`, bumped per commit) and one advanced lazily by a periodic
  background process for an unrelated reason (here, `snapshot_index`, bumped
  only at compaction, to bound WAL/log size) — a redundancy check must use
  the eager one. The lazy one answers "what's the oldest thing I can still
  prove I have," not "what's the newest thing I've already got"; using it
  for the latter question silently reopens a window every time the
  background process lags, and the window's width is exactly the gap
  between the two watermarks, not a fixed race.
- **This bug hid behind a *different*, correctly-fixed bug in the same
  function.** The short-circuit's `&& !self.state_machine_behind` clause
  (issue #554) was added for a real, narrower gap and is itself correct —
  but its presence, and the detailed doc explaining exactly why comparing
  against `snapshot_index` can be *too conservative* in that one case,
  made it easy to read right past the fact that the SAME field could also
  be too permissive in a different, more common case. A field with one
  well-documented failure mode still needs auditing for the opposite one.
- **A rewind-shaped bug's regression test doesn't need the full flooding
  scenario that discovered it live** — it needs the minimal shape: a
  receiver whose eager watermark has advanced past an offer's claimed
  position, then deliver that offer and assert the watermark doesn't move
  backward. `crates/animus-control/tests/stale_snapshot_no_rewind.rs` drives
  a real leader/follower `RaftCore` pair through ordinary replication
  (confirming `last_applied` genuinely leads `snapshot_index`, exactly as it
  does live), then hand-delivers a synthetic stale `InstallSnapshot` and
  asserts `last_applied` is unchanged — reverting the fix (comparing
  against `snapshot_index` again) reproduces the exact rewind on this test,
  confirming it isn't testing something the guard already handled by
  accident.
- **Building a two-node `RaftCore` test by hand needs two things every
  existing snapshot test in this file already knew but are easy to forget
  in a fresh one**: (1) `mark_durable_through` before any `propose`d entry
  can apply on the LEADER side (`apply()`'s frontier is `commit_index.min(
  durable_index)`, and `durable_index` starts at 0) and (2) a **second**
  heartbeat/tick round after the first entries+ack exchange, because a
  follower's own `commit_index` only ever advances via a LATER
  `AppendEntries`' `leader_commit` field — the leader doesn't push it out of
  band the instant its own commit index moves. A pump loop that stops the
  moment messages go quiet after round one will show `leader.last_applied()
  > 0` but `follower.last_applied() == 0` and look like a completely
  different bug.
- **When enriching a hard-panic's message with more context** (here:
  `assert_ts_monotonic` now reports the offending entry's own `(index,
  term, KvCommand variant)` and the previous ts-carrying entry's), thread
  the extra state exactly like the existing invariant's own bookkeeping —
  same lifetime, same reset points, same single-writer discipline
  (`LastAppliedTsEntry` mirrors `max_applied_ts` in
  `crates/animus-cp-data/src/lib.rs`'s `apply_and_compact`) — rather than
  adding a parallel logging path. A bare "value A didn't exceed value B"
  panic from deep inside a hot apply loop is close to undiagnosable in a
  live incident; this is what made the live repro traceable to
  `InstallSnapshot` at all.
