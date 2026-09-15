# A state machine's applied watermark must be the state machine's own, never the log's; a log that matches the leader proves nothing about the engine beneath it (2026-09-02, issue #554).

**A state machine's applied watermark must be the state machine's own,
never the log's; a log that matches the leader proves nothing about the
engine beneath it (2026-09-02, issue #554).** Building the reconciler-side
destroy-and-reopen fix the entry above proposes surfaced a deeper,
independent gap: `RaftKvNode::drive()` seeded its `engine_applied`
watermark from `RaftCore::recovered`'s `core.last_applied()`, which
`recovered` sets to the replica's own `snapshot_index` — a fact about
where the LOG's own compaction left off, not about what the ENGINE holds.
That seed is sound only when the engine and the log both survive a
restart intact; it silently overstates progress the moment they don't (an
engine destroyed and reopened fresh behind an already-compacted log,
WAL intact). Worse, the failure is invisible at the log level: because the
replica's log tail from `snapshot_index + 1` onward still matches the
leader's byte-for-byte, `RaftCore::replicate_to`'s `next_index <=
snapshot_index` check — the ONLY thing that triggers a fresh
`InstallSnapshot` — never fires. The leader has no way to know the
follower's log matching it says nothing about what the follower's engine
actually holds. Reproduced deterministically at 90 writes (past
`COMPACT_THRESHOLD` = 64); a smaller reproduction at 20 writes (below the
threshold) passed, which is exactly why the existing corpus's
`MemoryEngine` tier — restarting every crash victim with a genuinely fresh
engine, but with workloads that apply only ~10-17 entries per replica —
never caught this: it exercised only the log-replay half of recovery, not
the compacted-prefix half.
**The fix has to be a REAL signal from the state machine's own true
progress, and it has to reach the peer that can supply what's missing**:
an engine-persisted applied watermark (a durable marker key, distinct from
and independent of the log's own snapshot index) seeds `engine_applied` at
`drive()` start instead of `core.last_applied()`; comparing the two
facts (engine watermark vs. recovered `snapshot_index`) is what detects
"my log says I should be caught up to X, but my engine's own durable
record says otherwise" — and that fact then has to ride a wire message
BACK to the leader, since nothing about `next_index`/`match_index` (both
purely log-level bookkeeping) can express it. Two further lessons that
only showed up once this was actually built, both worth generalizing:
- **Don't write a new durable marker on every commit just because "more
  often is safer" — check what it shares a physical file with.** The
  first draft wrote the watermark in the same engine batch as every
  ordinary commit's own rows (maximally fresh, and correct on its own
  terms), and it broke `LsmEngine::clone_to_filtered`'s split dead-space
  optimization for nearly every future split, not a rare edge case — the
  marker's own key sorts outside every row kind's declared byte range, so
  ANY table that ever contained it looked like it spanned kind boundaries
  it never actually touched, defeating whole-file linking. The fix wasn't
  to special-case the storage layer; it was to write the marker only at
  the coarser granularity the CONSUMER actually needs (here: exactly the
  two moments the compared-against fact, `snapshot_index`, can itself
  change) — a general instance of "match a value's write frequency to
  what actually reads it, not to 'as fresh as physically possible'."
- **A driver-latched flag cleared by a DIFFERENT async task than the one
  that reads it has a staleness window, and if what reads it feeds a
  remote peer's own state machine, that window can become a livelock, not
  just a slightly-late read.** The first version of the "am I behind"
  flag was set once at `drive()` start and cleared once, later, by the
  apply task once it actually finished merging a completed install. In
  between — a window spanning several of the peer's own heartbeat round
  trips, since the two are separate tasks with separate schedules — every
  response built during that window still reported "still behind," and a
  leader that took each such report at face value (reasonably!) kept
  resetting the transfer back to the start, forever: `next_index`
  oscillating between 1 and past-`snapshot_index`, the peer's own applied
  watermark never leaving zero, confirmed live with an instrumented
  build. Recomputing the flag LIVE, every tick, from the two facts that
  define it (mirroring an existing pattern in the same file,
  `set_quiesce_engine_caught_up`'s "feed the one external input the core
  can't see itself, once per loop iteration, before anything consults
  it") closed the staleness window but not the livelock alone — the
  leader-side fix (`snapshot_served_through`, don't re-trigger a transfer
  you've already fully shipped) was still needed, because the underlying
  race — "the peer's own report can lag what it already received" — is
  real regardless of how fresh the flag is, and a receiver-side symptom
  sometimes needs a sender-side fix (bound how eagerly the sender
  re-triggers work its own most recent full attempt should already have
  satisfied), not just a faster/fresher signal on the receiving end.
See ADR 0009's and ADR 0017's 2026-09-02 addenda for the as-built
mechanism (the engine-persisted watermark, the live `state_machine_behind`
recomputation, `AppendEntriesResp::needs_snapshot`, and
`snapshot_served_through`) and `animus-cp-data/CLAUDE.md`'s matching
entry.
