# Converting an unconditional idle poll into a wake-on-X signal requires enumerating every *state transition* that creates the work, not every *call site* that might seem related — and some of those transitions live entirely inside a different subsystem's own timer, with no shared event to hook.

**Converting an unconditional idle poll into a wake-on-X signal requires
enumerating every *state transition* that creates the work, not every
*call site* that might seem related — and some of those transitions live
entirely inside a different subsystem's own timer, with no shared event to
hook.** The apply task's `APPLY_IDLE_POLL` (ADR 0044 phase-1 PR1,
`animus-cp-data`) looked at first like it needed a signal wherever
`RaftCore::apply` could run (a `mark_durable_through` call, a follower's
in-line apply inside `handle`, a completed snapshot install's commit-index
jump, a single-node group's own commit-advancing propose) — all genuinely
correlated with `commit_index` advancing, so one before/after comparison
after stepping the core plus one call at `mark_durable_through` covers all
of them. But `RaftCore::take_snapshot_needed` — the lazy on-demand
snapshot-image-build request the apply task must also notice — is set by
`snapshot_chunk_for`, reached from the leader's ordinary
heartbeat/replicate cycle discovering a reconnected follower's log has
been compacted away, with **no commit advance anywhere in that step**: it
is purely a consequence of the consensus loop's own timer, which the apply
task has no reason to know about. Trying to add a fourth explicit signal
point for this would mean piping a new plumbing path across the two-task
split for one rare, already-bounded case. **General rule**: after wiring
the signal for every transition you can name, ask whether a transition
exists that's driven by a *different* loop's own timer/tick with no data
dependency the signal's owner can observe — if so, don't chase it with more
plumbing; keep (or add) a bounded safety-poll fallback and prove
convergence through it with a test, which is both simpler and strictly
safer than an enumeration you can't be sure is exhaustive. (2026-08-16,
`quiesce/1-apply-signal`, `tests/apply_signal.rs`'s
`apply_converges_via_safety_poll_on_a_signal_less_snapshot_build`.)
