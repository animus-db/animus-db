# Bound an in-flight limit by time when the proposer cannot observe completion

**Context:** ADR 0079 step 6 (repair pacing). The control leader proposes a
`CasTabletReplicas` per violating tablet, but the expensive part (a learner
catching up, a snapshot stream) is data-plane-local and never shows up in
replicated metadata. An "in flight until the repair completes" limit would need
a completion signal the leader does not have.

**Lesson:** when the party that admits work cannot see it finish, define
"in flight" as a time window from the accepted proposal (here an existing
constant, `REPAIR_DWELL`) and release it early only on facts it can see (tablet
gone, leadership lost). A limit keyed on an unobservable completion is a limit
that wedges the first time one item never completes, and that is exactly the
case a repair queue exists for. State the throughput consequence (here
`R_inflight / TTL`, lower than the per-tick bound) in the ADR instead of hiding it.

**Also:** a re-proposal of a not-yet-applied command is a retry, not new work;
count it as neither a new slot nor a fresh TTL, or a lost proposal either
starves the queue or never ages out.

**Amendment (same day): make completion observable, keep the time bound.**
"Unobservable" was the wrong place to stop. The maintainer's call was to add the
signal: the party that does the work (the tablet's data-plane leader) reports
completion through a replicated command (`RepairSettled`, gated), and the
limiter frees the slot on it. The time bound stays as the safety net, so a
repair that never completes (or a lost report) still cannot wedge the queue. A
TTL-only definition capped throughput at `R_inflight / TTL` (about 5 minutes
for 1000 tablets) regardless of how fast repairs really finish; with the report
the measured burst ends at the per-tick bound. Check first what replicated
state already shows: here `Metadata` holds only the *desired* replica set, so
the "promotion is visible in Metadata" guess was false and the report needed a
new (gated) command. Order of preference: observable completion + time-bound
fallback, over time bound alone.
