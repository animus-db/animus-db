# A plan-then-execute reconciler re-checks live state before a destructive step; a learner is not "excluded"

**Context**: the tablet-host reconciler gathers facts, plans `Release`, then
executes the teardown (stop the driver, wait, erase the files). A 4-hour soak
logged permanent `refusing to start as a voter` refusals on replicas nobody
wiped. The release predicate tested `RaftCore::config()`, which is voters
only, so a mid-catch-up learner always looked "removed"; and the teardown
erased files from tick-start facts while the tablet leader, whose own view
still listed the node, promoted it. Re-hosting the erased replica empty tripped
the wiped-voter guard, which is sticky and silent.

**Lesson**:
- When a plan is decided from facts gathered at the start of a pass, the
  executor's *destructive* step (erase, drop, truncate) must re-read the live
  state immediately before acting and skip if the plan is stale. The check
  belongs after the last await that yields the scheduler (here, after the
  driver has stopped), not only at plan time; skipping must leave the system
  recoverable (clear the claim, keep the data) so the next pass re-adopts it.
- A membership predicate must name every non-excluded class. "Not in the voter
  set" is not "removed": learners (and any other pre-voter state) are members.
  Put the predicate in one function used by both the planner and the recheck.
- A sticky, silent safety refusal needs its own observability (a named log
  line, a per-group admin field, a level gauge); otherwise the only symptom is
  a group quietly running short.

**Where**: `crates/animus-cp-data/src/host.rs` (`replica_excluded`,
`finish_teardown`), `tests/release_race_corpus.rs`
(`ANIMUS_RELEASE_RACE_SEEDS`), ADR 0031's 2026-09-30 addendum.
