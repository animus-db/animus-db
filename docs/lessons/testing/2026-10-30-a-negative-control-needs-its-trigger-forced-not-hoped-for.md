# A corpus oracle or negative control must force its trigger and read the state itself, never infer it from a counter or a before/after delta (issue #1271).

Four of the five nightly corpus failures behind #1271 were one mistake: a check
that holds on most seeds because the random schedule usually produces the
condition, and fails on the rest.

- **Before/after deltas race unrelated writes.** `mixed_version` compared the old
  replica's log length before and after the bad emit; an unrelated entry landed
  in between (pre 41, bad entry 43, old replica stopped at 42). Assert against
  the entry's OWN identity (`ProposeResult::Accepted { index }`: the old replica's
  last index stays below it).
- **A counter's attribution is not the state.** `lsm_write_stall` read "L0 reached
  the stop level" from `StallStats::writes[2]`, which counts a write under the
  cause it FIRST stalled for, so a writer that entered in the slowdown ramp and
  escalated never counted. Read the level (`l0_table_count()`), drive writes until
  it is reached with a bound, and fail loudly if the bound is hit.
- **A negative control that needs a coincidence is not a control.** The MREC
  `lww_by_arrival` control needed the random ops to leave two conflicting writes
  whose arrival order differed from their stamp order; one seed did not. Add a
  forced round to the plan (every region writes one key before anything ships).
- **Retry a transfer, do not widen its wait.** Leadership-transfer attempts can be
  lost; re-issue until the leader moves, bounded, rather than waiting longer once.
- **A "converges while X is down" cell must check X is not a desired member.** The
  revert cell killed the control leader, which is seed-dependent; when it was a
  kept-Region node the reconfigure ladder correctly refused to drop the extra
  voters (steps 5 and 6 need every desired member caught up). The new
  `revert_kept_region_node_kill` cell makes that branch deterministic.

The CI side: per-push CI runs every corpus at 1 seed, so such cells merge "born
red". `scripts/pr-corpus-depth.sh` (job `corpus-changed`) runs a corpus at its
nightly depth when its test file changes; `check` keeps its table in step with
`corpus-deep.yml`.
