# A deferral must gate the message path that finishes the round, not only the timer

**Context.** ADR 0078's campaign governor gates the *timer* path: when a
`PreCandidate`'s round lapses, the driver returns its permit and asks again;
on a refusal `RaftCore::tick_gated` records `campaign_hold` and starts
nothing. The Tier 1 `SimEnv` corpus held `campaign_inflight_hwm <= K`. The
`ProdEnv` Tier 3 test (`tests/election_backpressure_liveness.rs`) passed nine
times alone, then, with three copies running at once on four cores, reported
`inflight_hwm=35` with `nonexempt_hwm=32`: three campaigns ran on exempt
permits although nothing in the test takes one (no transfer, no split).

**What happened.** The deferred `PreCandidate` is still a `PreCandidate`. A
`PreVoteResp { granted: true }` from its lapsed round, delayed by the CPU
contention, arrived while it waited in the queue; `handle_pre_vote_resp`
counted it, reached a majority and called `start_election`, which re-armed
the timer and cleared `campaign_hold`. The driver then saw an active
campaign with no permit and no hold, the shape it reserves for `TimeoutNow`,
and handed it an exempt permit over the cap. Under `SimEnv`'s near-zero,
FIFO message latency the late grant practically never lands inside the hold.

**Fix.** `handle_pre_vote_resp` ignores a grant while `campaign_hold` is set
(the retry runs a fresh round once admitted). Regression:
`animus-control` `campaign_admission::a_deferred_precandidate_does_not_finish_its_lapsed_round_on_a_late_grant`.

**Lessons.**

- A gate on "may this start?" placed on one entry point (the timer) does not
  gate the state machine: list every transition out of the gated state (here
  `PreCandidate -> Candidate` on a message) and check each against the gate.
- A "catch-all" in a driver that classifies whatever is left as the exempt
  case (`campaign_active && permit.is_none() && !deferred` = `TimeoutNow`) turns
  any missed path into a silent over-cap instead of a failure. Assert the
  non-exempt high-water mark and the total separately, and treat a total above
  the cap with no exempt source in the test as a bug.
- Real-thread tests find message-reordering windows the simulator's latency
  model makes vanishingly rare; run them under deliberate contention (several
  copies at once) before trusting a pass.
