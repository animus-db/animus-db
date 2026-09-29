# Assert a chase's outcome plus a floor; put a ceiling only where the design guarantees slack (issue #1080)

**Context**: `forward_hop_timeout_tests::forward_to_tablet_leader_bounds_each_hop_so_a_slow_candidate_cannot_starve_the_chase`
(issue #585's regression) failed rarely under CPU contention. First diagnosis
(instrumented run): the killed victim was the tablet's Raft *leader*, its
re-election outlasted the 2s guessed hop, both live replicas refused with no
hint, and the chase's last resort re-dialled the stub as a *hinted* candidate
(`HINTED_FORWARD_HOP_TIMEOUT`, 6s). 2s + 6s = 8s equalled the test's outer
`timeout(8s)`, which fired ~10ms before the hop's own cap. Every hop honoured
its cap; there was no production bug.

**What went wrong, twice**:
- The first bounds (8s outer, `elapsed < 6s`) were sized for the happy path,
  not the sum of the caps of every stage the chase can legitimately run.
- The first *fix* replaced them with `elapsed < FORWARD_HOP_TIMEOUT +
  HINTED_FORWARD_HOP_TIMEOUT` and made the victim a follower. That is still
  zero margin: the design's worst case for one pass IS that sum, plus lag, and
  starvation can still cause election churn after the kill (a live leader
  loses leadership, or a follower starts an election), producing the same
  stub-retry stage and every `WaitElection` round adds more hops. One failure
  in ~360 loop runs was left unexplained; this is the most likely cause.

**Rules**:
1. Assert the call's *outcome* (here `Ok(())` within its own deadline: the
   thing the regression exists to protect) plus a *floor* proving the intended
   path ran (`elapsed >= FORWARD_HOP_TIMEOUT`: the stub was dialled first).
2. Put a ceiling only where the design guarantees slack. A bound equal to the
   sum of stage caps has none; a bound with real margin is the call's own
   deadline, which the outcome assert already enforces.
3. The outer `timeout` is only a "don't hang the suite" guard: call deadline
   plus margin (`SCHEMA_COMMIT_TIMEOUT + 5s`), never a magic number.
4. Conditionally-triggered stages (victim happened to be leader; election
   outlasted the hop) stay invisible until CPU contention stretches the
   trigger window. Make the scenario deterministic about which branch it
   exercises (transfer leadership away, require the non-victim leader to be
   stable for a window, re-check right before the kill, keep the setup assert
   loud).
5. Prove a timing test's teeth by temporarily reintroducing the original bug
   and watching it fail, not by arguing the bound is tight.
6. Per-chase-tagged hop-start/hop-end instrumentation (chase deadline as id)
   disentangles concurrently running sibling tests.
