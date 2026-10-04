# A relay-arm regression test must switch off the leader's own upkeep, or it stays green without the arm

**Context.** ADR 0073 P2-C added `ReportNodeVersion => true` to `is_relayable_command` so a
follower-connected or data-only node's boot-time self-report reaches the control leader. The first
SimCluster tests ("a data-only / follower node re-reports a narrower range, the record updates")
passed with the arm flipped back to `false`.

**Lesson.** The leader's own era upkeep (`era_on_proposals`) independently notices a changed
observed range and proposes the identical command, so the end state is reached either way and a
test that only checks the end state proves nothing about the relay path. A negative control (flip
the arm, re-run) caught it. The sharp version switches the competing mechanism off for the test
(`SimCluster::set_raft_own_range(leader, None)`: the era is sticky, the leader just stops
upkeeping), after which the test is red without the arm and green with it.

**Generalizes to:** any "this path must carry X" regression where a second, redundant mechanism
converges to the same state. Always run the negative control (revert the fix, watch it go red)
before trusting the test, and when it stays green, remove the redundancy from the scenario rather
than adding assertions about the end state.
