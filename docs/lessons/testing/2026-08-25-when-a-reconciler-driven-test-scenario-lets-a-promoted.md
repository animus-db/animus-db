# When a reconciler-driven test scenario lets a promoted learner become eligible for LEADERSHIP, the test's own tick/poll loop must include that node, not just the original voters

**When a reconciler-driven test scenario lets a promoted learner become
eligible for LEADERSHIP, the test's own tick/poll loop must include that
node, not just the original voters** — the same corpus above
(`learner_crash_is_replaced_by_a_new_target`) hung for the entire poll
budget on one seed in ~14% of variants: the newly-promoted replica won
the next election (perfectly legitimate — it is a real voter the moment
it is promoted), but the test's convergence loop only ticked the
*original* three nodes' reconcilers, so the one node that could actually
see itself leading and propose the final "remove the old replica" step
was never given a chance to. The group was correctly converged in every
way that mattered (right voters, right learners, keeps serving) —only
the test's own harness was blind to who was driving. **General rule**:
a test that lets membership grow past its initial cast of leader
candidates must poll/tick every member that could plausibly hold
leadership by the time the property under test is checked, not just the
set that started the scenario.
