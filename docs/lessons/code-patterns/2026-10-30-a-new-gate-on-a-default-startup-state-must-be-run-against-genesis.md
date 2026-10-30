# A new gate on a default startup state must be run against genesis

The pre-vote cluster-check gate (fb7617bd) refused whenever the check was
`Pending`. That is the correct state to guard for a wiped voter, but it is also
the start state of every ordinary freshly hosted replica, so a genesis group's
first pre-vote round was refused and the first election slipped a full election
timeout. The control-plane tests passed because the tests that build genesis
groups through the real cp-data host were not run for the change.

When gating a path on a state, ask which healthy flows also start in that state
and run their tests (here `animus-cp-data`, not just `animus-control`). Prefer a
discriminator that separates the bad case from genesis using data already on the
message: a wiped voter faces a candidate with a non-empty log, genesis has none.
