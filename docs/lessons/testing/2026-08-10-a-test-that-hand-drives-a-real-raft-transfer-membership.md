# A test that hand-drives a real Raft transfer/membership change must retry the whole arm/propose sequence on a poll tick, never assert success on one attempt — even when the immediately-preceding `is_leader()` check was synchronous and just returned true.

**A test that hand-drives a real Raft transfer/membership change must retry
the whole arm/propose sequence on a poll tick, never assert success on one
attempt — even when the immediately-preceding `is_leader()` check was
synchronous and just returned true.** Building the ADR 0031 PR5 reconciler
lifecycle corpus, two hand-rolled "force a real membership removal" test
helpers passed every run at low seed depth and then hit
`NotLeader{leader: Some(<the exact node just confirmed as leader>)}` from
`change_membership`/`transfer_leadership` at `ANIMUS_RECONCILER_SEEDS=60`
and `=150` — a real, already-documented core behavior (`propose`/
`change_membership` **freeze**, returning `NotLeader` with the *transfer
target* as the "leader" hint, while a leadership transfer is armed
elsewhere in the group; see this file's "two-layer gate" entry) that a
single-shot assert cannot distinguish from a genuine failure. No amount of
sleeping between the `is_leader()` check and the propose call closes this,
because the freeze can arm *after* the check — the fix is to fold the
whole "check → act" sequence into the body of a bounded retry poll (`check
condition; if not met, attempt the action; return false; poll again`) and
only fail once the bound is exhausted, exactly like every production retry
loop in this codebase already must (`ProposeResult::Accepted` isn't
`committed`, and `NotLeader` isn't necessarily permanent). This is the same
discipline as the standing "a retry loop over a Raft write must distinguish
never-accepted from accepted-unconfirmed" entry, just showing up inside a
*test's* orchestration code instead of production code — seed depth is what
surfaced it, at low depth every run happened to avoid the race window.
(`animus-cp-data/tests/reconciler_corpus.rs::remove_replica_for_real`,
`scenario_partition_blocks_release`.)
