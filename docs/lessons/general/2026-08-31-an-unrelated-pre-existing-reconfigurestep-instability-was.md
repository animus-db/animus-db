# An unrelated, pre-existing `reconfigure_step` instability was found (not fixed) while validating the above: a 2-of-3-replica-swap target can make live Raft membership oscillate instead of converge (ADR 0062 rung 6)

While building the real end-to-end test for the settle-window fix above, a
target requiring the reconfigure sequence to replace **two** of a tablet's
three replicas at once (grow a 3-node cluster by two lower-sorting-id
nodes, so `select_replicas` prefers both new nodes over two of the
parent's three) reliably made the live CP-data Raft group's own voter set
**oscillate** under a real `ProdEnv` cluster: it would reach the transient,
genuinely-over-replicated 5-member intermediate state
`reconfigure_step`'s add-before-remove sequencing produces (add both new
learners, promote both, only then remove the two extras), then revert
partway back toward the original 3-replica set, repeating indefinitely —
never settling within a 60s budget. **This is provably unrelated to the
completion loop above**: it reproduces with zero `MarkSplitPlacingDone`
proposes ever having fired (confirmed by instrumenting the loop and
observing the oscillation begin before the loop's own settle window had
even elapsed once), so the cause sits in `host::Reconciler`/
`reconfigure_step` itself (`animus-cp-data`, ADR 0058 Train 1), pre-dating
and unmodified by ADR 0062. A target that replaces only **one** of three
replicas (a strictly simpler add-one/remove-one sequence, never reaching a
5-voter intermediate state) converged cleanly and quickly on every run.

Not investigated further or fixed — out of this rung's own scope, and
worth a dedicated investigation rather than a guess folded into an
unrelated change (this repo's own "an incidental bug gets its own PR"
rule). `tests/split_placing_completion.rs` deliberately exercises only the
one-replica-difference shape, with its own doc comment explaining why, so
a future investigation of the two-replica case has a name, a reproduction
recipe (grow-by-two, RF3, real `ProdEnv`), and a already-written
regression-in-waiting to flip on once the underlying issue is found. Filed
here rather than silently worked around, per this repo's own "record a
generalizable lesson, including one you didn't chase to the end" practice.

**Amendment (2026-08-31, issue #513 dedicated investigation): re-attempted
and NOT reproduced.** A follow-up investigation drove the exact
grow-by-two-lower-sorting-nodes/RF3/real-`ProdEnv` recipe named above —
`crates/animusd/tests/split_placing_two_replica_diff_e2e.rs`, run 30+
consecutive times, several with continuous write traffic and several
where the tablet's own leader genuinely transferred mid-sequence (one of
the two suspects named when the issue was filed) — plus a `SimEnv` side
(`crates/animus-cp-data/tests/reconfigure_multi_replica_diff.rs`, 60
seeds) across five harness shapes of increasing production-fidelity:
calling `reconfigure_step` directly in a poll loop; `spawn_reconfigure_
loop` with one shared static target; `spawn_reconfigure_loop` with EVERY
group member (including the two about-to-be-removed originals)
independently polling its OWN control-plane replica — genuinely
Raft-replicated, not a shared closure — for `desired`/`down`; the real
`host::Reconciler` driven uniformly across all group members each tick;
and combinations of the above with continuous write traffic and forced
leadership churn via network partition. **Every single run converged**:
each reached the transient over-replicated 5-voter intermediate the
original finding names, then shrank monotonically to the target with no
reversion ever observed.

**Likely explanation for the original finding, based on this
investigation's own repeated experience**: building each of the five
`SimEnv` harnesses above, the very first draft of the "did it converge"
check independently made the SAME mistake three separate times before it
was caught — checking `config()`/`voters` on a replica that HAD ALREADY
BEEN EXCLUDED from the group's voter set (one of the two originals being
replaced), not one of the surviving/target members. A removed replica
stops receiving `AppendEntries` the instant it's excluded, so its own
locally-cached config **freezes** at whatever it last observed rather
than updating (there is nothing left to update it — that is what "removed
from the group" means). Comparing that frozen value against a live,
still-converging replica's value — or worse, alternating which replica an
observer reads from over time (e.g. "whichever node currently answers a
routing/admin query," which can itself change as a group's own admin
routing state settles) — produces exactly the "grows to N, then reverts"
shape the original finding described, without any defect in
`reconfigure_step` or its caller: the group genuinely converged; the
OBSERVATION read a stale snapshot and mistook it for regression. This is
the same class of mistake as the "convergence check races the proposer
that sets the target" entry immediately above this one in this file (a
naive comparison against live/eventual state, on a system that is by
design mid-flight), generalized to "and make sure which REPLICA you're
reading the live state FROM is still a live member of the thing you're
checking."

**General lesson**: when a convergence check spans multiple replicas of a
group whose membership itself is changing (not just its data), the check
must pin its observations to replicas that will remain members of the
final target set — reading from, or comparing against, a replica that's
mid-removal produces a frozen/stale value that looks exactly like a live
regression. This is a narrower instance of the "converging vs. stable
value" audit already named in this file's `placing_relocates_a_child...`
entry above, worth calling out on its own because it is easy to introduce
by accident when a repro harness (rather than production code) polls
"any" node's own admin/observability surface without checking whether
that node is still supposed to be part of the answer.

No code in `reconfigure_step`, `host::Reconciler`, or any of their
production callers changed as a result of this investigation — there was
no concrete defect to change. `crates/animus-cp-data/tests/
reconfigure_multi_replica_diff.rs` and `crates/animusd/tests/
split_placing_two_replica_diff_e2e.rs` are the two regressions this
investigation leaves behind: if the oscillation is ever genuinely observed
again (under conditions this investigation didn't hit, or because a future
change reintroduces it), these are the first things that should catch it.
ADR 0062 carries the matching amendment.
