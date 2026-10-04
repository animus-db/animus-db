# A negative control must pin the observable the mutation removes, not just "something is wrong"

Context: ADR 0073 Phase 2 mixed-version corpus (P2-D), negative control N1 (a
premature era variant wedges a Phase 1 replica).

The first N1 asserted "replica 2 is wedged (applied < leader commit)" and
"era safety tripped". The mutation "capped decode logs the rejection but still
delivers" did **not** fail it: once the era entry is applied the leader's
dial-side handshake refusal stops sending to the empty-ext node anyway, so the
replica stays behind either way. The control passed for a reason that has nothing
to do with the mechanism under test.

Fix: assert the specific thing only the mechanism produces (the Phase 1 replica's
`last_log_index` did not move, i.e. the batch took the undecodable branch), and
require the rejection to have fired on at least one seed of the cell (it can
legitimately lose a race against the handshake refusal on another). Then run the
mutation and confirm the control goes red.

Rule: when a control can be satisfied by two independent protections, assert on
the one you are testing. A mutation check is the only way to find this out.
