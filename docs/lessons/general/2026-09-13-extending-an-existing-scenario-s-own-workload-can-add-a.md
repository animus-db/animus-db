# Extending an existing scenario's own workload can add a later assertion without invalidating an earlier point-in-time one (ADR 0061 rung M, C-13 PR 4)

Two of `tests/seed_join_allocated.rs`'s five real-socket tests
(`no_node_join_becomes_active_and_gets_a_replica`, `follower_connected_
seed_completes_the_allocate_node_id_round_trip`) turned out, once read
directly rather than assumed from the file's shared module doc, to need
either MORE of an already-existing sim scenario's own workload or nothing
new at all — not a fresh scenario apiece. The one genuinely new piece
needed was proving a BALANCE-driven (not violation-driven) replica
placement for a self-minted COMBINED joiner, which the existing scenario's
own single-table setup structurally could not exercise (one tablet at RF 3
across exactly 3 pre-existing nodes is already at `rebalance_step`'s own
`max - min <= 1` convergence threshold the moment a 4th node joins — no
move is ever triggered). The fix was not a new scenario; it was adding two
more tables to the EXISTING scenario's setup (creating real balance
pressure) and a TRAILING poll-based assertion after its own pre-existing,
point-in-time forwarding proof ("the joiner hosts no replica of `t1`
immediately after promotion"). The two assertions do not conflict even
though the later poll can eventually move `t1`'s own tablet onto the
joiner too: the earlier assertion is checked, and is true, at an earlier
moment in virtual time, and nothing about a LATER fact being different
un-asserts an EARLIER one that already ran. **The general rule**: before
reaching for a new scenario to prove one more property of an existing
mechanism, check whether the existing scenario's own workload merely
needs to be widened (more tables, more nodes, more writes) — and check
that any new assertion is either checked at a moment that cannot yet
have been disturbed by the widened workload, or is itself a converged-
or-timeout poll for a LATER fact, never a re-assertion of an EARLIER
point-in-time fact that a wider workload might have since moved past.
This is materially cheaper to build, review, and maintain than a second
near-duplicate scenario, and it is what let two of five real-socket tests
in this file collapse into zero new sim scenarios rather than two.
