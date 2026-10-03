# A SimEnv safety invariant is only trusted after you break the code under test

ADR 0073 P2-A's version corpora assert "the era is never on while any required
node is still a Phase 1 binary" and "an era-on peer refuses empty-ext peers".
Both passed on the first run, which proves nothing by itself: a checker that is
never exercised by the unsafe path also passes. Each was therefore
mutation-checked before commit: make precondition P accept `range: None`, stop
flipping `set_require_peer_ext`, stop latching `halted`, observe only
`Heartbeat` envelopes. Each mutation failed the matching cell with a printed
seed. Do this for every new safety invariant (a 2-minute change, revert it) and
record which mutation each cell catches.

Related: a new always-on loop on `RaftNode` is spawned last in
`start_with_orphan_sweep_after`, so it does not shift the task ids (and hence
the tie-break order) of the loops existing fixed-seed tests were tuned against.
