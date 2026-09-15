# `Simulator::crash`'s crashed-check happens at *delivery* time, not send time

**`Simulator::crash`'s crashed-check happens at *delivery* time, not send
time** (`fire_event`'s `Event::Deliver` arm, `animus-sim/src/lib.rs`) —
which makes "kill a node while a message to it is still in flight" a
deterministic, seed-reproducible scenario rather than a race to script by
hand: set a nonzero `NetConfig::base_delay`, send, sleep for less than
that delay, then `crash` the target — the message is guaranteed to still
be in the timeline (not yet delivered) at the moment of the crash, so it
is dropped exactly as if the node had died before the message arrived.
Used to test `ClusterSegmentStore`'s "node death mid-put" case
deterministically instead of accepting a flaky race or, worse, only ever
testing "target already down before the call starts" (a strictly weaker
scenario the two are easy to conflate). (`crates/animus-cp-data/tests/
cluster_segment_store.rs`, ADR 0043 round-3 PR3, 2026-08-14.)
