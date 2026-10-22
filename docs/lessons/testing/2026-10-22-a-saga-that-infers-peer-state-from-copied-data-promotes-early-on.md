# A saga that infers "the peer has it" from shipped data promotes early on empty input — gate on an explicit acknowledgement.

**A saga that infers "the peer has it" from shipped data promotes early on empty
input — gate on an explicit acknowledgement.** The MREC `Creating` replica went
ACTIVE once the initial copy scan finished, treating "copied" as proof the peer
table existed. An empty source table produces no batch, so no peer contact
happened and the replica was ACTIVE with no peer table. Track the peer's `Done`
reply to the create message (node-local, in memory; a new driver re-sends the
idempotent create) and require it before promotion. Test the degenerate input
(empty table) with a partition around the create, asserting the externally
visible state (`DescribeTable`) never runs ahead of the peer's real state.
