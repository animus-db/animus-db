# Per-peer leader progress must reset when a peer is (re)introduced, not only when it is dropped

**Context**: a tablet leader re-added a node as a learner after the node's
replica was removed and erased. The leader still held the old `match_index`,
so `learner_caught_up` (a pure predicate over that map) said yes and the empty
node was promoted, then permanently refused by the wiped-voter guard.

**Lesson**: cleanup keyed on one exit path (`drop_departing` on `RemovedAck`)
misses the others (a removed learner is never departing; a re-add can land
before the ack). Make freshness a property of the *entry* side: whenever a
membership change introduces a peer, overwrite its progress and transfer
state. A predicate that trusts a remembered per-peer number is only as sound
as the reset discipline of that number.

**Why**: the test must leave the leader's state stale on purpose (here: a
directed partition dropping only the peer's replies) and promote at the moment
of re-add; letting replication run first hides the bug.
