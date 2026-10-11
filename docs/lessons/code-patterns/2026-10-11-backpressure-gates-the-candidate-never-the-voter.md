# Throttle the side that starts work, never the side that answers it: gating Raft voters as well as candidates deadlocks the admitted campaigns.

**Throttle the side that starts work, never the side that answers it.** (ADR 0078, issue #1199.) When designing per-node election backpressure, the tempting move is to make a deferred group fully passive: re-arm its election timer so it waits its turn. But `handle_pre_vote` grants only when `now >= election_deadline` (or no leader is believed), so a re-armed follower starts *refusing* pre-votes, and the few campaigns the cap did admit cannot reach a majority. They hold their permits to timeout and the cap makes recovery slower, not faster. The fix is to leave the deadline lapsed and add a separate `campaign_hold`, so a deferred follower still grants.

Generalizes: before adding admission control to a protocol, grep which existing state the *responder* reads (here the election deadline) and make sure the throttle does not mutate it. The same applies to any "be quiet while overloaded" change in a quorum protocol.

Also: a SimEnv `pause` (all-or-nothing freeze) cannot reproduce load-proportional slowness, so a cliff found only under `ProdEnv` needs a simulated starvation model (serialized per-node event cost) before a fix can be tested deterministically.
