# A FIFO CPU backlog is a slow clock, not a starved node

*Found building ADR 0078's starvation model (`Simulator::set_cpu_cost`), 2026-10-11.*

The obvious model of an overloaded node is "serialize its events, each costs
`c`, serve them in the order they arrived". It does not reproduce the failure
it is meant to (issue #1199: leaders lost, terms climbing, never recovering).
Measured: 300 Raft groups on three nodes at 1.15x capacity for 110 s of
virtual time, with the backlog growing to ~60k events, produced **zero** term
changes.

**Why.** Under FIFO every event, heartbeats included, is delayed by the same
backlog in the same order. A follower sees heartbeats spaced `1.15 x 50 ms`
apart, not `150 ms+`. The whole node merely runs on a slower clock, and a
slower clock is invisible to a protocol whose only inputs are relative
timers and message order. A starved real node loses leadership because its
lateness is *uneven*: a runtime with thousands of ready tasks does not serve
them in arrival order, so one group's heartbeat can wait many times the mean
while another's is served at once. (Priority for expired timers, which is also
real, did not matter on its own: the follower's timer is re-armed by each
heartbeat processed, so it only fires if a heartbeat is starved.)

**What to do.** For a "node is behind" corpus serve the waiting events in a
seeded random order (`CpuOrder::Random`). Any standing queue then gives each
event a long, high-variance wait, which is the false trigger, while an
under-loaded node (short queue) is unaffected, so the cliff sits where the
capacity does. Keep the order deterministic (draw from the sim RNG only when a
costed node has more than one waiting event) so `ANIMUS_SEED` still replays.

**Also:** deferring every blocked event by re-inserting it at `busy_until`
(the shape `pause` uses) is O(backlog) per fired event; a per-node queue plus
one drain event is O(1). A model that works on 10 events and takes minutes on
60k is not usable as a per-push corpus.

**General form.** Before trusting a fault model, check it can produce the
failure it exists for: write the negative control first and let it say the
model is too benign.
