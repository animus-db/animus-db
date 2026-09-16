# A quorum primitive's "who do I need acks from" and "how many acks do I need" must both read the group's *live* Raft config — never a peer set captured once at construction, even one that looks read-only/immutable.

**A quorum primitive's "who do I need acks from" and "how many acks do I
need" must both read the group's *live* Raft config — never a peer set
captured once at construction, even one that looks read-only/immutable.**
Building automatic replica rebalancing (ADR 0029) needed a *healthy* replica
move (not just failure repair), which for the first time could rotate a
majority of a tablet's Raft group onto nodes that were never in any
surviving replica's original peer set. `RaftKvNode`'s ReadIndex barrier
(`animus-cp-data`) had silently keyed both its ack-quorum threshold
(`majority()`) and its probe fanout on `all_nodes` — the group's peer set at
*hosting time* — instead of `RaftCore::config()` (the live, dynamically
updated voter set already used everywhere else in the same crate). This was
invisible for the entire life of the feature it was built for: every
membership change before ADR 0029 was a same-size, pre-known swap (a
failure-repair spare was already listed in every replica's `all_nodes` from
the moment the group formed), so the stale and live sets never actually
diverged. The break only showed up once a *different* feature (rebalancing)
exercised a membership shape (a full rotation) the original code was never
tested against — a stale-quorum leader could only ever self-ack, so every
linearizable read on that tablet timed out and reported the key **absent**,
indistinguishable from real data loss from outside. A second, compounding
bug in the same feature made it worse: `animusd`'s CP-routing short-circuit
(`resolve_cp_route`) trusted "I have a locally registered group handle" as
proof of being a *current* replica — true before ADR 0029, false during the
new removed-replica GC's deliberate grace window — so a node that had just
been rebalanced off a tablet, but not yet GC'd, waited forever instead of
forwarding to the tablet's actual current replicas. **General check when
adding a new way an existing invariant can change** (here: "a group's peer
set can evolve after hosting," where before it was fixed for a group's whole
lifetime): grep every place that invariant's *original* form was cached or
assumed stable, not just the one mechanism you're adding to change it — an
optimization that skips re-deriving a fact from live state ("no `Metadata`
clone needed, I already have a local handle") is exactly where this hides,
because it was correct on every input anyone had tried before. Caught by
building a genuine end-to-end integration test (`animusd/tests/
cp_rebalance.rs`, a 5-node cluster with tables provisioned before growth) —
no unit or sim test at either layer alone exercised a *full* replica-set
rotation through a *linearizable read*, only through `local_get`/config
equality. When writing a regression test for "a stale peer keeps
responding," make the peer actually stop responding (`shutdown()`), not
just remove it from the current config — a still-live departed peer can
accidentally still ack on a bare term match and mask the very bug the test
exists to catch. (`animus-cp-data::RaftKvNode::majority`/read-barrier probe
fanout; `animusd::resolve_cp_route`'s `has_local_replica` gate.)
