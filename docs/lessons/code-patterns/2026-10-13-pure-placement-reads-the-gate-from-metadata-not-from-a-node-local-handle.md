# A pure function evaluated by the leader reads its gate from the replicated state it is given

`Gate::NodeCapacity` changes what the control leader's placement passes compute,
not what any entry carries. The usual emit-site check (`ClusterFeatures`) is a
node-local handle fed on a tick, so two replicas (or the leader before and after
a failover) can disagree about whether the gate is open for a few hundred
milliseconds. That is harmless for an emitted command (a closed verdict only
delays it) and wrong for a pure function whose output every replica is expected
to reproduce: `CutoverSplit`'s directed-Placing target is computed at *apply*
time, and the rebalance property tests assume one answer per input.

So the placement inputs are derived from the `Metadata` itself: `effective_capacities()`
is the declared map iff `cluster_version() >= Gate::NodeCapacity.version()`, empty
otherwise, and an empty map selects the original (pre-capacity) functions rather than
the capacity-aware ones with all-default inputs. Gate closed is then literally the old
code path, and the equivalence is a fact about which function runs, not about two
algorithms agreeing. Keep the node-local handle for what it is good at: deciding
whether to *propose* (`sync_node_capacity` polls it and never proposes through a closed
gate, because `RaftNode::propose` debug-asserts on a closed verdict).

A sync loop that reconciles configuration into replicated state should compare first and
return when equal, before looking at the gate: a cluster that declares nothing must not
wait for, or even notice, a gate it will never use.
