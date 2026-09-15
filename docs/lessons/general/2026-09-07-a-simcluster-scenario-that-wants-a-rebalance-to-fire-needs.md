# A `SimCluster` scenario that wants a rebalance to fire needs more imbalance than "one extra idle node," not just more nodes than tablets (ADR 0061 rung D4 PR 4)

`sim_cluster_growth.rs`'s scenario (b) (growth then a `CreateTable`
placing and rebalancing onto the new node) first provisioned exactly ONE
table on a cluster just grown from 3 to 4 nodes, at the wire's own fixed
RF 3 — and the rebalance-driven move it was asserting on never happened,
even though the fourth node was genuinely idle and every precondition
looked satisfied. The cause: `animus-placement`'s `rebalance_step` only
ever performs a **balance-driven** move, converging to max−min ≤ 1 load
across nodes (`animus-placement/CLAUDE.md`) — and a single tablet held by
3 of 4 nodes is already at loads `{1, 1, 1, 0}`, whose max−min is exactly
1. The placement engine is correctly judging that state balanced enough
and has nothing to improve; a scenario expecting a move needs an
imbalance the engine actually recognizes as worth fixing. **Fix**:
provision **three** tables instead of one (`provision_soak_tables_and_
wait_for_replica`'s own doc has the full reasoning) — reproducing
`sim_cluster_dynamo_table_ops.rs`'s own #715 regression setup, whose
loads land at `{3, 3, 3, 0}` after the wire's own deterministic
first-three-`Active`-members placement, comfortably outside the balanced
band. **General lesson, not specific to this scenario**: "more nodes than
replicas" and "the placement engine will move something" are NOT the same
claim — `rebalance_step`'s own convergence bound is stated in terms of
per-node *load*, not node count versus replication factor, and a
single-tablet setup is the smallest case that can accidentally already sit
inside that bound. Any test asserting a rebalance-driven move needs enough
tablets (or enough skew) that the *starting* load spread genuinely exceeds
max−min ≤ 1, checked against the real formula, not assumed from "there's
an idle node."
