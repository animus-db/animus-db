# A rebalance-dependent test needs enough independent tablets that the pre-growth cluster is *not already balanced* — one table can leave a joined/grown node with zero replicas forever, not just an ambiguous choice of which table to route through.

**A rebalance-dependent test needs enough independent tablets that the
pre-growth cluster is *not already balanced* — one table can leave a
joined/grown node with zero replicas forever, not just an ambiguous
choice of which table to route through.** `rebalance_step` only proposes a
move while it improves the *global* `max − min` imbalance; with exactly
one table (one tablet, RF = the pre-growth node count) every pre-growth
node already holds exactly one replica and the joined node holds zero —
`max − min == 1`, already at the stopping condition, so the rebalancer
never moves anything and a test polling for "the joined node gained a
replica" times out completely (not flakily — every run). This is a
sharper version of the already-documented "the rebalancer converges the
*global* imbalance and makes no per-table promise" lesson (which is about
which table a test must route through once *some* replica has moved) —
the additional wrinkle is that with too few tablets, the imbalance can be
zero from the start and *no* replica ever moves. Fix: seed several
independent tables (`tests/decommission.rs` uses three, mirroring
`tests/seed_join.rs`'s `TABLES`), so the pre-growth distribution is
imbalanced enough to guarantee at least one move onto the new node.
