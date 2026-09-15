# `tokio::join!` over two bare `&self` teardown calls (e.g. `Node::shutdown_graceful`) is the right way to manufacture a genuinely SIMULTANEOUS multi-fault scenario in a `ProdEnv` test — sequential kills, even back-to-back with no `sleep` between them, understate overlap, because the first call's async teardown can fully finish before the second one's even starts.

**`tokio::join!` over two bare `&self` teardown calls (e.g.
`Node::shutdown_graceful`) is the right way to manufacture a genuinely
SIMULTANEOUS multi-fault scenario in a `ProdEnv` test — sequential kills,
even back-to-back with no `sleep` between them, understate overlap,
because the first call's async teardown can fully finish before the
second one's even starts.** Building the ADR 0035 multi-fault chaos tests
(control-leader failover + a data-node failure at the same instant;
`crates/animusd/tests/split_cluster.rs`), `shutdown_graceful` takes `&self`,
so `tokio::join!(control_nodes[i].shutdown_graceful(),
data_nodes[j].shutdown_graceful())` races both teardowns on the executor
instead of resolving one before starting the other — the same
"concurrent, not sequential" property `tokio::join!` already gives any pair
of independent futures. **A related, less obvious point found while
designing the crossover-racing sibling test** (a tablet split triggered at
the same time as a decommission/drain of one of that tablet's current
replicas): `admin_drain` (`ClientCtx::admin_drain`) only proposes
`UpsertMember{status: Leaving}` and returns — it does **not** itself touch
any tablet's replica set. The actual replica evacuation happens
asynchronously, on the placement reconciler's own event-driven schedule,
entirely decoupled from when the admin call returns. So a test wanting to
exercise "a drain landing mid-split-crossover" does not need
millisecond-precise interleaving between the two admin RPCs — firing both
via `tokio::join!` is sufficient, because the actual race (the split's
freshly-minted child inheriting the parent's about-to-be-evacuated replica
set) is created by the reconciler's background convergence timing, not by
the order the two admin calls happen to land in. Both new tests passed on
their very first run and stayed stable across 5+ isolated re-runs plus
repeated full-file runs — no product bug surfaced (the fences/reconciler
work ADR 0028/0031/0033 already built handled the crossover correctly).
