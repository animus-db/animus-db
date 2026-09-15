# A control plane's own correct, unconditional load-balancing pass is a real hazard for any test fixture that hand-hosts (or partially hosts) tablets without a full reconciler behind it (ADR 0061 rung D3 PR 2a)

Once a test fixture (`SimCluster`) populates real cluster membership and a
wire-provisioned tablet carries a genuine placement policy, the
production control-plane leader's own `reconcile_loop`/`rebalance_
placement` — unconditional, spawned by `RaftNode::start` itself, with no
opt-out for a test harness — is live over it and will act exactly as
designed: on a cluster with more members than a tablet's replication
factor, `rebalance_placement`'s load-balancing pass will genuinely move a
tablet's replica set to spread load across an otherwise-idle member,
proven deterministic across 25 seeds in this investigation (the exact same
two moves, every time, given the exact same starting shape). This is not
a bug in the control plane — it is the control plane doing precisely its
documented job. The hazard is entirely on the *fixture* side: any
mechanism a fixture builds to physically host a tablet's replicas in
response to `Metadata` (here, a minimal watcher standing in for the real
per-node `Reconciler`) must handle **both directions** of a replica-set
change — adding a newly-named replica's own hosting, *and* tearing down a
dropped one's — or the fixture can produce a genuinely inconsistent,
split-brain-shaped state (two different-membership consensus groups for
one tablet id) with no fault injection at all, purely from ordinary
placement rebalancing.

**The general lesson**: when a test fixture stands up a partial,
purpose-built substitute for a production reconciliation loop, audit it
against every *direction* of change the real mechanism it's standing in
for handles, not just the direction the fixture's own first use case
happened to need (here: "host a newly-created tablet" was the need that
motivated the watcher; "un-host a tablet whose replica set changed" was
never on that need's own critical path, so it was never built, and the
gap sat undiscovered until a scenario with genuine placement imbalance
went looking for it). When the fixture's own limitation makes a class of
otherwise-legitimate test scenario ((here: `node_count >
MAX_REPLICATION_FACTOR`) unsafe, document the bound explicitly (in the
fixture's own module doc and any per-crate guide) rather than leaving
future test authors to discover it the same way this investigation did.
