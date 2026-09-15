# An admin/introspection surface is a pure *observer* over per-instance handles, aggregated live — and per-instance state makes it meaningful *per node*, not cluster-wide.

**An admin/introspection surface is a pure *observer* over per-instance handles,
aggregated live — and per-instance state makes it meaningful *per node*, not
cluster-wide.** The admin interface (ADR 0020) only *reads* node state (Raft
accessors, promoted `LsmEngine` introspection — kept snapshot-shaped so it can't
perturb the measured path, like the metrics seam) or drives an explicit gated
action; it never changes the path it inspects. Two consequences bit during the
build: (1) **metrics/Raft counters are per-node sinks** — a *follower's*
leader-only counters (`elections_won`, `append_entries_sent`) are legitimately 0,
so an admin/metrics endpoint is sound only *per node* (scrape the leader for
leader-only state; the test asserts election counters only on the control leader).
(2) **the in-process `--cluster` shared `ClusterEdgeState` lists *every* node's CP
group handle**, so one node's `/admin/raftkv` shows all replicas, while a
one-process-per-node deployment (separate edge each) is node-local — match the
test's bring-up (`run_node` per node) to the semantics you assert. Reuse the
documented port-TOCTOU retry for any `free_addrs`-style `ProdEnv` bring-up.
