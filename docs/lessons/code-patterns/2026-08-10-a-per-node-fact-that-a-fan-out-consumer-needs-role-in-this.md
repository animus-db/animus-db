# A per-node fact that a fan-out consumer needs (role, in this case) is cheapest to replicate alongside the node's own self-registered state, not to fetch by adding a second round trip per node.

**A per-node fact that a fan-out consumer needs (role, in this case) is
cheapest to replicate alongside the node's own self-registered state, not
to fetch by adding a second round trip per node.** `/admin/peers` used to
return only addresses; the dashboard already had to fetch every peer's
`/admin/config` just to learn ITS role, meaning "gate/label by role"
structurally depended on that peer's own fan-out succeeding first — the
exact coupling the ADR 0035 PR7 "self-only probe" lesson above warns
against, just one layer further out (there, gating *this* node's own tabs
on a peer fan-out; here, labeling *other* nodes' rows on each of *their*
individual fetches succeeding). Since a node only ever authoritatively
knows its own role, the fix is not a new endpoint or a bulk-query
mechanism — it is adding the fact to the exact structure that already
replicates "this node's own stuff, once, at startup"
(`animus_control::meta::NodeAddrs`, mutated by `RegisterNodeAddrs`): every
node stamps its own role into the very `NodeAddrs` it already
self-registers, and any reader gets every node's role for free from
`Metadata.node_addrs`, already synced, already fanned-out via the one
`/admin/peers` call. **General check when a fan-out/dashboard-style
consumer needs a per-node fact that changes rarely and each node already
knows about itself: is there an existing "self-registered, replicated,
read-by-everyone" structure to add the field to, rather than a new query
the consumer must issue per node (which inherits that node's own
reachability as a precondition for learning a fact about it)?** Kept
strictly additive: the pre-existing `admin_addrs` field was untouched, a
new `peers: [{admin, role}]` field carries the addition, and the dashboard
treats the peer-sourced role as a *fallback* behind each node's own richer
`/admin/config` fetch (which still runs, for the other fields it returns)
rather than replacing that fetch outright — so a node whose own fan-out
fails now degrades to "shown, correctly labeled, marked unreachable"
instead of "invisible". (`animus-control` `meta::NodeAddrs::role`;
`animusd` `admin.rs::peers_view`, `dashboard_core.js::loadAll`; ADR 0035
residual follow-up.)
