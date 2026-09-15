# A *per-node* decision must dedup on *per-node* state, never on the shared `ClusterEdgeState` — in `--cluster N` that edge is shared across nodes and silently reports another node's state.

**A *per-node* decision must dedup on *per-node* state, never on the shared
`ClusterEdgeState` — in `--cluster N` that edge is shared across nodes and silently
reports another node's state.** The CP join-host loop (ADR 0023 provisioning) gated
"already hosting this tablet?" on `edge.local_cp(tablet)`. In one-process-per-node
that is this node's view; in an in-process `--cluster N` run the edge is **shared**,
so as soon as *one* replica hosted a freshly provisioned tablet and registered it,
every other replica's loop saw it via `edge.local_cp` and **skipped** — leaving the
tablet hosted on a single replica, no majority, no election, "no CP group leader
reachable". The signature was **bimodal flakiness** (race: all replicas host iff
they poll before the first registers, ≈1.5 s; else one hosts and it stalls to the
timeout). Dedup on the genuinely per-node `minted` claim set instead. This is the
*hosting-path* instance of the documented "shared `--cluster` edge masks per-node"
gotcha — assume any `edge.*` read is cluster-wide in `--cluster N`. (`animusd`
`cp_join_host_loop`. Both halves of this entry are historical now: ADR 0031
PR2 made the edge genuinely per-node, and PR4 replaced
`cp_join_host_loop`/`minted` with the tablet-host reconciler's own
`LocalState::hosted` — which is per-node *by construction*, since the
reconciler owns the hosted map outright.)
