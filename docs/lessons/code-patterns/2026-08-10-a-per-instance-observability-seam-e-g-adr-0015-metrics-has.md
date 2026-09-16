# A per-instance observability seam (e.g. ADR 0015 metrics) has *one sink per `Env`/role*, so the integration layer must aggregate, not pick one.

**A per-instance observability seam (e.g. ADR 0015 metrics) has *one sink per
`Env`/role*, so the integration layer must aggregate, not pick one.** A node
runs several `ProdEnv` roles on distinct ids (control/data/coord), each with its
**own** `metrics()` sink — `RaftNode::start` records into the *control* env's,
the replica/coordinator into theirs. A `/metrics` handler that read only one
(e.g. `node.raft.metrics()`) would silently drop the others' counters. Capture
every role's handle and sum the snapshots **at request time** (live, not cached);
capture the soon-to-be-moved handles before the envs are consumed. (`animusd`
`ClientCtx::metrics_text`.)
