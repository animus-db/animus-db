# SimWorld MREC tests: peer index == bridge cluster index; TTL has a 5-year window

- A sender-view `MrecConfig` must list peers so that a peer's position equals the
  `SimWorld` bridge's cluster index (`["pad", "b"]` for cluster A). Listing only `["b"]`
  routes to cluster A itself and the receiver answers "not a configured peer", which the
  saga then (correctly) treats as a permanent refusal -> `CREATION_FAILED`.
- The TTL reaper ignores an expiry older than 5 years (AWS-faithful). Under SimEnv the wall
  clock starts in 2020, so `ttl = 1` is never reaped; use an epoch within the window.
- An item that is already expired when written can be reaped before the shipper sees it, so a
  "TTL delete replicates" test must first prove the peer holds the item (use an expiry some
  virtual seconds ahead), otherwise it proves nothing.
- `SimCluster::drive_ttl_sweep` on a cluster that also runs the always-on reaper can wedge
  later reads on that cluster in a `SimWorld`; let the loop reap instead.
