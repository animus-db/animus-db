# A TCP delay proxy on loopback turns a real-process cluster test into a WAN-shaped one, and the claim must stay "emulated"

`animusd/tests/mrec_wan_emulated.rs` points each MREC cluster's configured peer
endpoint at a test-only proxy (`tests/wan_proxy/mod.rs`) that delays every chunk by a
one-way delay, so the real TLS client, shipper loop and saga run under an 80 ms RTT
without any simulation. Lessons:

- Put the proxy on the **configured peer endpoint**, not on a port the cluster binds:
  nodes bind before their config exists, so create the proxies between `Node::bind`
  and `run_bound_node`. TLS passes through untouched (the endpoint host stays
  `127.0.0.1`, which the test certificate covers).
- Keep per-direction ordering and propagate half-close, or TLS and framing break in
  ways that look like product bugs. A delay proxy is not a WAN: no loss, reordering or
  congestion control; label every number "emulated WAN on loopback".
- Measure lag as ack-time to visibility **in the same test clock**, and assert only
  structural bounds (lag at least half the delay proves the proxy is on the path; a
  generous ceiling). Print the percentiles; never gate on them.
- Subtract an idle baseline from proxy byte counters before dividing by writes (here it
  was near zero because empty ships send nothing, but that is a fact to measure, not assume).
- Several worktrees sharing one `CARGO_TARGET_DIR` can leave another branch's compiled
  workspace crates in place: a build error in code you did not touch (a missing enum
  arm) means stale artifacts. `touch` each workspace crate's `src/lib.rs` and rebuild
  in the same command before trusting a result.
