# A "no local replica, forward blindly to whoever else has one" fallback path is easy to miss when retargeting an address-resolution axis, because it looks like an edge case rather than a primary path.

**A "no local replica, forward blindly to whoever else has one" fallback
path is easy to miss when retargeting an address-resolution axis, because
it looks like an edge case rather than a primary path.** ADR 0047
retargeted every named machine-relay resolver (`cp_leader_hint`,
`other_tablet_replica_addr`, `propose_schema`'s relay/broadcast) to the
new intra routing table, but missed `resolve_cp_route`'s own
zero-local-replica fallback (the very first guess a node with no local
replica of a tablet makes) — it kept reading `route_snapshot()` (client
addresses). Invisible by code review (nothing about it looks
forwarding-specific at a skim), it surfaced immediately as a real
`ProdEnv` test failure once a control-only node tried to forward a write
(`cluster_split.rs`'s `single_shot_first_write_through_control_node_
succeeds`): `Error("forwarded is a cluster-internal request; send it to
this node's intra port")`. **General form**: when retargeting "which
address flavor answers a forwarding question," grep every function whose
*return type* is an address/route candidate (not just the ones with
"leader"/"forward" in the name) — a last-resort/degenerate-case branch
inside a bigger routing function is exactly where a mechanical retarget
misses a spot. (2026-08-16, ADR 0047 intra-port-split stack.)
