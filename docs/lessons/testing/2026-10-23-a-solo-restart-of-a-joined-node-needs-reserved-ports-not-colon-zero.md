# A test that restarts a joined node on its own bound ports must reserve them, not request `:0`

`tests/join_data_seed_settings_reach.rs` joined nodes with `:0` addresses
(issue #1042: bind before claim) and then, after shutdown, restarted the
joined node's directory on the ports it had been assigned
(`join_defaults_to_shared_wal_matching_a_bare_config_node` and its
`no_shared_wal` sibling). The kernel-assigned ports are released at
shutdown, so the restart gap is unclaimed: another test process's `bind(:0)`
(or any outgoing `connect()` source port) can take one, and the restart fails
with `Address already in use` (observed on main CI, `prod-liveness-animusd
(2/4)`, and reproduced under three concurrent copies of the shard's
real-process tests: 1 failure in about 45 runs of the 111-test subset).

Fix: `support::reserve_addrs` (issue #1094) for the join's six addresses. The
node binds and listens atop the `SO_REUSEADDR` reservation, so #1042's
bind-before-claim order is intact, and the reservation keeps the port in the
kernel's bind table across the restart gap. Rule: `:0` is for nodes that are
never re-bound; anything restarted "on the same addresses" gets
`reserve_addrs`. See also
`2026-09-29-hold-a-reservation-socket-across-a-same-address-restart-the-node-binds-its-own-listeners.md`.

The flake is rare and load-dependent; a single-test hammer (even with 6
parallel copies and a connection-churn load) did not reproduce it, so
hammer the whole real-process subset with several concurrent copies.
