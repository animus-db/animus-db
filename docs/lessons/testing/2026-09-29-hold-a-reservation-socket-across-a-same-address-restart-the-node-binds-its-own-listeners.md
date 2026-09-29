# Hold a non-listening `SO_REUSEADDR` reservation across a same-address restart when the node binds its own listeners (issue #1094)

Bind-and-hold (issue #627, see `2026-09-20-allocate-test-ports-by-binding-and-holding-never-probe-and-release.md`)
fixes the *first* bring-up: bind `:0`, keep the listener, start the node behind
it. It cannot fix a **same-address restart**: the node's shutdown drops its own
listeners, and the restarted node must rebind the *same* ports. Nothing the
test holds can be handed over — `ProdEnv` binds its internal socket itself (and
`animus-env`'s `prod.rs` is off limits) — so the ports are unclaimed for the
whole gap, and another test binary's `bind(:0)` or any outgoing `connect()`'s
ephemeral source port can take one. It showed as a sustained
`Address already in use` (30s in `control_mirror_restart`), not a microsecond
blip, because the thief is often another test's node *holding the port for its
whole life*. A bounded rebind retry does not fix that; it only delays the panic.

**Fix: a reservation socket.** `support::reserve_addrs` (what `free_addrs` and
the bring-up helpers now use) binds `127.0.0.1:0` with `SO_REUSEADDR`, never
`listen()`s, and parks the socket in a process-global list. On Linux:
- the node's own listener (`SO_REUSEADDR` too — std, tokio and mio all set it on
  Unix) can bind and `listen()` atop it, since a reuse-bind is only refused
  against a socket in `LISTEN`; so **no seam in the node is needed**;
- the reservation survives the node's shutdown, and while it is bound the kernel
  skips that port for `bind(:0)` and for outgoing-connect source-port selection.
  Proof (root, sandbox): with `net.ipv4.ip_local_port_range` narrowed to 16
  ports, a `:0` thief that takes every port it is offered got all 16 after a
  probe-and-release, but only the other 15 with a reservation held (and no
  `connect()` source port hit it either). Restore the sysctl afterwards.

Not covered: a thief that names the *exact* port with `SO_REUSEADDR`. Nothing in
the suite does. `tests/port_reservation.rs` asserts the property
deterministically with a no-`SO_REUSEADDR` thief (fails on the old code, passes
now). Linux-only mechanism (BSD/macOS `SO_REUSEADDR` would refuse the node's bind
atop it), so `reserve_addrs` falls back to releasing off Linux.

Also: attach `support::port_holders(addr)` (`ss -tanp`, not just `-l`) to a
rebind-failure panic — a TCP self-connect squats a port without listening.
