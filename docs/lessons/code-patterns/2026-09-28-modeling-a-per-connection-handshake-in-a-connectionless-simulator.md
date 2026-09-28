# Modeling a per-connection handshake in a connectionless simulator: check at message granularity, from already-known values, in the same spot existing hard drops live

`SimEnv` (`animus-sim`) has no real connections — `send`/`recv` move
messages over an in-memory timeline, never a socket (ADR 0003). Porting
`ProdEnv`'s real per-connection handshake (ADR 0073 Phase 0, workstream D:
a preamble exchanged once per TCP connection, checked with
`animus_env::handshake::check_peer`) into the simulator therefore has no
connection to hang the check off. The fix generalizes: check the identical
property (do the two peers' declared protocol specs agree?) at **message**
delivery time instead, in both directions, and place it in the exact spot
other unconditional drops (`"crashed"`, `"partition"`) already live —
`fire_event`'s `Event::Deliver` arm. Each refused message then stands in
for one refused connection; nothing about the check itself needs a
connection concept.

The determinism argument this shape buys, worth restating because it's easy
to get backwards: the check must be a **pure comparison of values already
stored in `SimState`** (a per-node override defaulting to a build-wide
constant), never a fresh draw or a new scheduled event. A test that never
calls the new setter sees every node at its default, so the comparison
always trivially succeeds and the whole check is a no-op — byte-identical
to a build that never had it, which is what keeps every pre-existing seed's
trace and execution unperturbed. The moment a check like this reads
`self.rng` or inserts into `self.timeline`, even only on a path that's
"supposed to" be rare, it stops being provably inert for the untouched
case — the whole existing regression suite (every seed, every corpus) is
the only thing that actually proves this, and it's cheap to run before
declaring the change safe.

One more generalizable piece: when the real mechanism checks symmetrically
(`ProdEnv`'s `perform_handshake` runs identically on both the accept and
the dial side, each checking the peer's preamble against its own build's
spec), the simulated version should call the *same* production check
function twice — once per direction — rather than shortcutting to "these
two values must be equal." In this instance the two are mathematically
equivalent for v1's exact-equality policy, but Phase 2's planned relaxation
(a supported-version-range intersection) is not necessarily symmetric, and
calling the real function both ways is what keeps the simulated model
automatically correct if that policy ever changes, instead of silently
diverging from it.

See `crates/animus-sim/src/lib.rs`'s `network_protocol_refused`/
`network_preamble_for` and `Simulator::set_network_protocol_for`, and
`crates/animus-control/tests/protocol_version_refusal.rs` for the
end-to-end proof.
