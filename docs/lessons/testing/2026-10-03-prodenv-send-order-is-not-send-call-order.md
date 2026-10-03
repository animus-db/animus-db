# ProdEnv `send` order is not call order: assert sets, not sequences

`ProdEnv::send_stream` spawns one task per outbound frame (the issue #661
`SEND_TIMEOUT` bound), so three back-to-back `send(..).await` calls to one
peer can arrive in any order, even over a single pooled connection. A
real-socket test that asserts arrival order (`payload == [0]`, then `[1]`,
...) is flaky by construction; it failed on the first run while testing the
handshake `ext` stamping (ADR 0073 P2-A).

Collect the received payloads and compare them sorted (or as a set). Ordering
claims belong in `SimEnv` tests, where delivery order is a pure function of
the seed. A related negative-assertion rule: "this frame was never delivered"
needs a positive control (another peer's frame arriving afterwards) plus a
short bounded wait, never a bare sleep.
