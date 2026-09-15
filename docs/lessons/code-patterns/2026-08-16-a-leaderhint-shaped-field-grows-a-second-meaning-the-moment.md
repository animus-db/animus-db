# A `leader_hint`-shaped field grows a second meaning the moment a second network segment exists.

**A `leader_hint`-shaped field grows a second meaning the moment a second
network segment exists.** Adding the intra-cluster port (ADR 0047) meant
`ControlHandle::leader_addr_hint()`/`RemoteControlClient.leader_hint`
suddenly had **three** existing consumers wanting different address
flavors off the same field: `propose_schema`'s relay preference
(machine-to-machine, wants the new intra address),
`not_leader_error`'s human-facing "retry on {addr}" message surfaced
through the admin HTTP endpoint (must stay the client address — a human
operator dials it), and the dashboard's own leader-hint display (same,
explicitly documented as "the client-API address"). A naive
find-and-replace repoint would have silently broken the two human-facing
consumers on any `ControlHandle::Remote` node. Fix: add a **parallel**
hint (`intra_leader_hint` alongside `leader_hint`) rather than repoint the
existing one — before repointing any existing hint/route field to a new
address flavor, audit **every consumer's intended audience** (human
operator vs. machine relay), not just its current call sites. Standing
rule this established: machine relay → `intra_leader_hint`; anything a
human reads → `leader_hint`. (2026-08-16, ADR 0047 intra-port-split
stack.)
