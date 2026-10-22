# A copy step that needs no peer contact must not promote a replica the peer was never told about

Legacy global tables (ADR 0075, 2026-10-21) were the first MREC flow whose
source table is always empty. The saga's `Creating -> Active` promotion was
"every active tablet has `copied` set", and the shipper sets `copied` when its
base scan reaches the end. An empty scan sends no batch, so it never talks to
the peer: `copied` flipped on the first shipper pass even while a partition
dropped every `CreateReplica` / `LegacyReplica`, and the saga only sent that
message while `copied` was empty. The replica became `ACTIVE` with a peer that
was never told, and nothing retried. The 2019 flow had the same hole for any
empty source table; it was invisible because its tests created data first.

Rules:

- A state transition that means "the peer is ready" must be gated on the peer's
  acknowledgement of the step that makes it ready, not on a side effect that
  may complete without contacting the peer (an empty scan, a no-op copy).
- A retry loop guarded by "nothing done yet" breaks when a *different* step can
  satisfy the guard. Guard on the acknowledgement of the step being retried.
- Test a replication flow with the degenerate input (an empty source) under a
  partition during setup, and assert the state is not the success state while
  the partition holds (`ACTIVE while partitioned` in
  `sim_world_mrec_legacy_tests`); a test that only checks convergence after
  the heal cannot see this.
- A cluster being removed must not run the "tell the others to drop X" step for
  its own peers: it believed every peer was leaving and dissolved the rest.
