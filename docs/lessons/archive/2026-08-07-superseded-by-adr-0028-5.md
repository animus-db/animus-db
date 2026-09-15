# Superseded by ADR 0028

**Superseded by ADR 0028**: the split hook + `base + tablet*STRIDE` member-id
derivation this entry discusses are deleted (ADR 0026 Stage B made a
tablet's CP group member id simply the base `raftkv` id, at any split
depth, so there is no derivation left to get wrong). Retained for
historical record. **A recursive operation that "works" once may be relying on a depth-1 coincidence —
prove it at depth ≥ 2.** Tablet *split* worked the first time for two accidental
reasons that both break at depth 2: (a) only the *bootstrap* group was started with
a split hook, so a split-created child had no machinery to split *itself*; and (b)
the member-id derivation `base + tablet*STRIDE` (flat, from the node's base id)
equals the compounding `parent_member + tablet*STRIDE` *only* because the bootstrap
parent's member id == its base id — for a grandchild they diverge, and the
reconfigure loop (which translates the replicated base-id replica set flatly) then
churns forever on the mismatch. Fix recursive invariants to hold at any depth: give
**every** spawned instance the same machinery (a hook), and derive ids from a
**fixed root** (the base id), never the immediate parent. (ADR 0017 deep splits.)
