# Superseded by ADR 0028

**Superseded by ADR 0028**: split has no step 2 anymore — `SplitTablet` is
the whole operation, so this entry's specific scenario (a step-2
`propose_split` failure stranding step 1's mint) can no longer occur.
Retained for historical record; the *general* two-step-operation lesson
still applies elsewhere. **A two-step operation where step 1 is a cheap, always-visible metadata write and
step 2 is the expensive, failure-prone "make it real" step must never let a
background loop discard a step-2 failure — that silently strands step 1's
effect forever.** `animusd`'s tablet auto-split (`auto_split_loop`) commits
`MetaCommand::SplitTablet` (step 1 — instantly makes the new tablet visible in
`Metadata.tablets` with a real range/replica set) then calls `propose_split` on
the tablet's CP-group leader (step 2 — actually mints the Raft group). The loop
used to do `let _ = ctx.trigger_split(..).await`; under real leader churn (bulk
writes causing elections) step 2 fails with `NotLeader`/no-route often enough
that discarding it left permanently orphaned tablets — `leader: unknown` forever,
any read/write to that range hangs — and *worse*, since the underlying data never
physically moved on a step-2 failure, the source tablet kept re-triggering on
later ticks and minting *more* orphans from the same unshrunk dataset (observed:
9 of 13 splits orphaned in one bulk-seed run). Fix: track step-1-committed/
step-2-pending tablets and retry step 2 with the *same* split key every tick
until it succeeds (safe — `propose_split` is idempotent per group), and skip a
tablet for a *fresh* split while one is already pending (stops the cascade). The
general rule: when step 1 of a two-phase operation is itself durable/replicated
and only step 2 completes the effect, a caller that abandons step 2 must not also
let step 1's artifact linger unreconciled — either retry step 2 forever (this
case) or roll back step 1. Regression-tested as a pure decision function
(`is_fresh_split_candidate`, mirroring `topology::decide_cp_route`'s split), since
the real leader-churn race isn't reproducible on demand over real network/time —
when an integration-level repro of a race is impractical, extract the invariant
it depends on into a pure function and unit-test *that*.
