# An intermittent deficit that resists root-causing should get a permanent on-failure diagnostic landed as its own commit, *before* any fix attempt.

**An intermittent deficit that resists root-causing should get a permanent
on-failure diagnostic landed as its own commit, *before* any fix attempt.**
Weakening or retrying the assertion would destroy the evidence; a speculative
fix would be unfalsifiable. Land instead a dump that fires only on the
failure path and captures whatever distinguishes the competing hypotheses —
for a streams exactly-once deficit that is the missing ids, the shard each
delivered record arrived under, live vs retired tablets, and **the per-tablet
closed-chain length**. That last datum had already, once, redirected an
investigation out of the wrong subsystem entirely (the seal/`Freeze` path)
toward the right one (the open, never-sealed tail) — and it was recorded only
in a *comment* on a predecessor issue, not in the issue body that inherited
the investigation. Corollary: when an issue cites a prior issue or comment as
its evidentiary basis, fetch that comment; an issue body is not a complete
transcript of its own history. (#298,
`crates/animusd/tests/streams_e2e.rs`, 2026-08-20.)
