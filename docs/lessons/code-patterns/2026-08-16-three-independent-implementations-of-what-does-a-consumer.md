# Three independent implementations of "what does a consumer offset inherit across a split" each got it wrong in a different way, because no ADR ever named the shared invariant they were all separately solving.

**Three independent implementations of "what does a consumer offset
inherit across a split" each got it wrong in a different way, because no
ADR ever named the shared invariant they were all separately solving.**
The GSI drain's cursor, the Streams sealer's watermark, and the backfill
seeder's cursor (ADR 0041/0042/0043/0045) each re-derived split
inheritance on its own — yielding the #216 split-watermark data-loss bug,
a backfill-cursor fence rejection + same-named-index-recreation poisoning,
and the #220 split-seal duplication race, three genuinely different root
causes converging on the same missing rule: *a consumer offset crossing a
split must be inherited from a basis frozen at the cut, never re-derived
live from the parent's later state.* ADR 0046 (the tablet log model) names
this, and the other invariants like it, explicitly so a fourth offset
tracker gets checked against a stated rule instead of re-deriving its own.
**General rule**: when a second near-identical background loop/cursor/
offset mechanism is about to be built next to an existing one, look for
the shared invariant first — writing it down once is cheaper than paying
for its absence a third time. (ADR 0046, 2026-08-16.)
