# Regression cells that model a mechanism at the primitive level die with the mechanism — delete them WITH a tombstone naming where the replacement coverage lands, never silently

**Regression cells that model a mechanism at the primitive level die
with the mechanism — delete them WITH a tombstone naming where the
replacement coverage lands, never silently** (ADR 0050 Train B rung 2,
2026-08-16). F2b's immutable ranges deleted `narrow_scope`, which ~10
test cells across four binaries used to *model* the zero-copy split
(narrow + sibling-over-shared-rows) — including the #216/#220 data-loss
and duplication regressions, the highest-value cells in the streams
corpus. They could not be adapted (the seam they defend is structurally
unrepresentable now), so the honest move is deletion plus an in-file
tombstone stating (1) which cells died, (2) why they cannot be
re-expressed, (3) which surviving tests carry any still-live property,
and (4) which future rung rebuilds coverage on the successor mechanism.
A parked `#[ignore]` is NOT available for this class — ignored tests
still compile, and the API they call is gone; deleting without the
tombstone would make the coverage loss invisible to the very review
that must weigh it. Corollary: when a pivot disables a feature
mid-train, every corpus that exercised the feature's *defense stack*
(not just its happy path) needs an explicit disposition line in the
rung report — coverage debt is tracked like code debt.
