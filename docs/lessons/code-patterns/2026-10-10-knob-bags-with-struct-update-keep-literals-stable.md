# Adding a field to a widely-constructed knobs struct

`StreamSealKnobs` had ~37 struct-literal construction sites (tests, consts).
Adding PITR fields would have touched all of them. Appending
`..Default::default()` after the existing fields (and copying the old shared
value into the new field where a test relied on the old coupling, e.g.
`pitr_seal_age` = `seal_age`) kept every test's behaviour unchanged.

Two traps: `..Default::default()` is not allowed in a `const` initializer
(spell the extra fields out), and a scripted rewrite also hits the
`impl Default` body itself (infinite recursion) — check that diff by hand.
When a setting must reach a control-only role that has no such struct, put it
on `AdminInfo` (reachable from every `ClientCtx`) rather than reading
`ctx.data()`, which panics on a control-only node.
