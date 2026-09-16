# A test can prove "this path never blocks" by how it drives the path.

**A test can prove "this path never blocks" by how it drives the path.**
`animus-cp-data`'s `tests/stale_read.rs` deliberately drives the ADR 0055
eventual reads with `block_on` instead of this crate's usual spawn-and-
`run_for` `drive` helper. Under `SimEnv` nothing advances the clock unless
the simulator is driven, so a read that ever grew an internal `env.sleep`
— a barrier, a ceiling wait, an intent chase — **hangs that test** instead
of quietly costing what the expensive path costs. The cheap path's defining
property is a budget, and a budget that nothing enforces is a comment; this
turns it into a test failure. Applicable anywhere a "must not block / must
not round-trip" invariant matters and would otherwise only be documented.
