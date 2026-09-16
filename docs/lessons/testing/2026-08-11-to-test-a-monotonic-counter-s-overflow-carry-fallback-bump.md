# To test a monotonic counter's overflow-carry fallback (bump the next field up, reset this one to 0) without looping to the boundary, prime state one step *below* the boundary and let the very next real operation supply the final `+1` — don't prime state *at* the boundary and expect it to still be there.

**To test a monotonic counter's overflow-carry fallback (bump the next field
up, reset this one to 0) without looping to the boundary, prime state one
step *below* the boundary and let the very next real operation supply the
final `+1` — don't prime state *at* the boundary and expect it to still be
there.** Testing `Hlc`'s logical-overflow carry (ADR 0018 §2 PR1,
`crates/animus-cp-data/src/hlc.rs`) by calling `witness` with
`remote.logical = 2^LOGICAL_BITS - 1` to "prime" the clock at the top of its
budget failed: `witness`'s own receive rule adds `+1` to the max of the two
logical values *as part of computing the primed value*, so priming at the
boundary itself triggered the overflow carry immediately, leaving the
"primed" state already at `(wall+1, 0)` instead of at the intended
boundary — the assertion on the primed value failed, not the assertion on
the follow-up overflow. Fix: prime one below the boundary
(`2^LOGICAL_BITS - 2`) so the priming call's own `+1` lands exactly at the
boundary with no overflow yet, then a second, separate call is the one that
overflows and is asserted on. General rule: when a test needs to "jump to
right before X happens," check whether the very setup step used to get
there is itself governed by the same increment rule being tested — if so,
it will overshoot by exactly one step past where the story expects it to be.
