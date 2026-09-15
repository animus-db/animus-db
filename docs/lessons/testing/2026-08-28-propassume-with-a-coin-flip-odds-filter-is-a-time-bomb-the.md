# `prop_assume!` with a coin-flip-odds filter is a time bomb the moment anyone raises the case count — generate the dependent value directly instead (ADR 0061 Phase A, `next_compaction_plan` trigger-floor properties).

**`prop_assume!` with a coin-flip-odds filter is a time bomb the moment
anyone raises the case count — generate the dependent value directly
instead (ADR 0061 Phase A, `next_compaction_plan` trigger-floor
properties).** Two property tests wrote `l0 in 0usize..20, trigger in
1usize..20` then `prop_assume!(l0 < trigger)` (and the mirror-image `>=`)
to test the trigger floor from both sides. That passed at proptest's
default case count (256) but aborted with "Too many global rejects" the
moment case count was bumped for a manual stress run
(`PROPTEST_CASES=3000`): proptest's global-reject ceiling is a **fixed
constant (1024)**, not scaled to the requested case count, so a ~50%
rejection rate needs roughly 2× that many attempts and blows the ceiling
well before reaching a few thousand successes — completely independent of
whether the property itself is fine. **General rule**: when a generated
input has a companion input that must be above/below/equal to it, generate
the dependent one directly off the first (`(1usize..20).prop_flat_map(|t|
(Just(t), 0..t))` for "below", `(Just(t), t..t + N)` for "at or above")
rather than generating both independently and filtering with
`prop_assume!` — it also produces a better test (every attempt is a real
case, not a discard) and stays correct if someone later runs the corpus at
a deeper case count, which per this file's own "test-scaling knobs" table
is exactly the kind of thing this repo's nightly/deep tiers do.
(`crates/animus-storage/src/lsm.rs::compaction_policy_tests`.)
