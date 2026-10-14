# Keep an independent copy of the old algorithm when generalising a function

Generalising a pure function (placement load: replica count -> `replicas/weight`)
carries a promise: the default case is byte-identical to before. The crate had a
`rebalance_step_reference` with a "pinned equivalence" test, but it shares the
helpers with the new code, so once the helpers were generalised the reference
was generalised too and the test could no longer detect a drift in the default
case. The guard is a **test-local, independent copy of the pre-change
algorithm** (here the count-based rebalance), compared over random topologies
with default and uniform weights.

Also: a mutation (`<` -> `<=` in the strict-improvement test) survived the random
suite, because oscillation needs an exact fill tie that random weights rarely
produce, and a first mutation attempt silently did nothing (`sed` pattern
broken by `cargo fmt` line-wrapping; check the mutated file differs before
trusting "tests still pass"). Pin tie boundaries with a deterministic example
(weights 100/200, counts 2/3: must not move).
