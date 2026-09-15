# Prefer a frozen, *generated* scenario corpus over a live-randomized test.

**Prefer a frozen, *generated* scenario corpus over a live-randomized test.**
Generate scenarios (cluster + workload + an explicit fault schedule) with
randomness for breadth, but **materialize them into a committed, named set** so
the suite is reproducible and a failure maps to a specific scenario — not a
one-off RNG state. Aim for structured/combinatorial coverage of the fault
matrix (fault type × target class × timing × workload); keep bug-finding
scenarios in the corpus forever as regressions. (Done: ADR 0014 / `animus-test`
`corpus.rs` — ~119 frozen, name-seeded scenarios over Accord.)
