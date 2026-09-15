# A shared list-append checker has no "weak read" flag — exclude an eventually-consistent observation from its graph and check it against convergence instead, don't teach the checker a new mode (ADR 0061 rung D2 PR 2)

`sim_cluster_dynamo_corpus.rs` is the first corpus to drive `ConsistentRead:
false` (ADR 0055's replica-local, un-barriered read path) through
`animus_test::check::check_cycles` — every earlier corpus in this workspace
that reuses that same shared, cross-crate checker (`raftkv_linearizable.rs`,
`sim_cluster_corpus.rs`, `txn_serializable.rs`) only ever issues
linearizable reads, so the question never came up. `check_cycles`'s
`recover` requires every **observed** read of a key to be a prefix of that
key's one recovered (longest-observed) append order — sound for a
linearizable read (a forked/stale strong read is exactly the anomaly the
checker exists to catch), but a legitimately-stale *weak* read is not the
same defect class: feeding one into the same graph risks a false-positive
"divergence" violation the instant a lagging replica answers during a fault
window, purely an artifact of mixing a read consistency the model was never
built to represent into a model that has no way to say "this one's allowed
to lag." Teaching the shared checker a new "weak read" flag was rejected —
`check_cycles`/`check_durability`/`check_convergence` are used by several
crates' worth of corpora with a stable, well-understood contract, and
widening it for one caller's read-consistency dimension risks changing
behavior for every other caller silently. The fix: exclude the weak
observation from the shared history entirely and check it directly against
the scenario's own converged final state instead (`observed.starts_with`
the converged list) — sound specifically because single-writer-per-key
already guarantees a total order per key, so any replica's local snapshot
at any time is *some* prefix of that one order, never a value out of order
or one that never committed. This is the identical shape
`sim_cluster_corpus.rs` already used for `delete` (a tombstoning op the
list-append model can't represent either) — **a workload dimension the
shared checker's model doesn't fit gets its own direct check outside that
checker, not a checker extension.** General lesson: before adding a new
mode/flag to a shared, multi-caller correctness oracle to accommodate one
new corpus's workload shape, ask whether the new dimension can instead be
excluded from the shared model and verified by a purpose-built check
against the same converged state the shared checks already establish —
usually cheaper, and it can't regress every other caller's contract.
