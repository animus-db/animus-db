# A live-state-matches-target check can pass for a legitimately not-yet-converged tablet, twice, under different conditions — the same symptom recurred under continuous write load with no clear trigger (ADR 0062's cluster>RF amendment, 2026-09-01)

Rung 6 (this same log, "A 'just compare live state to the target'
convergence check races the very proposer that sets the target") already
fixed one instance of this class: the completion loop's own
`config() == target` check needed a settle window because a fork-first
child is born already satisfying it trivially. The cluster>RF bench
surfaced what looks like a second, unrelated recurrence of the same
*symptom* under real load: in 2 of 3 runs, one of two concurrently
converging children sat with its live `replicas` already **exactly**
matching its own `split_placing` target for the entire remainder of a
240-second poll budget, with `done` never flipping to `true` — while its
sibling child (same split, same tick cadence) converged normally. This
session did not root-cause it (continuous write load plus two children
converging simultaneously plus real host contention makes the search
space large, and root-causing a product defect is out of scope for a
bench-and-report task) — it is flagged here, with a reproduction recipe
(`tests/cluster_gt_rf_split_bench.rs`, grow-by-one-lower-sorting-node +
kickoff + a continuous paced writer, 3-node→4-node RF=3), as a genuine
open finding rather than smoothed into "the ADR's claim is confirmed."
**The lesson to carry forward**: a settle-window fix for one instance of
"live state transiently equals target" does not prove every instance of
that class is closed — re-run the SAME symptom check whenever a new load
shape (concurrent siblings, sustained writes, real contention) exercises
the mechanism for the first time, rather than assuming the rung-6 fix
covers it structurally.
