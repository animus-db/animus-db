# A fix that clearly improves the mechanism does not guarantee the same end-to-end pass rate — report both, don't let one imply the other (issues #532/#537)

Two independent, well-reasoned fixes to a shared Raft core (an
`AppendEntries` batch cap, and deferring compaction while a peer's
snapshot transfer is in flight) produced an unambiguous, large
improvement in a controlled `SimEnv` comparison — a learner that used to
plateau at a fixed `match_index` for an entire run now cleared 10x+ more
of an identical backlog before the same write window ended, and a direct
before/after toggle of each fix (batch cap alone, compaction-defer alone,
both, neither) showed exactly the additive contribution each one made.
That same fix, validated against the real, unmodified `ProdEnv` bench
(`animusd/tests/cluster_gt_rf_split_bench.rs`) 3 times, converged fully in
only 1 of 3 runs — an unchanged ratio from this same bench's own pre-fix
baseline, despite the successful run itself completing in a comparable
time to before. **The lesson**: a mechanism-level improvement (proven via
a controlled, isolatable comparison) and an end-to-end pass-rate
improvement (proven via repeated runs of the real, unmodified
reproduction) are two different claims, and evidence for one is not
evidence for the other — report both numbers honestly rather than letting
a convincing mechanism-level story imply an end-to-end result that the
actual repeated runs did not show. A residual, unidentified contributing
factor is flagged as an open follow-up rather than smoothed into "fixed."
