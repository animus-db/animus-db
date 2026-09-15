# A bench whose cluster size equals RF cannot exercise a design change whose whole benefit is "fewer/cheaper cross-node catch-ups" (ADR 0062 rung 7, bench re-validation)

Re-running `animusd/tests/inplace_split_bench.rs` (byte-for-byte the same
3-node/RF-3 workload ADR 0058's own rung-8 bench used) against fork-first
(this ADR) and, side by side on the same host/session, against a worktree
checked out at the commit immediately preceding it (ADR 0058's Stage 1/2
F5-learner-recruitment in-place design) found **no** improvement in
fork-to-Active wall clock or write blip — if anything, fork-first measured
slightly *worse* on both (median 1.347s vs. 1.149s wall clock; 616ms vs.
419ms write blip; N=3 each, one shared/contended host, load average ≈2.0).
This is not a regression finding: at N=3 nodes with RF=3, `select_replicas`/
`select_replicas_balanced` has no candidate node outside the parent's own
current replica set to ever recruit, so the pre-ADR-0062 baseline's own
Stage 1/2 learner-recruitment window is recruiting learners that are
*already* existing, already-caught-up voters — it never pays the
cross-cluster `InstallSnapshot` cost ADR 0062's own Context section names
as F5's actual cost driver, and the bench that used to prove ADR 0050's
own convergence-predicate-starvation problem for a *different* mechanism
did not, and structurally could not, prove or disprove this one's central
claim (see the ADR's rung-7 amendment for the full numbers and the honest
"neither confirms nor refutes" verdict).

**General lesson**: before trusting an existing bench to validate a new
design just because its own Testing plan named that bench as "the one that
should be pointed at this before it's trusted," check whether the bench's
own fixed parameters (here: node count == replication factor) structurally
foreclose the scenario the new design's benefit depends on — a bench
proven sound for one mechanism's regression (ADR 0050's cutover-starvation
problem, which ADR 0058's rung-8 bench genuinely does exercise at N=3) is
not automatically sound for a *different* mechanism's improvement claim
(ADR 0062's cross-cluster-catch-up-avoidance claim, which needs a cluster
*wider than RF* to ever engage the code path the claim is about). A
"re-run the existing bench and publish the numbers" instruction is not a
substitute for asking whether that bench can see the thing being measured;
report the mismatch plainly rather than let a flat or adverse number pass
silently as if it settled the question either way.
