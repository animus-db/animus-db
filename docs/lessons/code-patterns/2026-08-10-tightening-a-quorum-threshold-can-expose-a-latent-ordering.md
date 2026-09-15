# Tightening a quorum/threshold can *expose* a latent ordering bug elsewhere — re-derive the safe bound from first principles and check the whole pipeline.

**Tightening a quorum/threshold can *expose* a latent ordering bug elsewhere —
re-derive the safe bound from first principles and check the whole pipeline.**
Making Accord's fast quorum precise (`N-1`, down from `ceil(3N/4)`) let two
*conflicting* txns legitimately commit at the same `logical` timestamp (ordered
by the node tiebreak); the downstream MVCC `version` was `logical` alone, so
per-key LWW kept the wrong (first-applied) winner. Encode the *full* order
(`(logical<<16)|node`) wherever a total order is collapsed to one `u64`. Also:
pair a quorum bound with its *recovery* procedure — the smaller "optimized"
Accord/EPaxos fast quorum needs the full witness-recovery; the simplified
slow-path recovery requires the larger `N-1` bound.
