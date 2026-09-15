# A bench with no cross-cluster catch-up cost is not proof the catch-up gate never blocks — sustained write load, not idle-cluster size, is what starves it (ADR 0062's cluster>RF amendment, 2026-09-01)

The rung-7 bench (ADR 0062) ran an idle-then-split cluster at RF=3 nodes
and correctly flagged that it could show nothing about the design's
central "decoupled movement from placement" claim, since no node outside
the parent's own replicas ever exists to recruit. This session finally ran
the wider-than-RF bench that rung 7 named as the follow-up — but the
result that actually mattered wasn't the RF ceiling, it was the workload
shape: an IDLE 4-node cluster (population only, then kickoff, then poll —
no writer) converged the pre-ADR-0062 F5-fused split's learner catch-up in
single-digit seconds even with a real 2,000-row/512KB dataset and a real
cross-node recruit. Add a CONTINUOUS paced writer to the same splitting
tablet — the shape any of this repo's own benches use to measure a write
blip — and the identical scenario failed to complete its split within a
5-minute budget, in 3 out of 3 runs, with `/admin/raftkv`'s own
`commit_index` observed pinned for the entire window while `log_len` grew
from ~3,700 to ~25,000 entries. The isolating test that found this: same
population, same growth, same kickoff, WITHOUT the interleaved writer —
converged in ~10s. The mechanism these two facts point at (not confirmed
further, out of scope for a bench-and-report task): Stage 1/2's "the fork
can only proceed once the recruited learner has caught up" gate targets a
moving snapshot, and a continuous write stream to the same tablet can make
that target move faster than a contended host's InstallSnapshot pipeline
can close the gap — a genuine liveness risk this repo's own benches had
never combined with a "recruit an off-parent replica" scenario before.
**The general lesson**: when characterizing a gate whose cost is "wait for
X to catch up," an idle-then-once test proves the gate exists and can
resolve; it does not prove the gate resolves under the load the mechanism
is actually meant to survive. Pair the "does it have somewhere to move
to" axis (cluster size vs. RF) with the "is the target standing still"
axis (idle vs. continuously written) — a bench that only varies one can
report a clean pass while the combination the design exists to handle is
untested.
