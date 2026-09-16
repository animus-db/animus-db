# `animusd/tests/split_cluster.rs`'s two failures (`split_and_merge_over_a_ split_deployment`, `decommission_racing_a_tablet_split_converges_with_no_ data_loss`) were both 100%-deterministic in a genuine multi-process control-only + data-only deployment, tracing to one design gap in the ADR 0018 §2 amendment's range-seal handoff (`animus-cp-data/src/host.rs`).

**`animusd/tests/split_cluster.rs`'s two failures (`split_and_merge_over_a_
split_deployment`, `decommission_racing_a_tablet_split_converges_with_no_
data_loss`) were both 100%-deterministic in a genuine multi-process
control-only + data-only deployment, tracing to one design gap in the ADR
0018 §2 amendment's range-seal handoff (`animus-cp-data/src/host.rs`).**
The seal proposal (`propose_seal`) used to be a **one-shot side effect
bundled into the same tick as the local, irreversible action it was
supposed to precede** — `NarrowScope`'s local scope mutation (leader-gated
propose inline) or `Absorb`'s teardown (leader-gated propose, then a
drain-wait gated only on "nothing pending locally"). Two related bugs
followed from that one shape:
1. **A one-shot side effect hung off a self-erasing trigger never
   retries — make the trigger a persistent, re-derived condition
   instead.** `NarrowScope`'s local `narrow_scope()` call ran
   unconditionally, regardless of leadership; the paired seal-propose call
   only fired if this same replica *also* happened to be leader at that
   exact tick. Since narrowing immediately makes the triggering mismatch
   (`t.range != current`) vanish on that replica, a replica that narrowed
   while a *follower* and was only *later* promoted to leader had no
   second chance — the condition that would have re-triggered the attempt
   was already gone, permanently, even though the actual precondition
   ("does a covering seal exist yet") hadn't changed at all. Fixed by
   computing the seal-pending condition fresh every tick
   (`TabletFacts::pending_seals`, an async engine scan independent of
   local scope/teardown state) and turning it into its own action
   (`HostAction::ProposeSeal`), so whichever replica eventually holds
   leadership gets its chance regardless of when leadership shuffles
   relative to the local mutation.
2. **An irreversible local action ordered after a distributed action must
   gate on committed evidence of that action, not on local progress.**
   `Absorb`'s teardown considered itself free to tear down (delete the
   only local copy of the group's Raft WAL) once "nothing pending
   locally" — a check a quiescent follower satisfies trivially *before
   the leader has even proposed the seal*. A fast follower could
   therefore destroy its own voter before the seal ever committed,
   dropping the group below quorum and permanently stranding the
   leader's own, now-orphaned proposal (accepted locally, never able to
   commit again). Fixed by additionally requiring a **locally-observed
   committed** seal (the same `seal_covers` engine scan) before a replica
   may tear down — not "distributed progress inferred from local state,"
   the actual distributed fact itself. This gate is self-supporting, not
   a deadlock: requiring every absorbed replica to stay up until it
   observes the seal is exactly what keeps the quorum needed to commit
   that seal alive in the first place; a genuinely quorum-dead group (an
   unrelated double failure) correctly stalls loudly instead of tearing
   down early — the same correctness-over-liveness call this system makes
   everywhere else a durability/visibility gate is at stake.
**Diagnostic method**: reproduced deterministically (3/3 identical
failures, not contention-gated) in isolation first, then added temporary
`eprintln!`s directly at the two decision points (`Reconciler::teardown`'s
Absorb `fully_drained` check and propose-seal call; `gather_facts`'s
`parent_seal_observed` computation and `NarrowScope`'s propose-seal call)
and re-ran each failing test once — the trace immediately showed a
follower reaching "fully drained" with `commit == log_end` (nothing to
wait for) *before* any "leader proposing seal" line ever printed for test
1, and zero "leader proposing narrow-seal" lines across the entire ~150-
tick run for test 2 — the same per-call-site `eprintln!` idiom the prior
HLC witnessing-chain bug's entry above used, applied to a completely
different subsystem. Confirmed the fix by reverting just the source file
(keeping the new tests) and re-running: both new `reconciler_corpus.rs`
scenarios fail against the unfixed code, pass against the fixed code.
Regression: `animusd/tests/split_cluster.rs`'s original pair (the
real-world acceptance) plus two new deterministic `SimEnv` scenarios in
`animus-cp-data/tests/reconciler_corpus.rs` that force the exact
interleaving by hand (`absorb_follower_waits_for_committed_seal_before_
tearing_down`, `narrow_seal_survives_a_late_promotion_after_narrowing_
as_a_follower`) — see `animus-cp-data/CLAUDE.md`'s range-seal and
Absorb-drain invariant entries, and ADR 0018's PR2 amendment corrective
note #2, for the full mechanism.
