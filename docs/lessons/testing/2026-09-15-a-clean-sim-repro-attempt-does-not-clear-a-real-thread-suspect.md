# A clean `SimEnv` reproduction attempt does not clear a real-thread/real-network suspect — it only narrows where the bug can be

Found investigating issue #864's recurring S-07d (`animus-operator`) e2e
stall: the promoted ordinal-3 pod prints "ready" but its readiness probe
never returns 200, and `spec.controlNodes` growth stalls at 3 voters for the
full 120s guard. Issue #667's boot-time cluster check
(`RaftCore::begin_cluster_check`) was the prime suspect (it landed on this
stack and correlates with the stall rate jumping from 1/20 on `main` to
3/10 here).

## What the investigation established, and what it did not

Tracing the exact production ordering (`animus-operator::controller::
advance_control_growth`: the new pod boots with a `cluster.json` that
already lists it as a genesis-style voter of the FULL target group —
`ClusterConfig::control_ids()` includes `self` — *before* the operator ever
calls `POST /admin/control/member/add`) against `handle_cluster_probe_resp`'s
actual decision table shows the mechanism should resolve safely and quickly
for this ordering: the pre-growth peers' own real committed config does not
yet name the new node, which is the unambiguous, single-reply-decisive
"ordinary fresh voter" branch — no wait, no refusal.

A purpose-built `SimEnv` reproduction
(`crates/animus-control/tests/growth_join_self_inclusive_boot.rs`) confirmed
this empirically: a 3-voter established group, a 4th node started with a
self-inclusive config exactly matching the real `animusd` boot path, even
under an injected 20% per-message drop probability and elevated delay/jitter
on every link to/from the new node, converges to a healthy `leader_within`
state well inside a 60s budget, across ten seeds. The mechanism's *safety*
property (never a false permanent refusal) held in every configuration
tried.

**This does not clear the mechanism as a suspect.** `SimEnv` cannot model:
real OS thread scheduling staggering when each process's boot-time probe
task actually gets to run (the exact class of bug the sibling
"single-peer-evidence" lesson found via a real, not simulated,
`ProdEnv` regression); real TCP connection establishment latency/timeouts
on a freshly-created pod's first outbound dial; a real kubelet's own
probe/restart timing interacting with `animus-operator`'s 30s reconcile
cadence; or CNI-level packet behavior in a real `kind` cluster, which does
not obey `NetConfig`'s uniform delay/drop model. The mechanism's own commit
history already shows a REAL (not simulated) regression
(`wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving`) that a
full local `cargo test` run reported green on, and that only a targeted,
looped `ProdEnv` run surfaced 10/10.

## The generalizable rule

A `SimEnv` fault-injection reproduction that fails to reproduce a suspected
race is evidence the bug is not a pure ordering/logic defect the simulator's
message/task scheduling can express — it is not evidence the suspected
mechanism is innocent. Before clearing a suspect entirely, either build the
equivalent real-thread (`ProdEnv`) reproduction the sibling lesson describes,
or add the missing production-observable diagnostic (this investigation's
own fallback: `/admin/raft`'s new `cluster_check_pending`/`refused_as_voter`
fields, and `scripts/e2e-kind.sh`'s matching diagnostics-dump addition) so
the *next* real occurrence is decisive instead of re-litigating the same
"is it this mechanism" question from log silence.

See ADR 0009's 2026-09-15 amendments (issue #667) and issue #864 for the
full incident this was extracted from.
