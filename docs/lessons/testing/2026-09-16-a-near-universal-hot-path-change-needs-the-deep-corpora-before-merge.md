# A behavior change on a near-universal hot path needs the deep corpora before merge, not just after

Found root-causing issue #945 (`corpus-deep` red on `inplace_split_
reconciler_corpus` and `heartbeat_batch_corpus`) to PR #907 (issue #900's
tablet-group boot-time cluster check).

## What happened

PR #907 added `RaftCore::begin_cluster_check` to every fresh `RaftKvNode`
boot except the one replica per split child that sets
`campaign_immediately`. That carve-out missed that a real split hosts
*several* replicas of the same child, only one of which campaigns — the
others still ran the check, and `handle_request_vote` refuses a real vote
while a check is pending, so the campaigning replica's "instant" win was
gated behind a probe round trip on every single split. The PR's own gates
— `ANIMUS_INPLACE_SPLIT_SEEDS=10`, `ANIMUS_HEARTBEAT_SEEDS` at its
per-push default — stayed green. The regression only bites when a
scenario's simulated link latency for the extra probe round trip is large
enough relative to the scenario's own tight budget (20ms for the
immediate-campaign fast path, a `SETTLE` window for the heartbeat corpus)
— true for some of the 40 nightly seeds per cell, not for the first 1-10.

## The generalizable rule

A change to a path every (or nearly every) `SimEnv` node executes — a boot
sequence, a per-tick hot loop, anything "runs once per fresh group/node/
replica" — can introduce a regression whose probability of tripping a given
fixed-seed test scales with how many seeds that test samples, not with
whether the change is correct in the common case. The per-push tier (depth
1, sometimes 10) is not a substitute for the nightly deep corpus (depth 40)
for exactly this class of change: it will pass the PR's own gate and still
go red the next night. Two mitigations, both cheap relative to a red
nightly + a bisect:

1. **Before merging a change to a near-universal hot path, run the
   affected corpora at their nightly depth locally** (`ANIMUS_*_SEEDS=40`
   for whichever corpora exercise fresh replica/group formation — grep
   `docs/lessons/` and the crate `CLAUDE.md` for "boot path" to find them),
   not just the per-push default. This is slower but still minutes, not
   hours, for these corpora specifically.
2. **When a change exempts one specific caller/flag from a new safety
   gate "because it's proven safe by construction,"** audit every other
   party to the *same* operation for the identical premise before assuming
   one flag's exemption is complete — see the code-patterns lesson on this
   same issue for the specific pattern (a single-node exemption on a
   multi-node operation).

See `crates/animus-cp-data/CLAUDE.md`'s issue #945 note and ADR 0017's
2026-09-16 amendment for the concrete case.
