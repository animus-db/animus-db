# A readiness/liveness signal read straight off a consensus-internal "I currently believe I have a leader" flag inherits that flag's hair-trigger semantics (issue #595)

`animusd::admin::health()` — `/admin/health`, the Kubernetes readiness
probe (ADR 0060) — used to be exactly `RaftCore::leader().is_some()`.
`leader_id` is cleared by `start_pre_vote` the instant a follower's OWN
election timer lapses (ADR 0009 pre-vote), before any pre-vote round is
even answered — correct for consensus (pre-vote must not trust a stale
belief when deciding whether to grant a vote or pick a relay target: a
briefly-stalled node could otherwise disrupt a healthy leader), but it
means *any* boolean consumer of `leader()` inherits a false-negative window
on **every** transient one-sided delay of one election timeout (150ms
default) or more — a single scheduling stall on a pod, a GC pause, one
dropped heartbeat — even while the real cluster leader is fully healthy and
heartbeating every other replica the whole time. Reproduced directly at the
`RaftCore` level: partition a follower from an otherwise-healthy leader
(the leader keeps its majority via the other follower and never steps
down, term unchanged) and the partitioned follower's own `leader()` is
`None` within one election timeout, recovering only on the next heartbeat
after healing.

**The general lesson: safety and operability are different jobs, and a
consensus core's internal belief is tuned for the former.** A pure
`RaftCore`/state-machine's own "do I currently trust a leader" flag is
deliberately hair-triggered — it has to be, to keep a stalled node from
disrupting a healthy cluster. A health/readiness probe wants the opposite
property: "has this replica had genuine leader contact recently enough
that treating it as healthy is still honest" — which needs its own
hysteresis, entirely separate from and never read by the safety-critical
decision. Fixed by adding a second, purely observational field
(`RaftCore::last_leader_contact: Option<(NodeId, Nanos)>`) set only at a
genuine leader contact (`handle_append_entries`/`handle_install_snapshot`'s
valid-leader-for-this-term path, or `become_leader` recording itself) and
cleared only on a real higher-term step-down (never by `start_pre_vote`/
`start_election`'s own local-timeout-driven clear of `leader_id`, which
carries no evidence the leader actually failed) — `leader_within(now,
max_age)` reads it with a caller-chosen grace window, documented in the
field's own doc comment as **never** to be read by any election/pre-vote/
safety/replication decision. `admin::health()` is the sole consumer,
gating on `3 × election_timeout()` — enough slack for a couple of missed
heartbeats, still tight enough that a genuinely leaderless node degrades to
`503` in about a second rather than tens of seconds.

**A narrower, related lesson this fix had to get right**: not every
consumer of a stale-tolerant leader belief should get the hysteresis.
`ClientCtx::propose_schema`'s leader-hop uses the full `CLIENT_TIMEOUT`
(10s) for that one hop, with a `FORWARD_HOP_TIMEOUT`-capped (2s, issue
#585) broadcast fallback if it's wrong — handing it a possibly-stale
`leader_within` belief instead of the immediately-self-correcting raw one
would risk spending that whole hop budget on a node that isn't actually
reachable as leader, worse under `dynamo.rs`'s 5s `SCHEMA_COMMIT_TIMEOUT`
than falling through to the broadcast promptly. Only the *operational*
consumer (a probe that degrades gracefully and gets retried by its own
caller regardless) benefits from hysteresis; a *correctness-path* consumer
that pays a real timeout cost for guessing wrong should keep reading the
raw, self-correcting belief. When adding a hysteresis-bearing accessor
alongside a raw one, audit every existing caller of the raw accessor before
assuming the new one is strictly better — it usually isn't, for a caller
that already has its own bounded-cost fallback.

Regression: `animus-control/tests/leader_within_hysteresis.rs` (a one-sided
partition inside the grace, then held past it, across 12 seeds — proving
both the false-negative survival and the false-positive bound). See ADR
0020's 2026-09-04 amendment and ADR 0009/0012 for the full mechanism.
