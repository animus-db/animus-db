# A per-hop timeout cap does not bound a serial fallback's total cost — capping each candidate's own attempt still lets the total scale with candidate count against a caller with far less headroom

**A per-hop timeout cap does not bound a serial fallback's total cost —
capping each candidate's own attempt still lets the total scale with
candidate count against a caller with far less headroom.** Issue #316/#585
capped `forward_to_tablet_leader`'s per-hop transport timeout at
`FORWARD_HOP_TIMEOUT` so one dead/slow candidate couldn't consume a whole
chase's budget, and `ClientCtx::propose_schema`'s own "no locally-known
leader" broadcast fallback (`schema.rs`) got the identical fix at the same
time. That closed the "one candidate eats everything" hole, but it left a
narrower one open: a **serial** loop over the capped candidates still costs
up to `(N-1) * FORWARD_HOP_TIMEOUT` for a single call whenever an unlucky
ordering tries an unreachable candidate before a reachable one.
`forward_to_tablet_leader` never hit this in practice because its own
caller budget (`CLIENT_TIMEOUT`, 10s) was sized with real headroom over a
handful of 2s hops. `propose_schema`'s callers were not: `dynamo.rs`'s
`CreateTable` gives its whole propose-then-commit-wait loop only
`SCHEMA_COMMIT_TIMEOUT` (5s), so on a mere 3-node cluster two serialized
capped hops (4s) already leave almost no room for the commit-wait that has
to follow — and the shortfall only grows with cluster size. This is
exactly issue #610's "first `CreateTable` after bootstrap" flake: `await_
bootstrap` (every `ProdEnv` cluster fixture's own barrier) guarantees only
that *some* node is control leader and every node has non-empty
membership, never that the node a client happens to reach already knows
*who* leads — so a first `CreateTable` landing on a node whose own raw
`leader()` is transiently `None` (issue #595's `start_pre_vote`-clears-
`leader_id` mechanism — a bare election-timer expiry, not a real
partition) pays the broadcast's full serialized worst case, worse under
exactly the runner load that makes each capped hop run closer to its cap
instead of failing fast.

**The generalizable rule**: when a retry/fallback loop iterates several
candidates and caps *each attempt's own* timeout, that cap bounds one
candidate's cost, not the loop's total — the total is still
`(candidates tried) * (per-candidate cap)` in the worst case. Before
trusting such a loop's fit inside a caller's own deadline, check the ratio
between that deadline and `(N-1) * per-candidate cap` for the largest `N`
the loop can plausibly see, not just whether a single hop is capped at
all. Two candidates that fit under one caller's generous budget
(`CLIENT_TIMEOUT`) can just as easily blow through a *different* caller's
tighter one (`SCHEMA_COMMIT_TIMEOUT`) built on the same underlying
mechanism — a shared retry primitive does not inherit "safe" from one
caller to the next just because the timeout is capped somewhere inside it.
Where the candidates carry no vouching signal to prefer one over another
(unlike a hint-chasing forward's `Hinted`/`Guessed` split), there is
nothing to lose by racing them **concurrently** instead of serially
(`futures::future::select_all` in a loop, resolving on the first success
and retrying the survivors on failure — a plain `join_all` is not
equivalent here, since it still blocks on a dead candidate's own cap after
a live one already answered) — this bounds the whole call to one
per-candidate cap regardless of how many candidates exist, rather than
merely bounding each one's own contribution to a total that still grows
with them.

**Testing note**: reproducing "a node hasn't learned the leader yet"
deterministically under `SimCluster` cannot rely on the moment right after
construction — `SimCluster::new` itself runs the sim for a full 2s of
settle time before returning, which is far more than enough for every
node to have received several heartbeats already, closing the exact
window a real `await_bootstrap` barrier leaves open. The reliable way to
force a specific follower's own `leader()` to `None` without disturbing
anything else is `leader_within_hysteresis.rs`'s own repro shape for issue
#595: partition that one follower from the (ideally pinned, via
`SimCluster::transfer_control_leadership_to`) leader only, for just past
one election window (`RaftNode::election_timeout() * 2` plus a small
margin) — the leader keeps its majority through the untouched third node
and never steps down, while the partitioned follower's own belief clears
via its own `start_pre_vote`. A *permanent* partition like this also means
that follower's own Raft log can never advance to observe a schema commit
that succeeded through a relay via the third node — a correct limitation,
not a bug — so a regression built this way should measure the operation
under test directly (here, `propose_schema`'s own return value and
elapsed time, via a raw hook bypassing the DynamoDB wire's own
confirmation-poll loop) rather than a full end-to-end wire call whose own
confirmation step needs a different kind of connectivity than the one
being exercised.
