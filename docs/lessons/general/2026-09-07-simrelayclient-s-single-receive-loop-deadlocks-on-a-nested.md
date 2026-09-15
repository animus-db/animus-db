# `SimRelayClient`'s single receive loop deadlocks on a nested outbound relay call from inside a forwarded request's own handler (ADR 0061 rung F, C-06 PR 3)

Found building `sim_cluster_dynamo_transact.rs`'s scenario (g): a
transaction coordinator stages both participants of a cross-table
transaction, never decides (simulating a crashed coordinator), the
coordinator node is then `SimCluster::crash`ed for good measure, and a
strong read of the participant key from a different, live node is
expected to trigger on-demand recovery (`confirm_or_push`/`txn_recover`)
and converge to the committed value. It never converged, at any seed,
within a 40-second virtual-time budget — every poll attempt returned the
identical `SimRelayClient::relay`-native timeout text
(`"sim relay: timed out waiting for a reply to req_id=N"`), unchanging
across the whole budget.

**Root cause, confirmed by direct code reading, not guessed at**:
`animus_node::sim_relay::SimRelayClient::serve_loop` is one task per node,
processing `env.recv_stream(RELAY_STREAM)` messages **strictly
sequentially** — an inbound `Request`'s installed handler is `.await`ed
**inline**, and the loop cannot receive the *next* message (including a
`Reply` to one of ITS OWN outbound calls) until that handler returns. A
handler that itself needs to make a nested outbound `relay()` call — which
happens exactly when a forwarded read hits a foreign, still-`Pending`
transactional intent whose recovery requires querying a *different*
tablet's leader (`ClientCtx::txn_status`/`txn_recover`/`txn_verify`, each
potentially its own forward) — sends its request and then polls a shared
`pending` map for a reply that only this same, currently-blocked
`serve_loop` task could ever stash there. A genuine self-deadlock,
resolved only by the nested call's own timeout: not a race, not a timing
sensitivity — reproduced identically at every seed tried, with a
same-cluster-state plain (non-transactional) forwarded `GetItem` on a
different key succeeding immediately in the same run, isolating the
failure to the nested-relay path specifically.

**Why nothing found this earlier**: every `SimCluster`-based scenario
built before this rung, across `sim_cluster_corpus.rs`/`sim_cluster_
dynamo_corpus.rs`/every D3/D4 module, only ever needed a forwarded
operation's own handler to answer locally once it reached the tablet's
real leader — a single hop. Multi-participant transaction recovery's
foreign-intent read path is the first mechanism in this codebase whose
own *server-side* handling can need a second hop, and it only becomes
reachable through a **forwarded** (not locally-served) read — which itself
needs a node crash to force re-election away from whichever node
originally happened to lead every tablet a test touches. A scenario that
never crashes a node, or that only ever reads a key whose intent (if any)
resolves locally, can never exercise this path — which is exactly why nine
scenarios across five other `sim_cluster_dynamo_*`/`sim_cluster_corpus`-
family modules that DO crash nodes never tripped over it: none of them
happened to combine a crash with a *cross-tablet* transactional intent a
survivor then has to forward a read through.

**Production is unaffected** — `AnimusdRelayClient` (the real,
`ProdEnv`-backed `RelayClient` implementor) has no analogous single-task
bottleneck: each inbound TCP connection on the intra port is its own
`tokio::spawn`ed task, so a nested outbound relay call from inside one
connection's handler blocks only that one task, never a shared receive
loop serving every other inbound request too. This is a `SimRelayClient`
-only, fixture-only limitation.

**Disposition**: kept as a `#[ignore]`d characterization test (both the
pinned-seed and `_over_seeds` variants), not fixed, not reworked to dodge
the mechanism (every crash-plus-cross-tablet-recovery scenario would hit
the identical gap), and not silently dropped — the maintainer standing
instruction on a real finding under a fixture. The fix (spawning each
inbound request's handler onto its own task, mirroring production's own
per-connection-task shape) belongs in `animus-node::sim_relay`, a shared
testing primitive every `SimCluster`-based module in this crate depends
on — out of scope for the PR that found it. **General lesson**: a
single-task, inline-awaiting receive loop is a fine simplification for a
request/reply protocol only as long as no handler ever needs to make its
OWN outbound call through the identical channel — the moment one does
(a server-side handler that is itself also a client of the same peer, or
of a third peer sharing the same per-node loop), the design needs either
a per-request task (production's own shape) or an explicit re-entrancy
story; a protocol's own docs should say which guarantee holds, since the
failure mode (a silent, seed-stable timeout with no distinguishing error)
gives no hint that recursion, not routing, is the actual cause.
