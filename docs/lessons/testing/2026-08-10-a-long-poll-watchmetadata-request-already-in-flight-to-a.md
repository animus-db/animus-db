# A long-poll `WatchMetadata` request already in flight to a node at the exact moment that node is killed via `Node::shutdown()` does not fail over quickly — it can zombie-park for the full server-side timeout before replying with stale data, because `Node::shutdown()` doesn't (and, short of a larger refactor, can't cheaply) abort an already-spawned per-connection handler task.

**A long-poll `WatchMetadata` request already in flight to a node at the
exact moment that node is killed via `Node::shutdown()` does not fail over
quickly — it can zombie-park for the full server-side timeout before
replying with stale data, because `Node::shutdown()` doesn't (and, short of
a larger refactor, can't cheaply) abort an already-spawned per-connection
handler task.** `serve_clients`'s accept loop spawns each connection's
`handle_client` as a fire-and-forget `tokio::spawn` with no stored
`JoinHandle` — `shutdown()` aborts the accept loop itself and the node's two
internal `Env` role tasks (so no *new* connections are accepted and the
Raft drivers stop), but an already-accepted connection's handler task is
untracked and keeps running. Porting the ADR 0030 growth-node mirror onto
the ADR 0035 PR5 long-poll mechanism, this turned a previously-harmless gap
into a real, reproducible (3/3) test regression: `ClientCtx::watch_metadata`
parks on `select! { changed(last_seen), sleep(WATCH_METADATA_SERVER_TIMEOUT) }`
(8s) — once the driver that would `bump()` that watch is dead, `changed()`
can never resolve, so the zombie handler always falls through to the
`sleep` arm and eventually replies with whatever `effective_metadata()`/
`watch.latest()` the (now-frozen, but still-`Arc`-alive) `RaftCore` held at
the moment of death — a normal-looking `ClientResponse::Status`, just stale
by up to 8 seconds. The old fixed-200ms `Status` poll never had this
hazard (a plain `Status` request replies immediately, no server-side park,
so a dead node just fails the connect fast every ~200ms tick and the next
seed is tried in the same tick); the long-poll design's very mechanism (ask
a node to hold the connection until something changes) is what creates the
window. `tests/cluster_growth.rs::
dashboard_health_recovers_after_grown_cluster_loses_an_original_node` had a
**fixed 3-second sleep** after killing a node before asserting
leaderless/under-replicated counts — exactly the "fixed post-fault beat,
not a converged-or-timeout poll" anti-pattern this log already warns
against for eventual properties, and the ~8s worst case this hazard can now
add comfortably blew through it. Root-caused by adding temporary
`eprintln!` tracing to the watch loop (removed before commit) and observing
an 8.007s-elapsed reply logged right at the moment the test's own kill
should have been in flight. Fixed the test, not the mechanism: converted
the fixed sleep + one-shot assertion into a bounded poll (500ms interval,
30s budget) recomputing the health rollup each iteration — a real fix for
a real regression, but a fully general one, since **any** fixed-sleep
assertion following a node kill in this codebase's test suite is exposed to
the same class of latency once a long-poll mechanism is anywhere in the
path being waited on. **General checks:** (1) before shipping a long-poll
primitive, ask whether the harness's own "kill a node" mechanism actually
frees that node's in-flight server-side handlers, not just its listeners —
if not, every consumer of the long-poll inherits a worst-case-timeout
latency hazard on node death that a plain request/reply protocol never had;
(2) a fixed sleep-then-assert immediately after triggering a fault is
fragile independent of this specific bug — prefer a bounded poll whenever
the property being asserted is eventual, especially right after a fault
injection. (`animusd::watch_metadata`/`remote_metadata_watch_loop`;
`tests/cluster_growth.rs`.)
