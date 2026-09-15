# A shared `MetricsHandle::noop()` is silently harmless for a counter but corrupts a gauge outright — `SimCluster`'s own control `RaftNode`s needed `start_with_metrics`, not the identical-looking `start` (ADR 0061 rung H, C-08 PR 5)

The "`SimEnv::metrics()` is `MetricsHandle::noop()` by design" fact is
already documented (this log's own "Observe a multi-group-per-node metric
under `SimEnv`..." entry, ADR 0044 phase 2 C-02 PR 1) — `noop()` is one
process-wide `static` shared sink, and `RaftNode`/`RaftKvNode::
start_with_metrics` exist precisely so a caller who cares can hand in a
real, private one instead of the `env.metrics()` default. What that
earlier entry didn't need to spell out, because its own investigation
never depended on it: **a shared sink degrades gracefully for a plain
counter (`incr`) but not at all for a gauge (`set_leader`/`leader_gauge`,
last-write-wins).** `SimCluster::new`'s own control-plane construction
used the plain `RaftNode::start(env, ids, engine)` — the identical-looking
sibling that silently defaults its metrics to `env.metrics()` — for every
node, for the entire lifetime of this rung (D1 through G). Every node's
control `RaftNode` therefore shared the one static `is_leader` gauge, not
just with every OTHER node in the same cluster but with every OTHER
`SimCluster` **in the same test binary process**: the first control raft
anywhere to become leader stamped `is_leader: 1` onto that one shared
sink permanently (until some unrelated raft's own later role transition
happened to flip it back), regardless of which node's `RaftCore` a caller
was actually asking about. A summed *counter* under the same sharing is
merely inflated — still directionally correct, silently wrong only if a
test asserts an exact value (and none did, since nothing needs one) — so
this went unnoticed through five rungs' worth of `SimCluster`-based
corpora, all of which read counters (`Metric::ALL`) or a genuinely
per-node struct (`ctx.backup_janitor_progress`/`ctx.control.is_leader()`
itself, a direct `RaftCore` role check untouched by any of this), never
the metrics-exported `is_leader` gauge specifically. It surfaced only once
ADR 0061 rung H, C-08 PR 5's own `sim_cluster_admin.rs::admin_metrics_
surfaces_control_plane_counters` became the first `SimCluster` test ever
to read `GET /admin/metrics`'s `is_leader` field — every node in a fresh,
otherwise-healthy 3-node cluster reported `is_leader: 1`, at every seed,
reproducibly.

**Fix**: build one private `MetricsHandle::recording()` per node before
constructing that node's control `RaftNode`, and call `start_with_metrics`
(not `start`) with it — the exact API `start_with_metrics` was already
built for, just never actually reached from this particular construction
site. `DataRole::raftkv_metrics` reuses the SAME per-node handle, matching
production's own "a combined node's control Raft and CP group record into
the same sink" contract (`ClientCtx::metrics_json`'s own doc,
`is_same_sink`'s skip-if-identical guard) rather than adding a second,
merely-uncontaminated-but-still-separate sink.

**General form**: `X::start`/`X::start_with_metrics`-shaped API pairs (or
any "convenience default vs. explicit override" pair) read as
interchangeable at a call site — same arguments minus one, same return
type — which is exactly what makes picking the wrong one invisible in
review and in every test that only ever checks a counter's *direction*
(went up, stayed flat) rather than a gauge's *identity* (which node, right
now). Before trusting a `SimEnv`-based fixture's own `/admin/metrics`- or
`MetricsHandle`-shaped observability for anything beyond "some counter
incremented," check which constructor built each `RaftNode`/`RaftKvNode`
it hosts — `start`'s own default silently opts every caller into one
process-wide shared sink, correct-looking for counters and wrong for any
gauge, until something finally reads the gauge.
