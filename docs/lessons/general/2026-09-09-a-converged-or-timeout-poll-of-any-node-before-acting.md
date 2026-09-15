# A converged-or-timeout poll of "any node" before acting through "whichever node leads now" races `Metadata`'s own per-node apply lag, even with no fault injected (2026-09-09, issue #819)

`crates/animusd/tests/seed_join_allocated.rs::
ephemeral_identity_restart_gets_a_new_id_old_left_down_and_prunable` polled
`member_status(&core_nodes, &old_id)` — `nodes.iter().find_map(..)`, which
resolves to whichever core node is *first* in the slice with an entry for
`old_id` (in practice always `core_nodes[0]`, since every core node
registers one early in the test) — until it read back `NodeStatus::Down`,
then immediately POSTed `/admin/member/remove` to `core_admin[leader_index(
&core_nodes)]`: whichever node's own `RaftNode::is_leader()` currently
answers `true`, not necessarily `core_nodes[0]`. CI (job 34349214827)
failed with `left == right` on `status == 409` (`"node ... is not drained:
status is Active; drain it first"`) even though the immediately preceding
poll had already observed `Down`.

Root cause: `Metadata` is `StateMachine::DRIVER_APPLIED` (ADR 0038) — each
node's own async apply task publishes its cache independently
(`RaftNode::metadata`, `crates/animus-control/src/node.rs:682`, whose own
doc states this outright: "may briefly read a fresher node's `Metadata::
default()` before the apply task's first rebuild completes; a caller that
needs read-your-writes should confirm via `metadata_watch()`... instead of
assuming this call alone is synchronized with a just-issued `propose`").
`admin_remove_member` (`crates/animusd/src/lib.rs:10448`) reads `self.
control.metadata_cached()` on whichever node *receives* the HTTP request —
its own, possibly-lagging, apply-task cache — not a cluster-wide or
leader-synchronized view. So `core_nodes[0]` can apply the committed
`UpsertMember{Down}` transition before the node that happens to be control
leader at POST time has applied the very same commit — a genuine, everyday
skew this design accepts by construction (the async apply task is
deliberately decoupled from the sync Raft core so the driver never blocks
on apply), not a Down→Active flip from a stale heartbeat. (`Node::
shutdown()`'s abrupt task-abort — `ProdEnv::send_stream` spawns each send
onto its own task, `crates/animus-env/src/prod.rs:608`, so `abort()` can in
principle race an already-launched heartbeat write — was investigated and
ruled out for *this* failure: several seconds of real time (a second
node's whole join + its own `await_active` poll) elapse between `first.
shutdown()` and the `Down` poll, far past `send_stream`'s own 2s
`SEND_TIMEOUT` and `DETECT_TIMEOUT`'s 500ms, so no in-flight send from the
killed process could plausibly still be arriving at that point.)

This is the standing "poll the node you're about to act through, not the
node that made the earlier state change" rule (this file's own
`S-03 PR 2`/`RestoreTableFromBackup` entry, and the root `CLAUDE.md`'s
matching note) in a shape that needs **no fault injection and no leader
change** to trigger — ordinary per-node `DRIVER_APPLIED` apply-task timing
skew, amplified by a slow/contended CI runner, is enough on its own.
**Fixed in the test**: compute `leader_index` once, then poll *that same
node's* own `metadata()` for `Down` before ever POSTing to it — never an
arbitrary node's view followed by an action against a different one.
Reproduction: 85 real-thread runs on a 4-vCPU sandbox (30 unloaded, 25
under 6-way CPU pressure via `taskset -c 0,1`, 30 more as two concurrent
copies under the same pressure) never reproduced the original failure —
this sandbox's apply task evidently never lags enough, and the fixture's
`bring_up` reliably elects `core_nodes[0]`, which is exactly the node
`member_status` already reads, masking the bug locally; the mechanism was
confirmed by direct code inspection (the two divergent read sites named
above) rather than by a local repro. 40/40 runs green after the fix.
