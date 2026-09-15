# A `spawn_task`'d background disk-I/O task must be gracefully joined via its own `is_stopped()`-style contract before `Env`/runtime teardown — an outer `AbortHandle::abort()` is not enough, because it races the runtime's own blocking-pool teardown and can surface as a raw runtime-internal panic instead of a clean stop.

**A `spawn_task`'d background disk-I/O task must be gracefully joined via its
own `is_stopped()`-style contract before `Env`/runtime teardown — an outer
`AbortHandle::abort()` is not enough, because it races the runtime's own
blocking-pool teardown and can surface as a raw runtime-internal panic
instead of a clean stop.** `animusd`'s Ctrl-C path (`shutdown_graceful`)
flushed the control-plane WAL, then called `Node::shutdown()`, which
`ProdEnv::shutdown()`-aborts every task the node's two internal envs own —
including the CP-data apply task (`animus-cp-data`'s `apply_and_compact`).
`RaftKvNode::shutdown()`/`CpGroup::shutdown()` already document "a graceful
driver halt, not a kill" (a flag observed *between* full apply passes), and
the drop-table GC path (`cp_gc_tablet`) already uses the correct
shutdown-then-poll-`is_stopped()` pattern before touching files — but
process-level teardown skipped it and went straight to the hard abort. If
the apply task was mid-`storage.merge(..).await` (a `tokio::fs` op, which
internally runs on tokio's blocking thread pool), aborting the task while
its blocking op was still in flight surfaced as a `tokio`-internal panic —
`Backend("background task failed")` / `Backend("task was cancelled")` — on
every real `animusd` shutdown, harmless to durability (an un-acked write
just isn't durable yet) but a noisy, uncontrolled crash instead of a clean
exit. Fixed by adding `ClusterEdgeState::shutdown_all_cp_groups` (snapshot
the registered handles out of the lock, call `.shutdown()` on each, then
poll `.is_stopped()` bounded by `CP_GC_STOP_TIMEOUT` — the exact
`cp_gc_tablet` shape) and calling it from `shutdown_graceful` before the
hard-abort `shutdown()`. **General check: when a component documents its own
graceful-stop contract, make sure every caller that tears it down — not just
the one call site the contract was originally written for — actually uses
it**, especially a process-exit path that looks unconditionally safe because
"the process is exiting anyway." [`cp_gc_tablet` itself is gone as of ADR
0031 PR4 — its shutdown-then-poll-`is_stopped()` shape now lives in
`animus_cp_data::host::Reconciler`'s own `Release`/`Reclaim` teardown; the
lesson and `ClusterEdgeState::shutdown_all_cp_groups` this entry added are
otherwise unaffected and still current.]
