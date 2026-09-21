# A fire-and-forget spawn inside a tracked task is untracked (issue #1010)

Tracking a long-lived task's own `JoinHandle`/`AbortHandle` so a shutdown
path can abort it says nothing about work that task itself spawns with a
bare `tokio::spawn` and discards the handle for. `animusd`'s `serve_requests`
— the accept loop shared by the client and intra listeners — was tracked
correctly: its own task lived in `Node.tasks`, and `Node::shutdown_and_wait`
aborted it and waited for the abort to take effect, exactly per this
repo's own "`abort()` is a request, not a guarantee" lesson. But each
*accepted connection*'s handler was spawned with a plain `tokio::spawn(..)`
inside that loop, with the resulting `JoinHandle` immediately dropped. A
dropped `JoinHandle` does not abort its task — it only stops being able to
observe or cancel it — so every handler kept running as its own,
completely untracked task on the same runtime, regardless of what happened
to the accept loop that spawned it. A connection accepted a moment before
`Node::shutdown_and_wait()` was called would survive it: aborting the
accept-loop task freed the *listening* port (the property every existing
test asserted), but the live connection's handler — holding its own
`ClientCtx`, and a socket the "shut down" node's caller now believes is
gone — kept running past the point its owning node was supposedly torn
down.

**The general rule: tracking a task's own handle only covers that task —
it says nothing about anything that task spawns and does not itself
track.** Before trusting "the parent task is tracked, so shutdown covers
it" for any spawn-inside-a-loop shape, check whether the loop hands off
work to a *further* spawn, and if so, ask where *that* handle goes. A
`JoinHandle`/`AbortHandle` dropped without being aborted or awaited is not
cleanup, it is amnesia — the task it named keeps running exactly as if
nothing had happened.

**The fix: let the parent task own its children in a `tokio::task::
JoinSet`, so the *drop* of the parent's own future — not a second,
independently-maintained collection of handles — is what cascades the
abort.** `serve_requests` now holds a `JoinSet<()>` for its own listener's
handlers, spawns into it instead of into a bare `tokio::spawn`, and
`select!`s the listener's `accept()` against reaping finished handlers off
the set (logging a genuine panic at `error`, a mere cancellation at most
`debug`). Because the `JoinSet` is a local of the accept-loop's own
future, aborting that future (exactly what `Node::shutdown_and_wait`
already did, unchanged) drops the `JoinSet`, and a `JoinSet`'s `Drop`
aborts every task still registered in it. No new collection needed
threading through `Node`'s own shutdown path at all — the existing
"abort the accept-loop task" mechanism now transitively covers every
handler it ever spawned, for free, by construction.

**A process-global (or env-scoped) abort registry was considered and
rejected as the fix here.** `ProdEnv`'s own `Inner.tasks: Mutex<Vec<
AbortHandle>>` (`animus-env/src/prod.rs`) is exactly this shape, and it
already exists — routing `serve_requests`'s per-connection spawn through
`ctx.env.spawn_task` instead would have been the smallest possible diff.
It is the wrong home anyway: that registry is **append-only** for the
whole lifetime of the env, never pruned as tasks finish, because its
handful of existing occupants are long-lived per-node driver loops (the
Raft driver, the tablet-host reconciler, …) — one entry each, for the
life of the process. A per-*connection* handler is not that shape at all:
a long-lived server can accept many thousands of connections, and
registering one `AbortHandle` per accepted connection in a registry
nothing ever drains would be an unbounded memory leak completely
independent of whether shutdown itself worked correctly. **The right
scope for "stop when my structural parent stops" is a collection owned by
that parent, not a registry scoped to something that outlives it (the
whole env) or something narrower still (nothing) — matching the
collection's own lifetime to the lifetime of the thing that should decide
when its contents die is what makes the cleanup automatic instead of a
second bookkeeping obligation.**
