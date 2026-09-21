# A spawn before a later fallible step needs an RAII abort guard, and the guard can only request the abort (issue #1010)

`animusd`'s node-assembly functions (`BoundNode::start_with_growth`,
`BoundDataNode::start_data_with_growth`, `BoundControlNode::
start_control_with`) each build up a running node in one long async
function: start a driver task or two, open storage, build a couple of
handles, spawn a bundle of background loops, return `Ok(Node { .. })`.
Several of those steps are fallible (`build_segment_store`,
`build_backup_store`, `check_wal_layout`, `SharedWal::open`) and use plain
`?`. That is the bug: **every task already spawned before a `?` that then
fails is simply discarded along with the function's own locals** — a
`Vec<JoinHandle<_>>` built up so far, an env clone kept around to pass to
`Node`'s own `envs` field — none of it was ever assigned anywhere that
outlives the function, so it all drops silently on the early return. A
dropped `JoinHandle` does not abort its task (same amnesia as the sibling
lesson on `serve_requests`' own fire-and-forget spawn), and worse, the
*driver* itself (the control-plane Raft driver, started via `self.env`'s
own `Spawner` impl) isn't even tracked by a `JoinHandle` at all — it lives
in `ProdEnv`'s own internal abort registry, which nothing but `ProdEnv::
shutdown()`/`shutdown_and_wait()` ever drains. A `?` after that point,
with no explicit `env.shutdown()` on the error path, leaks a live Raft
driver, a live set of accept loops (holding their bound listening
sockets), and everything those accept loops have in turn accepted
(closing the loop with this same issue's layer-1 fix) — forever, on a
process that keeps running past the failed bring-up attempt.

**The general shape to watch for: a function that spawns background work
and then keeps doing fallible things.** Every `?` (or early `return
Err(..)`) after the first spawn is a potential leak, and the leak is easy
to miss precisely because the *success* path looks completely normal —
every spawned handle and env clone gets assigned into the eventual
`Ok(..)` value, so nothing about the success path hints that the failure
path drops the same values on the floor instead. Grepping for `tasks.push`
or `spawn_task` calls tells you where things start running; it's the `?`
operators *after* the first one, not the spawns themselves, that need the
audit.

**The fix is a small RAII guard (`StartupTasks`) that owns the
in-progress `Vec<JoinHandle<()>>`/`Vec<ProdEnv>` and, while still armed,
aborts every task and requests every env's shutdown on `Drop` — then gets
explicitly disarmed (`into_parts`) only once the function has no fallible
step left.** This turns "did I remember to unwind this on every error
path" from a per-`?`-site audit (fragile — a new fallible step added later
needs its author to remember the same discipline) into a structural
guarantee: **any** early return through the guard's scope — including one
a future edit adds without ever reading this lesson — cleans up
automatically, because unwinding out of scope always runs `Drop`,
`?`-triggered or not. Implementing `DerefMut<Target = Vec<JoinHandle<()>>>`
on the guard meant every existing `tasks.push(tokio::spawn(..))` call site
kept compiling completely unchanged — the only new lines are the guard's
own construction, one `.extend(..)` where a helper (`spawn_common_tail`)
hands back its own already-spawned `Vec`, and one `.into_parts()` at the
success point. Small, mechanical diff; large behavioral change.

**A `Drop` impl can only *request* cancellation, never wait for it — it
cannot `.await` at all, so there is no way to build a "wait for the abort
to finish" guard even in principle.** This is the same "abort() is a
request, not a guarantee" lesson as `Node::shutdown` vs. `shutdown_and_
wait`, just structurally forced rather than merely easy to forget: a
caller that needs "and the ports are provably free" after a guard-covered
failure needs its own bounded converged-or-timeout poll on the actual
resource (this issue's own regression test re-binds every address in a
loop, bounded, rather than asserting once immediately after the failing
call returns) — the guard makes the *abort request* unconditional, not the
observable cleanup's timing.
