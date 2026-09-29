# "Register every spawned handle so shutdown can abort it" is itself an unbounded per-spawn leak once `spawn` sits on a per-message path

`ProdEnv::Spawner::spawn` (`crates/animus-env/src/prod.rs`) has always kept
every task's `tokio::task::AbortHandle` in `Inner::tasks: StdMutex<Vec<
AbortHandle>>`, so `shutdown`/`shutdown_and_wait` can abort (and, for the
latter, wait on) every task the env owns at teardown. That mechanism is
correct for what it was designed around — a handful of long-lived
per-node driver loops (the accept loop, the demux pump, a Raft driver) — but
nothing ever *removed* a handle once its task finished; only `shutdown`/
`shutdown_and_wait`'s own `mem::take` of the whole vec ever shrank it. A
`tokio::task::AbortHandle` pins its task's `Cell` in the runtime for as long
as the handle lives, so a growing `tasks` vec is not merely a growing `Vec`
(a handful of bytes each) — it is a growing set of permanently-unfreeable
task allocations.

This was harmless as long as `spawn` was only ever called from long-lived
setup code. It stopped being harmless the moment a *per-message* code path
started going through it: `Network::send_stream`'s issue #661 fix spawns one
connect+write task per outbound frame (bounded by `SEND_TIMEOUT`, so a
Raft driver's own dispatch loop never blocks on one unreachable peer). Every
heartbeat, every `AppendEntries`, every write forward — one send, one
spawn, one handle pushed, and (since #661 predates this fix) never removed.
heaptrack of a live `animusd --cluster-control 3 --cluster-data 5` cluster
under bulk-seed + `PutItem` load found ~94 MB of a 254 MB RSS peak sitting in
exactly these leaked `tokio::runtime::task::core::Cell`s, all still "live"
purely because `Inner::tasks` still held their `AbortHandle` — the dominant
contributor to the node's ~200 MB/min RSS growth under load.

**The general lesson**: a registry built to let teardown reach every
handle it needs to abort is a correct, standard pattern — right up until
something on a per-message (not per-driver-loop) path starts registering
into it. At that point "append-only until the whole env shuts down" is
itself the leak, independent of whatever the individual spawned tasks do.
Before adding a new call site that spawns through an existing shutdown-
tracking registry, ask whether that call site's own frequency is bounded by
node/driver count (fine, register and forget) or by message/request volume
(not fine — the registry needs pruning, or the spawn needs a different home
entirely, e.g. a `tokio::task::JoinSet` scoped to the *specific* long-lived
task that owns the per-message work, the way `animusd::serve_requests`
already does for per-connection handlers rather than routing them through
`ProdEnv`'s own registry — see that function's own doc for why it was kept
deliberately separate).

**The fix, generalized**: when a registry must keep tracking every entry
for a correctness reason (here: `shutdown` needs to be able to abort
anything still running) but cannot afford to hold every entry ever added
forever, amortized high-water-mark pruning is a cheap middle ground between
"prune on every insert" (wasteful — most inserts don't need it) and "never
prune" (the leak). Track a `next_prune_threshold` starting at a floor; when
the collection's length reaches it, sweep out the entries that no longer
need tracking (here, `retain(|h| !h.is_finished())`) and set the next
threshold to `max(floor, 2 * <post-sweep count>)`. This bounds the
collection to roughly `2x` its genuinely-live content at `O(1)` amortized
cost per insert, and — critically — changes nothing about the collection's
actual correctness contract: every entry that still matters (a task that
hasn't finished) is never touched by the sweep, so `shutdown`'s "abort
everything still running" guarantee holds exactly as before.

See `crates/animus-env/CLAUDE.md`'s `Inner::tasks`/`Spawner::spawn` entries
for the as-built mechanism and `crates/animus-env/src/prod.rs`'s
`spawn_prunes_finished_handles_and_stays_bounded`/
`shutdown_still_aborts_a_long_running_task_after_pruning` for the red-before/
green-after regression pair.
