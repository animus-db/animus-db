# Allocate test ports by binding and holding, never by probing and releasing (issue #627)

A test fixture that needs `N` fresh loopback ports before it can build the
config it's about to start `N` processes/nodes from has an obvious-looking
shortcut: bind `127.0.0.1:0` `N` times, read back the OS-assigned address
each listener got, then **drop the listeners** (releasing the ports) so the
real node can rebind them a moment later. `crates/animusd/tests/support/
mod.rs::free_addrs` did exactly this, and it is a textbook TOCTOU
(time-of-check-to-time-of-use) by construction: the instant the probing
listener drops, the port is free for **any** process on the host to bind —
another test binary running concurrently, a sibling bring-up attempt in the
same process, or (see below) a zombie task left over from this same
fixture's own prior, torn-down attempt. The window is typically
microseconds, which is exactly what makes it a flake and not a reliable
failure: almost every run wins the race, so the fixture looks correct for
a long time before a loaded CI box or a sibling test's own contention
finally loses it.

**The generalizable fix: bind and hold, never probe and release.**
`std::net::TcpListener::bind("127.0.0.1:0")` (or the async equivalent) asks
the kernel to assign a free ephemeral port **atomically at bind time** —
there is no separate "ask, then use" step for anything else to race. If the
real component that will serve on that port can accept an *already-bound*
listener/socket (rather than an address it binds itself), the fix is
mechanical: bind every port the fixture needs up front, keep every listener
alive, and only then start whatever needs to run behind them. Nothing ever
observes a released port, so nothing can ever steal one.

**If the component can't accept a pre-bound socket, split it into a bind
half and a start half.** This repo's own production node assembly used to
be one function that both bound its own six listeners (`Node::bind`) *and*
started every protocol on top of them, in one call — fine for a single
node, but it meant a multi-node test fixture wanting to bind-then-start
every node of a cluster had no entry point to call. The fix (issue #627,
`crates/animusd/src/lib.rs`) added a genuinely additive **start half** —
`start_bound_node_with_streams_quiesce_and_ttl_sweep_interval`, taking an
already-bound `BoundNode` instead of binding one itself — mirroring the
`Node::bind`-then-`start_with_growth` shape the code already used
internally, and mirroring the `bind_cluster`/`start_cluster` split
production's own `--cluster N` dev-cluster path had already established for
exactly this reason. The existing all-in-one entry point becomes a thin
`bind` + `start` composition with byte-identical behavior; nothing about
its own callers needs to change.

**A bring-up that starts nodes one at a time and tears partially-started
ones down on failure creates a second, independent hazard beyond the raw
port TOCTOU — one that survives even a bind-and-hold fix if the fixture
still starts (not just binds) nodes sequentially.** If node *k*'s bind or
start fails and nodes `0..k-1` are already fully running processes with
live background tasks, the whole attempt has to tear those down
(`shutdown_graceful`) before a retry can reallocate fresh addresses and try
again. But a graceful shutdown that only aborts *tracked* tasks can still
leave a task alive if the component in question ever spawns work
fire-and-forget (this repo's own `serve_requests` spawns one untracked task
per accepted connection — see this crate's own `CLAUDE.md` gotcha). A
request already mid-flight on such a task at the instant of teardown keeps
running, on the same runtime, past the point its owning node was supposedly
torn down — and if the fixture reuses the same fixed identities
(`"n0"`, `"n1"`, …) across attempts, with no cluster/attempt identity
carried on the wire protocol itself, that survivor can dial out to whatever
a *later* attempt (or an entirely different, concurrently-running test)
has since bound at the address its own stale routing table names, and land
on a live peer with no way for either side to detect the mismatch. The
fix for *this* half is structural, not a retry-avoidance trick: **bind
every node first, and only start any of them once every bind has
succeeded** — a bind failure on node *k* then leaves zero tasks running
anywhere, for any node, so there is nothing left to tear down and nothing
left to survive a teardown. Once every node in a bring-up is bound before
any of them starts, the retry loop this whole class of fixture used to
need disappears entirely, rather than merely getting harder to trigger.

**The general rule, stated once**: a `:0`-bind allocator is either held
open until the real owner takes it over, or it is not a safe allocator at
all — "probe, release, hope nothing else looks in the meantime" is not a
smaller version of the real hazard, it is the whole hazard. And a bring-up
helper's own retry-on-failure loop is not free of this same class of bug
just because it reallocates fresh addresses each attempt — if a failed
attempt can leave *anything* still running (an untracked task, a leaked
process, a background loop with no tracked handle), the next attempt's
"fresh" addresses can still collide with that survivor the moment it tries
to act. Binding-and-holding removes the first hazard; binding everything
before starting anything removes the second.
