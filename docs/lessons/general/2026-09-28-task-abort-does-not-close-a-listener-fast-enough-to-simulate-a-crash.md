# `task.abort()` is not a synchronous socket close — a simulated node "kill" needs one to look like a real crash to its peers (issue #1060)

`forward_transport_failure_tests::forward_to_tablet_leader_survives_a_dead_first_guess`
(`crates/animusd/src/lib.rs`) flaked in CI with `"budget exhausted chasing
the leader (last hop: no time remained to attempt a hop); retry"`. The test
kills the tablet's deterministic first-guess replica via `nodes[victim].
shutdown()`, then forwards through a caller with no local replica, asserting
both a successful recovery and `elapsed < 5s`.

## The trap: `Node::shutdown()`'s bare `task.abort()` only *requests*
cancellation of the listener's own accept-loop task — it does not close the
listener's OS socket itself. The actual close happens only once the tokio
runtime gets around to dropping that task's future, which is scheduling-
dependent: fast under ordinary load, but not bounded, and can legitimately
take a stretch under real CPU contention (a loaded CI runner, several
sibling test binaries competing for the same physical cores). In the
meantime, a **fresh** `TcpStream::connect()` to the "killed" node's intra
port can still **succeed** — the kernel completes the TCP handshake
straight into the listen backlog, entirely independent of whether any
userspace `accept()` call is currently running to pull it off. The
connecting side then blocks on `read_frame`, waiting for a reply nobody is
coming to write, until either the listener finally closes (RST-ing the
half-open connection) or the caller's own hop timeout fires.

This is exactly the gap between an in-process *simulated* crash and a real
one: a genuinely killed OS **process** has every one of its file
descriptors reclaimed atomically by the kernel the instant it dies, so a
peer's connect attempt refuses immediately and unconditionally, regardless
of what any thread was doing. `Node::shutdown()`'s async-task-abort
approximation of "kill this node" does not have that property by
construction — and every fault-injection test in this crate that "kills" a
node this way was quietly relying on the runtime scheduling the abort's
drop fast enough that the gap never mattered in practice, which is not a
guarantee.

## Why this matters specifically for a hinted-retry forward

`ClientCtx::forward_to_tablet_leader`'s whole candidate-classification
scheme (issue #316/#585, `forwarding.rs`) depends on telling a
**confirmed-dead** candidate (`RELAY_TRANSPORT_FAILURE`, a fast,
within-budget connect/write/read failure) apart from a **merely slow**
one (`RELAY_HOP_TIMEOUT`, the hop's own cap elapsing with no answer) — the
former is excluded from ever being hinted back to, the latter deliberately
stays eligible for an immediate, generously-capped `Hinted` retry, on the
theory that it might be a genuinely slow-but-live leader (a real, tested,
load-bearing property — see `HINTED_FORWARD_HOP_TIMEOUT`'s own doc). A
first-guess kill that fails to close its listener fast enough gets
classified as the *second* case, not the first — and the scenario this
test exercises (a stale "not leader, try the node I just killed" hint from
a surviving replica who hasn't yet noticed the loss) is exactly the shape
that then spends a full `HINTED_FORWARD_HOP_TIMEOUT` chasing a node that
is actually gone, on top of the first `FORWARD_HOP_TIMEOUT` already spent
discovering it looks unresponsive. Under load, this can exhaust even a
widened shared deadline, and even where it doesn't, it burns real wall
time the test's own `elapsed < 5s` assertion has no slack for.

**Widening the shared forward-chase deadline would not fix this flake.**
Avoiding budget exhaustion is necessary but not sufficient: the test also
asserts the whole recovery stays well under 5 seconds, so a chase that
legitimately takes ~10s because of a slow first hop plus a wasted hinted
retry still fails, just via the other assertion instead of the first. A
bigger budget treats the symptom (the chase ran out of time) without
touching the cause (why the chase needed that much time in the first
place). Before reaching for more headroom, measure whether the *nominal*
fast path is actually fast — if a "recovers in well under a second
normally" case is taking multiple seconds, something in the mechanism
itself is slow, and a bigger budget just hides that until the budget
itself gets exhausted again.

## The fix: give a killed node's listener a synchronous close, not just a task cancellation

`Node::shutdown`/`Node::shutdown_and_wait` now call `socket2::SockRef::
from(&listener).shutdown(std::net::Shutdown::Both)` on the client + intra
listeners' own `Arc<TcpListener>` handles, **before** issuing any
`task.abort()`. This is a plain synchronous syscall with zero dependency on
tokio scheduling anything: on Linux, `shutdown(2)` on a *listening* socket
makes the kernel refuse any further `connect()` immediately (rather than
queuing it) and unblocks this node's own already-parked `accept()` call
with an error, so the accept-loop task notices and exits on its own too —
ahead of, and independent of, the `task.abort()` that follows. No `dup`, no
raw fd, no `unsafe`: `SockRef::from` is a zero-cost *borrowing* wrapper
over anything implementing `AsRawFd`, so this needs only a second `Arc`
clone of the listener already shared with `serve_requests`, not exclusive
ownership or fd manipulation.

Confirmed empirically, not just reasoned about: an isolated repro (a
`tokio::select!`-based accept loop matching this crate's own `serve_
requests` shape, killed via a bare `task.abort()`) showed a fresh connect
succeeding well after the "kill," reliably reproducible once the runtime
was under load; the same repro using `SockRef::shutdown` instead of
`task.abort()` alone refused a fresh connect in tens of microseconds, on
every attempt, with or without load. The real regression test
(`shutdown_makes_a_killed_nodes_intra_port_refuse_a_fresh_connect_fast_
issue_1060`, deliberately isolating this one mechanism — no cluster, no
Raft, no forwarding chase) reproduced the identical failure shape
(`Ok(Ok(TcpStream {..}))` — the connect genuinely succeeded) reliably under
a synthetic CPU-spin load once the fix was temporarily reverted, and passed
20/20 under the same load once restored.

## The general lesson

A test's own "kill" primitive is only a faithful crash simulation if it
reproduces the property the code under test actually relies on a crash
having — here, "a dead peer refuses a connection promptly and
unconditionally." An async task abort does not have that property by
construction; if anything downstream classifies "didn't answer within a
timeout" differently from "refused immediately" (a hinted-retry forward's
own `RELAY_HOP_TIMEOUT`/`RELAY_TRANSPORT_FAILURE` split, or any other
fast-vs-slow-failure distinction), the fidelity of the "kill" matters, not
just whether the target eventually becomes unreachable. When a
kill-then-observe test is flaky specifically under load and the failure
message describes something running out of an otherwise-generous time
budget, check whether the "kill" itself is actually prompt before widening
the budget — a synchronous OS-level teardown (a socket `shutdown(2)`, or
the local equivalent) is usually available and removes the scheduling
dependency entirely, rather than just making it less likely to matter.
