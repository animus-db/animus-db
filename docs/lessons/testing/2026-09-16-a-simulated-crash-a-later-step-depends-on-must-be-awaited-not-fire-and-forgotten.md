# A simulated node crash a later step *depends on already having taken effect* must be awaited (`shutdown_and_wait`), even though bare `shutdown()` is the documented-correct choice for crash simulation in general

`crates/animusd/src/lib.rs`'s `client_cancellation_tests::
abandoning_a_connection_cancels_its_stuck_write_and_counts_it` (issue
#638) strands a 3-node cluster's tablet leader by calling
`node.shutdown()` on the other two replicas, then immediately sends a
*new* write to the leader that the test expects can never reach 2-of-3
commit quorum and so gets stuck until `CLIENT_TIMEOUT` — the setup for
proving a stuck request gets cancelled when the client disconnects.

The existing 2026-08-10 entry in this same file already establishes that
`Node::shutdown()`/`abort()` is fire-and-forget — cancellation is only
*requested*, and the runtime may not actually poll-and-drop the aborted
tasks (freeing whatever they own) for some unbounded stretch under
contention — and it closes with an explicit carve-out: "bare `shutdown()`
remains the *correct* choice for a test that is deliberately simulating a
crash (no orderly teardown to race) rather than tearing down a clean
liveness harness." This test *is* deliberately simulating a crash, so
that carve-out looked like license to use bare `shutdown()` here too.

**It isn't, whenever a later step in the same test depends on the crash's
effects, not just on the crashed node no longer holding resources the
test wants back.** The 2026-08-10 entry's own examples are all about
racing a *rebind* (reacquiring a port/file the dead task owned) — this
test races something subtler: whether the "dead" follower can still
*participate in consensus*. A killed node's own driver task, if not yet
actually polled-and-dropped by the runtime, is still fully live and can
still ACK a fresh `AppendEntries` the leader sends in that window — so a
write issued immediately after a bare `shutdown()` can legitimately reach
quorum and complete normally instead of getting stuck, silently defeating
the whole scenario. No server-side bug: the write finishes, the response
is written back (the client hasn't closed yet), the connection loop reads
the next frame, observes the later abandon as a plain between-requests
EOF (never counted as an in-flight cancellation), and the metric this
test polls for never moves — it just times out.

**Confirmed directly, not just theorized**: a temporary diagnostic probe
(connect to each "killed" follower's own client port immediately after
`shutdown()` returns) showed it still accepting fresh connections up to
~0.5ms later in 3 of 8 local runs — on an idle 4-core sandbox, the exact
environment where this class of race is expected to be *smallest*. CI's
real 2-vCPU runners only widen it, which matches the reported symptom
precisely: a ~9.9s total run consistent with a normal-speed write
followed by the test's own unconditional 5s wait for a metric that was
never going to fire.

**Fix**: use `node.shutdown_and_wait().await` for every replica whose
death a later step in the test depends on having *already happened* —
not just `shutdown()`. `shutdown_and_wait` awaits every aborted task's
teardown (including the node's own internal `ProdEnv`, so its Raft driver
and every connection it owns) before returning, so by the time the call
completes the replica is genuinely gone, not merely asked to be.

**General rule, sharpening the 2026-08-10 entry rather than contradicting
it**: "simulating a crash" licenses bare `shutdown()` only when nothing
later in the test needs the crash to have *already taken effect* before
proceeding (the common case — most crash-simulation tests just want the
process gone eventually, e.g. before a restart or before observing
eventual convergence via a poll). The moment a test's very next action
is itself timing-sensitive to the crash being complete — "this write must
now be unable to reach quorum," "this node must now be unreachable" —
that action is racing the same unbounded abort-latency window the
2026-08-10 entry describes, and needs `shutdown_and_wait` (or an
equivalent explicit poll proving the target is actually gone) between the
kill and the dependent step, exactly like a rebind does.
