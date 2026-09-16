# `Network::send`'s "fire-and-forget" contract was a lie in `ProdEnv`: one unreachable peer's unbounded `TcpStream::connect` starved every OTHER peer queued behind it in the same dispatch loop — the network-path twin of issue #279 (S-07d growth e2e, PR #661, issue #661)

The third `kind` e2e failure on all three legs (plain TCP, TLS, S3) of
PR #661's `spec.controlNodes` growth: `storage.ephemeral: true` means an
`EmptyDir`-backed pod restart wipes both WAL and engine, so when the
StatefulSet's config-hash roll (the sibling entry above) reached an
*already-established* control voter (not the brand-new one growth had
just added), that ordinal came back with an empty persisted store and — per
`RaftNode::start_with_orphan_sweep_after`'s existing, previously-proven-safe
"empty persisted storage ⇒ treat as a genesis bootstrap participant" rule
(ADR 0060's "Why growth doesn't need genesis's own sequential join
answer") — rejoined as a fresh, term-0 follower with an empty log. That
part is *safe*: pre-vote's log-up-to-date check means a fresh rejoiner can
never win a real election against the established group's far-ahead log,
proven by three new `SimEnv` cells in
`crates/animus-control/tests/wiped_voter_rejoin.rs` (wiped *leader*,
wiped *follower*, and a no-growth control) — all three converge to a
single leader and a committing write within a handful of election
timeouts, seed-reproducible, no protocol change needed. So the Raft
*logic* was never the bug, and `SimEnv` correctly had nothing to say about
it — this is exactly the class of hazard `SimEnv` cannot reach at all (no
real sockets, no OS TCP timers), matching this crate's own "`SimEnv`
proves logic and ordering, not real-thread liveness" rule.

The real bug was one layer down, in `ProdEnv::send_stream`
(`crates/animus-env/src/prod.rs`): every outbound Raft message dispatch —
`animus-control`/`animus-cp-data`'s own `drive()` loop — does `for (to,
msg) in outs { env.send(to, bytes).await; }`, **sequentially, one peer at
a time, in the single driver task**. `send_stream`'s connect+write ran
*inline* on that same `.await`, with **no timeout at all** on the
`TcpStream::connect`. The moment a peer's address is silently
unreachable — no RST, no ICMP, packets just dropped, exactly what a
recreated pod's collapsed old network endpoint looks like to a sender
still holding a pooled connection to its previous incarnation — that
`connect`/write rides the OS's own multi-minute TCP retry timeout. Since
the dispatch loop is sequential and single-tasked, that one bad peer
blocked delivery to *every other* peer queued behind it in the same
round: whichever node was leader (the incident's own evidence: "possibly
the control leader") stopped heartbeating 0/1/3 too, they timed out and
re-elected, and the *new* leader hit the exact same peer on its very next
heartbeat round and stalled the same way — a leaderless livelock that
lasted 60+ seconds, `/admin/health`'s `leader_within` failing on every
node including the two the roll never touched, killing them via their own
liveness probe. `Network`'s own doc already documented `send`/
`send_stream` as "fire-and-forget: never report delivery" — the
*contract* was right, the `ProdEnv` implementation just didn't honor it.

**Fix** (`ProdEnv::send_stream`): spawn the connect+write onto its own
task (`self.spawn(...)`, so it is tracked by the same `AbortHandle` list
`shutdown`/`shutdown_and_wait` already drain) instead of running it inline
on the caller's `.await`, and additionally bound it with a
`SEND_TIMEOUT` (2s — generous relative to `heartbeat_interval`/
`election_base`, far below the OS's multi-minute default). Both halves
matter: the spawn decouples the *caller* (and every other peer's own
dispatch) from this one peer's fate; the timeout bounds the spawned
task's own lifetime so a black-holed peer can't accumulate unbounded
pending connections forever. Regression:
`crates/animus-env/src/prod.rs`'s
`a_send_to_an_unreachable_peer_does_not_delay_a_live_peers_delivery` —
points a peer entry at the reserved/unrouted `10.255.255.1` (confirmed by
direct probe to hang rather than fail fast in this sandbox, modelling a
real black hole without needing actual internet access) and asserts a
`send` to it, followed immediately by a `send` to a live peer, both
return within 1s and the live peer's frame still arrives. Verified this
actually catches the regression by reverting just the `send_stream` body
and re-running: the unfixed version hangs past a 20s bash timeout.

**General form**: a trait doc's stated contract ("fire-and-forget",
"never blocks", "best-effort") is not automatically upheld by every
implementation — check the *implementation*, not just the trait doc,
whenever that implementation does real, unbounded I/O (a `TcpStream::
connect` with no timeout is the classic offender; DNS resolution is
another). And: **a caller that sequentially `.await`s a "fire-and-forget"
call to N different peers in one task has silently made all N peers'
liveness depend on each other** — the fix belongs in the callee (make the
call actually non-blocking/bounded), not in asking every such caller to
remember to spawn or add its own per-call timeout. This is the network
twin of issue #279 (slow `fsync` inside the same driver loop starving
heartbeats) — same shape of bug, same fix idiom (bound it, or run it off
the loop's own critical path), different I/O.
