# A client-side fix that widens how many hops a caller tries can silently multiply the server-side work abandoned callers leave behind (issue #596)

`serve_requests`/`handle_connection` (`animusd/src/lib.rs`) spawn one
untracked, fire-and-forget task per accepted connection on both client-
protocol listeners (ADR 0047); before this issue's fix, that task always
ran `handle_request(..)` to completion regardless of whether anything was
still listening for the reply. A forwarded write's server-side confirm
loop (`poll_probe`, `write_path.rs`) can legitimately run for up to
`CLIENT_TIMEOUT` (10s) waiting for a propose to commit — and issue #585's
own fix (merged as #586) *correctly* widened `forward_to_tablet_leader`'s
hint-chasing retry so a client keeps trying other candidates instead of
giving up on the first slow hop, each attempt bounded by its own
`FORWARD_HOP_TIMEOUT` (2s). The two changes compose badly: a caller that
gives up on one hop after 2s and moves to the next leaves the *first*
hop's server-side handler running its own full 10s confirm-wait with
nobody listening — and a chase that tries several candidates per request
multiplies these abandoned, periodically-waking tasks. On a CPU-constrained
host (a 2-vCPU CI runner), enough of these piling up is real, measurable
scheduler pressure — and it surfaced not as a resource-exhaustion symptom
but as an apparently unrelated **Raft liveness failure** (issue #596's own
symptom: an intermittent leaderless window), because the abandoned tasks
were competing for the same runtime threads a leader election needed to
make progress on schedule.

**The general lesson: a correct fix to a *client's* retry/timeout budget
does not, by itself, bound the *server's* corresponding work — that
requires an independent decision about whether the receiving side's
handler lifetime is tied to the caller's continued interest at all.**
Widening how many hops or attempts a caller makes (more retries, shorter
per-attempt timeouts, more candidates tried) is invisible to the receiving
side unless the receiving side is specifically designed to notice the
caller went away. Before landing a client-side retry-budget change, audit
the receiving side's task lifetime too: does an abandoned attempt keep
consuming resources (threads, memory, open connections, in-flight Raft
proposals) after the caller has moved on, and if so, does anything bound
how many such abandoned attempts can accumulate concurrently?

**Fix**: `handle_connection` now splits the accepted socket
(`TcpStream::into_split`) and races each request's handler against a
`peer_closed` future reading the connection's read half, via `tokio::
select!`; the peer-closed branch drops the handler future and exits the
connection's task, incrementing a new `client_requests_abandoned` metric
(ADR 0015). This is safe because every CP mutation the client-protocol
listeners can reach already has to tolerate a mid-flight *process crash*
(the standing "accepted-unconfirmed" discipline — `ProposeResult::
Accepted` means appended to the local Raft log, never committed, and
every proposer already distinguishes never-accepted from
accepted-unconfirmed), so dropping the handler future at any await point
is no worse than the crash the design already handles; see
`crates/animusd/CLAUDE.md`'s "`handle_connection` cancels an in-flight
request" entry for the full per-request-family audit. This closes the
amplifier, not the root cause of #585/#586's own scenario (a genuinely
slow-but-live leader still gets its full budget from whichever candidate
the caller is *currently* waiting on) — it only stops paying for the hops
the caller has already abandoned.
