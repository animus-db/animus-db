# A hint-chasing forward's per-candidate transport timeout must be a bounded slice of the overall deadline, never the whole remaining budget (issue #585)

`ClientCtx::forward_to_tablet_leader` (`crates/animusd/src/forwarding.rs`)
chases a "not the leader here" refusal's own embedded hint, or another
known replica, across at most one pass over a tablet's replicas, bounded
by an overall `CLIENT_TIMEOUT` deadline computed once at entry. Issue
#316's fix made a **transport failure** (the candidate is outright
unreachable — a killed node) retryable, treating it exactly like a
refusal carrying no hint: try the next known replica rather than giving up
on the first dead hop. That fix's own regression
(`forward_transport_failure_tests`, `lib.rs`) proved it — but it only
covers a candidate that fails to connect *quickly*. It missed a second,
distinct failure shape: each hop's own transport timeout was computed as
`remaining = deadline.duration_since(now)` and handed to
`relay_request_with_timeout` **whole**. A candidate that accepts the TCP
connection but is merely slow (a loaded sandbox, a starved disk) or simply
never answers isn't a transport failure at all until its own timeout
fires — so it could consume the *entire* remaining `CLIENT_TIMEOUT` on
that one hop, and the loop would then find `now >= deadline` and give up
having tried exactly one replica, even with the tablet's real leader
reachable in well under a second the whole time. Observed live as a
bimodal `admin_endpoint` failure (a different test each time, always
`RELAY_TRANSPORT_FAILURE`) in a loaded sandbox — never reproducible on
CI, exactly the signature of a hop that happened to be slow enough to eat
the whole budget under contention but not under a quiet run.

The general lesson: **a chase over N candidates within an overall deadline
must cap each candidate's own timeout to a bounded slice of that deadline,
never hand any one candidate the whole thing** — otherwise a single slow
(not even dead) candidate defeats the entire point of having other
candidates to try. This applies to any future hint-chasing/broadcast loop
added to this pattern, not just the one instance fixed here — grep for
`relay_request_with_timeout`/`self.relay(` before adding a new one, and
check whether it's a single deliberate long-poll (which legitimately needs
its own long timeout, e.g. `remote_metadata_watch_loop`'s
`WATCH_METADATA_CLIENT_TIMEOUT`, which must outlive the serving node's own
server-side park) or a multi-candidate chase (which needs the bounded-slice
treatment). Fixed by adding `FORWARD_HOP_TIMEOUT` (a plain constant, not
derived from `CLIENT_TIMEOUT`) and using
`remaining.min(FORWARD_HOP_TIMEOUT)` per hop in
`forward_to_tablet_leader`, and applying the identical cap to
`ClientCtx::propose_schema`'s own broadcast-to-every-known-address chase
(the ADR 0030 growth-node fallback), which had the same shape. See
`crates/animusd/CLAUDE.md`'s forwarding section for the full mechanism and
regression test name.

**Testing note — this mechanism has no `SimEnv` shape today.** The actual
cross-node relay (`relay_request_with_timeout`) is a free function
hardcoded to a real `tokio::net::TcpStream`, not routed through `E: Env`'s
`Network` seam or the `R: RelayClient` capability trait `ClientCtx<E, R>`
otherwise carries for `RemoteControlClient`'s own relay — `animus-node`'s
own `host::RelayClient` doc names this gap explicitly ("a future sim-only
implementation, rung C3d, deferred"). The regression
(`forward_hop_timeout_tests`, `lib.rs`, real-socket `ProdEnv`, living
outside `forwarding.rs` because that module's hard
`#[deny(clippy::disallowed_methods)]` would refuse the test's own
`tokio::time`/`tokio::net` calls) models "reachable but never answers"
with a real raw `TcpListener` stub bound to the deterministic first-guess
candidate's own former address: it accepts every connection and simply
never writes a reply — no `sleep`, no randomness, so the only thing that
can ever fire is the real timeout under test. Confirmed red-before/
green-after: reverting the fix's `remaining.min(FORWARD_HOP_TIMEOUT)` back
to plain `remaining` makes the test fail deterministically with `Err(Elapsed(()))`
(the outer 8s test bound firing because the uncapped hop burns nearly the
whole 10s `CLIENT_TIMEOUT`), every run.
