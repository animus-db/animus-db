# An accept loop must outlive a single transient `accept()` error

Issue #592 (`index_backfill.rs`'s `a_tablet_that_appears_before_the_flip_
blocks_it_until_it_also_reports` panicking `connect: Connection refused`
against a `bring_up`-issued, already-`bind()+listen()`-succeeded address)
had a confirmed real-socket root cause, one layer past everything the prior
investigation (`2026-09-04-a-retry-s-own-justification-must-survive-the-
same-kernel.md`) had already ruled out.

That investigation correctly established a live listener cannot legitimately
observe `ECONNREFUSED` from scheduling delay once `bind()+listen()` has
returned. What it did not check is whether the listener was *still alive* at
the moment of the failing `connect()` — and it was not, structurally,
because of a bug in this crate's own listener code, not a race anywhere
near `bind()`/`free_addrs`.

All four of `animusd`'s own production accept loops — `serve_requests`
(`lib.rs`, the client/intra ports), `dynamo::serve`, `admin::serve`, and
`console::serve` — had the identical shape:

```rust
loop {
    match listener.accept().await {
        Ok((stream, peer_addr)) => { /* spawn a handler */ }
        Err(err) => {
            tracing::warn!(?err, "... accept failed");
            return;   // <-- kills the loop, drops the listener, closes the port
        }
    }
}
```

`TcpListener::accept`'s error cases (`EMFILE`/`ENFILE` at the process's or
system's file-descriptor limit, `ECONNABORTED`/`ECONNRESET` from a peer
that disconnected mid-handshake) are ordinarily *transient* — they say
nothing about that listener's own socket being broken. Returning on the
first one is the textbook `accept(2)` hazard: it silently and permanently
deafens the listener to every future connection, with the *process* staying
alive and every other symptom (the OS-level socket close) invisible unless
someone happens to try connecting to that exact address again. Once it
happens, any later `connect()` to that same address — even same-process,
even to a target that bound and started serving completely correctly
minutes earlier — legitimately gets `ECONNREFUSED`. No bind-side race, no
scheduling delay, no address mismatch is needed.

**This project had already fixed this exact bug once, for a different
listener, and documented it in detail** —
`animus_env::prod::spawn_accept`'s own doc comment describes a fresh
3-node control-plane election starved this same way (a connect-storm-
driven `EMFILE` killed that listener's accept loop, permanently deafening
the node to its Raft peers) and fixes it with a short backoff-then-retry
instead of a `return`. The four `animusd`-level loops each carried a doc
comment *claiming* to mirror that exact contract ("the loop keeps serving
every other connection, mirroring `animus_env::prod::spawn_accept`'s own
contract") while the code did not actually do it for a listener-level
`accept()` error — only for a per-connection TLS-handshake failure, which
*is* handled inside the spawned per-connection task and does not touch the
listener at all. A doc comment that names the exact right precedent and
then doesn't implement it is worth treating as seriously as a doc/code
mismatch anywhere else in this codebase (see the "grep every gating match
site" family of lessons) — it is often the fastest way to find that a fix
made once was never propagated to its siblings.

**Why nothing in the CI log hinted at this**: the failing test binary
installs no `tracing_subscriber`, so `tracing::warn!("accept failed")`
goes to a subscriber-less no-op — the exact log line that would have named
the real cause is unconditionally silent in this project's own test
binaries. Absence of an `EMFILE`/`Too many open files` message in a test
log is not evidence the process never touched its fd limit; it is only
evidence tracing has no sink there.

**Why the failure needed a *split* test to reproduce, not the plain one**:
an in-place split mints two new per-tablet engines and two new CP-data
Raft groups on every fork participant in one apply — exactly the kind of
short, real fd-usage burst that can tip a listener's own next `accept()`
into a transient error without ever pushing the whole process into the
*sustained* fd starvation that hard-panics the WAL append path first (this
codebase already treats an I/O error on `persist_wal` as an unconditional
hard panic unless the group is `halted` — a different, deliberate failure
mode, not the one this bug produces).

**Fix**: every one of the four loops now logs and backs off
(`ACCEPT_ERROR_BACKOFF`, matching `spawn_accept`'s own value) on an
`accept()` error and keeps looping, instead of returning. Each loop's own
doc comment was corrected to state what it now actually does, rather than
what it previously only claimed to do.

**General form**: when a function's doc comment claims to mirror a named
precedent's contract, don't take that on faith — read the precedent and
diff the *code*, not the prose, especially for anything shaped like an
accept/retry loop. A "the loop keeps serving" claim is easy to write once,
easy to copy to three siblings, and easy for none of the four to actually
be true if the fix that made it true elsewhere was never re-applied. A
live, deterministic regression for the accept()-level branch itself was
judged impractical here without `unsafe`/an rlimit crate this workspace
doesn't depend on (`unsafe_code = "forbid"`) — the same call this
project's own `spawn_accept` regression already made, testing the
adjacent, easily-reproducible failure mode (a bad connection that must not
kill the loop) rather than forcing a genuine `EMFILE`/`ECONNABORTED`; the
existing real-socket suite (including the three `index_backfill.rs` tests
themselves, run repeatedly) is what proves this fix compiles and holds
under real load.
