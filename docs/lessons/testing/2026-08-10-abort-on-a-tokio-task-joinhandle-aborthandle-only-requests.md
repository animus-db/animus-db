# `abort()` on a `tokio::task::JoinHandle`/`AbortHandle` only *requests* cancellation — it does not wait for the task to stop, and the resources that task owns (most importantly, a `TcpListener` it's blocked accepting on) are only released once the runtime actually polls and drops it, which can lag arbitrarily behind `abort()` returning under CPU contention.

**`abort()` on a `tokio::task::JoinHandle`/`AbortHandle` only *requests*
cancellation — it does not wait for the task to stop, and the resources
that task owns (most importantly, a `TcpListener` it's blocked accepting
on) are only released once the runtime actually polls and drops it, which
can lag arbitrarily behind `abort()` returning under CPU contention.**
This is the real root cause the previous entry's fleet-shrink/bound-raise
mitigated but didn't fix: `ProdEnv`'s internal accept-loop task owns the
env's `TcpListener` by value (moved into the `tokio::spawn`ed future), and
both `ProdEnv::shutdown()` and `animusd::Node::shutdown()` were fire-and-
forget — call `abort()` on every task, then return immediately, with no
wait for the cancellation to actually take effect. A same-address restart
test that calls `shutdown()` and immediately rebinds is thus racing its
*own* not-yet-dropped listener for the port: under light load the runtime
polls (and drops) the cancelled task within microseconds, so the race
window is usually too small to hit; under `cargo test --workspace`-level
CPU contention (dozens of test binaries and their own worker threads
fighting for the same cores) that window can stretch for seconds — long
enough to occasionally outlast even a 60s bounded rebind-retry, because
every failed rebind attempt in that retry loop was itself contending for
the same scarce CPU time the cancelled task needed to finally get polled.
Fixed by adding `ProdEnv::shutdown_and_wait`/`Node::shutdown_and_wait`
(`crates/animus-env/src/prod.rs`, `crates/animusd/src/lib.rs`): `abort()`
every task as before, then poll `is_finished()` on each handle (bounded,
a few seconds) before returning, so the caller only proceeds once the
listener is *provably* dropped. `Node::shutdown_graceful` — what every
restart test already calls before rebinding — now ends in this instead of
the plain hard-abort `shutdown`, so the fix required no test changes.
**General rule: `abort()` (or any cancellation-request API) is a request,
not a synchronous guarantee — code that aborts a task and then immediately
reacquires a resource that task owned (a port, a file lock, a fd) must wait
for confirmed termination (`is_finished()`/`JoinHandle::await`), not just
call `abort()` and move on. And when ruling out a race by checking a
socket option (`SO_REUSEADDR`), be precise about which race it rules out
(TIME_WAIT-reuse) versus which it says nothing about (a still-live
listener, in this process or another) — a clean diagnostic that answers
the wrong question reads as confirmation and can misdirect the next
person for months.** (`animus-env`, `animusd`;
`animusd/tests/split_cluster.rs::full_split_cluster_restart_recovers_metadata_and_data`.)
**Same general rule, a fresh instance (ADR 0038 PR3, ProdEnv liveness
tests over a real `LsmEngine`)**: a test's teardown calling the plain
`ProdEnv::shutdown()` (abort-and-return, not `shutdown_and_wait`) then
immediately `std::fs::remove_dir_all(dir)` can yank a directory out from
under a still-unaborted background task's in-flight file write — observed
as the control plane's apply task (`node.rs`'s `meta_apply_and_compact`)
panicking on `env.replace(WAL, ..).await.expect("wal compaction")` with a
`NotFound`-class I/O error, logged from a `tokio-rt-worker` thread after
the foreground test had already reported `ok` (a background-task panic
doesn't fail the test unless something joins/unwraps that handle). Not a
new bug introduced by the apply-task split — the same `env.replace(WAL,
..)` call already raced identically when it lived inline on `drive()`
pre-cutover — just newly visible because a `ProdEnv` liveness test now
exercises a real on-disk engine, and confirmed pre-existing by reproducing
it with only the *unmodified* `large_metadata_catch_up_stays_live` test
(`MemoryEngine`-backed, no PR3 code path involved). Left unfixed here
(per this repo's own "root-cause + fix incidental live bugs as their own
PR" discipline) — noted as a candidate follow-up: either every `ProdEnv`
liveness test's teardown should use `shutdown_and_wait` before deleting
its temp dirs, or `meta_apply_and_compact`'s WAL replace should tolerate a
torn-directory error the way `animus-cp-data`'s own compaction path does
(checked against a `halted` flag before asserting) — the latter needs a
shutdown/halted signal `animus-control::RaftNode` doesn't have yet.
**Environmental confound noted while debugging this (2026-07):** the day's
elevated failure rate (3 of 4 full-workspace runs) partly coincided with an
unrelated long-lived `animusd --cluster-control 3 --cluster-data 5` process
(started from a developer's own terminal, hours earlier) permanently
holding ~25 ports in the machine's ephemeral range
(`/proc/sys/net/ipv4/ip_local_port_range`, ~4096 ports wide here). It can
never be the *exact* port a test's `free_addrs()` probe collides on (the
kernel never hands `bind("…:0")` a port that's actually still listening),
but shrinking the effective ephemeral pool measurably tightens every
probe-then-drop-then-rebind race described above and in the port-TOCTOU
entries — more probes chasing fewer numbers means the freed slot a
`free_addrs()` probe just released is more likely to already be someone
else's next pick by the time the real bind happens. **Before writing off a
test-infra flake as purely a code bug, `ss -ltnp` the ephemeral range for
long-lived squatters** — the fix here was still a real self-inflicted race
in `shutdown`/`shutdown_and_wait` (confirmed by clean stress runs on this
same, still-polluted machine after the fix), but the environmental factor
is real too and worth ruling in/out explicitly rather than silently
absorbing it into "the test is flaky."
**Coda — the candidate follow-up above was taken (2026-08-10):** swept the
same bare-`shutdown()`-then-`remove_dir_all` idiom at the 5 remaining racy
teardown sites — `animus-control/tests/prod_liveness.rs` (2),
`animus-control/tests/control_membership_prod.rs` (1),
`animus-consensus/tests/accord_concurrent.rs` (2) — to
`shutdown_and_wait().await`; `animus-storage/tests/lsm_concurrent.rs` and
the `animusd` integration tests already used the waiting idiom, and
`animus-cp-data`'s own compaction path already checks a `halted` flag
(ADR 0033), so both were left as models rather than swept. **General rule
to take away: any test teardown that follows a `shutdown()` with removing
the directory/files that shutdown's background tasks were still writing to
must use `shutdown_and_wait()`, not bare `shutdown()`** — bare `shutdown()`
remains the *correct* choice for a test that is deliberately simulating a
crash (no orderly teardown to race) rather than tearing down a clean
liveness harness.
