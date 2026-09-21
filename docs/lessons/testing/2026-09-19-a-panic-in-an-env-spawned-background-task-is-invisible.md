# A panic in an env-spawned background task is invisible to the test harness unless it's counted at the spawn seam and checked at teardown

**A panic in an env-spawned background task is invisible to the test
harness unless it's counted at the spawn seam and checked at teardown**
(issue #939). `ProdEnv::spawn` (the `Spawner` impl every background
driver/apply loop reaches through `env.spawn_task`) only ever kept the
spawned task's `AbortHandle` — needed so `shutdown`/`shutdown_and_wait` can
tear a node down — never its `JoinHandle`. `tokio::spawn`'s own task
harness always catches a panic internally and turns it into a `JoinError`,
but that `JoinError` only reaches *anyone* if something `.await`s the
`JoinHandle`; with no `JoinHandle` kept at all, a spawned task's panic was
printed by the default panic hook (stderr) and then simply discarded —
the task stopped running, silently, and every other part of the system
(and the test driving it) carried on as if nothing had happened. The
issue's confirmed instance: `streams_e2e::
cascade_split_walks_the_grandparent_chain_with_closed_shard_shape`'s Run-6
saw a leader's apply task panic (`animus_cp_data::apply_and_compact`'s
split-fork seal-marker `.expect(..)` firing on a real `wal group-commit
sync failed` under disk pressure) and still reported ok — the test's own
assertions happened to be satisfied through other replicas/paths before
anything touched the now-dead one.

**Two fixes are needed, at two different layers, and neither is sufficient
alone.** Counting the panic (layer 1: `ProdEnv::spawn` wraps the future in
`futures::FutureExt::catch_unwind`, bumps an atomic and remembers the first
message, then `std::panic::resume_unwind`s so `tokio`'s own `JoinError`
semantics — and the default panic hook's stderr output — are completely
unaffected) makes the fact *observable*, but observable is not the same as
*checked*: nothing fails the test just because a counter went from 0 to 1.
Layer 2 (`animusd::tests::support::TaskPanicGuard`, a `Drop`-based teardown
check watching a set of `Node`s) is what actually turns that count into a
test failure. A test suite that only ships layer 1 has built a diagnostic
nobody reads; a suite that tries to skip layer 1 and build layer 2 directly
against `Node`/`ProdEnv` has nothing to check at all. The general form:
when a background task's failure needs to surface in a **test**, the
observation point (count/log/expose at the seam the task runs through) and
the assertion point (a teardown check that reads it and fails loudly) are
two separate, both-required pieces of work — don't stop after building
either one alone.

**A process-global `std::panic::set_hook` was considered and rejected** for
this: `cargo test`/`cargo test --workspace` runs many tests in parallel on
shared tokio worker threads, so a global hook has no way to attribute a
panic to the *test* (or even the *node*) whose spawned task produced it —
every test sharing that process would see every other test's panics, or
none reliably. Counting on the **env the task was spawned from** is the one
attribution this seam can make correctly, since a test constructs its own
`ProdEnv`s and only ever watches its own.

**A cancelled task must never count as a panic**, or every routine
`ProdEnv::shutdown()`/simulated-kill-node path — which the whole test suite
already relies on — would start failing a new teardown check that watches
this counter. This holds structurally, not by a special case:
`AbortHandle::abort` drops the task's future without ever resuming its
poll, and `catch_unwind` only ever wraps a *poll*, so an aborted task's
`Drop` never runs through the wrapped future at all. Verified directly
(`animus_env::prod::tests::spawn_aborted_task_never_counts_as_a_panic`) —
worth a dedicated regression whenever a "count X" mechanism sits next to an
existing "cancel X" path, since the two are easy to conflate by intuition
("the task stopped running" is true of both) despite being mechanically
unrelated (an unwind vs. a dropped, never-resumed future).

**An ordering bug hides in "increment a counter, then store a message
for it"**: storing the message *after* bumping the counter lets a poller
spinning on `spawned_task_panics() != 0` observe a nonzero count while
`first_spawned_task_panic()` still reads `None` — a real, reproducible race
in this exact commit's first draft (a `#[tokio::test(flavor =
"multi_thread")]` proof test caught it immediately, no flake needed). Fix:
write the message under its mutex, drop the guard (a release), *then*
bump the atomic counter (a `SeqCst` store) — a reader's own `SeqCst` load
of the counter happens-after that store, so by the time it observes a
nonzero count the message write is already visible when it goes to read
it. General form: when two pieces of state describe "the same event" but
live behind different primitives (an atomic counter + a mutex-guarded
payload), the writer must publish the payload before the counter a reader
polls, never after — "counter first, payload second" is the natural
writing order and the wrong one.

**Gates**: `cargo fmt --all --check`; `cargo clippy --workspace
--all-targets --all-features -- -D warnings`; `cargo test -p animus-env
--all-features` (the two `prod::tests::spawn*` proofs); `cargo test -p
animusd --lib`; `cargo test -p animusd --test task_panic_guard --test
streams_e2e`. The `task_panic_guard.rs` proof is deliberately structured to
show it depends on layer 1: temporarily commenting out `ProdEnv::spawn`'s
`fetch_add` turns `guard_drop_panics_when_a_watched_node_counted_a_task_
panic` red on its own bounded-poll timeout assertion (`injected task panic
was never counted on the node`) rather than on the guard's panic — proving
the test isn't accidentally passing for an unrelated reason.
