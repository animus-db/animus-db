# Discarding a `JoinHandle`'s result (`let _ = handle.await;`) silently swallows the task's panic

**Mechanism**: `tokio::spawn` returns a `JoinHandle<T>`; awaiting it yields
`Result<T, JoinError>`, and a `JoinError` is what you get back when the
spawned task panicked (or was cancelled) instead of returning normally.
`Result` is `#[must_use]`, so a bare `handle.await;` with no binding would
already be a compiler warning under `-D warnings` — which is exactly why the
anti-pattern is spelled `let _ = handle.await;` rather than left bare: the
`let _ =` binding is there specifically to silence that warning while
discarding the `Result`, including the `Err(JoinError)` case. The effect is
that a background task can panic — including on an assertion the test was
relying on to prove its own subject — and the foreground `.await` on its
handle returns cleanly regardless, so the test proceeds (and can pass) as if
every iteration of that task's loop had succeeded.

This was found in `crates/animusd/tests/split_placing_two_replica_diff_
e2e.rs` (issue #619): a paced background writer exists specifically to prove
writes stay available through a two-of-three replica swap, panicking via
`put`'s own `other => panic!("put failed: {other:?}")` arm after exhausting
its own 20s retry budget on a write that never lands. `let _ = writer.await;`
meant that panic was invisible — the test's own doc comment claimed a
guarantee the code no longer actually checked. Two more instances of the
identical shape were found by grepping the crate for `let _ = ` immediately
followed by `.await` and checking which targets came from `tokio::spawn`:
`dynamo_streams.rs` and `dynamo_pitr.rs` each spawn several concurrent
`PutItem` writer tasks (via `put_item_padded`, which also panics on a
non-200 response) and joined them with `for w in writers { let _ = w.await;
}` — the identical hole, just with several tasks instead of one.

**Fix**: propagate the result — `handle.await.expect("background writer
task panicked")` (or, for a `Vec<JoinHandle<_>>`, the same `.expect(..)`
inside the join loop) — so a task's panic fails the *foreground* test
immediately with a message naming what happened, rather than being dropped
on the floor. This is a pure visibility fix: it converts a masked bug into a
visible one and asserts nothing new about correctness, so a test that turns
red after this change was already broken — the writer was already failing,
just silently.

**General rule**: grep for `let _ = ` immediately followed by `.await` in
any test file that spawns tasks, and check whether the awaited value is a
`JoinHandle`. A `Result`/`Option`-returning async call that is deliberately
best-effort (a socket read/write during teardown, a metadata refresh) is a
legitimate use of `let _ = ...await` — the discriminator is whether the
`Err`/`None` case can only mean "the task panicked," in which case discarding
it removes the one signal that would otherwise fail the test for you.

**Gates**: `cargo fmt --all --check`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings`, `cargo test -p animusd --test
split_placing_two_replica_diff_e2e` (run 3x, unloaded), `cargo test -p
animusd --lib`. This fix alone can make
`two_of_three_replica_diff_placing_target_converges_end_to_end` red under
contention — that is the point (issue #619); see issue #670 for the
follow-up root-causing that visible failure.
