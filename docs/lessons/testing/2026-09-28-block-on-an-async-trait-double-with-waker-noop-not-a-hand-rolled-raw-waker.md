# Driving an `async-trait` test double with no `tokio` in scope: `Waker::noop()`, never a hand-rolled `RawWaker`

**When a crate needs to poll a synchronous `#[async_trait]` future to
completion in a test, but `tokio` (or any executor) isn't unconditionally
available — e.g. it's gated behind an optional feature like `animus-env`'s
`prod` — the safe, dependency-free answer is `std::task::Waker::noop()`
(stable since Rust 1.85), never a hand-rolled `RawWaker`/`RawWakerVTable`.**

Building the ADR 0073 Phase 0 `ADE1` golden fixture for
`animus-env/src/encrypted.rs`, the fixture/decode/round-trip tests need to
drive `EncryptedDisk<D: Disk, R: Rng>` (an `#[async_trait]` impl) against a
tiny in-memory `Disk` test double whose methods never actually suspend
across a real `.await`. Normally that calls for `#[tokio::test]`, but this
crate's own `tokio` dependency is `optional = true`, pulled in only by the
`prod` Cargo feature (ADR 0061 rung C0) — and the per-push CI gate runs
`cargo test --workspace` with **no** `--features`/`--all-features`, so a
test that only compiles under `prod` would silently never run in the gate
that matters. The obvious fallback — a minimal hand-rolled `RawWaker` with
no-op vtable functions — hits a second wall: this workspace sets
`unsafe_code = "forbid"` at the workspace-lint level (`Cargo.toml`), and
`Waker::from_raw` is `unsafe`; `forbid` cannot be locally overridden with an
`#[allow]`, unlike `deny`.

`std::task::Waker::noop()` returns a `&'static Waker` that is a genuine
no-op — correct exactly when nothing in the polled future ever actually
parks (true here, and true of most small synchronous test doubles) — and
needs zero `unsafe`, zero extra dependencies, and zero feature gates. The
resulting `block_on` is a five-line loop: `Context::from_waker(Waker::noop())`
plus `Box::pin(fut).poll(&mut cx)` in a `loop` returning on `Poll::Ready`.

Generalizes to any crate in this workspace that needs to test an
`async_trait` component against a synchronous double without pulling in an
executor dependency it doesn't otherwise need — check the crate's MSRV
(`rust-version` in the workspace `Cargo.toml`) supports 1.85+ first, which
it does workspace-wide today.
