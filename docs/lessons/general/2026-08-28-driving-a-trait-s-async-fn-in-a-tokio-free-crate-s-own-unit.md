# Driving a trait's `async fn` in a `tokio`-free crate's own unit tests: `std::task::Waker::noop()` + `std::pin::pin!`, no `unsafe`

`animus-node` has no `tokio` dependency at all, in `[dev-dependencies]`
either (ADR 0061's rung C0/C1 boundary is enforced for the whole crate, not
just its library target) — so unit-testing `animus_node::admin::dispatch`
(an `async fn` calling into `#[async_trait]` `AdminHost` methods) against a
fake host couldn't reach for `#[tokio::test]`. The fix isn't a hand-rolled
`RawWaker` (which needs `unsafe` for `Waker::from_raw`, and this workspace
lints `unsafe_code` at `forbid`): `std::task::Waker::noop()` (stabilized
well before this workspace's MSRV) is a ready-made no-op waker, and
`std::pin::pin!(fut)` stack-pins a future with no `unsafe Pin::new_unchecked`
and no heap allocation. A ~10-line `loop { if let Poll::Ready(v) =
fut.as_mut().poll(&mut cx) { return v; } }` around those two is a complete,
safe, dependency-free `block_on` — sound whenever the future under test
never genuinely parks (resolves on its first poll, or every intermediate
`Pending` is guaranteed transient), which covers exactly the shape a pure
routing/dispatch test wants: a fake implementor whose methods return
immediately. Reach for this before adding a `futures`/`pollster`
dependency (or, worse, quietly loosening the crate's "no `tokio`, no
`unsafe`" invariants) just to drive a small `async fn` in a test.
