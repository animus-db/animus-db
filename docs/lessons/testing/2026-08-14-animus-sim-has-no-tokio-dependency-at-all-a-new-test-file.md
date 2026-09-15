# `animus-sim` has no `tokio` dependency at all — a new test file/module in that crate cannot use `#[tokio::test]`, even though every other async-heavy crate in the workspace does.

**`animus-sim` has no `tokio` dependency at all — a new test file/module
in that crate cannot use `#[tokio::test]`, even though every other
async-heavy crate in the workspace does.** Writing the `SimSegmentStore`
fault-injection tests (ADR 0043 §A7, `animus-sim/src/segment_store.rs`)
as `#[tokio::test] async fn ...` compiled fine locally with `cargo check`
scoped to just that file mentally, but failed at `cargo build
--all-targets` with a missing-macro/missing-crate error — this crate's
own convention (`tests/disk_faults.rs`, `tests/determinism.rs`) is
`#[test] fn ...` (plain, sync) that spawns the async workload via
`env.spawn_task(async move { .. })` and drives it to completion with
`Simulator::run_for(dur)`/`run_until_quiescent(max_steps)`, reading the
result back out of a shared `Arc<Mutex<_>>` afterward. This isn't
cosmetic: the simulator's executor is the *only* thing that can resolve a
`Clock::sleep` (it needs the virtual timeline actually advanced), and a
panicking assertion inside the spawned block propagates out of the
`run_*` call exactly like any other panic, since polling happens
synchronously on the test's own thread — so the pattern costs nothing in
either determinism or assertion ergonomics versus a real `async fn` test,
it just has to be spelled differently. General check before writing a new
test in an unfamiliar crate: grep that crate's existing `tests/*.rs` for
its actual async-driving idiom before assuming `#[tokio::test]` — "this
crate is full of `.await`" does not imply "this crate depends on tokio."
(`crates/animus-sim/src/segment_store.rs`, ADR 0043 round-3 PR2,
2026-08-14.)
