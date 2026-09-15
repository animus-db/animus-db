# A test-only deterministic double for a trait bounded `Send + Sync` (the `Env`/`Rng` seam, ADR 0003) must use atomics, not `Cell`, even though the double never crosses a real thread boundary in the test itself.

**A test-only deterministic double for a trait bounded `Send + Sync` (the
`Env`/`Rng` seam, ADR 0003) must use atomics, not `Cell`, even though the
double never crosses a real thread boundary in the test itself.** A
scripted `Rng` built to prove `NodeId::mint`'s draw shape from a fixed
sequence used `Cell<usize>`/`Cell<u64>` for its cursor/fallback-counter —
compiles fine as a bare struct, then fails with "cannot be shared between
threads safely" the moment `impl Rng for ScriptedRng` is written, because
the *trait itself* requires `Send + Sync` (every `Env`/`Rng` implementor
must be usable from `tokio::spawn`'d code in production) — the compiler
enforces this at the `impl` site regardless of whether any given test
actually spawns the double across threads. Fix: `AtomicUsize`/`AtomicU64`
with `Ordering::Relaxed` (a single-threaded test never contends on them, so
the ordering choice is moot) — same shape as any other `Send + Sync`
interior-mutability need in this codebase. General rule: a test double for
an `Env`-seam trait is never exempt from that trait's own bounds just
because the specific test using it happens to be single-threaded — check
the trait's supertrait bounds before reaching for `Cell`/`RefCell`.
(`animus-env/src/lib.rs::tests::ScriptedRng`, ADR 0040 PR4.)
