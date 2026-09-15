# A long single-session build/test loop needs proactive disk-space discipline, not just cleanup-on-failure (this session, ADR 0069)

Iterating on this crate's own encryption-at-rest work — dozens of
`cargo build`/`cargo test` invocations across several crates, each
producing its own debug-symbol-heavy artifacts — silently exhausted the
sandbox's root filesystem mid-build (`rustc-LLVM ERROR: IO failure on
output stream: No space left on device`), and the failure mode compounded:
once the disk is fully out of space, even the *tool harness's own*
temp-output capture for a trivial `df -h` command fails
(`ENOSPC` writing to the session's own scratch directory), so diagnosing
the problem requires commands that themselves avoid writing meaningful
output until enough space is freed to unblock everything else. Deleting
`.rmeta`/`.d` files freed a little; the real fix was sweeping
`target/debug/deps` for **stale duplicate build artifacts** — cargo keeps
every previous build's hash-suffixed `.rlib`/binary alongside the current
one rather than replacing it in place, so a long session's repeated
`cargo build -p <crate>` invocations across many crates accumulate dozens
of superseded copies of the same dependency, several hundred MB to
several GB total. A script keeping only the newest file per
`(basename-without-hash, extension)` group freed over 11 GB in one pass.

**General form**: in any session doing many incremental `cargo
build`/`test` invocations, check `df -h` periodically (not just after a
build fails with ENOSPC) and proactively prune `target/debug/deps` to the
newest artifact per basename group well before the filesystem is full —
recovering from a *fully* exhausted disk is materially harder than
staying ahead of it, since the tools needed to diagnose and fix the
problem may themselves need scratch space to run.

**Addendum — a `df`-reported "Size" is not the real ceiling, and a
crate with many integration-test files needs a batched run, not a
one-shot `cargo test -p`.** Two more turns of the same problem, same
session: (1) `df -h /`'s Size column (252G here) can be the raw device
size while `resv_strict` reserved-block accounting caps what this uid can
actually write to a small fraction of it (Avail, ~11G) — trust
`Avail`/`stat -f`'s `Available` block count, never `Size`, when judging
headroom. (2) `cargo test -p <crate>` **links every integration-test
binary in `tests/*.rs` before running any of them** — a crate with 115
separate test files (`animusd`, each linking the full dependency graph,
~100MB apiece here) transiently needs over 11GB simultaneously just for
that one crate's test binaries, on top of whatever `target/debug/deps`
already holds. The fix that stayed inside an ~11-22GB headroom the whole
time: loop `cargo test -p <crate> --test <name>` one file at a time,
`rm`-ing that test's own binary and re-running the duplicate-artifact
prune after each one, so at most one extra test binary exists at a time
instead of all of them at once. The same shape generalizes to any crate
whose `tests/` directory has grown past a couple dozen files.
