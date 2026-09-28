# Full-workspace test builds can exhaust a shared sandbox disk regardless of worktree isolation

**Concurrent full-workspace builds in one container can exhaust the shared
disk even when every agent is correctly isolated into its own git
worktree.** Worktree isolation (separate branches, separate working trees)
says nothing about disk *capacity* — every worktree still shares the same
underlying filesystem, and `cargo build --workspace --all-targets`/`cargo
test --workspace` for this repo produces a large `target/` directory (debug
info, incremental compilation artifacts, multiple test binaries per crate).
Two or more sessions each running a full gate at the same time, even from
different worktrees, can drive the shared disk to `ENOSPC` well before any
single build looks abnormally large in isolation.

Mitigations that actually reduce the footprint (not just move it around):
`CARGO_PROFILE_DEV_DEBUG=0` (skip debug info in dev builds) and
`CARGO_INCREMENTAL=0` (skip incremental compilation's extra on-disk state) —
both cut a debug build's disk usage substantially with no behavior change.
Giving each session its own `CARGO_TARGET_DIR` avoids *lock contention*
between concurrent builds, but it does **not** save space — it costs
roughly double, since nothing is shared anymore; use a separate target dir
for isolation/correctness, not as a space-saving measure, and expect it to
raise total disk pressure, not lower it.

**An `ENOSPC` test failure's signature is environmental, not a code bug**:
a `LsmEngine`/WAL test failing with `No space left on device (os error 28)`
inside what looks like an ordinary `ProdEnv` fsync/write path is the shared
disk actually being full, not a real defect in the storage engine. Root-cause
it the same way as any other environmental failure — check `df -h` on the
relevant mount, free space (stale `target/` dirs from finished sessions,
old worktrees), and rerun — never by retrying blindly hoping the next run
lands when the disk has room, and never by treating it as a "flaky test" to
quarantine or ignore (Session operating mode item 4 in the root `CLAUDE.md`
still applies: root-cause every red gate, including this class).

**Push finished branches promptly.** A container restart loses every
unpushed commit — there is no recovery, the work has to be redone from
scratch. This happened during this exact task (ADR 0073 Phase 0 Workstream
B, the legacy CP-member address-book deletion): a prior agent completed the
full deletion, but the container restarted before the branch was pushed,
and the entire change — including its own record of what it had done — was
gone, requiring a full redo in a fresh session. Treat "push" as part of
finishing a unit of work, not an optional final step to get to eventually;
push as soon as a commit is ready, especially before any operation (a long
build, a long test gate) that risks running the session long enough to hit
a restart.
