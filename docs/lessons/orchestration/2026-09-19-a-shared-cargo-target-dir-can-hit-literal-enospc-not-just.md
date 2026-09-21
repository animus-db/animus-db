# A shared `CARGO_TARGET_DIR` can hit literal ENOSPC, not just slow/stale-binary contention — and clearing `target/debug/incremental` is a safe, fast reclaim

## What happened

While validating a single-crate (`animus-storage`) fix, `cargo build
--workspace --all-targets` failed three times in a row with `No space left
on device` — first inside `animusd`'s own test-binary compilation, then as
an outright inability for the sandbox's own tool-output tempfs to write at
all (`ENOSPC`, 0MB available). The failing crates (`animusd` test binaries)
had nothing to do with the change under test; `cargo clippy --workspace
--all-targets --all-features -- -D warnings` had *already* compiled the
same full set of targets cleanly moments earlier, with disk to spare.
Between the clippy run and the build attempt, several sibling sessions in
the same container (other stack layers, per Session operating mode item 5)
were independently building into the same `CARGO_TARGET_DIR`, and the
`target/debug/deps` + `target/debug/incremental` directories alone
accounted for essentially the whole filesystem's usable capacity.

Deleting `target/debug/incremental` (a pure compilation cache — cargo
regenerates it, at worst forcing a slower non-incremental recompile of
whatever it invalidates, never corrupting anything) reclaimed several GB
each time, immediately un-wedging a completely full disk and even
`ENOSPC`-broken tool output. But each reclaim was consumed again within
roughly a minute by the same concurrent sibling builds, so a single agent
cannot durably "fix" the disk pressure by cleaning alone while others are
still building — it can only buy itself one attempt's worth of headroom.

## The lesson

1. **A shared build resource can fail as outright ENOSPC, not just as the
   slowdowns/stale-binary risk the existing "one session per workstream"
   entry (2026-09-16) already covers.** Both are the same root cause (too
   many heavy builds sharing one `CARGO_TARGET_DIR`/container); ENOSPC is
   just the disk-axis version of the same contention, and it can appear
   suddenly even when an equivalent, equally expensive command (`clippy
   --all-targets`) succeeded moments before — the failure signature by
   itself does not distinguish "my change broke compilation" from "the
   disk emptied out from under me."
2. **`rm -rf target/debug/incremental` is a safe, fast, several-GB reclaim
   when a shared target dir is critically full** — it is a cache, not
   durable build output, so removing it (even mid another session's build)
   at worst costs that session an incremental-recompile of whatever unit
   it was mid-compiling, never a corrupted or wrong artifact. Prefer it
   over touching `target/debug/deps` (final linked artifacts another
   process may be actively reading).
3. **This is a one-shot reprieve, not a fix**, when other sessions are
   still actively building: expect the reclaimed space to be consumed
   again within roughly a minute, and don't loop retrying a full
   `--workspace --all-targets` build hoping contention will resolve itself
   — it wastes the very resource everyone is contending for. If a crate's
   own scoped tests, fmt, and clippy (which already compiles every target)
   are green, treat a subsequent `cargo build --workspace --all-targets`
   ENOSPC failure as an environment-contention data point to report, not a
   gate the change itself is failing — but still report it plainly rather
   than silently skipping the gate.
