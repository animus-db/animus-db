# A long chain of `cargo build`/`cargo test` runs across feature variants in one session can exhaust a fixed disk allowance mid-chain, and the failure it produces looks exactly like a compile bug, not a disk problem

**A long chain of `cargo build`/`cargo test` runs across feature variants
in one session can exhaust a fixed disk allowance mid-chain, and the
failure it produces looks exactly like a compile bug, not a disk
problem** — an ENOSPC-killed `rustc` surfaces as a plain nonzero exit
(often 101) with truncated/garbled output, the same shape as a real
compile error, so a session that hasn't been tracking free space burns
time debugging source code that was never broken. Check free disk space
before diagnosing a surprise compile failure that shows up late in a
session, especially right after a `--all-features`/multi-crate build
sweep.
