# Run `cargo test --workspace` after *each* merge, not just at the end of a batch.

**Run `cargo test --workspace` after *each* merge, not just at the end of a
batch.** Batching the gate run let a regression onto main via an earlier
merge before it was caught. All five gates (fmt, clippy `--all-features
-D warnings`, build, test, `cargo deny`) green per merge.
