# Sharing one `CARGO_TARGET_DIR` across two worktrees can serve stale crates

**What happened (2026-10-05, G-01 merges):** to save disk, a second worktree
was built with `CARGO_TARGET_DIR` pointing at the main checkout's `target/`.
The two checkouts were on different branches, and `animus-placement` differed
between them. A later `cargo clippy` in the main checkout reused the other
branch's `animus-placement` artifact and failed with `unresolved import
animus_placement::ZONE_LABEL` / `cannot find function zone_spread_policy`,
although the source on disk defined both. Touching
`crates/animus-placement/src/lib.rs` forced a rebuild and the errors vanished.

**Why:** cargo's freshness check for a path dependency compares mtimes against
the recorded fingerprint. Two checkouts writing the same unit into the same
target directory can leave a fingerprint that looks fresh for the wrong source.

**Rule:** give each worktree its own target dir. If disk forces a shared one,
`touch` (or `cargo clean -p`) every crate that differs between the branches
before trusting a build, and treat an "unresolved import" for an item that
plainly exists in the source as this staleness first, not a merge bug.
