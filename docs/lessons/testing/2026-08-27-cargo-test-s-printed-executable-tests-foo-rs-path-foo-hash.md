# `cargo test`'s printed `Executable tests/foo.rs (/path/foo-HASH)` line is the only reliable way to name the binary that matches current source

**`cargo test`'s printed `Executable tests/foo.rs (/path/foo-HASH)` line
is the only reliable way to name the binary that matches current
source** — the deps-dir filename hash derives from package id/deps/
profile, not source content, so the same hash is silently overwritten on
rebuild; hardcoding a previously-seen hash in a bisection/stress script
can run stale code with zero indication.
