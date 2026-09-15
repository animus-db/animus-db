# A shared `CARGO_TARGET_DIR` can serve a genuinely STALE test binary while `cargo test` reports `Finished ... in 0.1s` and no error

**A shared `CARGO_TARGET_DIR` can serve a genuinely STALE test binary
while `cargo test` reports `Finished ... in 0.1s` and no error** — caught
chasing issue #278's streams_e2e flake fix: a loop of concurrent
`cargo test` invocations (my own diagnostic script running several copies
in parallel against the identical worktree + target dir, on top of other
agents' own concurrent builds) produced panics whose message text
(`"assertion \`left == right\` failed: ..."`) matched code I had already
*replaced* with a retry loop — the binary being executed hadn't picked up
the edit, even though the very same target dir's `cargo build` had
compiled it correctly moments earlier. Root cause not fully isolated (a
fingerprint/mtime race between concurrent `cargo` processes hammering one
target dir is the leading theory), but the fix that made the signal
trustworthy again was mechanical: `touch` the changed source file, run a
single `cargo build` alone (not concurrently with any other `cargo`
invocation against the same target dir), confirm via `strings
target/debug/deps/<bin>-<hash> | grep <a string unique to the new code>`
that the *specific* binary hash about to run actually contains the edit,
and only then start a validation loop — and keep that loop to ONE
`cargo test` process at a time (sequential iterations), never several
invocations of the same test binary launched concurrently against a
shared target dir. **General rule**: on a shared-`CARGO_TARGET_DIR` box,
don't trust a failing (or passing) test loop's signal until you've
positively confirmed the binary under test contains your latest edit —
a "Finished" message is not proof of that, and a panic whose message
text doesn't match anything in the current source is a tell that you're
looking at a stale binary, not a new bug. (`animusd/tests/streams_e2e.rs`,
issue #278.)
