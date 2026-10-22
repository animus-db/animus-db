# A shared CARGO_TARGET_DIR serves another worktree's workspace crates

Two worktrees of the same workspace that share one `CARGO_TARGET_DIR` collide:
cargo's metadata hash for workspace members uses the workspace-relative path, so
both trees write the same artifact names. A build in tree A can then be reported
"fresh" and link tree B's rlib, or a test binary can silently lack modules that
exist only in A (`--list` shows 0 of your new tests; compile errors name items
your tree defines). Symptom to recognise: new tests are "filtered out", or
`unresolved import` for something you just added.

Fix: `touch` each changed crate's `src/lib.rs` immediately before every cargo
invocation (and before final verification), build and test in one command, and
set `CARGO_INCREMENTAL=0` to save disk. Prefer separate target dirs when the disk
allows it.

Related sim-harness trap found the same day: a `SimCluster` that idles through a
long finalize ladder after its table was created needs many client calls before
the first one is served (quiesced groups); open version gates before creating the
table.
