# Pruning stale duplicate `target/debug/deps` binaries under `if disk < 4 GB` must never run while a build using that same target directory is in flight

A workspace build (`cargo build --workspace --all-targets`) ran the target
directory down to ~300 MB free mid-build; the documented mitigation
("prune stale duplicate test binaries in `target/debug/deps`, newest per
basename only") was applied *while that same build was still running*,
filtering candidates by "not modified in the last 20 seconds" to avoid
touching anything the active build might still be writing. This was not
enough: cargo had already **finished** producing several dependency
`.rlib` files earlier in the build (their mtimes were long past the
20-second window) but had not yet reached the later link steps that
`open()` them — deleting the "stale" duplicate copy of one such rlib is
indistinguishable, from mtime alone, from deleting a genuinely-orphaned
one, and the running build's own linker immediately failed with `cannot
open .../libserde_json-*.rlib: No such file or directory` for several
targets, wasting that portion of the build. **General form**: an mtime
threshold answers "was this file recently *written*," never "is this file
still *needed*" — a file a build produced minutes ago can still be read
minutes later, at a link step far downstream of its own compile step; a
running build's target directory is not a safe pruning target *at all*
while that build is in flight, however old the candidate files look. The
correct sequence is: let the build finish or fail on its own, prune
afterward, then retry the build — never prune concurrently with the build
whose own output directory is being pruned. (The retry after this
particular incident succeeded once free space was restored, so no lasting
harm — but the same race with a less patient CI timeout could have turned
one disk-pressure warning into a build failure needing a second attempt
regardless.)
