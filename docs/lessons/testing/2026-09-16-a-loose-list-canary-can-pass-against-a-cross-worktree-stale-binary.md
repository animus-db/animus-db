# A `--list` canary that only checks a module/family count can pass against a cross-worktree-stale binary — assert the exact new test names

Resolving a merge conflict in one worktree (`/tmp/wt-662`) that pulled in
commits already built and tested in a sibling worktree (`/tmp/wt-954`)
sharing the same `CARGO_TARGET_DIR`: `cargo test -p animusd --lib
sim_cluster_control_membership_admin --no-run` reported "Finished in
0.15s" (far too fast to have actually compiled `animusd`, whose real build
takes ~60-100s) and the resulting binary's `--list` showed only 24 tests
under that module — a real, pre-existing count from *before* this
session's own 4 new tests were merged in, even though the on-disk source
file in this worktree was verified byte-identical to the other worktree's
(already-passing) copy. A canary that only checked "how many tests match
this module prefix" did not catch it, because 24 is itself a plausible,
non-zero, non-suspicious count — only checking for the 4 *specific* new
test names (`grep "control_member_add"` against `--list`) revealed they
were entirely missing.

**Root cause**: Cargo's on-disk fingerprint for a unit built from a path
dependency does not key on the dependency's absolute filesystem path (this
is the same mechanism `docs/lessons/testing/2026-09-16-a-canary-checked-
test-binary-can-still-be-swapped-between-build.md` documents for a
mid-window swap) — so a fresh `cargo test --no-run` invoked from a
*different* worktree than the one that last populated the shared target
dir can find an existing fingerprint entry it considers "fresh enough"
and skip recompilation entirely, silently serving the other worktree's
stale artifact rather than erroring or rebuilding. Holding one continuous
`flock` across the build-and-run pair (the existing lesson's own fix)
prevents a *mid-window* swap by a concurrent build, but does **not**
by itself force Cargo to *notice* that a sequential, already-finished
build from a sibling worktree left behind a fingerprint that doesn't
match this worktree's actual source.

**Fix that worked**: `touch` the changed source file(s) immediately
before the `--no-run` build, inside the same flock hold, to force a
`mtime`-based fingerprint miss and a genuine recompile — then re-run the
`--list` canary and confirm it now shows the expected count *and* the
specific new test names before trusting a subsequent run.

**General form**: in a shared-`CARGO_TARGET_DIR` multi-worktree setup, a
`--list` canary must assert the *specific* test name(s) this change is
supposed to have added or changed, never just a family/module count or
"the binary exists and lists something" — a stale artifact built from a
different worktree's earlier, still-plausible-looking test surface can
pass a loose canary silently. When in doubt, `touch` the just-merged or
just-edited files before the build that is about to be trusted.
