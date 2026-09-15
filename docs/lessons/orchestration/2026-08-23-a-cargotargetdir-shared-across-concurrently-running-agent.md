# A `CARGO_TARGET_DIR` shared across concurrently-running agent worktrees can silently link one session's build against ANOTHER session's stale source

**A `CARGO_TARGET_DIR` shared across concurrently-running agent
worktrees can silently link one session's build against ANOTHER
session's stale source** (2026-08-23, discovered mid-fix on the
`KindBatchOutcome` false-ack above). Two sibling sessions building
crates at the same package/version/profile into the same shared target
dir produce output artifacts whose filename hash is derived from the
dependency/profile graph, not from the source files' own content or
absolute path — so a session on a branch that has NOT yet landed a
struct change (e.g. an unmodified worktree still on `main`) and a
session that HAS landed it can both write to the identical `.rlib`/
`.rmeta` path. Observed directly: `cargo test -p animus-cp-data --test
<new file>` failed to compile with `variant Accepted does not have a
field named term` and `expected Option<KindBatchOutcome>, found
Option<(_, KindBatchOutcome)>` — both flatly contradicted by `grep`ping
the very source files cargo had just reported compiling — while `ps
aux` showed a sibling session's `cargo build`/`cargo test` running
concurrently against the same `CARGO_TARGET_DIR` from a different
worktree path. The error vanished on the next attempt with no source
change, confirming a race rather than a real compile error. **Diagnosis
rule**: a compile error that contradicts what's actually on disk (the
compiler complaining about a shape the source doesn't have) is a
first-class signal to check for a concurrent `cargo`/`rustc` process
(`ps aux | grep cargo`) before debugging the "wrong" source — don't
trust a single failing compile as proof the edit is broken. **Mitigation
used here**: point `CARGO_TARGET_DIR` at a private, session-scoped
directory (e.g. under the scratchpad) for the remainder of validation,
accepting the slower from-scratch build, then re-verify once against the
shared dir for the final gate run. This is a real gap in the "shared
build cache" convention the root `CLAUDE.md`/session setup currently
documents as safe by default — it is only safe when every concurrently-
building session's *relevant* crates are source-identical, which is not
guaranteed for parallel agents mid-way through independent, uncoordinated
changes to the same crate.
