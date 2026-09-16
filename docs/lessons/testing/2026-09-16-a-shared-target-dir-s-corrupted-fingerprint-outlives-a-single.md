# A shared `CARGO_TARGET_DIR`'s corrupted fingerprint for a dependency crate can outlive a single continuous `flock`-protected build+test invocation — `cargo clean -p` the affected packages first

Building against a shared `CARGO_TARGET_DIR` across concurrent worktree
sessions (each on its own branch, sharing the dir purely for disk/build
efficiency) already has a known hazard: two worktrees' builds of "the
same" path dependency can overwrite each other's rlibs, since Cargo's
artifact hash doesn't include the dependency's filesystem location. The
mitigation this repo's own working discipline already prescribes — hold
one continuous `flock` across a build AND the test run that consumes its
binary — closes the specific race where a *swap* happens in the gap
between two separately-locked commands. It does **not** close a related
but distinct failure mode found working issue #950: a *stale fingerprint*
for a dependency crate, written by another worktree's build that ran
**before** this session ever acquired the lock, can survive being
overwritten even by a fresh, correctly-sourced rebuild inside a single
continuous locked invocation — `cargo test -p animusd --lib --no-run`
repeatedly reported `error[E0599]: no variant named X found` for an enum
variant that unquestionably existed in this worktree's own source (`git
status`/`grep` both confirmed it), immediately after a `cargo build
-p animus-env -p animus-node -p animusd --lib` in the SAME locked shell
invocation had just finished cleanly. The two commands select different
Cargo profiles (plain `dev` vs. `test`), and evidently that difference is
enough for the test-profile fingerprint slot to have been left pointing
at another worktree's stale artifact from before this session's own lock
was ever acquired, with nothing in the immediately-preceding same-session
build touching (or invalidating) that specific slot.

**What actually worked, every time it was tried**: `cargo clean -p
<every affected package>` (the changed crate and everything downstream
that names its changed symbols) immediately before the build-and-test
sequence, inside the same locked invocation. This forces Cargo to
recompile every affected crate's every profile from this worktree's own
source with no fingerprint to (possibly wrongly) trust — strictly more
expensive (a full crate-group rebuild, ~60-90s for a 6-8 crate chain in
this workspace) than relying on incremental fingerprinting, but the only
approach that reliably produced a build actually reflecting the edits
just made, across roughly six repeated corruption incidents in one
session.

**The generalizable rule**: in a sandbox where a shared build cache
directory is deliberately reused across independent, concurrently-active
worktrees, do not trust "no changes reported, so it must be using my
source" from a partial build (one profile, one target selection) as proof
that a *different* profile/target selection sharing the same dependency
crates will too — a corrupted fingerprint is scoped per (crate, profile,
target-selection) tuple, and a clean build of one tuple says nothing
about the others. When a build inexplicably fails to see an edit that
`git status`/`grep` both confirm is present in the source tree, the
fastest reliable recovery is `cargo clean -p` the specific packages in
the dependency chain from the edited crate up through the crate under
test, not a longer investigation into why the fingerprint should have
been fine.
