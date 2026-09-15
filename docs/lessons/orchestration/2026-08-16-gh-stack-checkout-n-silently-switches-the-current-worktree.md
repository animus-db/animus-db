# `gh stack checkout <N>` silently switches the CURRENT worktree's checked- out branch

**`gh stack checkout <N>` silently switches the CURRENT worktree's checked-
out branch** — in a worktree-isolated agent, this is indistinguishable at a
glance from a scratch/tracking branch staying put, and it can happen mid-air
underneath a long-running background build or test. Finishing the ADR 0047
stack, a `gh stack checkout 228` run purely to inspect the stack's shape
(branch order, which PRs it already contained) reset this worktree's HEAD
from a local scratch branch to `intra/2-cutover` — while a `cargo test
--workspace` run was still executing in the background against the old
(correct) source tree. The build didn't crash immediately (already-linked
test binaries kept running), but the source tree was no longer the one the
run was supposed to verify, so its results couldn't be trusted — the safe
fix was killing the run, switching back, and restarting from scratch,
costing a full extra `cargo test --workspace` cycle. **General rule**:
before running any `gh stack`/similar branch-management subcommand (not
just the obviously-destructive ones), check `git branch --show-current`
immediately after — never assume a "read-only-sounding" stack-inspection
command left the working tree's checked-out branch untouched, and never run
one while a background build/test against the current tree is still in
flight.
