# A canary-checked test binary can still be swapped between build and run when a shared `$CARGO_TARGET_DIR` is used under per-command `flock` — hold the lock across build *and* run, not just the build

Working issue #662 in a worktree sharing `$CARGO_TARGET_DIR` with other
concurrent agent sessions (the documented mitigation: wrap every `cargo`
invocation in `flock /path/.lock <cmd>`), a new real-socket integration
test (`control_membership_admin::
control_member_add_accepts_a_hostname_dial_address`) passed cleanly right
after being written, then failed deterministically — three runs in a
row — with an error (`"invalid socket address syntax"`) that could only
come from the *pre-fix* code this same PR had already replaced. The
worktree's own source was correct throughout (`grep` confirmed the fixed
`String`-typed field); `cargo test ... --test control_membership_admin
--no-run` reported `Finished in 0.18s` (no rebuild) each time, and the
binary's own `--list` output already named the new test — the canary
check the standing brief calls for passed.

**The canary check alone is not enough.** `flock`ing each `cargo`
invocation *individually* only guarantees no two `cargo` processes write
the shared target dir at the same instant — it does not stop a **different
worktree's own later, separately-flocked build** from completing and
overwriting the identical-hash artifact *in the gap between this
worktree's build finishing and its next flocked test-run command
acquiring the lock*. Two worktrees on the same issue (or two independently
converging near-identical diffs) can produce path-independent artifact
fingerprints (per the companion 2026-09-04 lesson) that collide on name
*and* apparent freshness, so a `--no-run`/`--list` canary check right
before running proves only "a binary with this name and this test list
exists right now" — not "the binary about to actually execute is the one
this check just inspected". The gap between the check and the run is
exactly where the swap happened here.

**Fix that actually closed the gap**: touch the changed source files to
force a real recompile, then hold **one single `flock`** across the build
*and* every subsequent run of the resulting binary in the same shell
invocation — `flock $LOCK bash -c 'touch <files> && cargo test ... --no-run
&& "$BIN" --test-threads=1 && "$BIN" ... && "$BIN" ...'`. Since every other
worktree in this environment also flocks its own cargo calls on the same
lock file, holding it continuously from build through all three stability
runs means no other worktree's build can land in between — the three runs
came back consistently green afterward, confirming the earlier failures
were the artifact swap, not a real defect in the fix.

**General form**: in a shared-`$CARGO_TARGET_DIR` sandbox, "build, then
canary-check, then run" is three separate lock-acquisitions with two gaps
an interleaving build can exploit — collapse "run the freshly built
binary" into the *same* locked command as the build itself (or otherwise
never release the lock between "cargo says this is fresh" and "I actually
execute it") whenever a result looks like it contradicts source you have
just re-read and confirmed is correct. Don't accept "the source is right
but the test fails" as a real regression until you've ruled this out —
and don't accept a suspicious pass either, for the same reason.
