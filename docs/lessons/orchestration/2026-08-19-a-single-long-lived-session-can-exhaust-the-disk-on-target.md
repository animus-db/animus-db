# A single long-lived session can exhaust the disk on `target/` alone, with no parallel fan-out involved (2026-08-19)

**A single long-lived session can exhaust the disk on `target/` alone, with
no parallel fan-out involved (2026-08-19)** — a solo `cargo build
--workspace --all-targets` on this repo hit `rustc-LLVM ERROR: IO failure …
No space left on device` and `ld terminated with signal 7 [Bus error]` with
`target/debug` alone at 30 GB against a filesystem reporting single-digit
megabytes free (despite a large nominal size — the real quota is much
smaller than `df`'s `Size` column implies on this harness). `cargo clean`
reclaimed the full 30 GB in seconds and the rebuild succeeded; there was no
need to hunt for a partial/targeted clean. **Rule:** if `cargo
build`/`test`/`clippy` fails with an I/O or linker error whose message
mentions space (not a compile error), check `df -h` before debugging the
"failure" as if it were a code problem, and prefer a full `cargo clean`
over trying to selectively prune `target/` — the incremental cache is the
overwhelming majority of the size and buys little across a full rebuild
anyway. **Refinement (2026-08-24):** when the *linked test binaries*
dominate rather than the incremental cache — `du -sh target/debug/deps`
showing tens of GB of ~150-200 MB-each executables, one per `tests/*.rs`
file, is the tell — deleting only those executables (`find target/debug
target/debug/deps -maxdepth 1 -type f -executable ! -name '*.so'
-delete`) frees the same space in seconds while leaving every compiled
`.rlib`/`.rmeta` dependency artifact intact, so the next `cargo build`/
`test` only **relinks** the binaries it actually needs instead of
recompiling ~150 dependency crates from scratch. A full `cargo clean` is
still the right call when the incremental cache itself is the bulk of the
size (the original 2026-08-19 case, `target/debug/incremental` at 600
MB+); check which directory is actually large before choosing between
the two — they solve the same symptom at very different costs.
**Refinement (2026-08-24, issue #374 C2): at genuinely 0 bytes free (not
just "low"), the agent harness's own Bash tool output capture fails with
ENOSPC too** — `df`, `echo hi`, even a `Write` truncation of an unrelated
file all failed with the harness's own "temp filesystem is full" or
`ENOSPC` errors, on the SAME root filesystem the repo and `/tmp` share
here. Every synchronous (foreground) Bash call was unusable in this state.
What worked: issuing the cleanup (`rm -rf target`, no other commands
chained) with `run_in_background: true` — a background command apparently
needs less headroom to start than a foreground one needs to capture its
own output, and once it actually deleted enough (a full `rm -rf target/`,
not a partial executables-only sweep, since the workspace build that
caused this had already grown the whole tree past what executables-only
deletion could recover), ordinary foreground commands worked again.
**General rule**: if disk hits exactly 0 and even trivial read-only
commands (`echo`, `df`) start failing with a filesystem-full error, stop
trying to diagnose via Bash — go straight to a `run_in_background` deletion
of the single largest, safely-regenerable directory (a repo's own
`target/`), and only resume foreground commands once one such background
job reports genuine free space back. A `CARGO_PROFILE_DEV_DEBUG=0`
environment variable on every subsequent `cargo build`/`test`/`clippy`
invocation (not just the validation-pass advice below, but every single
command afterward) then keeps the rebuilt tree from reaching the same
ceiling again — debug info alone was the difference between builds that
fit in the freed headroom and ones that didn't, in the same session.
