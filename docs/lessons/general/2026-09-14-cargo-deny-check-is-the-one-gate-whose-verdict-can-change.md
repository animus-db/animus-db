# `cargo deny check` is the one gate whose verdict can change with no commit at all: a stack green at PR time can land red on `main` (RUSTSEC-2026-0285, 2026-09-14)

The C-14 stack (#876-#888) was green on every PR head, merged as one
stack at 16:24 UTC, and `main`'s own CI on the merge commit came back
red — on `cargo-deny check` alone, every other job (fmt, clippy, build,
the whole sim tier, the four `prod-liveness` shards) green. `Cargo.lock`
had not changed once during the rung. What changed was the world:
RUSTSEC-2026-0285 (`rustls` 0.23.43, TLS 1.3 handshake messages accepted
across an encryption-level boundary, fixed in 0.23.45) was published
between the last PR-head CI run and the merge. The fix was the advisory's
own remedy, `cargo update -p rustls`, as its own flat PR (a one-line
`Cargo.lock` bump is a single reviewable step, so no stack). **General
form**: advisories are a function of wall-clock time, not of the diff, so
(1) a red `cargo-deny` job on `main` after a merge is not evidence the
merged change was wrong — read the advisory ID before assuming so, and
(2) it is still a red `main`, so it is still fixed now, in the same
session, per Session operating mode item 4 — never left for the nightly,
and never silenced with an `ignore` entry when a patch release exists.
Every other open PR based on the same `main` inherits the same red
`cargo-deny` job until it rebases onto the fix; that is expected, not a
second bug.
