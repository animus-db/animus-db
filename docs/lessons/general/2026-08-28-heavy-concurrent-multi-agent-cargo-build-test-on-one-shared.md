# Heavy concurrent multi-agent `cargo build`/`test` on one shared `target/` dir can exhaust disk mid-gate, unrelated to the diff under test (2026-08-28)

Validating a small, self-contained `animusd` change (ADR 0061 rung A6) hit
repeated `error: ... No space left on device` / `couldn't create a temp
dir` / `LLVM ERROR: IO failure on output stream` failures from `cargo
build --workspace --all-targets`, `clippy`, and even single-test-binary
`cargo test` runs — not from any error in the code, but because several
other agent sessions were compiling in parallel against the *same*
`target/` directory (confirmed via `ps aux` showing concurrent `cargo
build --workspace --all-targets` from a different shell PID) on a
container whose real free space (`df -h /`, `Avail` column) is far smaller
than nominal size and swings from single-digit GB to under 100MB within
minutes as those builds run. `rm -rf target/debug/incremental` reliably
frees the most space per byte of risk (it is a recompute-only cache, never
a linked artifact another process depends on) but the freed space can be
consumed again within one more `cargo build --all-targets` invocation
(hundreds of MB per linked test binary, and this crate alone has ~100).
**Rules that held up**: prefer `cargo check`/`clippy` over `cargo build`
for a broad multi-crate sanity pass (no linking, far less disk); prefer
`-p <crate>` or a single `--test <name>` over `--workspace`/`--all-targets`
when disk is tight, since each linked binary is the expensive step, not
compilation; set `CARGO_INCREMENTAL=0` before a build run immediately
after clearing `incremental/` so the clearing isn't racing the same
build's own writes into it; and when a from-scratch full-workspace gate
genuinely cannot be completed, trust an **earlier, already-green run of
the identical byte-for-byte source** (verified via `diff -q` against a
saved copy) over repeatedly re-attempting a gate that fails purely on
`ENOSPC`/linker I/O errors — re-running it does not gain information once
the failure signature is unambiguously disk-exhaustion rather than a
compiler diagnostic. Never `rm -rf target/` itself while sibling sessions
may be mid-build; that destroys artifacts they still need mid-link, unlike
`incremental/`.
