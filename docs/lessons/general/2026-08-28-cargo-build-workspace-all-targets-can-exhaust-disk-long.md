# `cargo build --workspace --all-targets` can exhaust disk long before `cargo clippy --workspace --all-targets` does — they are not equivalent load for a space budget

On a workspace this size, `cargo clippy --all-targets --all-features`
type-checks every test/bench binary in every crate (including
`animus-operator`'s `kube`-client dependency tree) without linking full
executables for most of them, so it fits comfortably in a constrained
`target/` budget. `cargo build --workspace --all-targets` does the full
codegen + link for the *same* set of binaries — every `#[test]`
binary and bench in every crate, not just the ones a given task's gate
list actually needs — and on a session with single-digit GB of headroom
this can run the disk to zero mid-build (`rustc-LLVM ERROR: IO failure on
output stream: No space left on device`), which then fails *unrelated*
crates (`animus-storage`'s `lsm_clone_concurrent` test, `animus-cp-data`'s
`apply_signal` test) that have nothing to do with the change in flight —
a confusing false signal if read as "the change broke something," when the
actual cause is disk exhaustion from building targets the task's own gate
never asked for. This repo's stated validation gate is `cargo build
--workspace` (no `--all-targets`); reach for `--all-targets` only when a
task specifically needs every test binary to *build* (not just
type-check), and clean `target/debug/{deps,build,incremental}` proactively
between build attempts on a constrained disk rather than after the error —
the clippy pass already proved the code type-checks, so the extra
`--all-targets` build was buying confirmation the task didn't ask for at a
cost the disk couldn't afford.
