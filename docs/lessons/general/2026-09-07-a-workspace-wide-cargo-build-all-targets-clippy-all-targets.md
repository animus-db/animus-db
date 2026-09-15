# A workspace-wide `cargo build --all-targets`/`clippy --all-targets --all-features` pulls in every crate's full dependency tree (kube, reqwest, opentelemetry, rustls...) even for a change confined to two crates — budget disk accordingly, and dedupe `target/debug/deps` between runs (2026-09-07, W-07 PR 2)

A PartiQL change touching only `animus-dynamo`/`animusd` still requires
the *workspace* gates (`cargo build --workspace --all-targets`, `cargo
clippy --workspace --all-targets --all-features -- -D warnings`) before a
PR is done — and those pull in `animus-operator`'s full `kube`/
`k8s-openapi`/`rustls`/`reqwest`/OTLP dependency graph along with
everything else in the workspace, regardless of which crate the actual
diff touched. On a sandbox with a fixed (not literally-disk-sized) quota —
`df -h /` can report a large nominal `Size` with `Avail` reflecting a much
smaller quota boundary instead of real free bytes — a single such build
consumed the remainder of an already-tight quota mid-run and drove `Avail`
to effectively zero, which then broke unrelated things: the tool
harness's own small tmp output-capture filesystem started returning
ENOSPC on completely unrelated commands (`ls`, `rm`), because it shares
the same quota. Recovery was: delete stale duplicate `target/debug/deps`
binaries first (multiple hash-suffixed versions of the same crate/test
accumulate release over release; keep only the newest per basename — a
`(-[0-9a-f]{16})(\.\w+)?$` suffix strip gives the basename to group on),
which alone freed double-digit GB and unblocked everything. **Lesson for
any task that must run full-workspace gates on a large multi-crate repo
under a disk quota**: dedupe `target/debug/deps` *before* the first
full-workspace build, not only after hitting ENOSPC, and treat "avail
suddenly near zero right after a `--workspace --all-targets` build" as
the expected shape of the problem, not a mystery — the fix is almost
always duplicate build artifacts, never a leak in the code under test.
