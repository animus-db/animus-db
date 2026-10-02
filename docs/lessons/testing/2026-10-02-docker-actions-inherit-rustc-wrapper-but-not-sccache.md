# A Docker-container GitHub Action inherits `RUSTC_WRAPPER=sccache` but not the binary

When a job sets `RUSTC_WRAPPER: sccache` at job level, every step's env gets it,
including container actions such as `EmbarkStudios/cargo-deny-action` (it runs
`cargo metadata` inside its own image, where `sccache` does not exist). Result:
`could not execute process 'sccache .../rustc -vV' (never executed)`, a red
`lint` job even though fmt/clippy/build all passed. The sccache "Compilation
failures" counter is a red herring here; read the log for the first `ERROR`.
Fix: give that step `env: RUSTC_WRAPPER: ""` (cargo treats empty as unset).
Any new container action in a sccache job needs the same.
