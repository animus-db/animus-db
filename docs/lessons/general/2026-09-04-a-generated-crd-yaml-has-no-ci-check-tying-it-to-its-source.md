# A generated CRD YAML has no CI check tying it to its source struct — it silently drifts, and a deleted CLI flag stays hand-emitted until someone notices (issue #590)

`animus-operator`'s `deploy/operator/crd.yaml` is generated output
(`cargo run -p animus-operator -- crd`, `kube::CustomResourceExt::crd()`
serialized via `serde_yaml`) from `AnimusClusterSpec` in
`crates/animus-operator/src/crd.rs`. Nothing in CI regenerates it or diffs
it against the struct — no test, no workflow step — so the committed file
can go stale in two independent ways at once, discovered only when someone
happens to regenerate it for an unrelated change:

1. **Doc-comment drift**: `quiesce_after_secs`/`auto_split_bytes`'s Rust
   doc comments had already been rewritten for S-06 (moved from a CLI flag
   to the generated `cluster.json`'s `cluster_settings` section), but the
   checked-in `crd.yaml`'s `description:` fields for those properties still
   quoted the old, pre-S-06 text — found only while regenerating the file
   for this issue's unrelated `splitMode` removal.
2. **A field that outlived what it plumbed to**: `animusd`'s `--split-mode`
   flag and the copy-based split workflow it selected were deleted outright
   2026-09-01 (ADR 0058's rung 4 layer), but `AnimusClusterSpec.split_mode`
   and `entrypoint_script`'s conditional `--split-mode {mode}` emission
   were not removed in the same change — a separate crate, a separate PR,
   no compiler link between "this flag no longer exists" and "this operator
   field still emits it." Any `AnimusCluster` spec that set `splitMode`
   would fail at pod startup (`animusd` rejecting an unknown argument) with
   no test catching it, since the existing unit test
   (`entrypoint_omits_split_mode_on_data_branch_only`) asserted the flag
   *was* emitted on the combined branch — it was pinning the bug, not
   guarding against it.

**General form**: when a CLI's accepted-flags surface changes (a flag
deleted, renamed, or newly subcommand-restricted), grep every *generator*
of invocations of that CLI — not just other Rust crates that call into a
shared library — for the literal flag string: shell-script templates,
generated YAML/JSON, docs that list "flags this component passes through."
A hand-rolled arg parser (no `clap` derive, so no shared type to break the
build) makes this doubly silent: removing a match arm compiles cleanly
everywhere else. The regression test this issue added
(`entrypoint_flags_are_all_accepted_by_animusd`) generalizes past this one
flag: it tokenizes the generated script for every `--xxx`-shaped token and
checks it against an explicit allowlist transcribed from the target
binary's actual arg-parsing `match` arms (not its doc comments, which is
exactly what had gone stale) — any future flag deletion/addition on either
side now fails this test immediately instead of waiting for a live
pod-startup failure.
