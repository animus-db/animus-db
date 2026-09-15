# Two disk exhaustions in one session: a debug `--all-targets` build of this workspace is ~24GB, and `cargo clean -p <crate>` is the surgical tool

**Two disk exhaustions in one session: a debug `--all-targets` build of
this workspace is ~24GB, and `cargo clean -p <crate>` is the surgical
tool** (2026-08-22). `target/debug/deps` reached 23GB across 94 test
binaries over 100MB each (debuginfo=2 × animusd's test-binary count).
Symptoms mislead: the failure surfaced as a linker `signal 7 (Bus error)`
and `ld terminated`, not an obvious out-of-space error, and once as
`failed to write dep-graph.part.bin`. `cargo clean -p animusd -p
animus-cp-data -p animus-dynamo` freed 17GB while keeping every
dependency build cached (a full `cargo clean` would have forced tokio,
serde and the rest to rebuild); deleting `target/debug/incremental`
alone freed 5GB more. Prefer targeted `cargo test -p <crate> --test
<name>` over full sweeps when disk-constrained — it also happens to
match how CI runs. **Addendum (2026-08-26): `CARGO_PROFILE_DEV_DEBUG=0`
avoids the problem at the source** rather than cleaning up after it —
running the five gates for a plumbing-sized PR (ADR 0059 Train 1 PR②)
hit the identical symptom (`cargo build --workspace --all-targets`
exhausting a fresh sandbox's whole root filesystem, not just this
workspace's `target/`, since `/tmp` shares the same device), even after
the incremental/`cargo clean -p` cleanup above: `target/debug/deps`
alone held 2493 files, 24GB of which was debug-info-laden test/bin
**executables** (not `.rlib`/`.rmeta`/`.d`, which incremental
compilation actually needs) from binaries that had already run and
would never be reused. Two independent fixes, both safe to combine:
(1) `find target/debug/deps -maxdepth 1 -type f ! -name '*.rlib' !
-name '*.rmeta' ! -name '*.d' -delete` reclaims a spent test binary's
space without invalidating any dependency crate's incremental cache
(unlike `cargo clean -p`, which does); (2) prefixing the build/test
invocation with `CARGO_PROFILE_DEV_DEBUG=0` (a `dev`-profile override,
not a workspace `Cargo.toml` edit — nothing to revert) drops full debug
info from every artifact for that invocation, shrinking a from-scratch
`cargo build --workspace --all-targets` from ~28GB to ~9GB. Prefer (2)
proactively before a from-scratch `--all-targets` gate run in a
disk-constrained sandbox; reach for (1) if a run has already ballooned
`target/` and a full `cargo clean` would be too slow to recover from.
**Addendum (2026-08-29): the disk is shared across sibling worktree
agents, not just your own crate's `target/`.** A gate run flip-flopped
between ENOSPC and 12GB free within minutes with zero commands run in
between — `du -sh /home/user/animus-db/.claude/worktrees/*/target
/home/user/animus-db/target` showed 12–17GB apiece in *other* agents'
worktrees and the base checkout, ballooning and draining on their own
schedule as those agents built and cleaned. `cargo clean` in your own
worktree is necessary but can be insufficient — check sibling worktrees'
sizes before concluding the sandbox itself is out of room, and retry a
failed gate once or twice before escalating, since a neighbor's build
finishing can free multiple GB with no action on your part. `cargo
check --workspace --all-targets` (type-checks everything the `#[deny]`
lint machinery cares about, no linking) is a much cheaper stand-in than
`cargo build --workspace --all-targets` when only verifying that a
lint-attribute or type-level change compiles, and `cargo test -p <crate>
--lib -j 1` (low parallelism caps peak concurrent temp-file usage) plus
a handful of `--test <name>`s targeted at the changed modules is a
reasonable substitute for a full per-crate integration suite (84+ test
binaries here) when the disk is this contended.
