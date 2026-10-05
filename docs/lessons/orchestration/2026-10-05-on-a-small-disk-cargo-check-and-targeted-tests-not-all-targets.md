# On a small disk, use `cargo check` and targeted tests, not `--all-targets` builds

A full `cargo build/test --workspace --all-targets` links about 100 test
binaries (the `animusd` crate alone has ~100 `tests/*.rs` targets). On the
session container (about 250G volume shared with other agents' target dirs) that
exhausted the disk twice while implementing #1185: a Write failed with ENOSPC and
the linker died with Bus errors.

**Rules that worked.**

- Gate clippy with `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  (it type-checks without linking, so it is cheap on disk) and run *tests* only
  for the changed crates with a module filter (`--test it <module>::`, `--lib`).
- Use a dedicated `CARGO_TARGET_DIR` with `CARGO_INCREMENTAL=0` for the session.
- Run one heavy cargo at a time and watch `df -h` between steps.
- Before deleting anything to reclaim space, identify whose it is. Only an idle
  incremental cache of your own build is fair game; never touch another agent's
  target directory.
