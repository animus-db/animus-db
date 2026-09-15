# Follow-up to the disk-space entry above: when it genuinely is ENOSPC

**Follow-up to the disk-space entry above: when it genuinely is ENOSPC**
(real "No space left on device" errors from `rustc`/the linker, not a
garbled compile error), `rm -rf target` alone often doesn't create enough
headroom to finish a `--workspace --all-targets` build — this workspace's
~90 `animusd` integration test binaries each statically link the same
large dependency set (tokio, opentelemetry, reqwest, icu\*, …) with full
debug info by default, and linking one of them can itself need several
hundred MB of scratch space. The fix that actually restores headroom
without touching the checked-in `Cargo.toml` (no `[profile]` section
exists there, so this is a machine-local, non-committed change): add
`[profile.dev]`/`[profile.test]` `debug = 0` (plus `incremental = false`
if the incremental cache itself is a large chunk of the growth) to
`$CARGO_HOME/config.toml` (e.g. `/root/.cargo/config.toml`) — Cargo reads
`[profile.*]` from config files, not only from a manifest — which cuts
every test binary to a fraction of its debuginfo-enabled size and turns a
session that can't link `cargo test --workspace` even once into one that
fits comfortably. Confirm it took effect via the build summary line
(`unoptimized` vs. `unoptimized + debuginfo`), and check `df -h` before
and after a `rm -rf target` — if avail space right after the wipe is
already within a few hundred MB of what one large link step needs, the
wipe bought too little margin and the very next build can ENOSPC again
mid-link.
