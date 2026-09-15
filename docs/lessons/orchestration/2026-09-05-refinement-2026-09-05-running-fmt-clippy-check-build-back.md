# Refinement (2026-09-05): running `fmt`/`clippy`/`check`/`build` back to back in one session — not a parallel fan-out, just the ordinary per-push gate sequence — is itself enough to exhaust the disk, because each invocation mints its own content-hashed artifact filenames and cargo never garbage-collects a superseded one.

**Refinement (2026-09-05): running `fmt`/`clippy`/`check`/`build` back to
back in one session — not a parallel fan-out, just the ordinary per-push
gate sequence — is itself enough to exhaust the disk, because each
invocation mints its own content-hashed artifact filenames and cargo
never garbage-collects a superseded one.** `cargo clippy
--all-targets`/`cargo check --all-targets`/`cargo build --all-targets`
each compile the *same* ~100 test binaries under slightly different
flags, so `target/debug/deps/` accumulates several `-<16-hex-hash>`-named
copies of every one (e.g. `animusd-03e1a1...`, `animusd-9e4ac3...`,
`animusd-1f7ddd...`, `animusd-cb08f0...`, each 90-100 MB) — only the
newest actually backs the current build; the rest are pure dead weight
cargo has no reason to ever revisit. This is a different shape than
either existing entry above: it isn't the incremental cache (already
empty — `rm -rf target/debug/incremental` reclaimed almost nothing, ~44
MB, when tried first) and it isn't "delete every linked test binary" (that
would also delete the copies the *next* build still needs, forcing a full
relink of everything rather than nothing). **What actually worked**: group
`target/debug/deps/*` by filename with its trailing `-[0-9a-f]{16}` hash
stripped, and for every basename with more than one file, delete every
copy except the newest by mtime. On this repo that reclaimed **13.47 GB**
from 237 stale files in one pass, with zero rebuild cost — the very next
`cargo build --workspace --all-targets` found every dependency rlib it
needed already fresh and only relinked the handful of targets that had
actually changed. General rule: before reaching for `cargo clean` (nukes
everything, forces a full recompile of ~150 dependency crates) or an
executables-only sweep (still forces a full relink of every test binary),
check whether the bulk of `target/debug/deps/` is *duplicate* current-vs-
stale hash pairs from having run several separate gate invocations in one
session — pruning to one file per basename is strictly cheaper and just
as effective. **A second-order gotcha this hit**: at genuinely 0 bytes
free, even the `Edit`/`Write` tools failed with a raw `ENOSPC: no space
left on device, write` (a different message shape than the harness's own
"temp filesystem is full" Bash wrapper above, but the same underlying
cause) — so a repo-file edit failing with that exact message is a disk
symptom, not a tool bug or a signal to retry the edit differently; check
`df -h /` before assuming anything about the edit itself.
