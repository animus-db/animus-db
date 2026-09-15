# A safety fix scoped to "files that already opt into the shared support module" silently excludes every hand-rolled fixture — grep for the hazard itself, not for the module that happens to carry the fix (issue #511 residual, `animusd`'s `PanicSafeTempDir` migration)

PR #533 introduced `tests/support::PanicSafeTempDir` (leaks its directory
instead of removing it on a panicking drop, so a mid-test assertion
failure can't cascade into the control-plane WAL's unconditional
`.expect("wal append"/"wal sync")` panic — see that type's own doc and
`animusd/CLAUDE.md`'s Tests section for the full mechanism) and applied it
to the 59 `tests/*.rs` files that already had `mod support;`. That scoping
choice was reasonable for the PR's own size, but it meant the fix's
*reach* was defined by an incidental fact (which files happened to already
pull in the shared module) rather than by the hazard it closes (which
files construct a bare `TempDir` at all) — the two sets overlapped
heavily but weren't the same, and the ~42 files in the gap (several
hand-rolling their own bring-up, including the original issue #273 site,
`dynamo_index_scan.rs::setup()`) stayed exposed to the exact panic this PR
existed to prevent, silently, until a later residual sweep closed it.
**The general lesson**: when a safety fix is delivered as "add a helper to
the shared support module, and convert its current users," treat that as
two separate questions — is the *fix* right, and is its *reach* right —
and answer the second by grepping for the raw hazard pattern
(`TempDir::new`/`tempfile::tempdir` here) across the whole directory, not
by trusting that "uses the shared module" and "needs the fix" are the same
population. They're the same only when every fixture in the tree already
goes through one shared entry point — worth checking explicitly, not
assuming, the first time a directory this size is scoped.

**A second, narrower lesson from closing the gap**: the migration itself
was purely mechanical (`mod support;` + swap the bare constructor for
`support::panic_safe_tempdir()`) for every `tests/*.rs` file, but eight
in-crate `#[cfg(test)] mod`s inside `src/` (`dynamo.rs::
stream_write_path_tests`, `index_drain.rs::{gsi_drain_cursor_tests,
stream_sealer_tests}`, `lib.rs::{confirm_futility_tests,
forward_transport_failure_tests, halted_shutdown_tests, issue_412_tests,
issue_298_conflict_tests}`, `segment_janitor.rs::orphan_reap_tests`,
`admin.rs::system_table_tests`) construct a bare `TempDir` too and
**cannot** reach `tests/support` at all — it's a separate crate from
`src/`'s point of view (each `tests/*.rs` file is its own binary crate).
Closing that residual would need a *production-side* panic-safe temp-dir
helper shared across `src/`, which is a materially different, larger
design decision (do test-only semantics belong anywhere near production
code, even behind `#[cfg(test)]`?) than converting an existing test-only
helper's callers — don't fold that decision into a "migrate the stragglers"
task by inventing the helper unilaterally; name the residual and let it be
scoped on its own.
