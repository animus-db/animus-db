# A shared `CARGO_TARGET_DIR` across worktree sessions can silently serve a stale test binary missing newly-added `#[test]` fns — `--list` (or a forced `touch`) before trusting a count (2026-08-30, `pitr_fault_corpus.rs` merge validation)

Resolving PR #490's `docs/engineering-lessons.md` merge conflict and then
validating with `CARGO_TARGET_DIR=/home/user/animus-db-shared-target cargo
test -p animus-test --test pitr_fault_corpus` (as instructed, to avoid
duplicating multi-GB build artifacts across worktrees) first reported **"12
passed; 0 failed"** — a clean-looking green run — even though the checked-out
file on disk plainly had 17 `#[test]` functions (confirmed by `grep -c
'^#\[test\]'`) and the PR's own commit message and crate-guide entry both
say "17 tests". `cargo test`'s own summary line only ever counts what its
test binary actually contains; it has no way to notice that the binary
itself is stale, so a naive "did the count match expectations and did
everything pass" check would have reported a false clean bill of health
while silently missing the five tests this very PR exists to add
(`wal_fsync_lie_kill_sealing_leader`,
`chaotic_network_pitr_rollover`/`chaotic_network_idle_group_never_
proposes_a_pitr_seal`, `wal_torn_on_crash_kill_sealing_leader`,
`restore_to_random_second_under_clock_drift`) — the exact set most likely to
carry a real bug, being new. `cargo test -- --list` against the same
un-touched binary reproducibly showed the same wrong 12; `touch`-ing the
source file to force a fresh `rustc` invocation (visible via `-v`) made the
list jump to the correct 17, all passing. The mechanism was never fully
isolated (candidates include a leftover fingerprint/object from an earlier
build of an older revision of this same file at this same worktree path,
whose mtime cargo's fingerprint check treated as not-older-than the fresh
checkout's), but the shared-target-dir setup itself is exactly the
precondition that makes a stale-fingerprint hit both possible and likely: the
same crate/test-target combination gets rebuilt from this same worktree path
across many separate sessions over time, all landing in one directory tree
cargo's own dep-graph fingerprinting was never designed to be shared this
widely. **The general check going forward**: whenever a validation run's
result matters (a merge/rebase gate, a "should be N/N" acceptance check) and
`CARGO_TARGET_DIR` points at a directory shared across worktrees/sessions,
don't trust the bare pass count — run `cargo test -- --list` (or diff the
printed test names against a `grep -c '^#\[test\]'` of the file) at least
once to confirm the binary's test set actually matches the source before
reading a "N passed" line as proof of anything; a stale binary reports
green precisely because it silently tests less, never because it tests the
same thing and fails to notice a problem.

**2026-09-05 follow-up — mechanism confirmed, rule hardened to "never
share a target dir across concurrently-active worktrees".** Two agents
building the same workspace from two worktrees (`.claude/worktrees/s02`
and `.claude/worktrees/w08b`) into one `CARGO_TARGET_DIR`, alternating,
produced (a) a `cargo test --workspace` in the main checkout that failed
with 59 "unresolved import `animus_control::Policy`" / "no variant named
`PutCredential`" errors although the checked-out `meta.rs` plainly
contained both — the `animus-control` rlib cargo linked against had been
compiled from the *other* worktree, whose files lacked the credential
catalog; (b) endless whole-tree rebuilds every time the trees swapped;
(c) a target dir that grew from 17G to 26G and a `rust-lld` crash with
"No space left on device". The mechanism: cargo identifies a workspace
crate's artifacts by a hash of its **workspace-relative** path (so a
target dir survives a workspace move), so two worktrees of the same repo
write byte-identical artifact *names* into a shared target dir, and
freshness is then judged by comparing source mtimes against the recorded
artifact — a tree whose files are *older* than the other tree's build
sees its neighbour's library as up to date and silently links against it.
A "green" gate computed that way proves nothing about the tree it ran in.
**Rule**: a shared target dir is only ever safe for *strictly serialized*
use from *one* worktree at a time, with the workspace-crate artifacts (at
minimum `target/debug/{incremental,deps,.fingerprint}` — in practice
`rm -rf target/debug`, external deps rebuild in minutes) wiped whenever
the tree changes; concurrently-active worktrees each need their own
`CARGO_TARGET_DIR`, and if the disk allowance cannot hold two, the work
is serialized, not shared. Briefing an agent "wait until no cargo
process is running before each invocation" does **not** make sharing
safe — it only removes the lock contention, not the staleness.
