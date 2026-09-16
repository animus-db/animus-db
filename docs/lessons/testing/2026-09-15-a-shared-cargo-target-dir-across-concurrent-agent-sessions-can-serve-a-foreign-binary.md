# A shared `CARGO_TARGET_DIR` across concurrent agent sessions can silently serve a binary built from a DIFFERENT worktree, even when cargo reports the build as already up to date

**A shared `CARGO_TARGET_DIR` across concurrent agent sessions can silently
serve a binary built from a DIFFERENT worktree, even when cargo reports the
build as already up to date ("Finished in 0.1Xs").** Verifying issue #610's
fix (`crates/animusd/src/schema.rs`'s `propose_schema`), a `cargo test -p
animusd --lib` run's `--list` output was missing a test this exact session
had just added and previously seen pass — despite the preceding `cargo test
--no-run` reporting a fast, no-op "up to date" build. `touch`-ing `lib.rs`
to force an unconditional recompile immediately fixed it, and the rebuilt
binary's `--list` also picked up a test (`issue_911_diag`) that does not
exist anywhere in this worktree — later confirmed live via `ps aux`: at
least two *other* agent sessions (`/tmp/wt-911`, `/tmp/wt-864`) were running
`cargo build`/`cargo test` for the identical package (`animusd v0.0.0`)
against the identical `CARGO_TARGET_DIR` at the same time. One of their
builds had won a race for the shared output artifact path, and a later
`cargo test --no-run` in *this* session's own worktree — reading a
fingerprint/timestamp state a concurrent writer had touched — incorrectly
concluded nothing needed rebuilding and quietly kept running someone else's
binary.

**The practical fallout, and why this is worse than an ordinary flake**:
every test result observed against a binary in this state is meaningless
for *this* worktree's code — including apparent regressions (a module that
was never touched "failing") and false confirmations (an actual change
never getting exercised at all). A `git diff --stat` on the source in
question showing no changes is not reassurance here, since the problem is
which *binary* ran, not what the source says. This is strictly worse than
a normal flaky test, because there is no error, no warning, and no
divergent test count to notice by eye — `--list`'s total test count can
even coincidentally match.

**The mitigation used, and the rule going forward**: before trusting any
`cargo test` result gathered against a `CARGO_TARGET_DIR` known or
suspected to be shared with other concurrent sessions (this repo's own
web-session convention explicitly shares one), plant and check a cheap
**canary** immediately before the run that matters: `cargo test -p <crate>
--lib --no-run` followed by `<binary> --list | grep -c <a test name unique
to this change>` (and, if suspicious, also grep for a string that should
*not* be present). Only trust the subsequent test run if the canary
confirms the binary reflects the current worktree. If a build ever reports
suspiciously fast ("Finished in 0.1Xs") right after a source edit that
should have forced a real recompile, `touch` the changed file(s) and
rebuild before trusting anything further — do not assume cargo's own
freshness check is authoritative in a shared-target-dir environment. This
generalizes beyond this one incident: any two concurrent processes writing
into the same `CARGO_TARGET_DIR` for the same package (same name and
version, e.g. two worktrees/branches of the same repo) can race on the
same content-addressed output paths, and cargo's own locking serializes
*writes* but does not stop a reader's freshness check from being fooled by
another writer's timestamps.

**A related, compounding hazard the same shared host produced**: real
process-per-node `ProdEnv` integration tests that bind real TCP ports
(`schema_ddl_relay.rs`, `dynamo_index_writes.rs`, …) can fail with
`connect to 127.0.0.1:PORT failed: Connection refused` or the exact
"`CreateTable did not commit to the control plane in time`" timeout this
very issue is about, for reasons that have nothing to do with the code
under test, when *another* concurrent agent session's own real-socket
tests are competing for the same ephemeral-port range and the same 4 real
CPU cores at the same moment (`schema_ddl_relay.rs`'s own doc already
names the port-TOCTOU race as a known, accepted hazard of concurrent test
binaries on one host). Two other agent sessions' own `cargo test`
processes were independently confirmed live via `ps aux` at the exact
moments these were observed. Before concluding a real-socket `ProdEnv`
test result reflects the code under test in a environment shared with
other agents, check `ps aux`/`uptime` for competing `cargo`/test-binary
processes and, where feasible, re-run once ambient load has visibly
dropped — the repo's own standing rule that "a flaky ProdEnv test is a
real bug, not a determinism hole" presumes the load causing it originates
from the test's own cluster, not from an entirely unrelated process
sharing the same host.
