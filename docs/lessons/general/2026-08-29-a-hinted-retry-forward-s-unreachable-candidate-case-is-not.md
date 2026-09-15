# A hinted-retry forward's "unreachable candidate" case is not the same case its own fix already covers — and a race a sandbox's own fast localhost never widens is still a real bug (issue #316)

`tests/split_build.rs::split_survives_losing_one_childs_leader_mid_build`
(the copy-based split-build driver, ADR 0050, kills a `Building` child's own
leader mid-build) was reported hanging its full 180s budget on `main`,
nondeterministically. Root cause: `ClientCtx::forward_to_tablet_leader`
(`animusd/src/forwarding.rs`) — the one hint-chasing forward implementation
shared by every tablet-id-addressed internal RPC, including the split-build
driver's own `seed_child_rows` — only chases past a candidate that replies
with a parseable "not the leader here" refusal. A **transport** failure
(the candidate is simply unreachable — a killed node) folds into one
generic sentinel string (`relay_request_with_timeout`'s `"relay to peer
node failed"`), which doesn't parse as that refusal shape, so the pre-fix
code took the `else { return resp; }` branch and gave up on the very first
unreachable hop — never trying `other_tablet_replica_addr`'s fallback to
either of the tablet's other two live replicas. Because the first guess
itself is **deterministic** (`resolve_cp_route`'s no-local-replica fallback
always picks the tablet's first replica in `Metadata` order, a plain read,
never a liveness check), every subsequent call from the same caller —
including the split-build driver's own next 200ms tick — reproduced the
identical dead end forever, which is exactly the reported symptom: full
budget burned, both children stuck. The fix treats a transport failure the
same way the function already treats a refusal carrying no hint (mid-
election): try another known replica, never a terminal return — three
lines at the one shared choke point, reusing the existing (already-tested)
candidate-selection/backoff machinery unchanged.

**This function's own doc already named and fixed the *sibling* case** (a
wrong-but-*reachable* first guess) and even quotes the split-build driver
as the motivating incident for *that* fix. Reading a doc comment that says
"this bug is fixed, here's the regression test that proves it" is not the
same as verifying the fix actually covers the failure mode currently being
investigated — the earlier fix's own regression test only ever kills the
*parent's* leader, never a *child's*, so it could not have exercised this
half of the space at all. Two structurally similar failures (wrong-guess-
but-alive vs. right-guess-but-dead) sharing one symptom (spins forever)
are not automatically the same bug with the same fix already applied;
read the actual branch that fires for the *specific* error string observed,
not just the nearest doc comment that looks like it already explains the
shape.

**Reproduction note, useful for calibrating how hard to try before
concluding "can't repro, ship on code-reading confidence alone":** over 30
real runs of the target test in this sandbox (single and under 6x CPU
oversubscription via six `yes` busy-loops), it never failed — localhost
`TcpStream::connect` to an already-closed listener refuses near-instantly
here, and this workflow's `N=400` seed rows ship in a single bulk pass
fast enough that the test's own 30s victim-detection poll usually lands
*after* the vulnerable shipping window has already closed (the test's own
doc calls this out: "the kill may land before the build starts or after it
finishes"). CPU contention widens the window but doesn't reliably land the
kill *inside* it. **The fix was still provable red-before/green-after
without ever reproducing the original hang**, by isolating the exact
mechanism into a new, fully deterministic regression
(`forward_transport_failure_tests::
forward_to_tablet_leader_survives_a_dead_first_guess`, `animusd/src/
lib.rs`): pin RF to a known tablet, kill the *specific*, deterministically-
computable "first guess" replica (not whichever node the test happens to
catch leading), call the forwarding primitive directly from a caller with
zero local replicas, and assert prompt recovery. This is a general
technique worth reaching for before spending unbounded time chasing a
racy repro: once a root cause is understood precisely enough to name the
*exact* deterministic trigger, a narrow test that forces that trigger
every time is both cheaper to prove red/green with and a more precise
regression than widening the original racy test's odds of catching it.

**Test-authoring gotcha this surfaced**: `animusd`'s five `E: Env`-generic
client-path modules (`schema`/`read_path`/`write_path`/`txn_coordinator`/
`forwarding`, ADR 0061 Phase C's closing rung) carry a hard `#[deny(
clippy::disallowed_methods)]` on their `mod` declaration in `lib.rs` — this
covers the module's entire body, **including a `#[cfg(test)] mod` nested
inside it**. A real-socket `ProdEnv` integration test living in one of
these five files (the same in-crate-test idiom `lib.rs`'s own
`confirm_futility_tests`/`index_drain.rs`'s `gsi_drain_cursor_tests` already
use, needing a private `pub(crate)` handle no external `tests/` binary can
reach) cannot use `tokio::time::sleep`/`timeout`/`Instant::now` at all —
`cargo build` fails outright, not just clippy. Since `pub(crate)` is
crate-visible regardless of which file a test module lives in, the fix is
simply to put the test in a file that isn't one of the five deny-gated
ones (`lib.rs` itself, or another ordinary module) rather than beside the
function it tests — a reminder to check for a module-level `#[deny(...)]`
before writing a first `#[cfg(test)]` real-socket test into one of these
five files specifically, not just into `animusd` generally (whose
package-level `Cargo.toml` override is what makes `tokio::time` fine
*everywhere else* in this crate).

**A second, more mundane timing lesson from the same new test**: giving a
test's own outer `timeout(...)` a bound close to the *production* timeout
the code under test is itself internally bounded by (`CLIENT_TIMEOUT`,
10s) leaves too little margin — an 8s outer bound raced a legitimately-
succeeding-but-slightly-slow (occasional CI/sandbox jitter) inner call and
fired first, misreporting a real pass as `Err(Elapsed(()))`. The fix:
separate the two jobs. The outer `timeout` only needs to be safely *above*
every internal ceiling the code under test could legitimately hit (15s,
comfortably over the 10s internal budget) so it can never fire first and
be mistaken for the thing under test; the actual "did this stay fast"
assertion is a separate, tighter elapsed-time check against the *observed*
duration once the call has already resolved (5s here, well under the
internal ceiling but with real margin over the normal sub-second case).
Conflating "don't hang the test suite forever" with "prove this recovered
quickly" in one bound produces exactly this kind of rare, confusing false
red.
