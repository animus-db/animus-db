# Driving a production `E: Env`-generic type under `SimEnv` for the first time in a new crate: `block_on` hangs, a receiver borrow can conflict with a later move, and clippy catches what `cargo test` can't (ADR 0061 Phase C's closing rung)

Building the first `SimEnv`-driven `ClientCtx<SimEnv, _>` harness in
`animusd`'s own tests (no crate move — see that ADR's seventh/eighth
2026-08-28 amendments) surfaced three small, generalizable gotchas, all
likely to recur verbatim when Phase D's `SimCluster` does the same thing at
multi-node scale.

**1. `futures::executor::block_on` silently hangs over any future that can
genuinely `.await` an `env.sleep()` — there is no error, just a stuck test
process.** `ClientCtx::cp_kind_write_raw`/`cp_get` both contain retry loops
that `.await self.env.sleep(..)` (the confirm-poll backoff, the
route-resolution wait). `block_on` polls a future on the *calling* thread
with no relationship to `Simulator`'s own cooperative executor — nothing
ever advances the virtual clock or fires the pending timer, so a future
that would resolve instantly under real time never resolves at all here.
The fix (already established practice in `animus-cp-data`'s own tests, but
easy to reach for the wrong tool anyway when a codebase's "obvious" way to
run one `async fn` synchronously is `block_on`): spawn the future onto the
env (`env.spawn_task`) and drive it forward with `Simulator::run_for`/
`run_until`, capturing the result through a shared `Arc<Mutex<Option<T>>>`
slot read back out afterward. Any first-time `SimEnv` harness for a type
that wasn't written with `SimEnv` in mind from the start should budget for
finding at least one call site that can genuinely suspend, and reach for
this shape by default rather than trying `block_on` first and debugging the
hang.

**2. A method call's receiver-borrow and a later argument's whole-value move
of the same struct conflict, even when the method itself takes `&self`.**
`ctx.env.spawn_task(async move { .. ctx.cp_kind_write_raw(..) .. })` fails
to borrow-check: evaluating the receiver `ctx.env` borrows `ctx` for the
duration of the call, and the `async move` block constructed as the
*argument* to that same call tries to move the whole `ctx` — a conflict the
error message reports as a move-while-borrowed on `ctx`, not obviously
pointing at "the receiver and the argument are both touching the same
value." The fix is mechanical once recognized: bind the needed field to its
own local (`let env = ctx.env.clone(); env.spawn_task(async move { ..
ctx.cp_kind_write_raw(..) .. })`) so the move and the borrow no longer share
an expression. `animus-node`'s own sim tests already do this
(`let loop_env = node.env().clone(); loop_env.spawn_task(..)`) — worth
recognizing as a named idiom rather than rediscovering the borrow-checker
message each time a new harness hits it.

**3. `cargo test` proves the code compiles and the assertions pass; it does
not run clippy, and a fresh sim test's own literals are exactly the kind of
thing clippy catches that a passing test run won't.** A hand-picked hex seed
literal grouped for readability (`0x51_4E_0001`, meant to loosely spell a
mnemonic in hex) passed `cargo test -p animusd --lib` clean but failed
`cargo clippy -p animusd --all-targets --all-features -- -D warnings` on
`clippy::unusual_byte_groupings` (hex digits must group in fours from the
right). Unsurprising once stated, but worth the reminder that finishing a
rung's gate list in test-only order (test, then clippy) can produce a false
sense of "done" after step one — the validation gate's own ordering (fmt,
clippy, build, test) exists partly to catch exactly this before a test pass
is mistaken for a full green gate.

**A fourth, purely operational finding, worth recording beside the existing
"`cargo build --workspace --all-targets` can exhaust disk long before
`cargo clippy --workspace --all-targets` does" entry above**: the same gap
holds at single-crate scope, not just workspace scope. `cargo build -p
animusd --lib --tests` (compiling and *linking* all ~100 of this crate's
test binaries in one invocation) exhausted this session's disk mid-build,
while `cargo clippy -p animusd --all-targets --all-features -- -D warnings`
— checking the exact same set of targets — completed in well under a
minute using only a few GB, because clippy (like `cargo check`) never links
a final binary for any target it checks. When a task's own validation gate
calls for a crate-scoped `--all-targets` clippy run, it is safe to run as
written even under this repo's documented disk constraints; a
`--all-targets` *build* or *test* invocation for the same crate is the one
to keep scoped down (`--lib`, or one `--test <name>` at a time).
