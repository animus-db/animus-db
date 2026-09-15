# A mid-test panic can cascade into an unrelated durability panic that masks the real failure — because `Drop` latching a fault-tolerance flag is not the same as stopping the background task that flag gates (issue #511).

**A mid-test panic can cascade into an unrelated durability panic that
masks the real failure — because `Drop` latching a fault-tolerance flag
is not the same as stopping the background task that flag gates (issue
#511).** `crates/animusd/tests/*` bring up real `ProdEnv` clusters into a
`Vec<Node>` plus a caller-owned `tempfile::TempDir` (`--dir`). Issue #273
already established that a bring-up helper must never own-and-drop its
own `TempDir` internally; issues #282/#279 already gave `Node` a `Drop`
impl that latches every hosted CP group's `halted` flag before anything
else can touch its driver tasks, closing the "kill/panic races a live
WAL write" window for the CP-data plane. Issue #511 found this was
**not** actually closed for a mid-test panic, for two compounding
reasons neither prior fix addressed: (1) **`Drop for Node` deliberately
only latches — it never aborts the node's own tasks or tears down its
envs** (see that impl's own doc), so the control-plane Raft driver task
(and every other background loop) keeps running, detached, past the
point its `Node` was dropped — an ordinary passing test never notices
because nothing races it, but a panic can catch a write mid-flight; (2)
**the control-plane WAL has NO `halted`-gate at all** —
`animus-control::node::persist_wal`'s `env.append(WAL,
..).await.expect("wal append")`/`env.sync(WAL).await.expect("wal sync")`
are bare, unconditional `.expect()`s, live or shutting down (see that
crate's own CLAUDE.md entry) — so even a *correct* local drop order
(`TempDir` declared before its `Vec<Node>`, the safe LIFO shape) doesn't
help: the still-live control-plane driver task's next WAL append/sync
against the now-removed directory panics unconditionally, on whichever
thread is driving it, burying the actual assertion failure that
triggered the unwind. **Fix, deliberately NOT another `halted`-gate**
(that would mean widening a production I/O panic's tolerance for a
testing concern, and still wouldn't be deterministic — `task.abort()`
only *requests* cancellation): `crates/animusd/tests/support::
PanicSafeTempDir` wraps `TempDir` and, on a *panicking* drop only,
`std::mem::forget`s it instead of letting it remove the directory — the
files a still-live background task might be using simply never
disappear, so nothing can race their removal, regardless of how long
that task keeps running past the test. A normal (non-panicking) drop is
byte-identical to `TempDir` (no leak on the passing-test path); the
leaked directory on a panic is bounded by CI's ephemeral runner.
Mechanically applied to every one of the 59 `tests/*.rs` files that
already `mod support;` and constructed a `TempDir` directly — the ~40
files with their own hand-rolled, non-`support::`-using fixtures (a
`dynamo_index_scan.rs`-shaped in-file `setup()`, or simply never pulling
in `mod support;`) are a **named, documented gap this PR does not
close** (see the module's own doc for the exact file list). **Test
design lesson**: proving this deterministically does not require a real
ProdEnv cluster or any timing-dependent race at all — the actual
mechanism is "does a panicking `Drop` remove files a concurrent operation
is using," which isolates cleanly to a plain `std::thread` background
writer racing a `std::panic::catch_unwind`, coordinated by an explicit
liveness signal (wait for the writer's first successful write) rather
than a sleep — red-before/green-after
(`crates/animusd/tests/panic_safe_teardown.rs`; **this entry's original
"zero flakiness" claim was wrong** — the red test's own first assertion
turned out to still be a real, load-dependent race; see the 2026-09-02
entry below, issue #555, for the fix and the corrected claim). A live-ProdEnv attempt
at the same proof (hammering `propose_meta` against a real single-node
cluster right up to a deliberate panic) was tried first and abandoned:
the exact race window between "background driver task mid-I/O" and
"directory just removed" is a handful of instructions wide even under
real contention, and forcing it reliably would have meant a genuinely
flaky test proving a flakiness fix — the opposite of the point. When a
bug's mechanism can be isolated from the full system it was found in
without losing what makes it true, prefer the isolated, deterministic
proof over a bigger fixture that merely makes the race *more likely*.
