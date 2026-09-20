# A red-before proof must check whether a LATER, independently-landed fix for the SAME issue number already covers the one being reverted — issue #990/#811

Building the snapshot-caught-up-follower restart cell (issue #990) for
`crates/animus-test/tests/raftkv_linearizable.rs`, the prescribed red-before
step — revert PR #937's two-line `apply_and_compact` change (commit
`b6df83e`, "force a WAL rewrite after every InstallSnapshot install (issue
#811)") and confirm the new test fails — **did not fail**, on unmodified
`main`, either for the new cell or for `tests/restart_after_install_
snapshot.rs`, PR #937's own dedicated regression.

## Why

`crates/animus-cp-data/src/lib.rs`'s `apply_and_compact` carries **two**
independent, sequentially-landed fixes for issue #811, not one:

1. `b6df83e` (2026-09-10, "PR #937"): forces the compaction section's
   WAL-rewrite branch whenever a pass just processed a completed
   `InstallSnapshot`, so the durable WAL agrees with `RaftCore`'s own
   in-memory `snapshot_index` before a later restart can ever race it.
2. `e451ecd` (2026-09-15, PR #905, `docs/lessons/code-patterns/
   2026-09-15-eligible-is-not-done.md`): an **independently discovered**
   fix for the identical spin symptom, landed five days later — `did_work`
   now reflects only provable progress (`image_installed ||
   bytes_produced`) instead of being set unconditionally on merely being
   *eligible* to attempt compaction.

Both fixes close the same observable failure (an unbounded `apply_and_
compact` spin after a genuine restart of an install-snapshot-caught-up
voter) via different mechanisms, and both remain on `main` today, each
with its own regression test (`restart_after_install_snapshot.rs` and
`restart_caught_up_voter.rs` respectively). Fix 2's `did_work`-truthfulness
gate is strictly more general: it stops the *spin* regardless of whether
fix 1 ever ran, so reverting fix 1 alone leaves the scenario merely
*transiently* inconsistent (a real, narrower defect — see the code comment
this session left at the revert's own call site) rather than hung. A
red-before proof written against only fix 1 is therefore vacuous on
current `main`: reverting it alone changes nothing a wall-clock watchdog
can observe.

Confirmed directly: reverting **both** fixes together reproduces the
CPU-pinning hang in all three of the new cell, `restart_after_install_
snapshot.rs` (both engine tiers), and `restart_caught_up_voter.rs`;
restoring both makes all three green again. Two unrelated corpus cells
(`raftkv_baseline_is_linearizable`, `raftkv_corpus_covers_the_fault_
matrix`) stayed green under the double revert too, confirming the failure
is scenario-specific, not a broad breakage from the temporary edit.

## The general rule

Before trusting "revert commit X, confirm red" as a red-before proof,
**check whether any later commit touching the same function/mechanism
might already provide independent coverage for the same failure** — a
`git log` on the function's own file/issue number, not just reading the
one commit a task brief names. This repo's own convention of multiple
agent sessions working the same backlog concurrently makes duplicate,
independently-discovered fixes for one issue number a real and recurring
shape (see also `docs/lessons/code-patterns/2026-09-15-eligible-is-not-
done.md`'s own cross-plane version of this caution, about a *sibling*
module rather than a *later commit on the same module*). A red-before
proof that silently passes "green" when it should be red is not merely an
inconvenience — it is the exact failure mode this whole discipline exists
to catch in the *production* code; verifying the harness's own negative
control deserves the same suspicion.
