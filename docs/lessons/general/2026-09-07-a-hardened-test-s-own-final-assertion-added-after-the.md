# A hardened test's own final assertion, added after the assertion that failed the last time, can carry the identical one-shot-on-an-eventual-property bug the hardening pass was meant to eliminate (issue #742, `split_placing_completion.rs`)

**The failure**: `prod-liveness-animusd` on PR #717 (head 4d23cba0, unrelated
to split/placing — it only lifted `update_table_throughput` into
`dispatch_table_op`) failed
`mark_split_placing_done_tolerates_a_stale_or_duplicate_relayed_propose`
with `sibling child 3 unexpectedly not done` at the test's very last
assertion, after the test's own `left`/`child` convergence poll and the
duplicate-propose check both passed. The run took 5.4s, well inside its
120s guard — not a timeout, not a stuck completion loop.

**Root cause: (A), a test race, not a completion-loop defect.** The
earlier "settle window" hardening pass documented above (see this file's
"A 'just compare live state to the target' convergence check races the
very proposer that sets the target" entry) fixed exactly this failure
shape in this same file's *sibling* test
(`placing_relocates_a_child_off_the_parents_original_nodes_and_the_
completion_loop_marks_it_done`) — its own final section polls **both**
children's `replicas`/`done` to convergence before ever asserting on
either. But `mark_split_placing_done_tolerates_a_stale_or_duplicate_
relayed_propose`'s closing assertion —

```rust
assert!(
    split_placing_entry(&final_status, right).is_some_and(|(_, d)| d),
    "sibling child {right} unexpectedly not done"
);
```

— was a bare, unread one-shot check on `right`'s own `split_placing[..]
.done`, an independently-converging value the completion loop marks per
tablet, per tick, with no synchronization to this test's own poll
granularity — the *exact* mistake the sibling test's own hardening pass
named and fixed, just never applied to this second test in the same file.
`right` was never polled anywhere in this test; only `left`/`child` had a
bounded convergence loop, and by the time execution reached the final
assert `right` had almost always (but, per this issue, not provably
always) already converged too.

**Evidence, not just reasoning**: 20/20 reproduction-loop runs under three
`yes` spinners did not reproduce the failure directly (the race window is
narrow here — both children fork under one `CutoverSplit` and are
processed by the same per-tick completion loop, so by the time `left`'s
own bounded poll, the duplicate-propose round trip, and a 500ms settle
sleep have all elapsed, `right` has almost always caught up too).
Temporary instrumentation (a non-assertion-affecting side poll logging
`right`'s convergence latency at the point of the final assert) confirmed
`right` was already `done` at that point in every one of 15 additional
runs, converging in under 5ms once observed — consistent with "usually
already converged, occasionally not" rather than "stuck": exactly the (A)
signature, not (B) (a genuinely stuck completion loop would show `right`
converging slowly or never across repeated observation, not converging in
microseconds the moment it's checked).

**The fix**: replaced the one-shot assert with a bounded
converged-or-timeout poll for `right`, mirroring `child`'s own convergence
loop earlier in the same test (same 60s deadline shape, same failure
message convention naming the tablet and its current `split_placing`
entry) — never a wider timeout, never `#[ignore]`, never a retry loop.
Validated 20/20 under the identical spinner contention post-fix.

**General lesson**: when a hardening pass fixes a one-shot-assert-on-an-
eventual-property bug in one test, grep every *sibling* test in the same
file (and file family) for the identical shape before considering the
class closed — a fix applied to the test that happened to fail first does
not retroactively harden a structurally identical assertion in a test
that simply hadn't drawn the short straw yet. "The mechanism was already
fixed for this file" is not evidence a specific assertion in that file was
covered by the fix; check the assertion itself.
