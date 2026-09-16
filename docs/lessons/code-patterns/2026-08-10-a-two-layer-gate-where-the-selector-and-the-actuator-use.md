# A two-layer gate where the selector and the actuator use different thresholds fails silently — and a primitive's `bool`/`Result` return value that encodes "did this actually take effect" must never be discarded, however statement-shaped the call looks.

**A two-layer gate where the selector and the actuator use different
thresholds fails silently — and a primitive's `bool`/`Result` return value
that encodes "did this actually take effect" must never be discarded, however
statement-shaped the call looks.** ADR 0029's leadership-transfer primitive
had exactly this shape: `RaftKvNode::reconfigure_step`'s step 4 *selected* a
transfer target with `peer_match(n) >= commit_index()`, but
`RaftCore::transfer_leadership` only *armed* at `peer_match(target) ==
last_log_index()` — a stricter threshold the selector never checked — and
the caller wrote `self.transfer_leadership(target);` with the returned
`bool` dropped on the floor. `propose` is fire-and-forget (it appends to the
leader's local log and returns before any replication round trip), so on a
write-hot tablet `last_log_index` moves the instant a write is accepted
while every peer's `peer_match` still reflects the *previous* entry — the
two thresholds disagreed at essentially every sampling instant, so the arm
failed *forever*, and nothing ever surfaced it: no error, no log, no metric,
just a rebalance move that silently never completed for any tablet whose
move needed to relocate its leader. The correct fix is standard Raft §3.10
semantics, not just threshold alignment: relax the arm gate to match the
selector (`>= commit_index`), but that alone reintroduces the original
danger (arming to a target that isn't actually at `last_log_index` yet), so
**freeze `propose`/`change_membership`** while a transfer is armed (return
`NotLeader`, hinting the target) so the log stops growing and replication
can close the remaining gap, gate the actual `TimeoutNow` send on the
target *reaching* `last_log_index`, and **abort** (clear the arm, resume
proposing) if a deadline passes with no step-down — else a target that
crashes right after arming strands the group frozen forever. A related,
narrower bug in the same function compounded it: the down-extra search
reused a generic "lowest non-self extra" helper and only *then* filtered it
on down-ness, so a `Down` extra sorting after a healthy one was invisible —
the step fell through to a *different*, catch-up-gated removal path, which
could stall behind an unrelated survivor's lag. **General checks:** (1) when
a value is computed once to pick a candidate and re-derived/re-checked
inside the primitive that acts on the candidate, diff the two conditions —
"selects X" and "arms X" must agree on what "eligible" means, or the
narrower one silently wins every time; (2) grep for every call to a
bool/Result-returning mutator where the result is bound to `let _ =` or not
bound at all — if the primitive's doc says "returns whether it took effect,"
a discarded result is a designed-in blind spot; (3) a "search for the first
match of predicate P" helper reused with an *unrelated* predicate applied
only to the first result (`extra().filter(down.contains)`) is a common way
to accidentally scope a search to "the first element of the base sequence,"
not "the first element satisfying the actual predicate" — write the combined
predicate into the search itself. (`animus-control` `RaftCore::
transfer_leadership`/`propose`/`change_membership`/`broadcast_append`;
`animus-cp-data::RaftKvNode::reconfigure_step`; regressions in
`animus-control/tests/leadership_transfer.rs`,
`animus-cp-data/tests/leader_transfer_reconfigure.rs` — the hand-driven
variant is the one proven to fail against the pre-fix source — and
`animus-cp-data/tests/reconfigure_down_extra_priority.rs`.)
