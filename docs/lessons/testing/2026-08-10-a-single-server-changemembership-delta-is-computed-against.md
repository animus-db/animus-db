# A single-server `change_membership` delta is computed against the *current* config, not the original one — so a second growth step must add relative to the config the first step already produced, never restate the original set with one more id swapped in.

**A single-server `change_membership` delta is computed against the
*current* config, not the original one — so a second growth step must add
relative to the config the first step already produced, never restate the
original set with one more id swapped in.** Writing
`animus-control/tests/control_membership.rs`'s "gate reopens after commit"
case (PR1 of the control-plane membership-change stack), growing
`{0,1,2} -> {0,1,2,3}` and then, after it committed, trying
`{0,1,2,3} -> {0,1,2,4}` (drop 3, add 4) was rejected as a *multi-server*
delta (`symmetric_difference` = `{3,4}`, count 2) — correct behavior, wrong
test expectation. The fix was `{0,1,2,3} -> {0,1,2,3,4}` (append, don't
swap). `RaftCore::change_membership`'s `delta != 1` check is symmetric
difference against `self.config` (whatever the latest log entry set it
to), never the group's *original* `all_nodes` — a caller chaining several
growth/shrink steps must always diff against the *current* `config()`
right before each call, not a value computed once up front.
