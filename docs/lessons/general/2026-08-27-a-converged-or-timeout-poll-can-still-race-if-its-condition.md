# A converged-or-timeout poll can still race if its condition is weaker than what the assertion after it needs (issue #421)

`cluster.rs`/`per_process.rs`'s `await_bootstrap` helpers polled for
`!n.metadata().members.is_empty()` — true the instant the *first* member
registers — then the test's very next line asserted the *full* member
count (`meta.members.len() == 3`). Membership registration is one node at
a time, so this is the same "eventual property, one-shot assert" shape
the rest of this log warns about, just hiding one level down: the poll
loop itself wasn't a bare one-shot, but the condition it converged on
wasn't the condition the caller actually depended on. The fix is to poll
for the exact predicate the following assertion needs (here,
`n.metadata().members.len() == nodes.len()` on every node), not a weaker
stand-in that merely correlates with it. When reviewing a `await_*`-style
helper before trusting it to cover an assertion, check that its loop
condition and the assertion's condition are the same fact, not two facts
that are merely close together in bring-up order.
