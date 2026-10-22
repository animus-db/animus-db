# To make a multi-step walk reach a retired item, block the walk's first call with a partition, not a slow link

**Context.** Issue #1345: `grow_stream` snapshots a table's tablets and visits
them in order; the test needs a later tablet retired by an in-place split
before the walk reaches it.

**What failed.** Slowing the first tablet's relay with a 700-1500 ms link
delay and racing a split of the second tablet against it was a coin flip: the
retirement took 0.4 to 5 s and the walk's first call 3.6 to 13 s depending on
which hop timeouts the delay happened to cross, so some seeds never opened the
window (the walk finished before the victim retired).

**What works.** Partition the walker from the first tablet's leader so its
call cannot finish, drive the victim's split until it is gone from the
*walker's* metadata, assert the walk is still unresolved at that instant, then
heal. The window is then built by the test, not hoped for, and a guard
assertion fails loudly if it did not open. Two traps: keep the control leader
off the partitioned pair (on the third node), or the victim's split and the
walker's metadata mirror starve; and measure "the walk resolves quickly" from
the heal, not from the start.

**Bite check.** With the fix reverted the walk ran past the 15 s post-heal
bound on both shapes; with it, 150 ms to 4 s.
