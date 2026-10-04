# Relay regression tests must disable the masking path on every node, not the current leader

A test proving "X lands through the relay arm" switched off the era upkeep
(which would also make X land) only on the leader found at that moment. A
mid-test leadership change hands the work to an upkeep-capable node and the
test passes with the relay arm missing. Disable the alternative path on every
candidate (all control nodes), after any helper that restores per-node state
(`set_node_version`), and confirm the test goes red by flipping the arm it
guards.

Related: an admin handler that decides from a leader's apply-task cache must
first wait (bounded) for applied >= commit (issue #406 pattern), and a
long-blocking `propose_and_await` inside a periodic feeder loop must be spawned
with an in-flight guard so it cannot stall the loop's other duties.
