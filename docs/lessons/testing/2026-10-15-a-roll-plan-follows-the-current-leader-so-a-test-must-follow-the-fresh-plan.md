# A roll plan follows the current leader, so a roll test must follow the fresh plan

`animus cluster roll plan` orders the control voters with the *current* control leader
last. The previous-release upgrade test took the plan once and iterated it, then
asserted that every later re-plan listed the same nodes in the same order. Under CPU
load the control leader moved on its own mid-roll (n2 to n3 in a 4-node cluster), the
re-plan correctly put the new leader last, and the order assertion failed with exit
101 although nothing was wrong ("re-plan after rolling n1 must list exactly the
remaining nodes: left [n2, n3], right [n3, n2]").

Rule: a test that drives a roll must do what an operator does, follow the latest plan
after every step, and assert the *set* of nodes still to roll, not their order. The
order is a function of where the leader is now.
