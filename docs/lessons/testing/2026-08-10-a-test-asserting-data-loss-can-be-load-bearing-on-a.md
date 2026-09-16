# A test asserting data LOSS can be load-bearing on a consensus bug — when a correctness fix flips it, invert the test, don't weaken the fix.

**A test asserting data LOSS can be load-bearing on a consensus bug — when a
correctness fix flips it, invert the test, don't weaken the fix.** A restart
test asserted acked data on the memory backend is lost across restart; that
"expected loss" actually depended on a sole recovered voter never re-advancing
commit over its WAL tail (a real bug). The ReadIndex-gate fix surfaced it; the
test now asserts survival via Raft-WAL replay. (PR #25.)
