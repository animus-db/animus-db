# A sticky "disk full" verdict needs one fresh maintenance run before a writer fails on it

`background_full` is set when background maintenance last failed for space and is
cleared only by a successful maintenance run. A writer waiting in the write
stall is the only thing driving maintenance, so a stale verdict (space came back
while the writer waited) would keep failing it with `StorageFull` forever. The
stall now treats the flag as a hint: the first time it sees the flag it asks for
one fresh maintenance run (`confirmed_full`) and returns `StorageFull` only if
that run fails for space again.

Rules:

- A verdict that only a successful run can clear must be re-tested by the waiter
  that depends on it, once, before the waiter fails.
- A fail-stop for non-space failures is counted in failures, not seconds (a slow
  but working disk is not a failing one); a space failure is a different,
  recoverable path and must never take the apply task down.
