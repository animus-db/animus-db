# A new admission limit must queue explicit operator requests, not refuse them

**Context:** ADR 0079's per-node split pool (default 1) first answered a full pool
with a retryable `SPLIT_BUSY` on every path. The auto-split sweep (skip, ask again
next tick) was right, but every caller issuing splits back to back (stream grow,
a GSI drain test, a slower CI runner) broke, and the fix attempted was a retry
loop per caller.

**Lesson:** a limit that protects the node from its own background work should
skip for loops that will come back anyway, but an explicit request (an operator,
a script, an admin endpoint) should wait its turn at the choke point: FIFO,
cancellation-safe (a RAII ticket), bounded by a documented constant, and
`BUSY` only when the bound expires. Callers then stay simple. Where the wait
crosses a relay hop, bound it under the hop timeout and let the existing outer
loop re-ask.

**How to apply:** when adding a pool/permit limit, list its callers first and
split them into "will retry by itself" and "asked once"; only the first may be refused.
