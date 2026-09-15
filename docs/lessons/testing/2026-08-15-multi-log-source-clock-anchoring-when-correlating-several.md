# Multi-log-source clock anchoring: when correlating several log streams from one process for a timing bug, log one shared absolute-clock anchor event across all of them up front, before the race window opens.

**Multi-log-source clock anchoring: when correlating several log streams
from one process for a timing bug, log one shared absolute-clock anchor
event across all of them up front, before the race window opens.**
Debugging the same torn-pair investigation meant correlating the
writer's own step-by-step timeline against the reader's per-round
observations against the coordinator/recovery internals' own traces —
three log sources from the same test process, but on different clock
bases (some `std::time::Instant`-relative, some `SimEnv`/`HlcTimestamp`
wall-clock-derived). Without a single shared absolute-time anchor emitted
by all three at the very start, aligning "what did the reader see at the
exact moment the writer's step N committed" after the fact was
genuinely ambiguous — a real gap in the evidence that better
instrumentation design would have closed for free. Emit one anchor event
(the same wall-clock read, or the same monotonic counter value) from
every log source before anything interesting happens, not just whichever
timestamp format was most convenient at each individual call site. (Same
investigation, 2026-08-15.)
