# An index-shaped watermark cannot express the durability of a state change that moves no index (issue #279).

**An index-shaped watermark cannot express the durability of a state change
that moves no index (issue #279).** The natural way to release a buffered
Raft response is "wait until `durable_index` covers it" — and it silently
never fires for a granted vote, because a vote persists `(current_term,
voted_for)` and appends no log entry, so `mark_durable_through` is never
called. Worse, `drain_persist` marks the hard state persisted *at drain
time*, optimistically, so no core-level predicate can see a vote-only
round in flight either. Count the I/O (rounds), not the log positions, when
what you need to know is "has this batch reached the disk."
