# A per-peer queue needs message classes, supersede keys and an explicit producer policy (ADR 0080)

Task-per-frame sending had no bound and no priority: a stalled peer accumulated tasks and a vote
waited behind snapshot chunks.

- **Class by liveness, not by size**: Vote > Heartbeat > Replicate/Normal on one connection, and
  Bulk (snapshots) on its own connection, so a stuck bulk write cannot block a vote. A new
  message variant must pick a class (`transport_class` has no `_` arm).
- **Coalesce only what is genuinely superseded**: a bare `AppendEntries` heartbeat per
  (group, peer). A batched heartbeat frame carries different groups each tick, so it gets no
  supersede key; drop-oldest bounds it.
- **State what the producer does when full**: drop oldest for liveness traffic; for Bulk, wait
  for credit but only up to the send timeout, because the producer is the consensus driver loop and
  an unbounded wait stalls the group.
- `SimEnv` has no outbox: prove the policy on the pure queue with a seeded stalled-writer model
  plus a negative control per mechanism, and prove liveness on real sockets.
