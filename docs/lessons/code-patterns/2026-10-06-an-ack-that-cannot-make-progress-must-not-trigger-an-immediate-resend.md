# An ack that cannot make progress must not trigger an immediate resend

A Raft leader answers a successful `AppendEntriesResp` that leaves entries
unsent (`next_index <= last_log_index`) by replicating again **at once**, not at
the next heartbeat; that is what pipelines a long catch-up. It assumes every
success ack advances what the follower has. The #1228 fix made a storage-full
follower ack heartbeats again, *frozen* at its durable index so it never vouches
for an entry it could not persist. Frozen means the ack never advances, so the
leader's immediate resend was acked just as flatly and re-triggered itself: a
zero-latency leader/follower loop with no timer in it. It did not fail any
assertion. The all-full sim cell merely took 60 s instead of 0.1 s (1.4 M
simulator events in 10 s of virtual time) -- a real node would have pinned two
cores for the whole outage.

Rules:

1. A response that is deliberately conservative (clamped, frozen, "alive but
   not persisting") needs its consumer audited for **every reaction a normal
   response triggers**, not only the one it was built for. Here: commit
   (`maybe_advance_commit`), liveness stamps, the snapshot/needs-snapshot path,
   and the immediate-resend path. Only the last one looped.
2. Any "respond now to this message" edge must be conditioned on the message
   having changed something (`progressed`), or else it is a feedback loop the
   moment the peer stops making progress for a legitimate reason.
3. Give a corpus cell that exercises such a state an **event-count bound**
   (`FULL_WINDOW_MAX_TRACE_EVENTS`, `animus-test` `raftkv_linearizable.rs`), not
   only functional assertions: `SimEnv` runs a livelock to completion, so
   wall-clock time is the only symptom and nobody reads it.

See `RaftCore::handle_append_resp` (`progressed`) and ADR 0074's 2026-10-06
amendment.
