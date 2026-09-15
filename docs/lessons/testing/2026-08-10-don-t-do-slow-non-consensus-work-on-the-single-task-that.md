# Don't do slow, non-consensus work on the single task that must service Raft liveness — a per-loop stall past the election timeout becomes a self-sustaining leader-election storm, invisible to `SimEnv`.

**Don't do slow, non-consensus work on the single task that must service Raft
liveness — a per-loop stall past the election timeout becomes a self-sustaining
leader-election storm, invisible to `SimEnv`.** The CP-data driver ran engine
apply (a batch of LSM `merge`s) + compaction *inline* on the same loop as
`select(recv, timer)`; under bulk-write load that block took ~180–300ms — longer
than the 150ms election timeout — so the leader couldn't heartbeat and followers
couldn't process AppendEntries in time → they campaigned → the deposed leader's
in-flight writes were truncated → those writes hit the 10s client timeout and
retried → term climbed continuously and throughput collapsed to ~15/s (a fixed
count, `≈ concurrency / one-election-cycle`, that *looks* like a per-write latency
floor but is churn). `SimEnv`'s virtual time never trips a wall-clock election
timeout, so the whole suite was green. Fix: move apply + compaction to a **separate
task**, leaving the consensus loop to only persist + step + send (→ term flat,
~15–20× throughput). Two split invariants worth remembering: (1) once apply is
async, the core's `last_applied` **leads** the engine, so anything that reads the
engine after gating on an index (linearizable ReadIndex) must gate on a separate
*engine-applied* watermark, not `last_applied`; (2) if two tasks write one WAL
file, serialize them (async lock) and make the compaction rewrite bounded by
engine progress + discard the other task's pending records (WAL `replay` is
push-based → duplicates otherwise). **The guard is a `ProdEnv` load test asserting
the term barely moves under a bulk seed** (`animusd`
`seed_load_does_not_storm_cp_elections`) — the exact liveness property `SimEnv`
can't see. Same family as the group-commit-deadlock entry above: real-time,
timeout/assertion-guarded. (`animus-cp-data` `drive`/`apply_loop`.)
