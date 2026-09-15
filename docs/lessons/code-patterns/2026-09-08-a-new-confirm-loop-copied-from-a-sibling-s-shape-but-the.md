# A new confirm loop copied from a sibling's shape but the wrong sibling's constant is a silent ~50ms-per-call throughput regression, and no test guarded the latency floor (2026-09-08).

**A new confirm loop copied from a sibling's shape but the wrong sibling's
constant is a silent ~50ms-per-call throughput regression, and no test
guarded the latency floor (2026-09-08).** `ClientCtx::cp_kind_eval_local`
(`write_path.rs`) — the confirm loop for *every* single-item write since
ADR 0054 step 3 (`PutItem`/`UpdateItem`/`DeleteItem` via `dynamo::
kind_write_item_at_leader`, the TTL reaper, the admin seeder's per-item
images arm) — polled with a flat `leader.env().sleep(SCHEMA_POLL_
INTERVAL).await` (50ms) instead of the exponential back-off
(`CP_CONFIRM_POLL_INIT` 200µs doubling to `CP_CONFIRM_POLL_MAX` 5ms) every
sibling write-confirm loop in the same file uses (`cp_batch_local`,
`cp_put_local`/`cp_delete_local`). The first poll right after
`propose_kind_eval` is *always* `Inconclusive` (apply hasn't run yet), so
every single-item write paid the full 50ms floor once — capping
sequential single-item write throughput at ~20 ops/s, a ~40x
degradation from what the underlying Raft group can actually commit at.
Root cause: this loop's own doc comment shows it was modeled on the
*schema-DDL* commit-wait shape (`SCHEMA_POLL_INTERVAL`/
`SCHEMA_COMMIT_TIMEOUT`, a rare, human-latency-tolerant operation) rather
than on its true sibling, the ordinary per-write confirm loop — copying
the wrong nearby pattern rather than the one actually analogous in
*frequency*. **Nothing caught this for the whole ADR 0054/step-3
lifetime** because every existing test asserts write *correctness*
(does the value read back, does the condition apply), never write
*latency* — a 50ms-per-write floor makes every test slower but not
wrong, so it hid in plain sight until a maintainer noticed the
measured throughput collapse (~700 keys/s to ~17 keys/s on a
Stream-enabled table) directly. **The fix, and the general lesson**:
whenever a new confirm/commit-wait loop is added, grep the same file
for every existing loop of the same *shape* (propose → poll-until-
applied) and match the poll cadence of the one with the same call
frequency, not the one that happens to be textually nearest or most
recently read. **The test, and the general lesson for guarding a
latency floor**: a poll-interval regression is a timing property, so it
is asserted in **virtual** `SimEnv` time, never wall-clock — a bound
that would flake under real-thread contention is unusable here.
`write_path::kind_eval_confirm_backoff_tests` drives a single-voter
`RaftKvNode<SimEnv, MemoryEngine>` (mirroring `poll_probe_identity_
tests`' pre-existing harness) through 20 sequential `cp_kind_eval_local`
calls and asserts the total virtual elapsed time is `<= 200ms`
(`WRITE_COUNT * 10ms`) — a bound the fixed exponential back-off clears
with wide margin and the old flat 50ms floor fails deterministically
(confirmed by hand: reverting the fix reproduces exactly `WRITE_COUNT *
50ms` = 1s of virtual time, every run, no variance — the flat floor
makes the bug not just detectable but bit-for-bit reproducible).
